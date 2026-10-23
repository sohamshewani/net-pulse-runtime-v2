package com.netpulse.concurrency;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class TaskStealingExecutor implements Executor, AutoCloseable {

    private final int coreCount;
    private final EventLoopWorker[] workers;
    private final AtomicBoolean isShutdown;
    private final AtomicInteger nextWorkerIndex;

    public TaskStealingExecutor(int coreCount, ThreadFactory threadFactory) {
        if (coreCount <= 0) {
            throw new IllegalArgumentException("Core count must be greater than zero.");
        }
        this.coreCount = coreCount;
        this.workers = new EventLoopWorker[coreCount];
        this.isShutdown = new AtomicBoolean(false);
        this.nextWorkerIndex = new AtomicInteger(0);

        for (int i = 0; i < coreCount; i++) {
            MPSCQueue<Runnable> localQueue = new MPSCQueue<>(65536);
            EventLoopWorker worker = new EventLoopWorker(i, localQueue, this, threadFactory);
            this.workers[i] = worker;
            worker.start();
        }
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "Command cannot be null");
        if (isShutdown.get()) {
            throw new RejectedExecutionException("TaskStealingExecutor is shut down.");
        }

        int index = Math.abs(nextWorkerIndex.getAndIncrement()) % coreCount;
        EventLoopWorker worker = workers[index];
        
        if (!worker.submit(command)) {
            // Fallback: try to push to any other worker queue or execute locally if full
            boolean accepted = false;
            for (int i = 0; i < coreCount; i++) {
                int altIndex = (index + i) % coreCount;
                if (workers[altIndex].submit(command)) {
                    accepted = true;
                    break;
                }
            }
            if (!accepted) {
                throw new RejectedExecutionException("MPSC queue capacity limits breached; task dropped/rejected.");
            }
        }
    }

    public void executeOnWorker(int workerId, Runnable command) {
        Objects.requireNonNull(command, "Command cannot be null");
        if (isShutdown.get()) {
            throw new RejectedExecutionException("TaskStealingExecutor is shut down.");
        }
        int index = Math.abs(workerId) % coreCount;
        if (!workers[index].submit(command)) {
            throw new RejectedExecutionException("Target worker MPSC queue is full.");
        }
    }

    EventLoopWorker[] getWorkers() {
        return workers;
    }

    @Override
    public void close() {
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        for (EventLoopWorker worker : workers) {
            worker.signalShutdown();
        }

        for (EventLoopWorker worker : workers) {
            try {
                worker.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static final class EventLoopWorker implements Runnable {
        private final int workerId;
        private final MPSCQueue<Runnable> mpscQueue;
        private final TaskStealingExecutor parentExecutor;
        private final Thread thread;
        private final AtomicBoolean running;
        private final AtomicBoolean shutdownRequested;
        private final Queue<Runnable> overflowQueue;

        public EventLoopWorker(int workerId, MPSCQueue<Runnable> mpscQueue, TaskStealingExecutor parentExecutor, ThreadFactory threadFactory) {
            this.workerId = workerId;
            this.mpscQueue = mpscQueue;
            this.parentExecutor = parentExecutor;
            this.running = new AtomicBoolean(false);
            this.shutdownRequested = new AtomicBoolean(false);
            this.overflowQueue = new ConcurrentLinkedQueue<>();
            this.thread = threadFactory.newThread(this);
        }

        public void start() {
            if (running.compareAndSet(false, true)) {
                thread.start();
            }
        }

        public boolean submit(Runnable task) {
            if (shutdownRequested.get()) {
                return false;
            }
            if (!mpscQueue.offer(task)) {
                return overflowQueue.offer(task);
            }
            return true;
        }

        public void signalShutdown() {
            shutdownRequested.set(true);
            LockSupport.unpark(thread);
        }

        @Override
        public void run() {
            try {
                while (running.get()) {
                    Runnable task = pollTask();
                    if (task != null) {
                        executeTask(task);
                    } else {
                        if (shutdownRequested.get() && isDrained()) {
                            break;
                        }
                        // Try stealing before parking
                        task = stealTask();
                        if (task != null) {
                            executeTask(task);
                        } else {
                            LockSupport.parkNanos(this, 1000000L); // Park for 1ms
                        }
                    }
                }
            } finally {
                drainAndExecuteRemaining();
                running.set(false);
            }
        }

        private Runnable pollTask() {
            Runnable task = overflowQueue.poll();
            if (task != null) {
                return task;
            }
            return mpscQueue.poll();
        }

        private Runnable stealTask() {
            EventLoopWorker[] workers = parentExecutor.getWorkers();
            int length = workers.length;
            int startIdx = (workerId + 1) % length;
            for (int i = 0; i < length - 1; i++) {
                int victimIdx = (startIdx + i) % length;
                if (victimIdx == workerId) {
                    continue;
                }
                EventLoopWorker victim = workers[victimIdx];
                Runnable stolen = victim.mpscQueue.poll();
                if (stolen != null) {
                    return stolen;
                }
                stolen = victim.overflowQueue.poll();
                if (stolen != null) {
                    return stolen;
                }
            }
            return null;
        }

        private void executeTask(Runnable task) {
            try {
                task.run();
            } catch (Throwable t) {
                // Fault Isolation: intercept uncaught exceptions to keep core event loop thread running
                Thread.UncaughtExceptionHandler handler = thread.getUncaughtExceptionHandler();
                if (handler != null) {
                    handler.uncaughtException(thread, t);
                } else {
                    t.printStackTrace(System.err);
                }
            }
        }

        private boolean isDrained() {
            return mpscQueue.isEmpty() && overflowQueue.isEmpty();
        }

        private void drainAndExecuteRemaining() {
            Runnable task;
            while ((task = pollTask()) != null) {
                executeTask(task);
            }
        }
    }

    public static final class MPSCQueue<E> {
        private final Node<E> head;
        private final Node<E> tail;
        private final int capacity;
        private final AtomicInteger size;

        private static final class Node<E> {
            private volatile E value;
            private volatile Node<E> next;

            Node(E value) {
                this.value = value;
                this.next = null;
            }
        }

        public MPSCQueue(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("Capacity must be positive");
            }
            this.capacity = capacity;
            this.size = new AtomicInteger(0);
            Node<E> sentinel = new Node<>(null);
            this.head = sentinel;
            this.tail = sentinel;
        }

        public boolean offer(E e) {
            Objects.requireNonNull(e, "Element cannot be null");
            if (size.get() >= capacity) {
                return false;
            }
            Node<E> newNode = new Node<>(e);
            Node<E> prevTail;
            
            // Atomically exchange the tail to append the new node (Multi-Producer safe)
            while (true) {
                prevTail = tail;
                if (casTail(prevTail, newNode)) {
                    break;
                }
            }
            prevTail.next = newNode;
            size.incrementAndGet();
            return true;
        }

        public E poll() {
            Node<E> h = head;
            Node<E> next = h.next;
            if (next == null) {
                return null;
            }
            E value = next.value;
            next.value = null;
            head = next;
            size.decrementAndGet();
            return value;
        }

        public boolean isEmpty() {
            return head.next == null;
        }

        public int size() {
            return Math.max(0, size.get());
        }

        private boolean casTail(Node<E> expected, Node<E> update) {
            return TAIL_UPDATER.compareAndSet(this, expected, update);
        }

        @SuppressWarnings("rawtypes")
        private static final java.util.concurrent.atomic.AtomicReferenceFieldUpdater<MPSCQueue, Node> TAIL_UPDATER =
                java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater(MPSCQueue.class, Node.class, "tail");
    }
}