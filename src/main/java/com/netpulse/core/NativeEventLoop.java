package com.netpulse.core;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

public final class NativeEventLoop implements Runnable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize sun.misc.Unsafe", e);
        }
    }

    private static final int STATE_IDLE = 0;
    private static final int STATE_RUNNING = 1;
    private static final int STATE_SHUTTING_DOWN = 2;
    private static final int STATE_TERMINATED = 3;

    private static final AtomicIntegerFieldUpdater<NativeEventLoop> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(NativeEventLoop.class, "state");

    private volatile int state = STATE_IDLE;
    private volatile Thread executionThread;

    private final int eventLoopId;
    private final int maxEvents;
    private final long epollFd;
    private final int eventBufferAddress;
    private final int eventBufferSize;

    private final MpscTaskQueue taskQueue;
    private final TimerWheel timerWheel;
    private final ConnectionMap connectionMap;
    private final DirectBufferArena bufferArena;

    private static final int EVENT_SIZE = 12; // sizeof(struct epoll_event) on x86_64 Linux: uint32_t events + 64-bit data
    private static final int EPOLLIN = 0x001;
    private static final int EPOLLOUT = 0x004;
    private static final int EPOLLERR = 0x008;
    private static final int EPOLLHUP = 0x010;
    private static final int EPOLLET = 1 << 31;

    public NativeEventLoop(int eventLoopId, int maxEvents, int taskQueueCapacity) {
        this.eventLoopId = eventLoopId;
        this.maxEvents = maxEvents;
        this.taskQueue = new MpscTaskQueue(taskQueueCapacity);
        this.timerWheel = new TimerWheel();
        this.connectionMap = new ConnectionMap(1024);
        this.bufferArena = new DirectBufferArena(1024 * 1024 * 16, 64); // 16MB arena, 64-byte alignment

        try {
            this.epollFd = epollCreate1(0);
            if (this.epollFd < 0) {
                throw new IOException("epoll_create1 failed with errno: " + (-this.epollFd));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize epoll instance", e);
        }

        this.eventBufferSize = maxEvents * EVENT_SIZE;
        this.eventBufferAddress = (int) UNSAFE.allocateMemory(this.eventBufferSize);
        UNSAFE.setMemory(this.eventBufferAddress, this.eventBufferSize, (byte) 0);
    }

    public void start() {
        if (!STATE_UPDATER.compareAndSet(this, STATE_IDLE, STATE_RUNNING)) {
            return;
        }
        Thread t = new Thread(this, "net-pulse-event-loop-" + eventLoopId);
        this.executionThread = t;
        t.start();
    }

    @Override
    public void run() {
        this.executionThread = Thread.currentThread();
        
        final int epFd = (int) this.epollFd;
        final long evAddr = this.eventBufferAddress;
        final int maxEv = this.maxEvents;

        while (state == STATE_RUNNING) {
            try {
                long nextTimeoutMillis = timerWheel.processTimers();
                int timeoutMs = nextTimeoutMillis <= 0 ? 1 : (int) Math.min(nextTimeoutMillis, 1000L);

                int nEvents = epollWait(epFd, evAddr, maxEv, timeoutMs);
                if (nEvents < 0) {
                    int err = -nEvents;
                    if (err == 4) { // EINTR
                        continue;
                    }
                    break;
                }

                for (int i = 0; i < nEvents; i++) {
                    long eventOffset = evAddr + (long) i * EVENT_SIZE;
                    int events = UNSAFE.getInt(eventOffset);
                    long data = UNSAFE.getLong(eventOffset + 4);

                    int fd = (int) (data & 0xFFFFFFFFL);
                    long contextAddress = data >>> 32;

                    handleEvent(fd, contextAddress, events);
                }

                taskQueue.drain(this::executeTask);

            } catch (Throwable t) {
                handleLoopException(t);
            }
        }

        shutdownInternal();
    }

    private void handleEvent(int fd, long contextAddress, int events) {
        if (contextAddress == 0L) {
            return;
        }
        SocketContext ctx = connectionMap.get(fd);
        if (ctx == null) {
            epollCtl((int) epollFd, 2, fd, 0, 0); // EPOLL_CTL_DEL
            closeFd(fd);
            return;
        }

        try {
            if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
                ctx.transitionState(SocketContext.STATE_CLOSING);
                terminateConnection(ctx);
                return;
            }

            if ((events & EPOLLIN) != 0) {
                ctx.onReadable();
            }

            if ((events & EPOLLOUT) != 0) {
                ctx.onWritable();
            }
        } catch (Throwable t) {
            ctx.transitionState(SocketContext.STATE_CLOSING);
            terminateConnection(ctx);
            handlerException(ctx, t);
        }
    }

    public void execute(Runnable task) {
        if (!taskQueue.offer(task)) {
            throw new IllegalStateException("EventLoop task queue is full, backpressure triggered.");
        }
        if (Thread.currentThread() != executionThread) {
            wakeup();
        }
    }

    private void executeTask(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            handleLoopException(t);
        }
    }

    public void wakeup() {
        epollWakeup(epollFd);
    }

    public void registerSocket(SocketContext ctx) throws ClosedChannelException {
        if (Thread.currentThread() != executionThread) {
            throw new IllegalStateException("Thread confinement violation: registerSocket called off-thread.");
        }
        if (state != STATE_RUNNING) {
            throw new ClosedChannelException();
        }

        int fd = ctx.getFd();
        connectionMap.put(fd, ctx);

        long data = ((long) ctx.getAddress() << 32) | (fd & 0xFFFFFFFFL);
        int op = 1; // EPOLL_CTL_ADD
        int events = EPOLLIN | EPOLLOUT | EPOLLET;

        int res = epollCtl((int) epollFd, op, fd, events, data);
        if (res < 0) {
            connectionMap.remove(fd);
            throw new RuntimeException("epoll_ctl ADD failed with errno: " + (-res));
        }
        ctx.transitionState(SocketContext.STATE_ACTIVE);
    }

    public void terminateConnection(SocketContext ctx) {
        if (Thread.currentThread() != executionThread) {
            execute(() -> terminateConnection(ctx));
            return;
        }

        int fd = ctx.getFd();
        connectionMap.remove(fd);
        epollCtl((int) epollFd, 2, fd, 0, 0); // EPOLL_CTL_DEL
        
        ctx.releaseBuffers(bufferArena);
        ctx.closeNative();
        ctx.transitionState(SocketContext.STATE_CLOSED);
    }

    public Timeout scheduleTimeout(long delayMillis, Runnable task) {
        long absoluteEpochMillis = System.currentTimeMillis() + delayMillis;
        return timerWheel.schedule(absoluteEpochMillis, task);
    }

    private void handleLoopException(Throwable t) {
        // Core event loop fault isolation boundary
    }

    private void handlerException(SocketContext ctx, Throwable t) {
        // Channel handler fault isolation boundary
    }

    private void shutdownInternal() {
        if (!STATE_UPDATER.compareAndSet(this, STATE_SHUTTING_DOWN, STATE_TERMINATED) &&
            !STATE_UPDATER.compareAndSet(this, STATE_RUNNING, STATE_TERMINATED)) {
            return;
        }

        // Drain tasks
        taskQueue.drain(this::executeTask);

        // Cancel all timers
        timerWheel.clear();

        // Close all connections
        for (SocketContext ctx : connectionMap.values()) {
            terminateConnection(ctx);
        }
        connectionMap.clear();

        // Release native structures
        UNSAFE.freeMemory(eventBufferAddress);
        closeFd((int) epollFd);

        bufferArena.releaseAll();
    }

    public void shutdownGracefully(long timeoutMillis) {
        STATE_UPDATER.compareAndSet(this, STATE_RUNNING, STATE_SHUTTING_DOWN);
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (state != STATE_TERMINATED && System.currentTimeMillis() < deadline) {
            if (Thread.currentThread() == executionThread) {
                taskQueue.drain(this::executeTask);
                break;
            }
            wakeup();
            LockSupport.parkNanos(1000000L);
        }
        if (state != STATE_TERMINATED) {
            shutdownInternal();
        }
    }

    //region Native Calls
    private static native long epollCreate1(int flags);
    private static native int epollCtl(int epfd, int op, int fd, int events, long data);
    private static native int epollWait(int epfd, long events, int maxevents, int timeout);
    private static native int epollWakeup(long epollFd);
    private static native int closeFd(int fd);
    //endregion

    //region Subsystems
    public static final class MpscTaskQueue {
        private final Runnable[] queue;
        private final int mask;
        private volatile long producerIndex;
        private volatile long consumerIndex;

        private static final AtomicLongFieldUpdater<MpscTaskQueue> PRODUCER_INDEX_UPDATER =
                AtomicLongFieldUpdater.newUpdater(MpscTaskQueue.class, "producerIndex");
        private static final AtomicLongFieldUpdater<MpscTaskQueue> CONSUMER_INDEX_UPDATER =
                AtomicLongFieldUpdater.newUpdater(MpscTaskQueue.class, "consumerIndex");

        public MpscTaskQueue(int capacity) {
            int p2 = 1;
            while (p2 < capacity) {
                p2 <<= 1;
            }
            this.queue = new Runnable[p2];
            this.mask = p2 - 1;
        }

        public boolean offer(Runnable task) {
            if (task == null) throw new NullPointerException();
            long currentProducerIndex;
            long currentConsumerIndex;
            do {
                currentProducerIndex = producerIndex;
                currentConsumerIndex = consumerIndex;
                if (currentProducerIndex - currentConsumerIndex >= queue.length) {
                    return false;
                }
            } while (!PRODUCER_INDEX_UPDATER.compareAndSet(this, currentProducerIndex, currentProducerIndex + 1));

            queue[(int) (currentProducerIndex & mask)] = task;
            return true;
        }

        public void drain(java.util.function.Consumer<Runnable> consumer) {
            long currentConsumerIndex = consumerIndex;
            long currentProducerIndex = producerIndex;

            int batchSize = (int) (currentProducerIndex - currentConsumerIndex);
            if (batchSize <= 0) {
                return;
            }

            for (int i = 0; i < batchSize; i++) {
                int index = (int) ((currentConsumerIndex + i) & mask);
                Runnable task = queue[index];
                queue[index] = null;
                if (task != null) {
                    consumer.accept(task);
                }
            }
            CONSUMER_INDEX_UPDATER.set(this, currentConsumerIndex + batchSize);
        }
    }

    public static final class TimerWheel {
        private static final int[] WHEEL_SIZES = {512, 64, 64, 64, 64};
        private static final long[] WHEEL_RESOLUTIONS = {
                1L,
                512L,
                512L * 64L,
                512L * 64L * 64L,
                512L * 64L * 64L * 64L
        };

        private final TimeoutNode[][] wheels;
        private final long[] ticks;
        private long lastEpochMillis;

        public TimerWheel() {
            this.wheels = new TimeoutNode[5][];
            for (int i = 0; i < 5; i++) {
                this.wheels[i] = new TimeoutNode[WHEEL_SIZES[i]];
            }
            this.ticks = new long[5];
            this.lastEpochMillis = System.currentTimeMillis();
        }

        public Timeout schedule(long absoluteEpochMillis, Runnable task) {
            TimeoutNode node = new TimeoutNode(absoluteEpochMillis, task);
            addNode(node);
            return node;
        }

        private void addNode(TimeoutNode node) {
            long diff = node.absoluteEpochMillis - lastEpochMillis;
            if (diff < 0) diff = 0;

            for (int i = 0; i < 5; i++) {
                if (diff < WHEEL_RESOLUTIONS[i] * WHEEL_SIZES[i] || i == 4) {
                    int idx = (int) ((ticks[i] + (diff / WHEEL_RESOLUTIONS[i])) % WHEEL_SIZES[i]);
                    node.wheelLevel = i;
                    node.next = wheels[i][idx];
                    if (wheels[i][idx] != null) {
                        wheels[i][idx].prev = node;
                    }
                    wheels[i][idx] = node;
                    break;
                }
            }
        }

        public long processTimers() {
            long now = System.currentTimeMillis();
            if (now <= lastEpochMillis) {
                return 1L;
            }

            long steps = now - lastEpochMillis;
            lastEpochMillis = now;

            for (long s = 0; s < steps; s++) {
                cascade(0);
                ticks[0]++;
            }

            return 1L;
        }

        private void cascade(int level) {
            int idx = (int) (ticks[level] % WHEEL_SIZES[level]);
            TimeoutNode head = wheels[level][idx];
            wheels[level][idx] = null;

            TimeoutNode current = head;
            while (current != null) {
                TimeoutNode next = current.next;
                current.prev = null;
                current.next = null;

                if (!current.cancelled) {
                    if (level == 0) {
                        try {
                            current.task.run();
                        } catch (Throwable ignored) {}
                    } else {
                        addNode(current);
                    }
                }
                current = next;
            }

            if (idx == 0 && level < 4) {
                ticks[level + 1]++;
                cascade(level + 1);
            }
        }

        public void clear() {
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < WHEEL_SIZES[i]; j++) {
                    TimeoutNode curr = wheels[i][j];
                    while (curr != null) {
                        curr.cancelled = true;
                        curr = curr.next;
                    }
                    wheels[i][j] = null;
                }
            }
        }
    }

    public interface Timeout {
        void cancel();
        boolean isCancelled();
    }

    public static final class TimeoutNode implements Timeout {
        final long absoluteEpochMillis;
        final Runnable task;