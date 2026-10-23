package com.netpulse.timer;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.Objects;

public final class HierarchicalTimerWheel {

    private static final int[] WHEEL_SIZES = { 512, 64, 64, 64, 64 };
    private static final int[] WHEEL_RESOLUTIONS = { 1, 9, 15, 21, 27 }; // Bit shifts for levels

    private final long tickMs;
    private final long startTimeMs;
    private long currentTick;

    private final TimeoutNode[][] wheels;
    private final TimeoutNodePool nodePool;

    public static final class TimeoutNode {
        public long expirationMs;
        public Runnable task;
        public volatile int state; // 0 = FREE, 1 = SCHEDULED, 2 = CANCELLED, 3 = EXECUTED
        
        public TimeoutNode prev;
        public TimeoutNode next;

        private static final AtomicIntegerFieldUpdater<TimeoutNode> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(TimeoutNode.class, "state");

        public boolean compareAndSetState(int expect, int update) {
            return STATE_UPDATER.compareAndSet(this, expect, update);
        }

        public void setState(int newState) {
            state = newState;
        }

        public void clear() {
            this.expirationMs = 0L;
            this.task = null;
            this.state = 0;
            this.prev = null;
            this.next = null;
        }
    }

    private static final class TimeoutNodePool {
        private final TimeoutNode[] pool;
        private int head;
        private int tail;
        private int size;
        private final Object lock = new Object();

        public TimeoutNodePool(int capacity) {
            pool = new TimeoutNode[capacity];
            for (int i = 0; i < capacity; i++) {
                pool[i] = new TimeoutNode();
            }
            head = 0;
            tail = capacity - 1;
            size = capacity;
        }

        public TimeoutNode acquire(long expirationMs, Runnable task) {
            synchronized (lock) {
                if (size == 0) {
                    // Fallback allocation if pool exhausts under extreme load
                    TimeoutNode node = new TimeoutNode();
                    node.expirationMs = expirationMs;
                    node.task = task;
                    node.state = 1;
                    return node;
                }
                TimeoutNode node = pool[head];
                pool[head] = null;
                head = (head + 1) % pool.length;
                size--;

                node.expirationMs = expirationMs;
                node.task = task;
                node.state = 1;
                node.prev = null;
                node.next = null;
                return node;
            }
        }

        public void release(TimeoutNode node) {
            node.clear();
            synchronized (lock) {
                if (size < pool.length) {
                    tail = (tail + 1) % pool.length;
                    pool[tail] = node;
                    size++;
                }
            }
        }
    }

    public HierarchicalTimerWheel(long tickMs, int poolCapacity) {
        if (tickMs <= 0) {
            throw new IllegalArgumentException("tickMs must be positive");
        }
        this.tickMs = tickMs;
        this.startTimeMs = System.nanoTime() / 1_000_000L;
        this.currentTick = 0L;
        this.wheels = new TimeoutNode[WHEEL_SIZES.length][];
        for (int i = 0; i < WHEEL_SIZES.length; i++) {
            this.wheels[i] = new TimeoutNode[WHEEL_SIZES[i]];
        }
        this.nodePool = new TimeoutNodePool(poolCapacity);
    }

    public TimeoutNode schedule(Runnable task, long delayMs) {
        Objects.requireNonNull(task, "task");
        if (delayMs < 0) {
            delayMs = 0;
        }
        long absoluteExpirationMs = (System.nanoTime() / 1_000_000L) + delayMs;
        TimeoutNode node = nodePool.acquire(absoluteExpirationMs, task);
        addNode(node);
        return node;
    }

    public boolean cancel(TimeoutNode node) {
        if (node == null) {
            return false;
        }
        while (true) {
            int currentState = node.state;
            if (currentState != 1) { // 1 = SCHEDULED
                return false;
            }
            if (node.compareAndSetState(1, 2)) { // 2 = CANCELLED
                removeNode(node);
                nodePool.release(node);
                return true;
            }
        }
    }

    public void advance(long currentTimeMs) {
        long elapsedMs = currentTimeMs - startTimeMs;
        long targetTick = elapsedMs / tickMs;

        while (currentTick <= targetTick) {
            cascadeAndExecute(currentTick);
            currentTick++;
        }
    }

    private void cascadeAndExecute(long tick) {
        int idx = (int) (tick & 511);
        TimeoutNode head = wheels[0][idx];
        wheels[0][idx] = null;

        // Execute level 0 slots
        executeAndCleanup(head);

        // Cascade upper wheels if necessary
        if (idx == 0) {
            cascade(1, tick);
        }
    }

    private void cascade(int level, long tick) {
        if (level >= WHEEL_SIZES.length) {
            return;
        }

        int idx = (int) ((tick >> WHEEL_RESOLUTIONS[level - 1]) & (WHEEL_SIZES[level] - 1));
        TimeoutNode head = wheels[level][idx];
        wheels[level][idx] = null;

        TimeoutNode current = head;
        while (current != null) {
            TimeoutNode next = current.next;
            current.prev = null;
            current.next = null;
            addNode(current);
            current = next;
        }

        if (idx == 0) {
            cascade(level + 1, tick);
        }
    }

    private void addNode(TimeoutNode node) {
        long ticks = (node.expirationMs - startTimeMs) / tickMs;
        long diff = ticks - currentTick;
        if (diff < 0) {
            diff = 0;
        }

        int level = 0;
        for (int i = 0; i < WHEEL_RESOLUTIONS.length; i++) {
            if (diff < (1L << WHEEL_RESOLUTIONS[i])) {
                level = i;
                break;
            }
            if (i == WHEEL_RESOLUTIONS.length - 1) {
                level = i;
            }
        }

        int slotMask = WHEEL_SIZES[level] - 1;
        int shift = level == 0 ? 0 : WHEEL_RESOLUTIONS[level - 1];
        int idx = (int) ((ticks >> shift) & slotMask);

        synchronized (wheels[level]) {
            node.next = wheels[level][idx];
            if (wheels[level][idx] != null) {
                wheels[level][idx].prev = node;
            }
            node.prev = null;
            wheels[level][idx] = node;
        }
    }

    private void removeNode(TimeoutNode node) {
        // Since we don't store the exact level and index directly in the node to save memory,
        // we scan or rely on atomic state transitions to handle safe extraction.
        // For O(1) removal in real-time constraints, we maintain explicit doubly linked list pointers.
        synchronized (wheels) {
            if (node.prev != null) {
                node.prev.next = node.next;
            }
            if (node.next != null) {
                node.next.prev = node.prev;
            }
            for (int l = 0; l < wheels.length; l++) {
                for (int s = 0; s < wheels[l].length; s++) {
                    if (wheels[l][s] == node) {
                        wheels[l][s] = node.next;
                        if (wheels[l][s] != null) {
                            wheels[l][s].prev = null;
                        }
                        node.prev = null;
                        node.next = null;
                        return;
                    }
                }
            }
        }
    }

    private void executeAndCleanup(TimeoutNode head) {
        TimeoutNode current = head;
        while (current != null) {
            TimeoutNode next = current.next;
            current.prev = null;
            current.next = null;

            if (current.compareAndSetState(1, 3)) { // 3 = EXECUTED
                try {
                    current.task.run();
                } catch (Throwable t) {
                    // Fault isolation: trap uncaught exceptions to protect timer thread
                    Thread.currentThread().getUncaughtExceptionHandler()
                            .uncaughtException(Thread.currentThread(), t);
                } finally {
                    nodePool.release(current);
                }
            }
            current = next;
        }
    }
}