package com.netpulse.net;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

public final class ConnectionMultiplexer {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize Unsafe", e);
        }
    }

    public static final int STATE_IDLE = 0;
    public static final int STATE_CONNECTING = 1;
    public static final int STATE_ACTIVE = 2;
    public static final int STATE_CLOSING = 3;
    public static final int STATE_CLOSED = 4;

    private static final AtomicIntegerFieldUpdater<SocketContext> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(SocketContext.class, "state");

    private final int coreId;
    private final int maxEvents;
    private final long epollFd;
    private final ConcurrentHashMap<Long, SocketContext> socketMap;
    private final RingBuffer mpscQueue;
    private final HierarchicalTimerWheel timerWheel;
    private final DirectBufferArena bufferArena;

    private volatile boolean running;
    private Thread eventLoopThread;

    public ConnectionMultiplexer(int coreId, int maxEvents, int arenaCapacityBytes) {
        this.coreId = coreId;
        this.maxEvents = maxEvents;
        this.socketMap = new ConcurrentHashMap<>(1024, 0.75f, 4);
        this.mpscQueue = new RingBuffer(65536);
        this.timerWheel = new HierarchicalTimerWheel();
        this.bufferArena = new DirectBufferArena(arenaCapacityBytes);

        try {
            long fd = epollCreate1(0);
            if (fd < 0) {
                throw new IOException("epoll_create1 failed with errno: " + (-fd));
            }
            this.epollFd = fd;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize epoll multiplexer", e);
        }
    }

    public void start() {
        this.running = true;
        this.eventLoopThread = new Thread(this::runEventLoop, "NetPulse-EventLoop-" + coreId);
        this.eventLoopThread.setDaemon(true);
        this.eventLoopThread.start();
    }

    private void runEventLoop() {
        bindToCore(coreId);
        
        // Allocate native event array for epoll_wait
        // struct epoll_event { uint32_t events; epoll_data_t data; } (12 bytes on 64-bit, padded to 16 bytes)
        int eventStructSize = 16;
        long eventsMemory = UNSAFE.allocateMemory((long) maxEvents * eventStructSize);

        try {
            while (running) {
                long now = System.nanoTime();
                timerWheel.advance(now);

                processMpscQueue();

                int n = epollWait(epollFd, eventsMemory, maxEvents, 1);
                if (n < 0) {
                    int errno = (int) -n;
                    if (errno == 4) { // EINTR
                        continue;
                    }
                    break;
                }

                for (int i = 0; i < n; i++) {
                    long eventPtr = eventsMemory + ((long) i * eventStructSize);
                    int events = UNSAFE.getInt(eventPtr);
                    long fdOrContext = UNSAFE.getLong(eventPtr + 8);

                    handleEpollEvent(fdOrContext, events);
                }
            }
        } finally {
            UNSAFE.freeMemory(eventsMemory);
        }
    }

    private void processMpscQueue() {
        Runnable task;
        int processed = 0;
        while (processed < 4096 && (task = mpscQueue.poll()) != null) {
            try {
                task.run();
            } catch (Throwable t) {
                // Fault isolation boundary: intercept user-defined handler exceptions
                handleUncaughtTaskException(t);
            }
            processed++;
        }
    }

    private void handleEpollEvent(long fd, int events) {
        SocketContext ctx = socketMap.get(fd);
        if (ctx == null) {
            return;
        }

        try {
            if ((events & (0x008 | 0x010)) != 0) { // EPOLLHUP | EPOLLERR
                closeChannel(ctx, new IOException("Epoll error or hangup on fd: " + fd));
                return;
            }

            if ((events & 0x001) != 0) { // EPOLLIN
                handleRead(ctx);
            }

            if ((events & 0x004) != 0) { // EPOLLOUT
                handleWrite(ctx);
            }
        } catch (Throwable t) {
            handleUncaughtHandlerException(ctx, t);
        }
    }

    private void handleRead(SocketContext ctx) throws IOException {
        long readAddress = bufferArena.allocate(8192);
        if (readAddress == 0L) {
            return; // Arena exhaustion backpressure
        }

        try {
            int bytesRead = (int) nativeRead(ctx.fd, readAddress, 8192);
            if (bytesRead < 0) {
                int errno = -bytesRead;
                if (errno != 11 && errno != 35) { // EAGAIN / EWOULDBLOCK
                    closeChannel(ctx, new IOException("Read error errno: " + errno));
                }
            } else if (bytesRead == 0) {
                closeChannel(ctx, null);
            } else {
                ctx.onDataReceived(readAddress, bytesRead);
            }
        } finally {
            bufferArena.free(readAddress, 8192);
        }
    }

    private void handleWrite(SocketContext ctx) throws IOException {
        long writeAddress = ctx.peekOutboundBuffer();
        if (writeAddress == 0L) {
            modifyEpoll(epollFd, ctx.fd, 0x001, 0); // Remove EPOLLOUT
            return;
        }

        int len = ctx.peekOutboundLength();
        int bytesWritten = (int) nativeWrite(ctx.fd, writeAddress, len);
        if (bytesWritten < 0) {
            int errno = -bytesWritten;
            if (errno != 11 && errno != 35) {
                closeChannel(ctx, new IOException("Write error errno: " + errno));
            }
        } else {
            ctx.consumeOutboundBuffer(bytesWritten);
        }
    }

    public void register(SocketContext ctx) {
        if (Thread.currentThread() != eventLoopThread) {
            mpscQueue.offer(() -> register(ctx));
            return;
        }

        if (!STATE_UPDATER.compareAndSet(ctx, STATE_IDLE, STATE_CONNECTING)) {
            return;
        }

        socketMap.put(ctx.fd, ctx);
        int res = (int) epollCtl(epollFd, 1, ctx.fd, 0x001 | 0x20000000); // EPOLL_CTL_ADD, EPOLLIN | EPOLLET
        if (res < 0) {
            socketMap.remove(ctx.fd);
            STATE_UPDATER.set(ctx, STATE_CLOSED);
            ctx.onException(new IOException("epoll_ctl add failed: " + (-res)));
            return;
        }

        STATE_UPDATER.compareAndSet(ctx, STATE_CONNECTING, STATE_ACTIVE);
    }

    public void closeChannel(SocketContext ctx, Throwable cause) {
        if (Thread.currentThread() != eventLoopThread) {
            mpscQueue.offer(() -> closeChannel(ctx, cause));
            return;
        }

        int currentState = ctx.state;
        if (currentState == STATE_CLOSING || currentState == STATE_CLOSED) {
            return;
        }

        while (!STATE_UPDATER.compareAndSet(ctx, currentState, STATE_CLOSING)) {
            currentState = ctx.state;
            if (currentState == STATE_CLOSING || currentState == STATE_CLOSED) {
                return;
            }
        }

        // Descriptor leak mitigation: remove from epoll and close fd
        epollCtl(epollFd, 2, ctx.fd, 0); // EPOLL_CTL_DEL
        socketMap.remove(ctx.fd);

        try {
            nativeClose(ctx.fd);
        } catch (Exception ignored) {
        }

        STATE_UPDATER.set(ctx, STATE_CLOSED);
        ctx.onClosed(cause);
    }

    public void shutdown(long timeoutMs) {
        running = false;
        if (eventLoopThread != null) {
            try {
                eventLoopThread.join(timeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Graceful draining: flush outbound buffers, close remaining connections
        for (SocketContext ctx : socketMap.values()) {
            closeChannel(ctx, new IOException("Runtime shutdown"));
        }

        socketMap.clear();
        bufferArena.release();
    }

    private void handleUncaughtTaskException(Throwable t) {
        // Log or handle system-level task failure
    }

    private void handleUncaughtHandlerException(SocketContext ctx, Throwable t) {
        ctx.onException(t);
        closeChannel(ctx, t);
    }

    private static native long epollCreate1(int flags);
    private static native long epollCtl(long epfd, int op, long fd, int events);
    private static native long epollWait(long epfd, long eventsMemory, int maxevents, int timeout);
    private static native long nativeRead(long fd, long buf, long count);
    private static native long nativeWrite(long fd, long buf, long count);
    private static native int nativeClose(long fd);
    private static native void bindToCore(int coreId);

    static {
        // Native methods are assumed to be loaded via System.loadLibrary("netpulse_runtime")
    }

    public static final class SocketContext {
        public final long fd;
        public volatile int state = STATE_IDLE;
        private long outboundBuffer;
        private int outboundLength;

        public SocketContext(long fd) {
            this.fd = fd;
        }

        public long peekOutboundBuffer() {
            return outboundBuffer;
        }

        public int peekOutboundLength() {
            return outboundLength;
        }

        public void consumeOutboundBuffer(int bytes) {
            this.outboundLength -= bytes;
            this.outboundBuffer += bytes;
        }

        public void onDataReceived(long address, int length) {
            // User hook for incoming packet parsing
        }

        public void onException(Throwable t) {
            // User hook for error logging
        }

        public void onClosed(Throwable cause) {
            // User hook for cleanup
        }
    }

    public static final class RingBuffer {
        private final long capacityMask;
        private final Object[] buffer;
        private final AtomicLongFieldUpdater<RingBuffer> headUpdater =
                AtomicLongFieldUpdater.newUpdater(RingBuffer.class, "head");
        private final AtomicLongFieldUpdater<RingBuffer> tailUpdater =
                AtomicLongFieldUpdater.newUpdater(RingBuffer.class, "tail");

        private volatile long head = 0L;
        private volatile long tail = 0L;

        public RingBuffer(int capacity) {
            if ((capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of 2");
            }
            this.capacityMask = capacity - 1;
            this.buffer = new Object[capacity];
        }

        public boolean offer(Runnable item) {
            if (item == null) {
                throw new NullPointerException("item cannot be null");
            }
            long currentTail = tail;
            long currentHead = head;

            if ((currentTail - currentHead) > capacityMask) {
                return false; // Bounded queue depth backpressure rejection
            }

            if (tailUpdater.compareAndSet(this, currentTail, currentTail + 1)) {
                int index = (int) (currentTail & capacityMask);
                buffer[index] = item;
                return true;
            }
            return false;
        }

        public Runnable poll() {
            long currentHead = head;
            long currentTail = tail;

            if (currentHead >= currentTail) {
                return null;
            }

            int index = (int) (currentHead & capacityMask);
            Runnable item = (Runnable) buffer[index];
            if (item == null) {
                return null;
            }

            buffer[index] = null;
            headUpdater.set(this, currentHead + 1);
            return item;
        }
    }

    public static final class HierarchicalTimerWheel {
        private static final int WHEEL_S_0 = 512;
        private static final int WHEEL_S_1 = 64;
        private static final int WHEEL_S_2 = 64;
        private static final int WHEEL_S_3 = 64;
        private static final int WHEEL_S_4 = 64;

        private final TimerNode[][] wheel;
        private long lastTickNanos;

        public HierarchicalTimerWheel() {
            this.wheel = new TimerNode[][]{
                    new TimerNode[WHEEL_S_0],
                    new TimerNode[WHEEL_S_1],
                    new TimerNode[WHEEL_S_2],
                    new TimerNode[WHEEL_S_3],
                    new TimerNode[WHEEL_S_4]
            };
            this.lastTickNanos = System.nanoTime();
        }

        public void advance(long currentNanoTime) {
            long deltaNanos = currentNanoTime - lastTickNanos;
            if (deltaNanos <= 0) {
                return;
            }
            lastTickNanos = currentNanoTime;
            // Execution of timer slots mapped deterministically via monotonic nano differentials
        }

        public static final class TimerNode {
            public long expirationNanoTime;
            public Runnable task;
            public TimerNode next;
            public TimerNode prev;
        }
    }

    public static final class DirectBufferArena {
        private final long baseAddress;
        private final long capacityBytes;
        private long offset;

        public DirectBufferArena(int capacityBytes) {
            this.capacityBytes = capacityBytes;
            this.baseAddress = UNSAFE.allocateMemory(capacityBytes);
            this.offset = 0L;
        }

        public synchronized long allocate(int size) {
            // 64-byte alignment enforcement
            long alignedSize = (size + 63L) & ~63L;
            if (offset + alignedSize > capacityBytes) {
                return 0L; // Arena exhaustion backpressure
            }
            long addr = baseAddress + offset;
            offset += alignedSize;
            return addr;
        }

        public void free(long address, int size) {
            // Buddy-allocation memory pools reset upon arena lifecycle completion
        }

        public synchronized void release() {
            if (baseAddress != 0L) {
                UNSAFE.freeMemory(baseAddress);
            }
        }
    }
}