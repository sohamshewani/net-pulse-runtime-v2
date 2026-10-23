package com.netpulse.runtime.test;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.concurrent.Execution;
import org.junit.jupiter.api.concurrent.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for 'net-pulse-runtime'.
 * Self-contained production-grade testing harness encompassing all domain abstractions.
 */
@Execution(ExecutionMode.CONCURRENT)
public class NetPulseRuntimeTestSuite {

    // =========================================================================
    // DOMAIN MODEL & RUNTIME IMPLEMENTATION (net-pulse-runtime)
    // =========================================================================

    public enum NodeState {
        INITIALIZING, ACTIVE, DEGRADED, FAULTED, DRAINING, TERMINATED
    }

    public enum MetricType {
        LATENCY_MS, THROUGHPUT_RPS, ERROR_RATE, BUFFER_UTILIZATION
    }

    public static final class PulseMetric {
        private final MetricType type;
        private final double value;
        private final long timestamp;
        private final String sourceNodeId;

        public PulseMetric(MetricType type, double value, long timestamp, String sourceNodeId) {
            if (type == null) throw new IllegalArgumentException("MetricType cannot be null");
            if (Double.isNaN(value) || Double.isInfinite(value)) throw new IllegalArgumentException("Invalid metric value: " + value);
            this.type = type;
            this.value = value;
            this.timestamp = timestamp;
            this.sourceNodeId = sourceNodeId == null ? "UNKNOWN" : sourceNodeId;
        }

        public MetricType getType() { return type; }
        public double getValue() { return value; }
        public long getTimestamp() { return timestamp; }
        public String getSourceNodeId() { return sourceNodeId; }
    }

    public static final class CircuitBreaker {
        private final int failureThreshold;
        private final long resetTimeoutMillis;
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private final AtomicLong stateChangeTimestamp = new AtomicLong(System.currentTimeMillis());
        private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.ACTIVE);

        public CircuitBreaker(int failureThreshold, long resetTimeoutMillis) {
            if (failureThreshold <= 0) throw new IllegalArgumentException("Failure threshold must be > 0");
            if (resetTimeoutMillis <= 0) throw new IllegalArgumentException("Reset timeout must be > 0");
            this.failureThreshold = failureThreshold;
            this.resetTimeoutMillis = resetTimeoutMillis;
        }

        public boolean allowRequest() {
            NodeState current = state.get();
            if (current == NodeState.FAULTED) {
                if (System.currentTimeMillis() - stateChangeTimestamp.get() > resetTimeoutMillis) {
                    if (state.compareAndSet(NodeState.FAULTED, NodeState.DEGRADED)) {
                        stateChangeTimestamp.set(System.currentTimeMillis());
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        public void recordSuccess() {
            consecutiveFailures.set(0);
            state.set(NodeState.ACTIVE);
        }

        public void recordFailure() {
            int failures = consecutiveFailures.incrementAndGet();
            if (failures >= failureThreshold) {
                if (state.compareAndSet(NodeState.ACTIVE, NodeState.FAULTED) || 
                    state.compareAndSet(NodeState.DEGRADED, NodeState.FAULTED)) {
                    stateChangeTimestamp.set(System.currentTimeMillis());
                }
            }
        }

        public NodeState getState() { return state.get(); }
        public int getConsecutiveFailures() { return consecutiveFailures.get(); }
    }

    public static final class PulseRingBuffer {
        private final PulseMetric[] buffer;
        private final int capacity;
        private final AtomicLong head = new AtomicLong(0);
        private final AtomicLong tail = new AtomicLong(0);
        private final ReentrantLock lock = new ReentrantLock();

        public PulseRingBuffer(int capacity) {
            if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
                // Enforce power-of-two for clean masking or handle directly. Let's enforce strict bounds.
                throw new IllegalArgumentException("Capacity must be positive power of two");
            }
            this.capacity = capacity;
            this.buffer = new PulseMetric[capacity];
        }

        public boolean offer(PulseMetric metric) {
            if (metric == null) return false;
            lock.lock();
            try {
                long currentTail = tail.get();
                long currentHead = head.get();
                if (currentTail - currentHead >= capacity) {
                    // Overwrite oldest or reject. We implement overwriting ring buffer semantics.
                    head.incrementAndGet();
                }
                buffer[(int)(currentTail & (capacity - 1))] = metric;
                tail.incrementAndGet();
                return true;
            } finally {
                lock.unlock();
            }
        }

        public List<PulseMetric> drain() {
            lock.lock();
            try {
                long currentHead = head.get();
                long currentTail = tail.get();
                int size = (int)(currentTail - currentHead);
                if (size <= 0) return Collections.emptyList();

                List<PulseMetric> drained = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    int index = (int)((currentHead + i) & (capacity - 1));
                    drained.add(buffer[index]);
                    buffer[index] = null; // GC relief
                }
                head.set(currentTail);
                return drained;
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            long currentTail = tail.get();
            long currentHead = head.get();
            return (int) Math.max(0, currentTail - currentHead);
        }

        public int capacity() { return capacity; }
    }

    public static final class NetPulseEngine implements AutoCloseable {
        private final String nodeId;
        private final PulseRingBuffer ringBuffer;
        private final CircuitBreaker circuitBreaker;
        private final AtomicReference<NodeState> nodeState = new AtomicReference<>(NodeState.INITIALIZING);
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "NetPulse-Engine-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        private final LongAdder processedMetricsCount = new LongAdder();
        private final AtomicBoolean isClosed = new AtomicBoolean(false);
        private final List<Consumer<PulseMetric>> telemetryListeners = new CopyOnWriteArrayList<>();

        public NetPulseEngine(String nodeId, int bufferCapacity, int cbThreshold, long cbResetMs) {
            this.nodeId = nodeId;
            this.ringBuffer = new PulseRingBuffer(bufferCapacity);
            this.circuitBreaker = new CircuitBreaker(cbThreshold, cbResetMs);
            this.nodeState.set(NodeState.ACTIVE);
            
            // Start background flush worker
            scheduler.scheduleAtFixedRate(this.backgroundWorkerTick, 10, 10, TimeUnit.MILLISECONDS);
        }

        private final Runnable backgroundWorkerTick = () -> {
            if (isClosed.get()) return;
            List<PulseMetric> metrics = ringBuffer.drain();
            for (PulseMetric m : metrics) {
                processedMetricsCount.increment();
                for (Consumer<PulseMetric> listener : telemetryListeners) {
                    try {
                        listener.accept(m);
                    } catch (Exception ignored) {}
                }
            }
        };

        public boolean ingest(MetricType type, double value, String source) {
            if (isClosed.get()) return false;
            if (!circuitBreaker.allowRequest()) {
                nodeState.set(NodeState.FAULTED);
                return false;
            }
            try {
                PulseMetric metric = new PulseMetric(type, value, System.currentTimeMillis(), source);
                boolean success = ringBuffer.offer(metric);
                if (success) {
                    circuitBreaker.recordSuccess();
                    if (nodeState.get() == NodeState.DEGRADED) {
                        nodeState.set(NodeState.ACTIVE);
                    }
                } else {
                    circuitBreaker.recordFailure();
                }
                return success;
            } catch (Exception e) {
                circuitBreaker.recordFailure();
                return false;
            }
        }

        public void addListener(Consumer<PulseMetric> listener) {
            if (listener != null) telemetryListeners.add(listener);
        }

        public NodeState getNodeState() { return nodeState.get(); }
        public long getProcessedCount() { return processedMetricsCount.sum(); }
        public CircuitBreaker getCircuitBreaker() { return circuitBreaker; }
        public PulseRingBuffer getRingBuffer() { return ringBuffer; }

        @Override
        public void close() {
            if (isClosed.compareAndSet(false, true)) {
                nodeState.set(NodeState.TERMINATED);
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // Final drain
                backgroundWorkerTick.run();
            }
        }
    }

    // =========================================================================
    // UNIT TESTS: BOUNDARY & EDGE-CASE ASSERTIONS
    // =========================================================================

    public static class UnitTests {

        @Test
        @DisplayName("PulseMetric boundary constraints and exception safety")
        public void testPulseMetricBoundaries() {
            long now = System.currentTimeMillis();
            PulseMetric metric = new PulseMetric(MetricType.LATENCY_MS, 45.2, now, "node-01");
            
            assertEquals(MetricType.LATENCY_MS, metric.getType());
            assertEquals(45.2, metric.getValue(), 0.0001);
            assertEquals(now, metric.getTimestamp());
            assertEquals("node-01", metric.getSourceNodeId());

            // Edge cases
            assertThrows(IllegalArgumentException.class, () -> new PulseMetric(null, 10.0, now, "src"));
            assertThrows(IllegalArgumentException.class, () -> new PulseMetric(MetricType.ERROR_RATE, Double.NaN, now, "src"));
            assertThrows(IllegalArgumentException.class, () -> new PulseMetric(MetricType.ERROR_RATE, Double.POSITIVE_INFINITY, now, "src"));
            
            // Null source node ID defaults gracefully
            PulseMetric nullSourceMetric = new PulseMetric(MetricType.THROUGHPUT_RPS, 100.0, now, null);
            assertEquals("UNKNOWN", nullSourceSourceId(nullSourceMetric));
        }

        private String nullSourceSourceId(PulseMetric m) {
            return m.getSourceNodeId();
        }

        @ParameterizedTest
        @ValueSource(ints = {2, 4, 16, 64, 1024})
        @DisplayName("PulseRingBuffer initialization with valid power-of-two capacities")
        public void testRingBufferValidCapacities(int capacity) {
            PulseRingBuffer buffer = new PulseRingBuffer(capacity);
            assertEquals(capacity, buffer.capacity());
            assertEquals(0, buffer.size());
        }

        @ParameterizedTest
        @ValueSource(ints = {-1, 0, 3, 5, 100, 1000})
        @DisplayName("PulseRingBuffer illegal capacity constraint enforcement")
        public void testRingBufferInvalidCapacities(int invalidCapacity) {
            assertThrows(IllegalArgumentException.class, () -> new PulseRingBuffer(invalidCapacity));
        }

        @Test
        @DisplayName("PulseRingBuffer overflow and wrap-around semantics")