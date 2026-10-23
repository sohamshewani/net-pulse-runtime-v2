package com.netpulse.benchmark;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public final class NetPulseRuntimeMicrobenchmark {

    private static final int WARMUP_ITERATIONS = 2_000_000;
    private static final int MEASUREMENT_ITERATIONS = 10_000_000;
    private static final int REPORTING_BUCKET_SIZE = 1_000_000;

    // Simulated payload for net-pulse-runtime processing
    private static final class PulseContext {
        long timestamp;
        int nodeId;
        boolean active;

        void reset(long ts, int id) {
            this.timestamp = ts;
            this.nodeId = id;
            this.active = true;
        }
    }

    // Core execution engine simulating 'net-pulse-runtime' dispatch & evaluation
    private static final class NetPulseRuntime {
        public boolean evaluate(PulseContext context) {
            // Simulate low-latency deterministic network telemetry pulse logic
            long hash = context.timestamp ^ (context.nodeId * 31L);
            return (hash & 1L) == 0L && context.active;
        }
    }

    public static void main(String[] args) {
        NetPulseRuntime runtime = new NetPulseRuntime();
        PulseContext context = new PulseContext();

        long[] latencies = new long[MEASUREMENT_ITERATIONS];

        System.out.println("Initializing 'net-pulse-runtime' microbenchmark...");
        System.out.printf("Warmup iterations: %,d%n", WARMUP_ITERATIONS);
        System.out.printf("Measurement iterations: %,d%n%n", MEASUREMENT_ITERATIONS);

        // --- WARMUP PHASE ---
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            context.reset(System.nanoTime(), i);
            runtime.evaluate(context);
        }

        // Force GC prior to measurement
        System.gc();

        // --- MEASUREMENT PHASE ---
        long startTime = System.nanoTime();

        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long pulseStart = System.nanoTime();
            
            context.reset(pulseStart, i);
            runtime.evaluate(context);

            long pulseEnd = System.nanoTime();
            latencies[i] = pulseEnd - pulseStart;
        }

        long totalTimeNs = System.nanoTime() - startTime;

        // --- STATISTICAL ANALYSIS ---
        // Sort latencies for precise percentile extraction (p50, p99)
        Arrays.sort(latencies);

        double totalTimeSec = totalTimeNs / 1_000_000_000.0;
        double opsPerSec = MEASUREMENT_ITERATIONS / totalTimeSec;

        long p50 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.50)];
        long p95 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.95)];
        long p99 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.99)];
        long p999 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.999)];

        // --- FORMATTED OUTPUT ---
        System.out.println("==================================================================");
        System.out.println("         NET-PULSE-RUNTIME MICROBENCHMARK RESULTS                 ");
        System.out.println("==================================================================");
        System.out.printf("%-25s | %-20s%n", "Metric", "Value");
        System.out.println("------------------------------------------------------------------");
        System.out.printf("%-25s | %,15.2f ops/sec%n", "Throughput", opsPerSec);
        System.out.printf("%-25s | %15.3f ms%n", "Total Execution Time", totalTimeSec * 1000.0);
        System.out.printf("%-25s | %,15d%n", "Total Operations", MEASUREMENT_ITERATIONS);
        System.out.println("------------------------------------------------------------------");
        System.out.printf("%-25s | %15.2f ns%n", "Latency p50 (Median)", p50 / 1.0);
        System.out.printf("%-25s | %15.2f ns%n", "Latency p95", p95 / 1.0);
        System.out.printf("%-25s | %15.2f ns%n", "Latency p99", p99 / 1.0);
        System.out.printf("%-25s | %15.2f ns%n", "Latency p99.9", p999 / 1.0);
        System.out.println("==================================================================");
    }
}