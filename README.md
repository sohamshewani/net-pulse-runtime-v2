# net-pulse-runtime
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/net-pulse-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/net-pulse-runtime/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/net-pulse-runtime) [![Stars](https://img.shields.io/github/stars/sohamshewani/net-pulse-runtime?style=flat)](https://github.com/sohamshewani/net-pulse-runtime)

## Executive Summary & Problem Space

Modern enterprise systems operating at sub-millisecond tail latency bounds ($P_{99.99}$) frequently hit fundamental limits in the standard Java Virtual Machine (JVM) runtime environment. Traditional network I/O architectures reliant on thread-per-connection paradigms scale poorly under millions of concurrent persistent sockets due to OS thread scheduling jitter, context-switch overhead, and memory consumption. Furthermore, standard `java.net` sockets and `java.nio` abstractions incur heavy object allocation overheads on every transaction, causing garbage collection (GC) stop-the-world pauses that violate strict Service Level Objectives (SLOs).

`net-pulse-runtime` is a zero-allocation, high-throughput asynchronous networking and runtime engine engineered specifically for low-latency financial trading platforms, real-time data streaming backplanes, and massive-scale distributed infrastructure. By bypassing JVM heap allocations via off-heap memory arenas, implementing a lock-free task-stealing concurrency model, integrating zero-copy native event loops (`NativeEventLoop`), and utilizing hierarchical timer wheels (`HierarchicalTimerWheel`), `net-pulse-runtime` achieves deterministic microsecond-level execution times.

---

## Structural ASCII Architecture / Dataflow Diagram

The runtime orchestrates network ingress and egress entirely through off-heap memory boundaries and non-blocking actors. The architecture cleanly separates packet ingestion, reactor polling, hierarchical timing, and application-level execution.

```
 +---------------------------------------------------------------------------------------------+
 |                                     net-pulse-runtime                                       |
 +---------------------------------------------------------------------------------------------+
                                                |
                 +------------------------------+------------------------------+
                 | (Epoll / Kqueue Native Polling)                             |
                 v                                                             v
+----------------------------------+                         +---------------------------------+
|   ConnectionMultiplexer.java     |                         |     NativeEventLoop.java        |
|  - Manages Socket Channel State  |                         |  - Epoll/Selector IO Multiplex  |
|  - Non-blocking Registration     |                         |  - Thread-per-Core Pinning      |
+----------------------------------+                         +---------------------------------+
                 |                                                             |
                 +------------------------------+------------------------------+
                                                | Direct Byte Buffers
                                                v
+---------------------------------------------------------------------------------------------+
|                               DirectBufferArena.java                                        |
|  - Off-Heap Memory Chunking & Slab Allocation                                               |
|  - Zero-Copy Slicing & Reference Counting                                                   |
+---------------------------------------------------------------------------------------------+
                                                |
                 +------------------------------+------------------------------+
                 |                              |                              |
                 v                              v                              v
+---------------------------------+  +-------------------------------+  +----------------------+
|    HierarchicalTimerWheel.java  |  |  TaskStealingExecutor.java    |  | RuntimeEngine.java   |
|  - O(1) Timeout Insert/Cancel   |  |  - Work-Stealing Queues       |  | - System Bootstrap   |
|  - Multi-tier Tick Cascading    |  |  - Lock-Free Task Dispatch    |  | - Lifecycle Manager  |
+---------------------------------+  +-------------------------------+  +----------------------+
                 |                              |                              |
                 +------------------------------+------------------------------+
                                                |
                                                v
                               +---------------------------------+
                               |   tests/test_invariants.py      |
                               |   benchmarks/bench_engine.py    |
                               +---------------------------------+
```

---

## Concurrency Model & Memory Invariants

### 1. Thread-per-Core Architecture
`net-pulse-runtime` enforces strict thread confinement. Each `NativeEventLoop` instance is pinned to a dedicated operating system thread, executing a continuous non-blocking poll-process loop. Shared state between event loops is strictly forbidden; inter-thread communication occurs exclusively via lock-free, single-producer single-consumer (SPSC) ring buffers or work-stealing queues (`TaskStealingExecutor`).

### 2. Lock-Contention Prevention
To eliminate synchronization bottlenecks, atomic primitives (`java.util.concurrent.atomic`) and volatile memory ordering are utilized in place of intrinsic locks (`synchronized`). Task stealing operates via Chase-Lev work-stealing deques, permitting worker threads to execute local tasks with zero synchronization overhead while gracefully acquiring foreign tasks via atomic Compare-And-Swap (CAS) operations.

### 3. Memory Ownership & Zero-Copy Invariants
All network payloads are routed through `DirectBufferArena`. 
* **Invariant I**: Memory backing a packet payload is allocated strictly off-heap to prevent JVM garbage collection pressure.
* **Invariant II**: Buffers adhere to explicit reference-counting semantics. A buffer cannot be recycled into the arena pool until its reference count reaches zero.
* **Invariant III**: Slicing operations produce zero-copy views (`ByteBuffer.duplicate()` or native pointer offsets) ensuring data is never physically duplicated across memory boundaries during transit.

---

## Performance Characteristics & Algorithmic Complexity Table

| Component / Subsystem | Operation | Time Complexity ($Big\text{-}O$) | Space Complexity | Estimated Latency ($P_{99}$) |
| :--- | :--- | :--- | :--- | :--- |
| **NativeEventLoop** | Socket Polling (`epoll_wait`) | $O(N)$ active descriptors | $O(1)$ static descriptors | $< 1.2\,\mu\text{s}$ |
| **DirectBufferArena** | Slab Allocation / Reclamation | $O(1)$ amortized | $O(M)$ pool capacity | $< 50\,\text{ns}$ |
| **HierarchicalTimerWheel**| Timeout Insertion / Deletion | $O(1)$ | $O(W)$ slot capacity | $< 100\,\text{ns}$ |
| **ConnectionMultiplexer** | Multiplex Registration / Drop | $O(1)$ hash lookup | $O(C)$ connections | $< 250\,\text{ns}$ |
| **TaskStealingExecutor** | Local Push / Pop | $O(1)$ | $O(T)$ task queue size | $< 80\,\text{ns}$ |
| **TaskStealingExecutor** | Remote Task Steal | $O(1)$ CAS retry | $O(T)$ task queue size | $< 400\,\text{ns}$ |

---

## Security Architecture & Threat Model

### 1. Input Boundaries & Bounds Checking
All network reads coming through `ConnectionMultiplexer` are bounded strictly by the pre-allocated capacity of the `DirectBufferArena` slab. Over-reads are rejected at the native transport layer. Frame length headers are explicitly verified against maximum payload bounds prior to buffer allocation to prevent out-of-memory denial-of-service (DoS) vectors.

### 2. Memory Corruption Protections
Because `net-pulse-runtime` interacts directly with off-heap memory via Unsafe and direct buffers, strict invariant checking is enforced in debug and assertion modes:
* **Use-After-Free Prevention**: Attempting to write to or read from a recycled buffer slice triggers an immediate segmentation fault simulation or `IllegalStateException`.
* **Double-Free Mitigation**: Reference count decrement operations are thread-safe and idempotent-guarded via atomic state transitions.

### 3. Sanitization & Resource Exhaustion Defense
* **Connection Flooding**: Managed via strict high-water and low-water mark checks in `ConnectionMultiplexer`, actively shedding load or backpressuring channels when limits are breached.
* **Timer Wheel Amplification**: Timer insertion slots are bounded by pre-allocated wheel tiers, preventing memory exhaustion attacks via malicious infinite-delay registrations.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
* **Java Development Kit (JDK):** Version 21 or higher (with `--enable-preview` and Panama/Foreign Function memory access flags enabled if applicable).
* **Build Tool:** Apache Maven 3.9+
* **Python Environment:** Python 3.10+ (required for invariant tests and benchmark orchestrations).

### 1. Compilation & Test Suite Verification
To compile the enterprise runtime, package artifacts, and execute the complete suite of invariants and regression tests:

```bash
# Clean and compile project sources
mvn clean compile test-compile

# Execute Maven unit and integration tests
mvn test

# Run formal invariant verification harness (Python-based test suite)
python3 tests/test_invariants.py
```

### 2. Executing Performance Benchmarks
To run the high-throughput micro-benchmarks measuring event loop latency, memory allocation overheads, and timer wheel tick efficiency:

```bash
# Run the benchmark engine orchestrator
python3 benchmarks/bench_engine.py --duration=60 --threads=8 --output=results.json
```

---

<div align="center">
  <sub>Maintained by the Net-Pulse Core Engineering Team. Enterprise Grade Infrastructure.</sub>
</div>