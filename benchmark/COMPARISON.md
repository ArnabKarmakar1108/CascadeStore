# CascadeStore vs RocksDB — YCSB Comparison

Living log for external engine comparison. CascadeStore is an embedded Java LSM; RocksDB is the native C++ reference ceiling.

## Comparison profile (compaction stress)

| Setting | Value | Notes |
|---------|-------|-------|
| Scale | **250,000** records/ops | Minimum comparison scale |
| Threads | 1 | Embedded binding; avoids false multi-thread contention |
| Shards | 1 | One engine instance per process |
| MemTable / write buffer | **64 MB** | More L0 churn than 256 MB baseline |
| L0 compaction trigger | **2** | Exercises compaction during load/run |
| Flush interval (CascadeStore) | **5 s** | |
| Compaction interval (CascadeStore) | **~10 s** (`0.17` min) | |
| Block cache | **128 MB** | Per engine instance |
| JVM | `-Xms2G -Xmx4G`, G1 | |
| Trials | **1** (trial 1) | Cold datadir per workload cell |
| Workloads | **A, B, C, F** | All four |

RocksDB uses leveled compaction with Snappy compression (default). CascadeStore default strategy for comparison runs is **LEVEL_TIERED**.

## Run commands

**CascadeStore alone** (run once before side-by-side analysis):

```bash
./scripts/run-comparison-cascade-stress.sh
```

**RocksDB alone:**

```bash
./scripts/run-comparison-rocksdb-stress.sh
```

**Full suite (CascadeStore then RocksDB):**

```bash
./scripts/run-comparison-suite.sh
```

Override scale:

```bash
COMPARISON_SCALE=250000 ./scripts/run-comparison-suite.sh
```

Results land in `benchmark/results/comparison/`.

Summarize a result file:

```bash
./scripts/collect-ycsb-metrics.sh benchmark/results/comparison/workloada-LEVEL_TIERED-run-*.txt
./scripts/collect-ycsb-metrics.sh benchmark/results/comparison/rocksdb-workloada-run-*.txt
```

## What to report

| Metric | Why |
|--------|-----|
| Load throughput (ops/s) | Write + flush + compaction pressure |
| Run throughput (ops/s) | Steady-state mixed/read workload |
| Read / Update / RMW p99 (µs) | Tail latency, not just averages |
| Errors | Must be 0 |
| Disk footprint | Optional; note compression difference |

Record environment: CPU model, RAM, disk type, JDK version, `rocksdbjni` version, git commit.

## Fairness notes

- **Key/value encoding** is identical (`YcsbRecordCodec`) in both bindings.
- **CascadeStore `update()`** uses native `merge()` (one LSM walk). **RocksDB `update()`** uses get + put.
- **Workload F** (RMW) always does `read()` + `update()` in YCSB — two walks on both engines for the RMW half.
- RocksDB is native C++ with mature compaction tuning; expect higher throughput on read-heavy Workload C.
- Do not claim parity with production-tuned RocksDB — document trade-offs.

## Results table

**Run:** 2026-07-31 — `benchmark/results/comparison-20260801/` (post CleanupService/compaction fixes). All cells completed with 0 errors.

### Workload A (50/50 read/update, zipfian) @ 250k

| Engine | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|--------|-------------|-------------|---------------|-----------------|
| CascadeStore (LTCS) | 1,611 | 2,096 | 3,697 | 3,797 |
| RocksDB | 4,877 | 2,505 | 293 | 827 |

### Workload B (95/5 read/update, zipfian) @ 250k

| Engine | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|--------|-------------|-------------|---------------|-----------------|
| CascadeStore (LTCS) | 2,166 | 3,230 | 2,461 | 3,105 |
| RocksDB | 6,720 | 2,765 | 628 | 4,999 |

### Workload C (100% read, latest) @ 250k

| Engine | Load (ops/s) | Run (ops/s) | Read p99 (µs) |
|--------|-------------|-------------|---------------|
| CascadeStore (LTCS) | 2,052 | 3,031 | 3,193 |
| RocksDB | 5,453 | 9,126 | 146 |

### Workload F (50/50 read/RMW, zipfian) @ 250k

| Engine | Load (ops/s) | Run (ops/s) | Read p99 (µs) | RMW p99 (µs) |
|--------|-------------|-------------|---------------|--------------|
| CascadeStore (LTCS) | 4,069 | 927 | 7,051 | 73,663 |
| RocksDB | 7,083 | 4,079 | 196 | 387 |
