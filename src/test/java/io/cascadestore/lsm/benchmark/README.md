# CascadeStore Benchmarks

Performance harnesses for CascadeStore live in `src/test/java/io/cascadestore/lsm/benchmark/`. Two approaches complement each other: **JMH** for isolated micro-ops and **YCSB** for end-to-end workload comparison.

## JMH Microbenchmarks

`CascadeBenchmark` measures average latency (microseconds per op) for individual engine operations:

| Benchmark | Operation |
|-----------|-----------|
| `benchmarkPut` | Single put |
| `benchmarkGet` | Point read |
| `benchmarkDelete` | Tombstone delete |
| `benchmarkGetRange` | Range map fetch |
| `benchmarkGetIterator` | Iterator over a key range |
| `benchmarkSequentialPut` | Back-to-back puts |
| `benchmarkPutWithTTL` | Put with expiration |

Default JMH settings: 3×1 s warmup, 5×1 s measurement, single fork with 2 GiB heap, 10k keys (16-byte keys, 100-byte values).

### Run JMH

```bash
mvn clean package
java -jar target/benchmarks.jar
```

Or run `CascadeBenchmarkRunner.main()` from your IDE.

Tune `DATA_SIZE`, `KEY_SIZE`, and `VALUE_SIZE` in `CascadeBenchmark`, or adjust `@Warmup` / `@Measurement` / `@Fork` annotations.

## YCSB Macrobenchmarks

YCSB integration is under `benchmark/ycsb/`:

| Class | Role |
|-------|------|
| `CascadeStoreYcsbClient` | YCSB `DB` binding |
| `CascadeStoreYcsbFactory` | Client factory and property parsing |
| `CascadeStoreShardRouter` | Optional multi-shard routing |
| `SharedCascadeStoreRegistry` | Process-wide store instances |
| `YcsbRecordCodec` | Efficient field patch for read-modify-write workloads |

Properties file: `src/test/resources/ycsb/cascadestore.properties`  
Custom workloads: `src/test/resources/ycsb/workloads/`

### Run YCSB

Helper scripts in `scripts/` set up the YCSB checkout, classpath, and JVM flags:

```bash
# Smoke test (1k records)
./scripts/run-ycsb.sh all workloada-dryrun LEVEL_TIERED

# Single workload @ custom scale
THREADS=1 MEMTABLE_MB=256 RECORDCOUNT=100000 OPERATIONCOUNT=100000 \
  ./scripts/run-ycsb-matrix.sh workloada 100000 100000

# Strategy comparison (read / write / space amplification)
./scripts/run-strategy-comparison.sh read    # or write | space | all

# Engine comparison @ 250k
COMPARISON_SCALE=250000 ./scripts/run-comparison-suite.sh

# Scaling matrix (scale × shards × threads)
./scripts/run-scaling-matrix.sh
```

Common environment variables (see `scripts/ycsb-env.sh` and `scripts/run-ycsb.sh`):

| Variable | Effect |
|----------|--------|
| `THREADS` | YCSB client threads (default 1; embedded binding is single-store) |
| `SHARDS` | Number of independent `CascadeStore` instances |
| `MEMTABLE_MB` | MemTable size per shard |
| `COMPACTION_THRESHOLD` | SSTable count trigger |
| `BLOCK_CACHE_MB` | Per-shard block cache; `0` disables |
| `SSTABLE_LZ4_ENABLED` | LZ4 compression on SSTable data blocks |
| `METRICS_ENABLED` | Prometheus `/metrics` and browser dashboard |
| `COMPACTION_INTERVAL_MINUTES` | Background compaction cadence |
| `TRIALS` / `WARMUP_SECONDS` | Matrix repeat count and per-trial JVM warmup |

Raw output lands in `benchmark/results/`. Summarize a run:

```bash
./scripts/collect-ycsb-metrics.sh benchmark/results/workloada-THRESHOLD-run-*.txt
```

Recorded results and comparison tables: [benchmark/README.md](../../../../../../../benchmark/README.md).

## Interpreting Results

- **JMH** — lower `Score` (µs/op) is better; compare within the same fork and data size
- **YCSB** — `[OVERALL] Throughput(ops/sec)` is the headline metric; check `[INSERT]` / `[READ]` / `[UPDATE]` sections for phase breakdown
- **Fair comparisons** — match MemTable size, compaction strategy, shard/thread count, block cache setting, and JVM heap across runs; note that repeated matrix trials warm the OS page cache

## Tests

`src/test/java/io/cascadestore/lsm/ycsb/CascadeStoreYcsbClientTest.java` validates the YCSB binding without running a full workload.
