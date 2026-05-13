# CascadeStore Prometheus Metrics

Observability for the embedded LSM engine via a Prometheus scrape endpoint.

## Quick start

```java
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;

CascadeConfig config =
    new CascadeConfig(
            4 * 1024 * 1024,
            "./data",
            4,
            30,
            1,
            5,
            CompactionStrategyType.THRESHOLD)
        .withMetricsEnabled(true)
        .withMetricsPort(9090);

CascadeStore store = new CascadeStore(config);
```

Scrape (Prometheus / machines):

```bash
curl -s http://localhost:9090/metrics | grep cascadestore_
```

**Browser dashboard (humans):** open [http://localhost:9090/](http://localhost:9090/) — grouped cards, auto-refresh every 2s.

`/metrics` is intentionally plain text (Prometheus scrape format). Use `/` for a readable UI.

### Demo workload (keeps running)

```bash
./scripts/run-metrics-demo.sh
```

In another terminal:

```bash
curl -s http://localhost:9090/metrics | grep cascadestore_
```

Stop the demo with `Ctrl+C`.

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `metricsEnabled` | `false` | Start Prometheus HTTP server and record metrics |
| `metricsPort` | `9090` | TCP port for `/metrics` |

Use `CascadeConfig.withMetricsEnabled(true)` and `withMetricsPort(port)`.

Metrics are **disabled by default** so benchmarks and unit tests are unaffected.

## Architecture

```
CascadeStore
  ├── CascadeMetrics          (collectors + dedicated CollectorRegistry)
  ├── PrometheusHttpServer
  │     ├── GET /             (HTML dashboard)
  │     └── GET /metrics      (Prometheus scrape text)
  ├── PutStore / GetStore     (writes, reads, bloom, amplification)
  ├── FlushService            (flush latency + SSTable bytes)
  ├── CompactionService       (compaction latency + pending/in-progress)
  ├── BlockCache              (hit/miss + occupancy gauges)
  └── WALManagerImpl          (append bytes + fsync latency)
```

Gauges for MemTable size, SSTable counts, and block cache are refreshed whenever the storage layout is published (flush, compaction, startup).

## Metric reference

### Gauges (current state)

| Name | Description |
|------|-------------|
| `cascadestore_memtable_bytes` | Bytes in active + immutable MemTables |
| `cascadestore_memtable_entries` | Entry count in active + immutable MemTables |
| `cascadestore_immutable_memtables_pending` | Immutable MemTables waiting to flush |
| `cascadestore_sstable_count{level="N"}` | Live SSTables per level |
| `cascadestore_compaction_pending` | `1` when SSTable count ≥ compaction threshold |
| `cascadestore_compaction_in_progress` | `1` while compaction is running |
| `cascadestore_block_cache_bytes` | Block cache occupancy |
| `cascadestore_block_cache_entries` | Blocks in cache |

### Counters (monotonic)

| Name | Description |
|------|-------------|
| `cascadestore_flush_total` | Completed MemTable flushes |
| `cascadestore_compaction_total` | Completed compactions |
| `cascadestore_read_operations_total` | Point reads (`get` / `lookup`) |
| `cascadestore_sstable_lookups_total` | SSTable probes per read (amplification numerator) |
| `cascadestore_bloom_probes_total` | Bloom filter evaluations per read |
| `cascadestore_bloom_negative_total` | Bloom negatives (skipped SSTable lookups) |
| `cascadestore_block_cache_hits_total` | Block cache hits |
| `cascadestore_block_cache_misses_total` | Block cache misses |
| `cascadestore_user_write_bytes_total` | User key + value bytes written |
| `cascadestore_wal_bytes_written_total` | Bytes appended to WAL |
| `cascadestore_sstable_bytes_written_total` | Bytes written to SSTables (flush + compaction) |

### Histograms

| Name | Description |
|------|-------------|
| `cascadestore_flush_duration_seconds` | MemTable flush latency |
| `cascadestore_compaction_duration_seconds` | Compaction latency |
| `cascadestore_wal_fsync_duration_seconds` | WAL `fsync` latency |

## Derived ratios (Grafana / PromQL)

**Read amplification** (avg SSTable probes per read):

```promql
rate(cascadestore_sstable_lookups_total[5m])
/
rate(cascadestore_read_operations_total[5m])
```

**Write amplification** (bytes written to SSTables per user byte):

```promql
rate(cascadestore_sstable_bytes_written_total[5m])
/
rate(cascadestore_user_write_bytes_total[5m])
```

**Bloom filter hit rate** (negatives / probes):

```promql
rate(cascadestore_bloom_negative_total[5m])
/
rate(cascadestore_bloom_probes_total[5m])
```

**Block cache hit rate**:

```promql
rate(cascadestore_block_cache_hits_total[5m])
/
(rate(cascadestore_block_cache_hits_total[5m]) + rate(cascadestore_block_cache_misses_total[5m]))
```

## Prometheus scrape config

```yaml
scrape_configs:
  - job_name: cascadestore
    static_configs:
      - targets: ["localhost:9090"]
```

## Notes

- Each enabled `CascadeStore` instance uses its own `CollectorRegistry`; only metrics from that store appear on its endpoint.
- `exists()` checks do not increment `read_operations_total` (only value reads do).
- JVM hotspot metrics (`jvm_*`) are also exported when the HTTP server starts (`DefaultExports`).
- Call `store.shutdown()` to stop the metrics server and background services cleanly.

## Source files

| File | Role |
|------|------|
| `CascadeMetrics.java` | Metric definitions and recording API |
| `PrometheusHttpServer.java` | HTTP `/metrics` endpoint |
| `MetricsDemo.java` | Runnable demo workload for local inspection |
