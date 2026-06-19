package io.cascadestore.lsm.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Prometheus instrumentation for CascadeStore. Use {@link #noop()} when metrics are disabled. */
public final class CascadeMetrics {

  private static final double[] LATENCY_BUCKETS_SECONDS = {
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0, 2.5, 5.0, 10.0
  };

  public static final CascadeMetrics NOOP = new CascadeMetrics(false, null);

  private final boolean enabled;
  private final CollectorRegistry registry;

  private final Gauge memtableBytes;
  private final Gauge memtableEntries;
  private final Gauge immutableMemtablesPending;
  private final Gauge sstableCount;
  private final Gauge compactionPending;
  private final Gauge compactionInProgress;
  private final Gauge blockCacheBytes;
  private final Gauge blockCacheEntries;

  private final Counter flushTotal;
  private final Counter compactionTotal;
  private final Counter readOperationsTotal;
  private final Counter sstableLookupsTotal;
  private final Counter bloomProbesTotal;
  private final Counter bloomNegativeTotal;
  private final Counter blockCacheHitsTotal;
  private final Counter blockCacheMissesTotal;
  private final Counter userWriteBytesTotal;
  private final Counter walBytesWrittenTotal;
  private final Counter sstableBytesWrittenTotal;

  private final Histogram flushDurationSeconds;
  private final Histogram compactionDurationSeconds;
  private final Histogram walFsyncDurationSeconds;

  private CascadeMetrics(boolean enabled, CollectorRegistry registry) {
    this.enabled = enabled;
    this.registry = registry;

    if (!enabled) {
      memtableBytes = null;
      memtableEntries = null;
      immutableMemtablesPending = null;
      sstableCount = null;
      compactionPending = null;
      compactionInProgress = null;
      blockCacheBytes = null;
      blockCacheEntries = null;
      flushTotal = null;
      compactionTotal = null;
      readOperationsTotal = null;
      sstableLookupsTotal = null;
      bloomProbesTotal = null;
      bloomNegativeTotal = null;
      blockCacheHitsTotal = null;
      blockCacheMissesTotal = null;
      userWriteBytesTotal = null;
      walBytesWrittenTotal = null;
      sstableBytesWrittenTotal = null;
      flushDurationSeconds = null;
      compactionDurationSeconds = null;
      walFsyncDurationSeconds = null;
      return;
    }

    memtableBytes =
        Gauge.build()
            .name("cascadestore_memtable_bytes")
            .help("Total bytes in active and immutable MemTables")
            .register(registry);
    memtableEntries =
        Gauge.build()
            .name("cascadestore_memtable_entries")
            .help("Total entries in active and immutable MemTables")
            .register(registry);
    immutableMemtablesPending =
        Gauge.build()
            .name("cascadestore_immutable_memtables_pending")
            .help("Number of immutable MemTables waiting to flush")
            .register(registry);
    sstableCount =
        Gauge.build()
            .name("cascadestore_sstable_count")
            .help("Number of live SSTables by level")
            .labelNames("level")
            .register(registry);
    compactionPending =
        Gauge.build()
            .name("cascadestore_compaction_pending")
            .help("1 when SSTable count is at or above compaction threshold, else 0")
            .register(registry);
    compactionInProgress =
        Gauge.build()
            .name("cascadestore_compaction_in_progress")
            .help("1 while a compaction job is running, else 0")
            .register(registry);
    blockCacheBytes =
        Gauge.build()
            .name("cascadestore_block_cache_bytes")
            .help("Current block cache occupancy in bytes")
            .register(registry);
    blockCacheEntries =
        Gauge.build()
            .name("cascadestore_block_cache_entries")
            .help("Current number of blocks in the block cache")
            .register(registry);

    flushTotal =
        Counter.build()
            .name("cascadestore_flush_total")
            .help("Total MemTable flushes completed")
            .register(registry);
    compactionTotal =
        Counter.build()
            .name("cascadestore_compaction_total")
            .help("Total compactions completed")
            .register(registry);
    readOperationsTotal =
        Counter.build()
            .name("cascadestore_read_operations_total")
            .help("Total point read operations")
            .register(registry);
    sstableLookupsTotal =
        Counter.build()
            .name("cascadestore_sstable_lookups_total")
            .help("SSTable get/contains probes during reads (read amplification numerator)")
            .register(registry);
    bloomProbesTotal =
        Counter.build()
            .name("cascadestore_bloom_probes_total")
            .help("Bloom filter probes during reads")
            .register(registry);
    bloomNegativeTotal =
        Counter.build()
            .name("cascadestore_bloom_negative_total")
            .help("Bloom filter negatives that skipped an SSTable lookup")
            .register(registry);
    blockCacheHitsTotal =
        Counter.build()
            .name("cascadestore_block_cache_hits_total")
            .help("Block cache hits")
            .register(registry);
    blockCacheMissesTotal =
        Counter.build()
            .name("cascadestore_block_cache_misses_total")
            .help("Block cache misses")
            .register(registry);
    userWriteBytesTotal =
        Counter.build()
            .name("cascadestore_user_write_bytes_total")
            .help("User payload bytes written (keys + values)")
            .register(registry);
    walBytesWrittenTotal =
        Counter.build()
            .name("cascadestore_wal_bytes_written_total")
            .help("Bytes appended to the WAL")
            .register(registry);
    sstableBytesWrittenTotal =
        Counter.build()
            .name("cascadestore_sstable_bytes_written_total")
            .help("Bytes written to SSTable data files during flush and compaction")
            .register(registry);

    flushDurationSeconds =
        Histogram.build()
            .name("cascadestore_flush_duration_seconds")
            .help("MemTable flush latency")
            .buckets(LATENCY_BUCKETS_SECONDS)
            .register(registry);
    compactionDurationSeconds =
        Histogram.build()
            .name("cascadestore_compaction_duration_seconds")
            .help("Compaction latency")
            .buckets(LATENCY_BUCKETS_SECONDS)
            .register(registry);
    walFsyncDurationSeconds =
        Histogram.build()
            .name("cascadestore_wal_fsync_duration_seconds")
            .help("WAL fsync latency")
            .buckets(LATENCY_BUCKETS_SECONDS)
            .register(registry);
  }

  public static CascadeMetrics noop() {
    return NOOP;
  }

  public static CascadeMetrics create() {
    return new CascadeMetrics(true, new CollectorRegistry());
  }

  public CollectorRegistry registry() {
    return registry;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setMemtableState(long bytes, long entries, int immutablePending) {
    if (!enabled) {
      return;
    }
    memtableBytes.set(bytes);
    memtableEntries.set(entries);
    immutableMemtablesPending.set(immutablePending);
  }

  public void setSstableCountByLevel(Map<Integer, Integer> countsByLevel) {
    if (!enabled) {
      return;
    }
    for (Map.Entry<Integer, Integer> entry : countsByLevel.entrySet()) {
      sstableCount.labels(String.valueOf(entry.getKey())).set(entry.getValue());
    }
  }

  public void setCompactionPending(boolean pending) {
    if (!enabled) {
      return;
    }
    compactionPending.set(pending ? 1 : 0);
  }

  public void setCompactionInProgress(boolean inProgress) {
    if (!enabled) {
      return;
    }
    compactionInProgress.set(inProgress ? 1 : 0);
  }

  public void setBlockCacheState(int bytes, int entries) {
    if (!enabled) {
      return;
    }
    blockCacheBytes.set(bytes);
    blockCacheEntries.set(entries);
  }

  public void recordFlush(long durationNanos, long bytesWritten) {
    if (!enabled) {
      return;
    }
    flushTotal.inc();
    flushDurationSeconds.observe(nanosToSeconds(durationNanos));
    if (bytesWritten > 0) {
      sstableBytesWrittenTotal.inc(bytesWritten);
    }
  }

  public void recordCompaction(long durationNanos, long bytesWritten) {
    if (!enabled) {
      return;
    }
    compactionTotal.inc();
    compactionDurationSeconds.observe(nanosToSeconds(durationNanos));
    if (bytesWritten > 0) {
      sstableBytesWrittenTotal.inc(bytesWritten);
    }
  }

  public void recordRead(int sstableLookups, int bloomProbes, int bloomNegatives) {
    if (!enabled) {
      return;
    }
    readOperationsTotal.inc();
    if (sstableLookups > 0) {
      sstableLookupsTotal.inc(sstableLookups);
    }
    if (bloomProbes > 0) {
      bloomProbesTotal.inc(bloomProbes);
    }
    if (bloomNegatives > 0) {
      bloomNegativeTotal.inc(bloomNegatives);
    }
  }

  public void recordBlockCacheHit() {
    if (!enabled) {
      return;
    }
    blockCacheHitsTotal.inc();
  }

  public void recordBlockCacheMiss() {
    if (!enabled) {
      return;
    }
    blockCacheMissesTotal.inc();
  }

  public void recordUserWriteBytes(long bytes) {
    if (!enabled || bytes <= 0) {
      return;
    }
    userWriteBytesTotal.inc(bytes);
  }

  public void recordWalBytesWritten(long bytes) {
    if (!enabled || bytes <= 0) {
      return;
    }
    walBytesWrittenTotal.inc(bytes);
  }

  public void recordWalFsync(long durationNanos) {
    if (!enabled) {
      return;
    }
    walFsyncDurationSeconds.observe(nanosToSeconds(durationNanos));
  }

  public AmplificationSnapshot snapshot(long liveSstableDataBytes, long liveSstableCount) {
    if (!enabled) {
      return AmplificationSnapshot.EMPTY;
    }
    return new AmplificationSnapshot(
        (long) readOperationsTotal.get(),
        (long) sstableLookupsTotal.get(),
        (long) bloomProbesTotal.get(),
        (long) bloomNegativeTotal.get(),
        (long) userWriteBytesTotal.get(),
        (long) sstableBytesWrittenTotal.get(),
        (long) compactionTotal.get(),
        liveSstableDataBytes,
        liveSstableCount);
  }

  private static double nanosToSeconds(long nanos) {
    return nanos / (double) TimeUnit.SECONDS.toNanos(1);
  }
}
