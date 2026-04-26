package io.cascadestore.lsm.benchmark.ycsb;

import java.util.Properties;

/**
 * Helpers for wiring CascadeStore into YCSB.
 *
 * <p>YCSB 0.17 loads bindings by fully qualified class name ({@link #DB_CLASS_NAME}), not via a
 * pluggable {@code DBFactory} subclass.
 */
public final class CascadeStoreYcsbFactory {

  public static final String DB_CLASS_NAME =
      "io.cascadestore.lsm.benchmark.ycsb.CascadeStoreYcsbClient";

  public static final String PROP_DATADIR = "cascadestore.datadir";
  public static final String PROP_RESET_DATADIR = "cascadestore.reset.datadir";
  public static final String PROP_MEMTABLE_MB = "cascadestore.memtable.mb";
  public static final String PROP_COMPACTION_STRATEGY = "cascadestore.compaction.strategy";
  public static final String PROP_COMPACTION_THRESHOLD = "cascadestore.compaction.threshold";
  public static final String PROP_COMPACTION_INTERVAL_MINUTES =
      "cascadestore.compaction.interval.minutes";
  public static final String PROP_FLUSH_INTERVAL_SECONDS = "cascadestore.flush.interval.seconds";
  public static final String PROP_CLEANUP_INTERVAL_MINUTES = "cascadestore.cleanup.interval.minutes";

  private CascadeStoreYcsbFactory() {}

  /** Example properties matching {@code YCSB_BENCHMARK_PLAN.md}. */
  public static Properties exampleProperties() {
    Properties properties = new Properties();
    properties.setProperty(PROP_DATADIR, "/tmp/ycsb-cascade-data");
    properties.setProperty(PROP_RESET_DATADIR, "true");
    properties.setProperty(PROP_MEMTABLE_MB, "16");
    properties.setProperty(PROP_COMPACTION_STRATEGY, "LEVEL_TIERED");
    properties.setProperty(PROP_COMPACTION_THRESHOLD, "4");
    properties.setProperty(PROP_COMPACTION_INTERVAL_MINUTES, "30");
    properties.setProperty(PROP_FLUSH_INTERVAL_SECONDS, "10");
    properties.setProperty(PROP_CLEANUP_INTERVAL_MINUTES, "1");
    return properties;
  }
}
