package io.cascadestore.lsm.benchmark.ycsb;

import java.util.Properties;

/** Property keys for the embedded RocksDB YCSB binding. */
public final class RocksDbYcsbFactory {

  public static final String DB_CLASS_NAME =
      "io.cascadestore.lsm.benchmark.ycsb.RocksDbShardedYcsbClient";

  public static final String PROP_DATADIR = "rocksdb.datadir";
  public static final String PROP_RESET_DATADIR = "rocksdb.reset.datadir";
  public static final String PROP_MEMTABLE_MB = "rocksdb.memtable.mb";
  public static final String PROP_COMPACTION_THRESHOLD = "rocksdb.compaction.threshold";
  public static final String PROP_BLOCK_CACHE_MB = "rocksdb.block.cache.mb";
  public static final String PROP_SHARDS = "rocksdb.shards";
  public static final String PROP_MAX_BACKGROUND_JOBS = "rocksdb.max.background.jobs";

  private RocksDbYcsbFactory() {}

  public static Properties exampleProperties() {
    Properties properties = new Properties();
    properties.setProperty(PROP_DATADIR, "/tmp/ycsb-rocksdb-data");
    properties.setProperty(PROP_RESET_DATADIR, "true");
    properties.setProperty(PROP_MEMTABLE_MB, "64");
    properties.setProperty(PROP_COMPACTION_THRESHOLD, "2");
    properties.setProperty(PROP_BLOCK_CACHE_MB, "128");
    properties.setProperty(PROP_SHARDS, "1");
    properties.setProperty(PROP_MAX_BACKGROUND_JOBS, "4");
    return properties;
  }
}
