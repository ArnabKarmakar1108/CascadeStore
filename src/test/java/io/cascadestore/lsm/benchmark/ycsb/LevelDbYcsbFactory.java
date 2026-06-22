package io.cascadestore.lsm.benchmark.ycsb;

import java.util.Properties;

/** Property keys for the embedded LevelDB YCSB binding. */
public final class LevelDbYcsbFactory {

  public static final String DB_CLASS_NAME =
      "io.cascadestore.lsm.benchmark.ycsb.LevelDbShardedYcsbClient";

  public static final String PROP_DATADIR = "leveldb.datadir";
  public static final String PROP_RESET_DATADIR = "leveldb.reset.datadir";
  public static final String PROP_MEMTABLE_MB = "leveldb.memtable.mb";
  public static final String PROP_BLOCK_CACHE_MB = "leveldb.block.cache.mb";
  public static final String PROP_SHARDS = "leveldb.shards";

  private LevelDbYcsbFactory() {}

  public static Properties exampleProperties() {
    Properties properties = new Properties();
    properties.setProperty(PROP_DATADIR, "/tmp/ycsb-leveldb-data");
    properties.setProperty(PROP_RESET_DATADIR, "true");
    properties.setProperty(PROP_MEMTABLE_MB, "64");
    properties.setProperty(PROP_BLOCK_CACHE_MB, "128");
    properties.setProperty(PROP_SHARDS, "1");
    return properties;
  }
}
