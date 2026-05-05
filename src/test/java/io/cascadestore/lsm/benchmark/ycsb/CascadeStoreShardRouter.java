package io.cascadestore.lsm.benchmark.ycsb;

import java.util.Arrays;

/** Routes YCSB storage keys to a fixed number of independent CascadeStore shards. */
final class CascadeStoreShardRouter {

  private CascadeStoreShardRouter() {}

  static int shardIndex(byte[] storageKey, int shardCount) {
    if (shardCount <= 1) {
      return 0;
    }
    return (Arrays.hashCode(storageKey) & 0x7FFFFFFF) % shardCount;
  }
}
