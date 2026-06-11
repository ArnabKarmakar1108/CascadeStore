package io.cascadestore.lsm.benchmark.ycsb;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import io.cascadestore.lsm.io.BlockCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * YCSB {@link DB} binding for embedded {@link CascadeStore}.
 *
 * <p>Operations route to one of {@code cascadestore.shards} independent stores under
 * {@code <datadir>/shard-N}, enabling multi-threaded YCSB runs without lock contention on a single
 * LSM tree.
 */
public class CascadeStoreYcsbClient extends DB {

  private static final ThreadLocal<ReadThroughCache> READ_THROUGH_CACHE =
      ThreadLocal.withInitial(ReadThroughCache::new);

  private CascadeStore[] shards;
  private int shardCount;
  private String baseRegistryKey;
  private List<String> shardRegistryKeys;
  private final Map<String, byte[]> tablePrefixes = new HashMap<>();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String dataDir = props.getProperty(CascadeStoreYcsbFactory.PROP_DATADIR, "/tmp/ycsb-cascade-data");
    boolean resetDataDir =
        Boolean.parseBoolean(
            props.getProperty(CascadeStoreYcsbFactory.PROP_RESET_DATADIR, "true"));
    shardCount =
        Integer.parseInt(props.getProperty(CascadeStoreYcsbFactory.PROP_SHARDS, "1"));
    if (shardCount < 1) {
      throw new DBException("cascadestore.shards must be >= 1");
    }

    Path dataPath = Path.of(dataDir);
    int memTableMb =
        Integer.parseInt(props.getProperty(CascadeStoreYcsbFactory.PROP_MEMTABLE_MB, "16"));
    int compactionThreshold =
        Integer.parseInt(
            props.getProperty(CascadeStoreYcsbFactory.PROP_COMPACTION_THRESHOLD, "4"));
    double compactionIntervalMinutes =
        Double.parseDouble(
            props.getProperty(CascadeStoreYcsbFactory.PROP_COMPACTION_INTERVAL_MINUTES, "30"));
    int cleanupIntervalMinutes =
        Integer.parseInt(
            props.getProperty(CascadeStoreYcsbFactory.PROP_CLEANUP_INTERVAL_MINUTES, "1"));
    int flushIntervalSeconds =
        Integer.parseInt(
            props.getProperty(CascadeStoreYcsbFactory.PROP_FLUSH_INTERVAL_SECONDS, "10"));
    CompactionStrategyType strategyType =
        parseCompactionStrategy(
            props.getProperty(
                CascadeStoreYcsbFactory.PROP_COMPACTION_STRATEGY, "THRESHOLD"));

    int blockCacheBytes =
        resolveCacheSizeBytes(
            props,
            CascadeStoreYcsbFactory.PROP_BLOCK_CACHE_MB,
            BlockCache.DEFAULT_SIZE_BYTES,
            shardCount);

    baseRegistryKey =
        buildRegistryKey(
            dataDir,
            shardCount,
            memTableMb,
            compactionThreshold,
            compactionIntervalMinutes,
            cleanupIntervalMinutes,
            flushIntervalSeconds,
            strategyType,
            blockCacheBytes);

    synchronized (SharedCascadeStoreRegistry.class) {
      try {
        if (resetDataDir && !isShardGroupOpen(baseRegistryKey, shardCount)) {
          deleteRecursively(dataPath);
        }
        Files.createDirectories(dataPath);
      } catch (IOException e) {
        throw new DBException("Failed to prepare data directory: " + dataDir, e);
      }

      shards = new CascadeStore[shardCount];
      shardRegistryKeys = new ArrayList<>(shardCount);
      for (int shard = 0; shard < shardCount; shard++) {
        Path shardDir = dataPath.resolve("shard-" + shard);
        String shardRegistryKey = shardRegistryKey(baseRegistryKey, shard);
        CascadeConfig config =
            new CascadeConfig(
                memTableMb * 1024 * 1024,
                shardDir.toString(),
                compactionThreshold,
                compactionIntervalMinutes,
                cleanupIntervalMinutes,
                flushIntervalSeconds,
                strategyType,
                blockCacheBytes,
                true,
                3);
        shards[shard] =
            SharedCascadeStoreRegistry.acquire(shardRegistryKey, () -> new CascadeStore(config));
        shardRegistryKeys.add(shardRegistryKey);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (shardRegistryKeys != null) {
      for (String shardRegistryKey : shardRegistryKeys) {
        SharedCascadeStoreRegistry.release(shardRegistryKey);
      }
      shardRegistryKeys = null;
      shards = null;
      baseRegistryKey = null;
    }
  }

  @Override
  public Status read(
      String table,
      String key,
      Set<String> fields,
      Map<String, ByteIterator> result) {
    byte[] storageKey = toStorageKey(table, key);
    byte[] record = shardFor(storageKey).get(storageKey);
    if (record == null) {
      READ_THROUGH_CACHE.get().clear();
      return Status.NOT_FOUND;
    }
    READ_THROUGH_CACHE.get().remember(storageKey, record);
    YcsbRecordCodec.decodeInto(record, fields, result);
    return Status.OK;
  }

  @Override
  public Status scan(
      String table,
      String startKey,
      int recordCount,
      Set<String> fields,
      java.util.Vector<HashMap<String, ByteIterator>> result) {
    byte[] scanStart = toStorageKey(table, startKey);
    byte[] scanEnd = YcsbRecordCodec.scanEndKey(table);
    String prefix = table + ":";

    PriorityQueue<ShardScanCursor> cursors =
        new PriorityQueue<>(Comparator.comparing(cursor -> cursor.nextKey));

    for (int shard = 0; shard < shardCount; shard++) {
      KeyValueIterator iterator = shards[shard].getIterator(scanStart, scanEnd);
      ShardScanCursor cursor = ShardScanCursor.advance(shard, iterator, prefix);
      if (cursor != null) {
        cursors.add(cursor);
      } else {
        iterator.close();
      }
    }

    while (!cursors.isEmpty() && result.size() < recordCount) {
      ShardScanCursor cursor = cursors.poll();
      HashMap<String, ByteIterator> record = new HashMap<>();
      YcsbRecordCodec.decodeInto(cursor.nextValue, fields, record);
      result.add(record);

      ShardScanCursor next = cursor.advance(prefix);
      if (next != null) {
        cursors.add(next);
      } else {
        cursor.iterator.close();
      }
    }

    while (!cursors.isEmpty()) {
      cursors.poll().iterator.close();
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = toStorageKey(table, key);
    CascadeStore shard = shardFor(storageKey);
    ReadThroughCache cache = READ_THROUGH_CACHE.get();
    byte[] cachedRecord = cache.takeIfMatches(storageKey);
    boolean ok;
    if (cachedRecord != null) {
      ok =
          shard.merge(
              storageKey,
              cachedRecord,
              existing -> YcsbRecordCodec.merge(existing, values));
    } else {
      ok = shard.merge(storageKey, existing -> YcsbRecordCodec.merge(existing, values));
    }
    return ok ? Status.OK : Status.NOT_FOUND;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = toStorageKey(table, key);
    byte[] encoded = YcsbRecordCodec.encode(values);
    return shardFor(storageKey).put(storageKey, encoded) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    byte[] storageKey = toStorageKey(table, key);
    return shardFor(storageKey).delete(storageKey) ? Status.OK : Status.NOT_FOUND;
  }

  /** Clears reference-counted stores; for unit tests only. */
  public static void resetSharedStoresForTests() {
    SharedCascadeStoreRegistry.resetForTests();
  }

  static String buildRegistryKey(
      String dataDir,
      int shardCount,
      int memTableMb,
      int compactionThreshold,
      double compactionIntervalMinutes,
      int cleanupIntervalMinutes,
      int flushIntervalSeconds,
      CompactionStrategyType strategyType,
      int blockCacheBytes) {
    return String.join(
        "|",
        dataDir,
        Integer.toString(shardCount),
        strategyType.name(),
        Integer.toString(memTableMb),
        Integer.toString(compactionThreshold),
        Double.toString(compactionIntervalMinutes),
        Integer.toString(cleanupIntervalMinutes),
        Integer.toString(flushIntervalSeconds),
        Integer.toString(blockCacheBytes));
  }

  /**
   * Resolves per-shard cache bytes. Explicit property wins. Otherwise single-shard runs use full
   * defaults; multi-shard runs divide defaults by shard count so the JVM does not hold N× cache
   * budget (which caused heavy GC at 250k).
   */
  static int resolveCacheSizeBytes(
      Properties props, String propertyKey, int singleShardDefault, int shardCount) {
    String explicit = props.getProperty(propertyKey);
    if (explicit != null && !explicit.isBlank()) {
      int megabytes = Integer.parseInt(explicit.trim());
      return megabytes <= 0 ? 0 : megabytes * 1024 * 1024;
    }
    if (shardCount <= 1) {
      return singleShardDefault;
    }
    int scaled = singleShardDefault / shardCount;
    int floor = 8 * 1024 * 1024;
    return Math.max(floor, scaled);
  }

  private byte[] toStorageKey(String table, String userKey) {
    return YcsbRecordCodec.storageKey(
        tablePrefixes.computeIfAbsent(table, YcsbRecordCodec::tablePrefix), userKey);
  }

  private CascadeStore shardFor(byte[] storageKey) {
    return shards[CascadeStoreShardRouter.shardIndex(storageKey, shardCount)];
  }

  private static String shardRegistryKey(String baseRegistryKey, int shard) {
    return baseRegistryKey + "|shard-" + shard;
  }

  private static boolean isShardGroupOpen(String baseRegistryKey, int shardCount) {
    return SharedCascadeStoreRegistry.isOpen(shardRegistryKey(baseRegistryKey, 0));
  }

  private static CompactionStrategyType parseCompactionStrategy(String value) throws DBException {
    try {
      return CompactionStrategyType.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new DBException("Unknown cascadestore.compaction.strategy: " + value, e);
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(CascadeStoreYcsbClient::deleteQuietly);
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // Best effort cleanup for benchmark temp dirs.
    }
  }

  /**
   * Holds the raw value from the most recent {@link #read} on this thread so a following
   * {@link #update} (YCSB read-modify-write) can merge without a second LSM lookup.
   */
  private static final class ReadThroughCache {
    private byte[] key;
    private byte[] value;

    private void remember(byte[] storageKey, byte[] record) {
      this.key = storageKey;
      this.value = record;
    }

    private byte[] takeIfMatches(byte[] storageKey) {
      if (key != null && value != null && Arrays.equals(key, storageKey)) {
        byte[] cached = value;
        clear();
        return cached;
      }
      clear();
      return null;
    }

    private void clear() {
      key = null;
      value = null;
    }
  }

  private static final class ShardScanCursor {
    private final int shard;
    private final KeyValueIterator iterator;
    private final ByteArrayWrapper nextKey;
    private final byte[] nextValue;

    private ShardScanCursor(
        int shard, KeyValueIterator iterator, ByteArrayWrapper nextKey, byte[] nextValue) {
      this.shard = shard;
      this.iterator = iterator;
      this.nextKey = nextKey;
      this.nextValue = nextValue;
    }

    private static ShardScanCursor advance(int shard, KeyValueIterator iterator, String prefix) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        String storageKey = new String(entry.getKey(), StandardCharsets.UTF_8);
        if (!storageKey.startsWith(prefix)) {
          continue;
        }
        return new ShardScanCursor(
            shard, iterator, new ByteArrayWrapper(entry.getKey()), entry.getValue());
      }
      return null;
    }

    private ShardScanCursor advance(String prefix) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        String storageKey = new String(entry.getKey(), StandardCharsets.UTF_8);
        if (!storageKey.startsWith(prefix)) {
          continue;
        }
        return new ShardScanCursor(
            shard, iterator, new ByteArrayWrapper(entry.getKey()), entry.getValue());
      }
      return null;
    }
  }
}
