package io.cascadestore.lsm.benchmark.ycsb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * YCSB {@link DB} binding for embedded RocksDB.
 *
 * <p>Keys and values use the same {@link YcsbRecordCodec} layout as {@link CascadeStoreYcsbClient}
 * so workloads are comparable byte-for-byte.
 */
public class RocksDbShardedYcsbClient extends DB {

  static {
    RocksDB.loadLibrary();
  }

  private RocksDB[] shards;
  private final List<Options> shardOptions = new ArrayList<>();
  private final List<LRUCache> blockCaches = new ArrayList<>();
  private int shardCount;
  private final Map<String, byte[]> tablePrefixes = new HashMap<>();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String dataDir = props.getProperty(RocksDbYcsbFactory.PROP_DATADIR, "/tmp/ycsb-rocksdb-data");
    boolean resetDataDir =
        Boolean.parseBoolean(props.getProperty(RocksDbYcsbFactory.PROP_RESET_DATADIR, "true"));
    shardCount = Integer.parseInt(props.getProperty(RocksDbYcsbFactory.PROP_SHARDS, "1"));
    if (shardCount < 1) {
      throw new DBException("rocksdb.shards must be >= 1");
    }

    int memTableMb =
        Integer.parseInt(props.getProperty(RocksDbYcsbFactory.PROP_MEMTABLE_MB, "64"));
    int compactionThreshold =
        Integer.parseInt(
            props.getProperty(RocksDbYcsbFactory.PROP_COMPACTION_THRESHOLD, "2"));
    int blockCacheMb =
        Integer.parseInt(props.getProperty(RocksDbYcsbFactory.PROP_BLOCK_CACHE_MB, "128"));
    int maxBackgroundJobs =
        Integer.parseInt(props.getProperty(RocksDbYcsbFactory.PROP_MAX_BACKGROUND_JOBS, "4"));

    Path dataPath = Path.of(dataDir);
    try {
      if (resetDataDir) {
        deleteRecursively(dataPath);
      }
      Files.createDirectories(dataPath);
    } catch (IOException e) {
      throw new DBException("Failed to prepare RocksDB data directory: " + dataDir, e);
    }

    shards = new RocksDB[shardCount];
    for (int shard = 0; shard < shardCount; shard++) {
      Path shardDir = dataPath.resolve("shard-" + shard);
      try {
        Files.createDirectories(shardDir);
        Options options = buildOptions(memTableMb, compactionThreshold, blockCacheMb, maxBackgroundJobs);
        shardOptions.add(options);
        shards[shard] = RocksDB.open(options, shardDir.toString());
      } catch (IOException e) {
        closeOpenedShards(shard);
        throw new DBException("Failed to create RocksDB shard directory " + shard, e);
      } catch (RocksDBException e) {
        closeOpenedShards(shard);
        throw new DBException("Failed to open RocksDB shard " + shard, e);
      }
    }
  }

  private Options buildOptions(
      int memTableMb, int compactionThreshold, int blockCacheMb, int maxBackgroundJobs)
      throws RocksDBException {
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    if (blockCacheMb > 0) {
      LRUCache cache = new LRUCache((long) blockCacheMb * 1024 * 1024);
      blockCaches.add(cache);
      tableConfig.setBlockCache(cache);
    }

    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setWriteBufferSize((long) memTableMb * 1024 * 1024);
    options.setMaxWriteBufferNumber(3);
    options.setMinWriteBufferNumberToMerge(1);
    options.setLevel0FileNumCompactionTrigger(compactionThreshold);
    options.setMaxBytesForLevelBase(10L * 1024 * 1024);
    options.setTargetFileSizeBase(64L * 1024 * 1024);
    options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
    options.setMaxBackgroundJobs(maxBackgroundJobs);
    options.setTableFormatConfig(tableConfig);
    return options;
  }

  @Override
  public void cleanup() throws DBException {
    if (shards != null) {
      closeOpenedShards(shards.length);
      shards = null;
    }
    for (Options options : shardOptions) {
      options.close();
    }
    shardOptions.clear();
    for (LRUCache cache : blockCaches) {
      cache.close();
    }
    blockCaches.clear();
  }

  private void closeOpenedShards(int upToExclusive) {
    if (shards == null) {
      return;
    }
    for (int shard = 0; shard < upToExclusive; shard++) {
      if (shards[shard] != null) {
        shards[shard].close();
        shards[shard] = null;
      }
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] storageKey = toStorageKey(table, key);
    try {
      byte[] record = shardFor(storageKey).get(storageKey);
      if (record == null) {
        return Status.NOT_FOUND;
      }
      YcsbRecordCodec.decodeInto(record, fields, result);
      return Status.OK;
    } catch (RocksDBException e) {
      return Status.ERROR;
    }
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

    PriorityQueue<ShardScanCursor> cursors = new PriorityQueue<>();

    for (int shard = 0; shard < shardCount; shard++) {
      RocksIterator iterator = shards[shard].newIterator();
      iterator.seek(scanStart);
      ShardScanCursor cursor = ShardScanCursor.advance(iterator, scanEnd, prefix);
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

      ShardScanCursor next = cursor.advance(scanEnd, prefix);
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
    RocksDB db = shardFor(storageKey);
    try {
      byte[] existing = db.get(storageKey);
      if (existing == null) {
        return Status.NOT_FOUND;
      }
      byte[] merged = YcsbRecordCodec.merge(existing, values);
      db.put(storageKey, merged);
      return Status.OK;
    } catch (RocksDBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = toStorageKey(table, key);
    byte[] encoded = YcsbRecordCodec.encode(values);
    try {
      shardFor(storageKey).put(storageKey, encoded);
      return Status.OK;
    } catch (RocksDBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    byte[] storageKey = toStorageKey(table, key);
    try {
      byte[] existing = shardFor(storageKey).get(storageKey);
      if (existing == null) {
        return Status.NOT_FOUND;
      }
      shardFor(storageKey).delete(storageKey);
      return Status.OK;
    } catch (RocksDBException e) {
      return Status.ERROR;
    }
  }

  private byte[] toStorageKey(String table, String userKey) {
    return YcsbRecordCodec.storageKey(
        tablePrefixes.computeIfAbsent(table, YcsbRecordCodec::tablePrefix), userKey);
  }

  private RocksDB shardFor(byte[] storageKey) {
    return shards[CascadeStoreShardRouter.shardIndex(storageKey, shardCount)];
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(RocksDbShardedYcsbClient::deleteQuietly);
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // Best effort cleanup for benchmark temp dirs.
    }
  }

  private static final class ShardScanCursor implements Comparable<ShardScanCursor> {
    private final RocksIterator iterator;
    private final byte[] nextKey;
    private final byte[] nextValue;

    private ShardScanCursor(RocksIterator iterator, byte[] nextKey, byte[] nextValue) {
      this.iterator = iterator;
      this.nextKey = nextKey;
      this.nextValue = nextValue;
    }

    @Override
    public int compareTo(ShardScanCursor other) {
      return compareLex(this.nextKey, other.nextKey);
    }

    private static ShardScanCursor advance(RocksIterator iterator, byte[] scanEnd, String prefix) {
      while (iterator.isValid()) {
        byte[] key = iterator.key();
        if (compareLex(key, scanEnd) >= 0) {
          return null;
        }
        String storageKey = new String(key, StandardCharsets.UTF_8);
        if (!storageKey.startsWith(prefix)) {
          iterator.next();
          continue;
        }
        return new ShardScanCursor(iterator, key, iterator.value());
      }
      return null;
    }

    private ShardScanCursor advance(byte[] scanEnd, String prefix) {
      iterator.next();
      ShardScanCursor next = advance(iterator, scanEnd, prefix);
      if (next == null) {
        iterator.close();
      }
      return next;
    }

    private static int compareLex(byte[] left, byte[] right) {
      int limit = Math.min(left.length, right.length);
      for (int i = 0; i < limit; i++) {
        int diff = Byte.toUnsignedInt(left[i]) - Byte.toUnsignedInt(right[i]);
        if (diff != 0) {
          return diff;
        }
      }
      return left.length - right.length;
    }
  }
}
