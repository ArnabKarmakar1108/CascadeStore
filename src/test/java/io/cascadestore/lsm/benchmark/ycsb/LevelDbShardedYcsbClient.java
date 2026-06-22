package io.cascadestore.lsm.benchmark.ycsb;

import java.io.File;
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
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * YCSB binding for embedded LevelDB (JNI).
 *
 * <p>Uses the same {@link YcsbRecordCodec} layout as {@link CascadeStoreYcsbClient} for comparable
 * workloads. LevelDB is always leveled — there is no compaction strategy knob.
 */
public class LevelDbShardedYcsbClient extends site.ycsb.DB {

  private DB[] shards;
  private final List<Options> shardOptions = new ArrayList<>();
  private int shardCount;
  private final Map<String, byte[]> tablePrefixes = new HashMap<>();
  private final WriteOptions writeOptions = new WriteOptions();
  private final ReadOptions readOptions = new ReadOptions();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String dataDir = props.getProperty(LevelDbYcsbFactory.PROP_DATADIR, "/tmp/ycsb-leveldb-data");
    boolean resetDataDir =
        Boolean.parseBoolean(
            props.getProperty(LevelDbYcsbFactory.PROP_RESET_DATADIR, "true"));
    shardCount = Integer.parseInt(props.getProperty(LevelDbYcsbFactory.PROP_SHARDS, "1"));
    if (shardCount < 1) {
      throw new DBException("leveldb.shards must be >= 1");
    }

    int memTableMb =
        Integer.parseInt(props.getProperty(LevelDbYcsbFactory.PROP_MEMTABLE_MB, "64"));
    int blockCacheMb =
        Integer.parseInt(props.getProperty(LevelDbYcsbFactory.PROP_BLOCK_CACHE_MB, "128"));

    Path dataPath = Path.of(dataDir);
    try {
      if (resetDataDir) {
        deleteRecursively(dataPath);
      }
      Files.createDirectories(dataPath);
    } catch (IOException e) {
      throw new DBException("Failed to prepare LevelDB data directory: " + dataDir, e);
    }

    shards = new DB[shardCount];
    for (int shard = 0; shard < shardCount; shard++) {
      File shardDir = dataPath.resolve("shard-" + shard).toFile();
      try {
        Options options = buildOptions(memTableMb, blockCacheMb);
        shardOptions.add(options);
        shards[shard] = JniDBFactory.factory.open(shardDir, options);
      } catch (IOException e) {
        closeOpenedShards(shard);
        throw new DBException("Failed to open LevelDB shard " + shard, e);
      }
    }
  }

  private static Options buildOptions(int memTableMb, int blockCacheMb) {
    Options options = new Options();
    options.createIfMissing(true);
    options.writeBufferSize(memTableMb * 1024 * 1024);
    if (blockCacheMb > 0) {
      options.cacheSize((long) blockCacheMb * 1024 * 1024);
    }
    return options;
  }

  @Override
  public void cleanup() throws DBException {
    if (shards != null) {
      closeOpenedShards(shards.length);
      shards = null;
    }
    for (Options options : shardOptions) {
      // iq80 Options has no close()
    }
    shardOptions.clear();
  }

  private void closeOpenedShards(int upToExclusive) {
    if (shards == null) {
      return;
    }
    for (int shard = 0; shard < upToExclusive; shard++) {
      if (shards[shard] != null) {
        try {
          shards[shard].close();
        } catch (IOException ignored) {
          // Best effort cleanup for benchmark temp dirs.
        }
        shards[shard] = null;
      }
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] storageKey = toStorageKey(table, key);
    try {
      byte[] record = shardFor(storageKey).get(storageKey, readOptions);
      if (record == null) {
        return Status.NOT_FOUND;
      }
      YcsbRecordCodec.decodeInto(record, fields, result);
      return Status.OK;
    } catch (org.iq80.leveldb.DBException e) {
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
      DBIterator iterator = shards[shard].iterator(readOptions);
      iterator.seek(scanStart);
      ShardScanCursor cursor = ShardScanCursor.advance(iterator, scanEnd, prefix);
      if (cursor != null) {
        cursors.add(cursor);
      } else {
        try {
          iterator.close();
        } catch (IOException ignored) {
          // ignore
        }
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
        try {
          cursor.iterator.close();
        } catch (IOException ignored) {
          // ignore
        }
      }
    }

    while (!cursors.isEmpty()) {
      try {
        cursors.poll().iterator.close();
      } catch (IOException ignored) {
        // ignore
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = toStorageKey(table, key);
    DB db = shardFor(storageKey);
    try {
      byte[] existing = db.get(storageKey, readOptions);
      if (existing == null) {
        return Status.NOT_FOUND;
      }
      byte[] merged = YcsbRecordCodec.merge(existing, values);
      db.put(storageKey, merged, writeOptions);
      return Status.OK;
    } catch (org.iq80.leveldb.DBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = toStorageKey(table, key);
    byte[] encoded = YcsbRecordCodec.encode(values);
    try {
      shardFor(storageKey).put(storageKey, encoded, writeOptions);
      return Status.OK;
    } catch (org.iq80.leveldb.DBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    byte[] storageKey = toStorageKey(table, key);
    try {
      DB db = shardFor(storageKey);
      byte[] existing = db.get(storageKey, readOptions);
      if (existing == null) {
        return Status.NOT_FOUND;
      }
      db.delete(storageKey, writeOptions);
      return Status.OK;
    } catch (org.iq80.leveldb.DBException e) {
      return Status.ERROR;
    }
  }

  private byte[] toStorageKey(String table, String userKey) {
    return YcsbRecordCodec.storageKey(
        tablePrefixes.computeIfAbsent(table, YcsbRecordCodec::tablePrefix), userKey);
  }

  private DB shardFor(byte[] storageKey) {
    return shards[CascadeStoreShardRouter.shardIndex(storageKey, shardCount)];
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(LevelDbShardedYcsbClient::deleteQuietly);
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
    private final DBIterator iterator;
    private final byte[] nextKey;
    private final byte[] nextValue;

    private ShardScanCursor(DBIterator iterator, byte[] nextKey, byte[] nextValue) {
      this.iterator = iterator;
      this.nextKey = nextKey;
      this.nextValue = nextValue;
    }

    @Override
    public int compareTo(ShardScanCursor other) {
      return compareLex(this.nextKey, other.nextKey);
    }

    private static ShardScanCursor advance(DBIterator iterator, byte[] scanEnd, String prefix) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.peekNext();
        byte[] key = entry.getKey();
        if (compareLex(key, scanEnd) >= 0) {
          return null;
        }
        String storageKey = new String(key, StandardCharsets.UTF_8);
        if (!storageKey.startsWith(prefix)) {
          iterator.next();
          continue;
        }
        iterator.next();
        return new ShardScanCursor(iterator, key, entry.getValue());
      }
      return null;
    }

    private ShardScanCursor advance(byte[] scanEnd, String prefix) {
      ShardScanCursor next = advance(iterator, scanEnd, prefix);
      if (next == null) {
        try {
          iterator.close();
        } catch (IOException ignored) {
          // ignore
        }
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
