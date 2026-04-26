package io.cascadestore.lsm.benchmark.ycsb;

import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
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
 * <p>All YCSB threads in a run share one {@link CascadeStore} instance per unique configuration
 * key (datadir + engine settings). Configure via properties prefixed with {@code cascadestore.};
 * see {@link CascadeStoreYcsbFactory}.
 */
public class CascadeStoreYcsbClient extends DB {

  private CascadeStore store;
  private String registryKey;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String dataDir = props.getProperty(CascadeStoreYcsbFactory.PROP_DATADIR, "/tmp/ycsb-cascade-data");
    boolean resetDataDir =
        Boolean.parseBoolean(
            props.getProperty(CascadeStoreYcsbFactory.PROP_RESET_DATADIR, "true"));

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

    registryKey =
        buildRegistryKey(
            dataDir,
            memTableMb,
            compactionThreshold,
            compactionIntervalMinutes,
            cleanupIntervalMinutes,
            flushIntervalSeconds,
            strategyType);

    CascadeConfig config =
        new CascadeConfig(
            memTableMb * 1024 * 1024,
            dataDir,
            compactionThreshold,
            compactionIntervalMinutes,
            cleanupIntervalMinutes,
            flushIntervalSeconds,
            strategyType);

    synchronized (SharedCascadeStoreRegistry.class) {
      try {
        if (resetDataDir && !SharedCascadeStoreRegistry.isOpen(registryKey)) {
          deleteRecursively(dataPath);
        }
        Files.createDirectories(dataPath);
      } catch (IOException e) {
        throw new DBException("Failed to prepare data directory: " + dataDir, e);
      }

      store = SharedCascadeStoreRegistry.acquire(registryKey, () -> new CascadeStore(config));
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (registryKey != null) {
      SharedCascadeStoreRegistry.release(registryKey);
      registryKey = null;
      store = null;
    }
  }

  @Override
  public Status read(
      String table,
      String key,
      Set<String> fields,
      Map<String, ByteIterator> result) {
    byte[] record = store.get(YcsbRecordCodec.storageKey(table, key));
    if (record == null) {
      return Status.NOT_FOUND;
    }
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
    byte[] scanStart = YcsbRecordCodec.storageKey(table, startKey);
    byte[] scanEnd = YcsbRecordCodec.scanEndKey(table);
    String prefix = table + ":";

    try (KeyValueIterator iterator = store.getIterator(scanStart, scanEnd)) {
      while (iterator.hasNext() && result.size() < recordCount) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        String storageKey = new String(entry.getKey(), StandardCharsets.UTF_8);
        if (!storageKey.startsWith(prefix)) {
          continue;
        }

        HashMap<String, ByteIterator> record = new HashMap<>();
        YcsbRecordCodec.decodeInto(entry.getValue(), fields, record);
        result.add(record);
      }
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = YcsbRecordCodec.storageKey(table, key);
    byte[] existing = store.get(storageKey);
    if (existing == null) {
      return Status.NOT_FOUND;
    }
    byte[] merged = YcsbRecordCodec.merge(existing, values);
    return store.put(storageKey, merged) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] storageKey = YcsbRecordCodec.storageKey(table, key);
    byte[] encoded = YcsbRecordCodec.encode(values);
    return store.put(storageKey, encoded) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return store.delete(YcsbRecordCodec.storageKey(table, key)) ? Status.OK : Status.NOT_FOUND;
  }

  /** Clears reference-counted stores; for unit tests only. */
  public static void resetSharedStoresForTests() {
    SharedCascadeStoreRegistry.resetForTests();
  }

  static String buildRegistryKey(
      String dataDir,
      int memTableMb,
      int compactionThreshold,
      double compactionIntervalMinutes,
      int cleanupIntervalMinutes,
      int flushIntervalSeconds,
      CompactionStrategyType strategyType) {
    return String.join(
        "|",
        dataDir,
        strategyType.name(),
        Integer.toString(memTableMb),
        Integer.toString(compactionThreshold),
        Double.toString(compactionIntervalMinutes),
        Integer.toString(cleanupIntervalMinutes),
        Integer.toString(flushIntervalSeconds));
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
}
