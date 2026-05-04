package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategy;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.compaction.LevelTieredCompactionStrategy;
import io.cascadestore.lsm.core.compaction.SizeTieredCompactionStrategy;
import io.cascadestore.lsm.core.compaction.ThresholdCompactionStrategy;
import io.cascadestore.lsm.core.store.StorageLayoutPublisher;
import io.cascadestore.lsm.io.BlockCache;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CompactionService extends AbstractBackgroundService {

  private static final int MERGE_MEMTABLE_MIN_MULTIPLIER = 10;
  private static final double MERGE_MEMTABLE_INPUT_FACTOR = 1.2;

  private final List<SSTable> ssTables;
  private final CascadeConfig config;
  private final AtomicLong sequenceNumber;
  private final CompactionStrategy compactionStrategy;
  private final StorageLayoutPublisher layoutPublisher;
  private final BlockCache blockCache;
  private final LongSupplier layoutVersionSupplier;

  public CompactionService(
      List<SSTable> ssTables, CascadeConfig config, AtomicLong sequenceNumber) {
    this(ssTables, config, sequenceNumber, null, null, null);
  }

  public CompactionService(
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      StorageLayoutPublisher layoutPublisher) {
    this(ssTables, config, sequenceNumber, layoutPublisher, null, null);
  }

  public CompactionService(
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      StorageLayoutPublisher layoutPublisher,
      BlockCache blockCache) {
    this(ssTables, config, sequenceNumber, layoutPublisher, blockCache, null);
  }

  public CompactionService(
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      StorageLayoutPublisher layoutPublisher,
      BlockCache blockCache,
      LongSupplier layoutVersionSupplier) {
    super("Compaction");
    this.ssTables = ssTables;
    this.config = config;
    this.sequenceNumber = sequenceNumber;
    this.compactionStrategy = createCompactionStrategy(config);
    this.layoutPublisher = layoutPublisher;
    this.blockCache = blockCache;
    this.layoutVersionSupplier = layoutVersionSupplier;
  }

  private CompactionStrategy createCompactionStrategy(CascadeConfig config) {
    CompactionStrategyType strategyType = config.compactionStrategyType();

    return switch (strategyType) {
      case THRESHOLD -> new ThresholdCompactionStrategy(config);
      case SIZE_TIERED -> new SizeTieredCompactionStrategy(config);
      case LEVEL_TIERED -> new LevelTieredCompactionStrategy(config);
    };
  }

  @Override
  public void start() {
    CascadeConfig.CompactionInterval interval = config.compactionInterval();
    scheduleTask(interval.initialDelay(), interval.period(), interval.unit());
  }

  @Override
  protected void doExecute() {
    List<SSTable> tablesToCompact = null;
    long layoutVersionAtStart = -1;
    try {
      synchronized (ssTables) {
        if (!compactionStrategy.shouldCompact(ssTables)) {
          logger.info(
              "Skipping compaction, compaction strategy ({}) determined it's not needed",
              compactionStrategy.getName());
          return;
        }

        tablesToCompact = compactionStrategy.selectTableToCompact(ssTables);
        if (tablesToCompact.isEmpty()) {
          logger.info("Skipping compaction, no SSTables selected for compaction");
          return;
        }

        tablesToCompact = new ArrayList<>(tablesToCompact);
        if (layoutVersionSupplier != null) {
          layoutVersionAtStart = layoutVersionSupplier.getAsLong();
        }
        for (SSTable ssTable : tablesToCompact) {
          ssTable.pin();
        }
      }

      logger.info(
          "Starting compaction process with {} SSTables using {}",
          tablesToCompact.size(),
          compactionStrategy.getName());

      logger.info(
          "Compacting {} SSTables spanning levels {}",
          tablesToCompact.size(),
          levelSummary(tablesToCompact));

      ExecutorService executor =
          Executors.newCachedThreadPool(
              r -> {
                Thread thread = new Thread(r, "compaction-io");
                thread.setDaemon(true);
                return thread;
              });

      MemTable mergedMemTable = null;
      boolean committed = false;
      try {
        long mergeCapacity = computeMergeMemTableCapacity(tablesToCompact);
        mergedMemTable = new MemTable(mergeCapacity);

        Map<ByteArrayWrapper, byte[]> mergedData =
            mergeSSTableEntries(tablesToCompact, executor);
        if (mergedData == null) {
          logger.error("Compaction aborted during SSTable read; input SSTables preserved");
          return;
        }

        if (mergedData.isEmpty()) {
          logger.warn("No data to merge during compaction; input SSTables preserved");
          return;
        }

        if (!loadIntoMemTable(mergedMemTable, mergedData)) {
          logger.error(
              "Compaction aborted: merge MemTable capacity {} bytes exceeded; input SSTables preserved",
              mergeCapacity);
          return;
        }

        int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
        long newSequenceNumber = sequenceNumber.getAndIncrement();
        SSTable compactedTable =
            new SSTable(
                mergedMemTable, config.dataDirectory(), outputLevel, newSequenceNumber, blockCache);
        mergedMemTable.close();
        mergedMemTable = null;

        if (!commitCompaction(tablesToCompact, compactedTable, layoutVersionAtStart)) {
          compactedTable.forceCloseAndDelete();
          return;
        }
        committed = true;

        logger.info(
            "Compaction completed. Created new SSTable at level {} with sequence number {}",
            outputLevel,
            newSequenceNumber);
      } catch (OutOfMemoryError oom) {
        logger.error(
            "Compaction aborted due to out-of-memory; input SSTables preserved", oom);
      } finally {
        if (mergedMemTable != null) {
          mergedMemTable.close();
        }
        executor.shutdown();
        if (tablesToCompact != null) {
          for (SSTable ssTable : tablesToCompact) {
            ssTable.unpin();
          }
        }
        if (!committed && tablesToCompact != null) {
          logger.debug("Compaction rolled back; {} input SSTables remain live", tablesToCompact.size());
        }
      }
    } catch (Exception e) {
      logger.error("Error during compaction", e);
    }
  }

  private boolean commitCompaction(
      List<SSTable> tablesToCompact, SSTable compactedTable, long layoutVersionAtStart) {
    synchronized (ssTables) {
      if (layoutVersionSupplier != null
          && layoutVersionSupplier.getAsLong() != layoutVersionAtStart) {
        logger.warn("Compaction aborted: storage layout changed during merge");
        return false;
      }
      for (SSTable input : tablesToCompact) {
        if (!ssTables.contains(input)) {
          logger.warn("Compaction aborted: input SSTable set changed during merge");
          return false;
        }
      }

      ssTables.add(compactedTable);
      for (SSTable ssTable : tablesToCompact) {
        ssTables.remove(ssTable);
      }
    }

    if (layoutPublisher != null) {
      layoutPublisher.publishStorageLayout();
    }
    for (SSTable ssTable : tablesToCompact) {
      ssTable.retire();
    }
    return true;
  }

  long computeMergeMemTableCapacity(List<SSTable> tablesToCompact) {
    long inputBytes = 0;
    for (SSTable table : tablesToCompact) {
      inputBytes += table.getSizeBytes();
    }
    long floor = (long) config.memTableMaxSizeBytes() * MERGE_MEMTABLE_MIN_MULTIPLIER;
    long fromInput = (long) (inputBytes * MERGE_MEMTABLE_INPUT_FACTOR);
    return Math.max(floor, fromInput);
  }

  private Map<ByteArrayWrapper, byte[]> mergeSSTableEntries(
      List<SSTable> tablesToCompact, ExecutorService executor) {
    tablesToCompact.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

    Map<ByteArrayWrapper, byte[]> mergedData = new HashMap<>();
    List<CompletableFuture<List<Map.Entry<byte[], byte[]>>>> futures = new ArrayList<>();

    for (SSTable ssTable : tablesToCompact) {
      CompletableFuture<List<Map.Entry<byte[], byte[]>>> future =
          CompletableFuture.supplyAsync(() -> getEntriesFromSSTable(ssTable), executor);
      futures.add(future);
    }

    for (int i = 0; i < tablesToCompact.size(); i++) {
      SSTable ssTable = tablesToCompact.get(i);
      try {
        List<Map.Entry<byte[], byte[]>> entries = futures.get(i).get();

        if (entries.isEmpty()) {
          logger.warn(
              "No entries found in SSTable with sequence number {}",
              ssTable.getSequenceNumber());
        }

        for (Map.Entry<byte[], byte[]> entry : entries) {
          ByteArrayWrapper key = new ByteArrayWrapper(entry.getKey());
          if (!mergedData.containsKey(key) && entry.getValue() != null) {
            mergedData.put(key, entry.getValue());
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Compaction interrupted during SSTable read", e);
        return null;
      } catch (ExecutionException e) {
        if (unwrapOutOfMemoryError(e) != null) {
          throw unwrapOutOfMemoryError(e);
        }
        logger.error("Error processing SSTable during compaction", e);
        return null;
      }
    }

    return mergedData;
  }

  private boolean loadIntoMemTable(
      MemTable mergedMemTable, Map<ByteArrayWrapper, byte[]> mergedData) {
    for (Map.Entry<ByteArrayWrapper, byte[]> entry : mergedData.entrySet()) {
      if (!mergedMemTable.put(entry.getKey().getData(), entry.getValue(), 0)) {
        return false;
      }
    }
    return true;
  }

  private static OutOfMemoryError unwrapOutOfMemoryError(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof OutOfMemoryError oom) {
        return oom;
      }
      current = current.getCause();
    }
    return null;
  }

  private static String levelSummary(List<SSTable> tables) {
    return tables.stream()
        .mapToInt(SSTable::getLevel)
        .distinct()
        .sorted()
        .mapToObj(level -> "L" + level)
        .reduce((a, b) -> a + "," + b)
        .orElse("none");
  }

  private List<Map.Entry<byte[], byte[]>> getEntriesFromSSTable(SSTable ssTable) {
    List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
    Map<byte[], byte[]> rangeEntries = ssTable.getRange(null, null);
    for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
      if (entry.getValue() != null) {
        entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
      }
    }
    return entries;
  }
}
