package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategy;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.compaction.LevelTieredCompactionStrategy;
import io.cascadestore.lsm.core.compaction.SizeTieredCompactionStrategy;
import io.cascadestore.lsm.core.compaction.ThresholdCompactionStrategy;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionService extends AbstractBackgroundService {

  private static final int MERGE_MEMTABLE_MIN_MULTIPLIER = 10;
  private static final double MERGE_MEMTABLE_INPUT_FACTOR = 1.2;

  private final List<SSTable> ssTables;
  private final CascadeConfig config;
  private final AtomicLong sequenceNumber;
  private final CompactionStrategy compactionStrategy;

  public CompactionService(
      List<SSTable> ssTables, CascadeConfig config, AtomicLong sequenceNumber) {
    super("Compaction");
    this.ssTables = ssTables;
    this.config = config;
    this.sequenceNumber = sequenceNumber;
    this.compactionStrategy = createCompactionStrategy(config);
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
    scheduleTask(30, config.compactionIntervalMinutes(), TimeUnit.MINUTES);
  }

  @Override
  protected void doExecute() {
    try {
      synchronized (ssTables) {
        // Use the compaction strategy to determine if compaction should be performed
        if (!compactionStrategy.shouldCompact(ssTables)) {
          logger.info(
              "Skipping compaction, compaction strategy (%s) determined it's not needed",
              compactionStrategy.getName());
          return;
        }

        // Use the compaction strategy to select the tables to compact
        List<SSTable> tablesToCompact = compactionStrategy.selectTableToCompact(ssTables);

        // Skip if no tables were selected for compaction
        if (tablesToCompact.isEmpty()) {
          logger.info("Skipping compaction, no SSTables selected for compaction");
          return;
        }

        logger.info(
            "Starting compaction process with %d SSTables using %s",
            ssTables.size(),
            compactionStrategy.getName());

        logger.info(
            "Compacting %d SSTables spanning levels %s",
            tablesToCompact.size(),
            levelSummary(tablesToCompact));

        // Process SSTables in parallel to improve I/O throughput during merge
        ExecutorService executor =
            Executors.newCachedThreadPool(
                r -> {
                  Thread thread = new Thread(r, "compaction-io");
                  thread.setDaemon(true);
                  return thread;
                });

        MemTable mergedMemTable = null;
        try {
          // Create a temporary MemTable to merge the SSTables (sized from input bytes)
          long mergeCapacity = computeMergeMemTableCapacity(tablesToCompact);
          mergedMemTable = new MemTable(mergeCapacity);

          // Merge SSTables; newer SSTables take precedence over older ones
          Map<ByteArrayWrapper, byte[]> mergedData =
              mergeSSTableEntries(tablesToCompact, executor);
          if (mergedData == null) {
            logger.error("Compaction aborted during SSTable read; input SSTables preserved");
            return;
          }

          // Check if we have any data to merge
          if (mergedData.isEmpty()) {
            logger.warn(
                "No data to merge during compaction; input SSTables preserved");
            return;
          }

          // Add all merged data to the MemTable
          if (!loadIntoMemTable(mergedMemTable, mergedData)) {
            logger.error(
                "Compaction aborted: merge MemTable capacity {} bytes exceeded; input SSTables preserved",
                mergeCapacity);
            return;
          }

          // Create a new SSTable at the output level and remove the old SSTables
          commitCompaction(tablesToCompact, mergedMemTable);
          mergedMemTable.close();
          mergedMemTable = null;
        } catch (OutOfMemoryError oom) {
          // Abort without deleting inputs when the JVM runs out of heap during merge
          logger.error(
              "Compaction aborted due to out-of-memory; input SSTables preserved", oom);
        } finally {
          if (mergedMemTable != null) {
            mergedMemTable.close();
          }
          // Shutdown the executor
          executor.shutdown();
        }
      }
    } catch (Exception e) {
      logger.error("Error during compaction", e);
    }
  }

  private void commitCompaction(List<SSTable> tablesToCompact, MemTable mergedMemTable)
      throws IOException {
    // Create a new SSTable at the output level determined by the compaction strategy
    int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
    long newSequenceNumber = sequenceNumber.getAndIncrement();

    SSTable compactedTable =
        new SSTable(mergedMemTable, config.dataDirectory(), outputLevel, newSequenceNumber);

    // Add the new SSTable to the list
    ssTables.add(compactedTable);

    // Remove the old SSTables
    for (SSTable ssTable : tablesToCompact) {
      ssTables.remove(ssTable);
      ssTable.delete(); // Delete the files
      ssTable.close(); // Release resources
    }

    logger.info(
        "Compaction completed. Created new SSTable at level %d with sequence number %d",
        outputLevel,
        newSequenceNumber);
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
    // Sort by sequence number in descending order (newer first)
    tablesToCompact.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

    // Create a map to store the merged key-value pairs
    Map<ByteArrayWrapper, byte[]> mergedData = new HashMap<>();
    List<CompletableFuture<List<Map.Entry<byte[], byte[]>>>> futures = new ArrayList<>();

    // Create a future for each SSTable
    for (SSTable ssTable : tablesToCompact) {
      CompletableFuture<List<Map.Entry<byte[], byte[]>>> future =
          CompletableFuture.supplyAsync(() -> getEntriesFromSSTable(ssTable), executor);
      futures.add(future);
    }

    // Wait for all futures to complete and process the results
    for (int i = 0; i < tablesToCompact.size(); i++) {
      SSTable ssTable = tablesToCompact.get(i);
      try {
        List<Map.Entry<byte[], byte[]>> entries = futures.get(i).get();

        // If no entries are found, log a warning but continue
        if (entries.isEmpty()) {
          logger.warn(
              "No entries found in SSTable with sequence number %d",
              ssTable.getSequenceNumber());
        }

        // Process the entries
        for (Map.Entry<byte[], byte[]> entry : entries) {
          ByteArrayWrapper key = new ByteArrayWrapper(entry.getKey());
          // Only add if the key hasn't been seen yet (newer SSTables take precedence)
          if (!mergedData.containsKey(key) && entry.getValue() != null) {
            mergedData.put(key, entry.getValue()); // Skip tombstones
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
      if (!mergedMemTable.put(
          entry.getKey().getData(), entry.getValue(), 0)) { // No TTL for simplicity
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

    // Get all entries from the SSTable using the getRange method
    Map<byte[], byte[]> rangeEntries = ssTable.getRange(null, null);

    // Convert the map entries to a list
    for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
      if (entry.getValue() != null) {
        entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
      }
    }
    return entries;
  }
}
