package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategy;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
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
          logger.info("Skipping compaction, compaction strategy (%s) determined it's not needed", compactionStrategy.getName());
          return;
        }

        logger.info("Starting compaction process with %d SSTables using %s", ssTables.size(), compactionStrategy.getName());

        // Use the compaction strategy to select the tables to compact
        List<SSTable> tablesToCompact = compactionStrategy.selectTableToCompact(ssTables);

        // Skip if no tables were selected for compaction
        if (tablesToCompact.isEmpty()) {
          logger.info("Skipping compaction, no SSTables selected for compaction");
          return;
        }

        // Get the level of the first table for logging purposes
        int level = tablesToCompact.get(0).getLevel();

        logger.info("Compacting %d SSTables at level %d", tablesToCompact.size(), level);

        // Create a temporary MemTable to merge the SSTables
        MemTable mergedMemTable =
            new MemTable(config.memTableMaxSizeBytes() * 10); // Larger size for merging

        // Merge the SSTables, with newer SSTables taking precedence over older ones
        // Sort by sequence number in descending order (newer first)
        tablesToCompact.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

        // Create a map to store the merged key-value pairs
        Map<ByteArrayWrapper, byte[]> mergedData = new HashMap<>();

        // Process SSTables in parallel to improve I/O throughput during merge
        List<CompletableFuture<List<Map.Entry<byte[], byte[]>>>> futures = new ArrayList<>();

        ExecutorService executor =
            Executors.newCachedThreadPool(
                r -> {
                  Thread thread = new Thread(r, "compaction-io");
                  thread.setDaemon(true);
                  return thread;
                });

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
              logger.warn("No entries found in SSTable with sequence number %d",
                  ssTable.getSequenceNumber());
            }

            // Process the entries
            for (Map.Entry<byte[], byte[]> entry : entries) {
              ByteArrayWrapper key = new ByteArrayWrapper(entry.getKey());
              // Only add if the key hasn't been seen yet (newer SSTables take precedence)
              if (!mergedData.containsKey(key)) {
                byte[] value = entry.getValue();
                if (value != null) { // Skip tombstones
                  mergedData.put(key, value);
                }
              }
            }
          } catch (InterruptedException | ExecutionException e) {
            logger.error("Error processing SSTable during compaction", e);
          }
        }

        // Shutdown the executor
        executor.shutdown();

        // Check if we have any data to merge
        if (mergedData.isEmpty()) {
          logger.warn("No data to merge during compaction - this is expected with the " +
              "current implementation.");
          return;
        }

        // Add all merged data to the MemTable
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : mergedData.entrySet()) {
          mergedMemTable.put(
              entry.getKey().getData(), entry.getValue(), 0); // No TTL for simplicity
        }

        // Create a new SSTable at the output level determined by the compaction strategy
        int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
        long newSequenceNumber = sequenceNumber.getAndIncrement();

        try {
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

          logger.info("Compaction completed. Created new SSTable at level %d with sequence number %d", outputLevel, newSequenceNumber);
        } catch (IOException e) {
          logger.error("Error creating compacted SSTable", e);
        }
      }
    } catch (Exception e) {
      logger.error("Error during compaction", e);
    }
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
