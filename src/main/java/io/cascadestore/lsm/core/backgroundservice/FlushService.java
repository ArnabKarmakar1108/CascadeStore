package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlushService extends AbstractBackgroundService {

  private final List<MemTable> immutableMemTables;
  private final List<SSTable> ssTables;
  private final CascadeConfig config;
  private final AtomicLong sequenceNumber;
  private final CompactionService compactionService;

  public FlushService(
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      CompactionService compactionService) {
    super("Flush");
    this.immutableMemTables = immutableMemTables;
    this.ssTables = ssTables;
    this.config = config;
    this.sequenceNumber = sequenceNumber;
    this.compactionService = compactionService;
  }

  @Override
  public void start() {
    scheduleTask(10, config.flushIntervalSeconds(), TimeUnit.SECONDS);
  }

  @Override
  protected void doExecute() {
    try {
      List<MemTable> tablesToFlush;

      // Get a snapshot of immutable MemTables to flush
      synchronized (immutableMemTables) {
        if (immutableMemTables.isEmpty()) {
          logger.info("No immutable MemTables to flush");
          return;
        }
        tablesToFlush = new ArrayList<>(immutableMemTables);
        logger.info("Found " + tablesToFlush.size() + " immutable MemTables to flush");
      }

      for (MemTable memTable : tablesToFlush) {
        try {
          // Log the MemTable size and entries
          logger.info(
              "Flushing MemTable with size: "
                  + memTable.getSizeBytes()
                  + " bytes, entries: "
                  + memTable.getEntries().size());

          // Create a new SSTable from the MemTable
          long seqNum = sequenceNumber.getAndIncrement();
          logger.info(
              "Creating SSTable with sequence number: "
                  + seqNum
                  + " in directory: "
                  + config.dataDirectory());

          // Check if the MemTable is immutable
          if (!memTable.isImmutable()) {
            logger.warn("MemTable is not immutable, making it immutable before flushing");
            memTable.makeImmutable();
          }

          // Check if the data directory exists
          File dir = new File(config.dataDirectory());
          if (!dir.exists()) {
            logger.warn("Data directory does not exist, creating it: " + config.dataDirectory());
            dir.mkdirs();
          }

          SSTable ssTable = new SSTable(memTable, config.dataDirectory(), 0, seqNum);

          // Add the SSTable to the list
          synchronized (ssTables) {
            ssTables.add(ssTable);
            logger.info("Added SSTable to list, total SSTables: " + ssTables.size());
          }

          // Remove the MemTable from the immutable list
          synchronized (immutableMemTables) {
            immutableMemTables.remove(memTable);
            logger.info(
                "Removed MemTable from immutable list, remaining: " + immutableMemTables.size());
          }

          // Close the MemTable to release resources
          memTable.close();

          logger.info("Flushed MemTable to SSTable: " + ssTable.getSequenceNumber());

          // Check if the SSTable files were created
          String filePrefix = String.format("sst_L%d_S%d", 0, seqNum);
          File dataFile = new File(config.dataDirectory(), filePrefix + ".data");
          File indexFile = new File(config.dataDirectory(), filePrefix + ".index");
          File filterFile = new File(config.dataDirectory(), filePrefix + ".filter");

          logger.info(
              "SSTable files created: data="
                  + dataFile.exists()
                  + ", index="
                  + indexFile.exists()
                  + ", filter="
                  + filterFile.exists());
        } catch (IOException e) {
          logger.error("Error flushing MemTable to disk", e);
        }
      }

      // Check if compaction is needed
      synchronized (ssTables) {
        if (ssTables.size() >= config.compactionThreshold()) {
          logger.info("Compaction threshold reached, triggering compaction");
          compactionService.executeNow();
        }
      }
    } catch (Exception e) {
      logger.error("Error during MemTable flush", e);
    }
  }
}
