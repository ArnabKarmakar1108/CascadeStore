package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.store.StorageLayoutPublisher;
import io.cascadestore.lsm.io.BlockCache;
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
  private final Runnable walTruncationHook;
  private final StorageLayoutPublisher layoutPublisher;
  private final BlockCache blockCache;
  private final Object flushMonitor = new Object();

  public FlushService(
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      CompactionService compactionService) {
    this(immutableMemTables, ssTables, config, sequenceNumber, compactionService, null, null);
  }

  public FlushService(
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      CompactionService compactionService,
      Runnable walTruncationHook) {
    this(
        immutableMemTables,
        ssTables,
        config,
        sequenceNumber,
        compactionService,
        walTruncationHook,
        null);
  }

  public FlushService(
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      CompactionService compactionService,
      Runnable walTruncationHook,
      StorageLayoutPublisher layoutPublisher) {
    this(
        immutableMemTables,
        ssTables,
        config,
        sequenceNumber,
        compactionService,
        walTruncationHook,
        layoutPublisher,
        null);
  }

  public FlushService(
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      CompactionService compactionService,
      Runnable walTruncationHook,
      StorageLayoutPublisher layoutPublisher,
      BlockCache blockCache) {
    super("Flush");
    this.immutableMemTables = immutableMemTables;
    this.ssTables = ssTables;
    this.config = config;
    this.sequenceNumber = sequenceNumber;
    this.compactionService = compactionService;
    this.walTruncationHook = walTruncationHook;
    this.layoutPublisher = layoutPublisher;
    this.blockCache = blockCache;
  }

  @Override
  public void start() {
    scheduleTask(10, config.flushIntervalSeconds(), TimeUnit.SECONDS);
  }

  @Override
  protected void doExecute() {
    synchronized (flushMonitor) {
      try {
        List<MemTable> tablesToFlush;
        synchronized (immutableMemTables) {
          if (immutableMemTables.isEmpty()) {
            logger.info("No immutable MemTables to flush");
            return;
          }
          tablesToFlush = new ArrayList<>(immutableMemTables);
        }

        logger.info("Found {} immutable MemTables to flush", tablesToFlush.size());

        for (MemTable memTable : tablesToFlush) {
          synchronized (immutableMemTables) {
            if (!immutableMemTables.contains(memTable)) {
              continue;
            }
          }
          flushMemTable(memTable);
        }

        maybeTruncateWal();
        maybeTriggerCompaction();
      } catch (Exception e) {
        logger.error("Error during MemTable flush", e);
      }
    }
  }

  private void flushMemTable(MemTable memTable) {
    if (memTable.getEntries().isEmpty()) {
      synchronized (immutableMemTables) {
        immutableMemTables.remove(memTable);
      }
      memTable.close();
      logger.info("Skipping flush of empty MemTable");
      return;
    }

    long seqNum = sequenceNumber.getAndIncrement();

    logger.info(
        "Flushing MemTable with size: {} bytes, entries: {}",
        memTable.getSizeBytes(),
        memTable.getEntries().size());
    logger.info(
        "Creating SSTable with sequence number: {} in directory: {}",
        seqNum,
        config.dataDirectory());

    if (!memTable.isImmutable()) {
      logger.warn("MemTable is not immutable, making it immutable before flushing");
      memTable.makeImmutable();
    }

    File dir = new File(config.dataDirectory());
    if (!dir.exists() && !dir.mkdirs()) {
      logger.warn("Data directory does not exist and could not be created: {}", config.dataDirectory());
    }

    try {
      SSTable ssTable =
          new SSTable(memTable, config.dataDirectory(), 0, seqNum, blockCache);

      synchronized (immutableMemTables) {
        synchronized (ssTables) {
          ssTables.add(ssTable);
          immutableMemTables.remove(memTable);
        }
        logger.info("Added SSTable to list, total SSTables: {}", ssTables.size());
      }

      if (layoutPublisher != null) {
        layoutPublisher.publishStorageLayout();
      }

      memTable.close();
      logger.info("Flushed MemTable to SSTable: {}", ssTable.getSequenceNumber());
    } catch (IOException e) {
      logger.error("Error flushing MemTable to disk", e);
      SSTable.deleteFiles(config.dataDirectory(), 0, seqNum);
      requeueMemTable(memTable);
    }
  }

  private void requeueMemTable(MemTable memTable) {
    synchronized (immutableMemTables) {
      immutableMemTables.add(memTable);
      logger.info(
          "Re-queued MemTable for flush retry, pending immutable MemTables: {}",
          immutableMemTables.size());
    }
  }

  private void maybeTruncateWal() {
    if (walTruncationHook != null) {
      walTruncationHook.run();
    }
  }

  private void maybeTriggerCompaction() {
    synchronized (ssTables) {
      if (ssTables.size() >= config.compactionThreshold()) {
        logger.info("Compaction threshold reached, triggering compaction");
        compactionService.executeNow();
      }
    }
  }
}
