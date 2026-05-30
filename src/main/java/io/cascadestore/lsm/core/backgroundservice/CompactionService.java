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
import io.cascadestore.lsm.metrics.CascadeMetrics;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CompactionService extends AbstractBackgroundService {

  private final Object compactionMonitor = new Object();
  private final List<SSTable> ssTables;
  private final CascadeConfig config;
  private final AtomicLong sequenceNumber;
  private final CompactionStrategy compactionStrategy;
  private final StorageLayoutPublisher layoutPublisher;
  private final BlockCache blockCache;
  private final LongSupplier layoutVersionSupplier;
  private final CascadeMetrics metrics;

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
    this(ssTables, config, sequenceNumber, layoutPublisher, blockCache, layoutVersionSupplier, CascadeMetrics.noop());
  }

  public CompactionService(
      List<SSTable> ssTables,
      CascadeConfig config,
      AtomicLong sequenceNumber,
      StorageLayoutPublisher layoutPublisher,
      BlockCache blockCache,
      LongSupplier layoutVersionSupplier,
      CascadeMetrics metrics) {
    super("Compaction");
    this.ssTables = ssTables;
    this.config = config;
    this.sequenceNumber = sequenceNumber;
    this.compactionStrategy = createCompactionStrategy(config);
    this.layoutPublisher = layoutPublisher;
    this.blockCache = blockCache;
    this.layoutVersionSupplier = layoutVersionSupplier;
    this.metrics = metrics != null ? metrics : CascadeMetrics.noop();
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
    synchronized (compactionMonitor) {
      doExecuteCompaction();
    }
  }

  private void doExecuteCompaction() {
    List<SSTable> tablesToCompact = null;
    long layoutVersionAtStart = -1;
    long compactionStart = System.nanoTime();
    long inputBytes = 0;
    metrics.setCompactionInProgress(true);
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
        inputBytes = totalInputBytes(tablesToCompact);
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

      int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
      boolean dropTombstones = computeDropTombstones(outputLevel, tablesToCompact);
      long newSequenceNumber = sequenceNumber.getAndIncrement();
      int estimatedEntries = 0;
      for (SSTable table : tablesToCompact) {
        estimatedEntries += table.countEntries();
      }

      SSTable compactedTable = null;
      boolean committed = false;
      try {
        // Streaming k-way merge: each input is read sequentially through a sorted cursor and merged
        // via a min-heap (newest sequence wins on key ties), then written straight to the output
        // file. Peak memory is O(number of inputs), not O(total keys).
        try (KWayMergeSource mergeSource = new KWayMergeSource(tablesToCompact, dropTombstones)) {
          compactedTable =
              new SSTable(
                  config.dataDirectory(),
                  outputLevel,
                  newSequenceNumber,
                  blockCache,
                  mergeSource,
                  Math.max(estimatedEntries, 1));
        }

        SSTable committedTable = compactedTable.countEntries() > 0 ? compactedTable : null;
        if (!commitCompaction(tablesToCompact, committedTable, layoutVersionAtStart)) {
          compactedTable.forceCloseAndDelete();
          return;
        }
        if (committedTable == null) {
          // Everything merged away (e.g. bottom-level tombstones); drop the empty output file.
          compactedTable.forceCloseAndDelete();
        }
        committed = true;

        metrics.recordCompaction(System.nanoTime() - compactionStart, inputBytes);
        metrics.setCompactionPending(ssTables.size() >= config.compactionThreshold());
        logger.info(
            "Compaction completed. Created SSTable at level {} seq {} ({} entries)",
            outputLevel,
            newSequenceNumber,
            compactedTable.countEntries());
      } catch (IOException e) {
        logger.error("Compaction merge failed; input SSTables preserved", e);
        if (compactedTable != null) {
          compactedTable.forceCloseAndDelete();
        }
      } catch (OutOfMemoryError oom) {
        logger.error("Compaction aborted due to out-of-memory; input SSTables preserved", oom);
        if (compactedTable != null) {
          compactedTable.forceCloseAndDelete();
        }
      } finally {
        if (tablesToCompact != null) {
          for (SSTable ssTable : tablesToCompact) {
            ssTable.unpin();
          }
        }
        if (!committed && tablesToCompact != null) {
          logger.debug(
              "Compaction rolled back; {} input SSTables remain live", tablesToCompact.size());
        }
      }
    } catch (Exception e) {
      logger.error("Error during compaction", e);
    } finally {
      metrics.setCompactionInProgress(false);
    }
  }

  private static long totalInputBytes(List<SSTable> tables) {
    long bytes = 0;
    for (SSTable table : tables) {
      bytes += table.getSizeBytes();
    }
    return bytes;
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

      if (compactedTable != null) {
        ssTables.add(compactedTable);
      }
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

  /**
   * Deletion tombstones may only be discarded once no older value for the key can exist below the
   * merged output — i.e. the output level is strictly deeper than every surviving table. New tables
   * are always added at shallow levels, so a snapshot taken here is safe against concurrent flushes.
   */
  private boolean computeDropTombstones(int outputLevel, List<SSTable> inputs) {
    int maxSurvivingLevel = -1;
    synchronized (ssTables) {
      for (SSTable table : ssTables) {
        if (!inputs.contains(table)) {
          maxSurvivingLevel = Math.max(maxSurvivingLevel, table.getLevel());
        }
      }
    }
    return outputLevel > maxSurvivingLevel;
  }

  /**
   * Merges several sorted SSTables into a single ascending record stream. On duplicate keys the
   * record from the newest source (highest sequence number) wins; older duplicates are discarded.
   */
  private final class KWayMergeSource implements SSTable.SortedRecordSource {
    private final PriorityQueue<SSTable.RecordCursor> heap;
    private final List<SSTable.RecordCursor> cursors = new ArrayList<>();
    private final boolean dropTombstones;
    private byte[] currentKey;
    private byte[] currentValue;
    private long currentExpiration;

    private KWayMergeSource(List<SSTable> inputs, boolean dropTombstones) throws IOException {
      this.dropTombstones = dropTombstones;
      this.heap =
          new PriorityQueue<>(
              Math.max(inputs.size(), 1),
              (a, b) -> {
                int keyOrder = Arrays.compareUnsigned(a.key(), b.key());
                if (keyOrder != 0) {
                  return keyOrder;
                }
                return Long.compare(b.sourceSequence(), a.sourceSequence());
              });
      try {
        for (SSTable input : inputs) {
          SSTable.RecordCursor cursor = input.openRecordCursor();
          cursors.add(cursor);
          if (cursor.advance()) {
            heap.add(cursor);
          }
        }
      } catch (IOException e) {
        close();
        throw e;
      }
    }

    @Override
    public boolean advance() throws IOException {
      while (!heap.isEmpty()) {
        SSTable.RecordCursor top = heap.poll();
        byte[] key = top.key();
        byte[] value = top.value();
        long expiration = top.expirationTime();
        advanceCursor(top);
        while (!heap.isEmpty() && Arrays.equals(heap.peek().key(), key)) {
          advanceCursor(heap.poll());
        }
        if (value == null && dropTombstones) {
          continue;
        }
        currentKey = key;
        currentValue = value;
        currentExpiration = expiration;
        return true;
      }
      return false;
    }

    private void advanceCursor(SSTable.RecordCursor cursor) throws IOException {
      if (cursor.advance()) {
        heap.add(cursor);
      }
    }

    @Override
    public byte[] key() {
      return currentKey;
    }

    @Override
    public byte[] value() {
      return currentValue;
    }

    @Override
    public long expirationTime() {
      return currentExpiration;
    }

    @Override
    public void close() {
      for (SSTable.RecordCursor cursor : cursors) {
        cursor.close();
      }
    }
  }
}
