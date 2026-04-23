package io.cascadestore.lsm.core.compaction;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeTieredCompactionStrategy implements CompactionStrategy {
  private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);

  // Default configuration values
  private static final int DEFAULT_MIN_THRESHOLD = 4;
  private static final int DEFAULT_MAX_THRESHOLD = 32;
  private static final double DEFAULT_BUCKET_LOW = 0.5;
  private static final double DEFAULT_BUCKET_HIGH = 1.5;

  private final CascadeConfig config;
  private final int minThreshold;
  private final int maxThreshold;
  private final double bucketLow;
  private final double bucketHigh;

  public SizeTieredCompactionStrategy(CascadeConfig config) {
    this(
        config,
        config.compactionThreshold(),
        DEFAULT_MAX_THRESHOLD,
        DEFAULT_BUCKET_LOW,
        DEFAULT_BUCKET_HIGH);
  }

  public SizeTieredCompactionStrategy(
      CascadeConfig config,
      int minThreshold,
      int maxThreshold,
      double bucketLow,
      double bucketHigh) {
    this.config = config;
    this.minThreshold = minThreshold;
    this.maxThreshold = maxThreshold;
    this.bucketLow = bucketLow;
    this.bucketHigh = bucketHigh;
  }

  @Override
  public boolean shouldCompact(List<SSTable> ssTables) {
    // Check if we have enough SSTables to consider compaction
    if (ssTables.size() < minThreshold) {
      return false;
    }

    // Group SSTables by size and check if any bucket has enough tables
    Map<Long, List<SSTable>> buckets = getBuckets(ssTables);

    for (List<SSTable> bucket : buckets.values()) {
      if (bucket.size() >= minThreshold) {
        return true;
      }
    }

    return false;
  }

  @Override
  public List<SSTable> selectTableToCompact(List<SSTable> ssTables) {
    if (ssTables.size() < minThreshold) {
      logger.info("Not enough SSTables for size-tiered compaction");
      return Collections.emptyList();
    }

    // Group SSTables by size
    Map<Long, List<SSTable>> buckets = getBuckets(ssTables);

    // Find the bucket with the most SSTables
    List<SSTable> selectedBucket = null;
    int maxSize = 0;

    for (List<SSTable> bucket : buckets.values()) {
      if (bucket.size() >= minThreshold && bucket.size() > maxSize) {
        selectedBucket = bucket;
        maxSize = bucket.size();
      }
    }

    if (selectedBucket == null) {
      logger.info("No bucket has enough SSTables for compaction");
      return Collections.emptyList();
    }

    // Limit the number of SSTables to compact
    if (selectedBucket.size() > maxThreshold) {
      // Sort by sequence number (oldest first) and take the oldest maxThreshold tables
      selectedBucket.sort(Comparator.comparingLong(SSTable::getSequenceNumber));
      selectedBucket = selectedBucket.subList(0, maxThreshold);
    }

    // Log the selected tables
    long totalSize = selectedBucket.stream().mapToLong(SSTable::getSizeBytes).sum();
    logger.info(
        String.format(
            "Selected %d SSTables for size-tiered compaction, total size: %d bytes",
            selectedBucket.size(), totalSize));

    return selectedBucket;
  }

  @Override
  public int getCompactionOutputLevel(List<SSTable> tablesToCompact) {
    if (tablesToCompact.isEmpty()) {
      return 0;
    }

    // Find the maximum level among the tables to compact
    int maxLevel = tablesToCompact.stream().mapToInt(SSTable::getLevel).max().orElse(0);

    // The new level is the maximum level + 1
    return maxLevel + 1;
  }

  @Override
  public String getName() {
    return "SizeTieredCompactionStrategy";
  }

  private Map<Long, List<SSTable>> getBuckets(List<SSTable> ssTables) {
    // Sort SSTables by size
    List<SSTable> sortedTables = new ArrayList<>(ssTables);
    sortedTables.sort(Comparator.comparingLong(SSTable::getSizeBytes));

    Map<Long, List<SSTable>> buckets = new HashMap<>();

    for (SSTable table : sortedTables) {
      long size = table.getSizeBytes();
      boolean foundBucket = false;

      // Try to find an existing bucket for this table
      for (Map.Entry<Long, List<SSTable>> entry : buckets.entrySet()) {
        long bucketAvg = entry.getKey();

        // Check if the table's size is within the bucket's range
        if (size >= bucketAvg * bucketLow && size <= bucketAvg * bucketHigh) {
          // Add to this bucket
          List<SSTable> bucket = entry.getValue();
          bucket.add(table);

          // Recalculate the average size for this bucket
          long newAvg = bucket.stream().mapToLong(SSTable::getSizeBytes).sum() / bucket.size();

          // If the average has changed, update the bucket key
          if (newAvg != bucketAvg) {
            buckets.remove(bucketAvg);
            buckets.put(newAvg, bucket);
          }

          foundBucket = true;
          break;
        }
      }

      // If no bucket was found, create a new one
      if (!foundBucket) {
        List<SSTable> newBucket = new ArrayList<>();
        newBucket.add(table);
        buckets.put(size, newBucket);
      }
    }

    return buckets;
  }
}
