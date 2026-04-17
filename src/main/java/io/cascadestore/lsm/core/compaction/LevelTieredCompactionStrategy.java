package io.cascadestore.lsm.core.compaction;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelTieredCompactionStrategy implements CompactionStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(LevelTieredCompactionStrategy.class);

  private static final int DEFAULT_L0_COMPACTION_TRIGGER = 4;
  private static final int DEFAULT_MAX_L0_COMPACTION_FILES = 4;
  private static final long DEFAULT_BASE_LEVEL_SIZE_BYTES = 10L * 1024 * 1024;
  private static final int DEFAULT_LEVEL_SIZE_MULTIPLIER = 10;
  private static final int DEFAULT_MAX_LEVELS = 7;

  private final CascadeConfig config;
  private final int l0CompactionTrigger;
  private final int maxL0CompactionFiles;
  private final long baseLevelSizeBytes;
  private final int levelSizeMultiplier;
  private final int maxLevels;

  public LevelTieredCompactionStrategy(CascadeConfig config) {
    this(
        config,
        DEFAULT_L0_COMPACTION_TRIGGER,
        DEFAULT_MAX_L0_COMPACTION_FILES,
        DEFAULT_BASE_LEVEL_SIZE_BYTES,
        DEFAULT_LEVEL_SIZE_MULTIPLIER,
        DEFAULT_MAX_LEVELS);
  }

  public LevelTieredCompactionStrategy(
      CascadeConfig config,
      int l0CompactionTrigger,
      int maxL0CompactionFiles,
      long baseLevelSizeBytes,
      int levelSizeMultiplier,
      int maxLevels) {
    this.config = config;
    this.l0CompactionTrigger = l0CompactionTrigger;
    this.maxL0CompactionFiles = maxL0CompactionFiles;
    this.baseLevelSizeBytes = baseLevelSizeBytes;
    this.levelSizeMultiplier = levelSizeMultiplier;
    this.maxLevels = maxLevels;
  }

  @Override
  public boolean shouldCompact(List<SSTable> ssTables) {
    if (countAtLevel(ssTables, 0) >= l0CompactionTrigger) {
      return true;
    }

    Map<Integer, List<SSTable>> byLevel = groupByLevel(ssTables);
    for (Map.Entry<Integer, List<SSTable>> entry : byLevel.entrySet()) {
      int level = entry.getKey();
      if (level >= 1 && totalSizeBytes(entry.getValue()) >= levelBudgetBytes(level)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<SSTable> selectTableToCompact(List<SSTable> ssTables) {
    List<SSTable> l0Tables = tablesAtLevel(ssTables, 0);
    if (l0Tables.size() >= l0CompactionTrigger) {
      l0Tables.sort(Comparator.comparingLong(SSTable::getSequenceNumber));
      int count = Math.min(maxL0CompactionFiles, l0Tables.size());
      List<SSTable> selected =
          expandL0SelectionWithOverlappingL1(
              ssTables, new ArrayList<>(l0Tables.subList(0, count)));
      logger.info(
          "Selected {} SSTables for L0 -> L1 level-tiered compaction ({} L0, {} overlapping L1)",
          selected.size(),
          count,
          selected.size() - count);
      return selected;
    }

    for (int level = 1; level < maxLevels; level++) {
      List<SSTable> atLevel = tablesAtLevel(ssTables, level);
      if (atLevel.isEmpty() || totalSizeBytes(atLevel) < levelBudgetBytes(level)) {
        continue;
      }

      SSTable picked =
          atLevel.stream()
              .max(Comparator.comparingLong(SSTable::getSizeBytes))
              .orElse(null);
      if (picked == null) {
        continue;
      }

      List<SSTable> selected = new ArrayList<>();
      selected.add(picked);

      int nextLevel = level + 1;
      if (nextLevel <= maxLevels) {
        for (SSTable candidate : tablesAtLevel(ssTables, nextLevel)) {
          if (picked.overlaps(candidate)) {
            selected.add(candidate);
          }
        }
      }

      logger.info(
          "Selected {} SSTables for L{}→L{} level-tiered compaction",
          selected.size(),
          level,
          nextLevel);
      return selected;
    }

    logger.info("No SSTables selected for level-tiered compaction");
    return List.of();
  }

  @Override
  public int getCompactionOutputLevel(List<SSTable> tablesToCompact) {
    if (tablesToCompact.isEmpty()) {
      return 0;
    }

    boolean includesL0 = tablesToCompact.stream().anyMatch(t -> t.getLevel() == 0);
    if (includesL0) {
      return Math.min(1, maxLevels);
    }

    int sourceLevel =
        tablesToCompact.stream().mapToInt(SSTable::getLevel).min().orElse(0);
    return Math.min(sourceLevel + 1, maxLevels);
  }

  /**
   * Adds every L1 SSTable whose key range overlaps the L0 batch span or any table already selected.
   * Repeats until the set stabilizes so overlapping L1 siblings are included together.
   */
  private List<SSTable> expandL0SelectionWithOverlappingL1(
      List<SSTable> ssTables, List<SSTable> selected) {
    Set<SSTable> chosen = new HashSet<>(selected);
    boolean expanded;

    do {
      expanded = false;
      byte[] spanMin = null;
      byte[] spanMax = null;

      for (SSTable table : chosen) {
        byte[] tableMin = table.getMinKey();
        byte[] tableMax = table.getMaxKey();
        if (tableMin == null || tableMax == null) {
          continue;
        }
        spanMin = spanMin == null ? tableMin : minKey(spanMin, tableMin);
        spanMax = spanMax == null ? tableMax : maxKey(spanMax, tableMax);
      }

      if (spanMin == null || spanMax == null) {
        break;
      }

      for (SSTable candidate : tablesAtLevel(ssTables, 1)) {
        if (chosen.contains(candidate)) {
          continue;
        }
        boolean overlapsSpan =
            SSTable.keyRangesOverlap(
                spanMin, spanMax, candidate.getMinKey(), candidate.getMaxKey());
        boolean overlapsSelected =
            chosen.stream().anyMatch(existing -> existing.overlaps(candidate));
        if (overlapsSpan || overlapsSelected) {
          chosen.add(candidate);
          expanded = true;
        }
      }
    } while (expanded);

    List<SSTable> ordered = new ArrayList<>(selected);
    tablesAtLevel(ssTables, 1).stream()
        .filter(chosen::contains)
        .sorted(Comparator.comparingLong(SSTable::getSequenceNumber))
        .forEach(ordered::add);
    return ordered;
  }

  private static byte[] minKey(byte[] left, byte[] right) {
    return new ByteArrayWrapper(left).compareTo(new ByteArrayWrapper(right)) <= 0
        ? left
        : right;
  }

  private static byte[] maxKey(byte[] left, byte[] right) {
    return new ByteArrayWrapper(left).compareTo(new ByteArrayWrapper(right)) >= 0
        ? left
        : right;
  }

  @Override
  public String getName() {
    return "LevelTieredCompactionStrategy";
  }

  long levelBudgetBytes(int level) {
    if (level < 1) {
      return Long.MAX_VALUE;
    }
    long budget = baseLevelSizeBytes;
    for (int i = 1; i < level; i++) {
      budget *= levelSizeMultiplier;
    }
    return budget;
  }

  private static Map<Integer, List<SSTable>> groupByLevel(List<SSTable> ssTables) {
    Map<Integer, List<SSTable>> byLevel = new HashMap<>();
    for (SSTable table : ssTables) {
      byLevel.computeIfAbsent(table.getLevel(), k -> new ArrayList<>()).add(table);
    }
    return byLevel;
  }

  private static List<SSTable> tablesAtLevel(List<SSTable> ssTables, int level) {
    List<SSTable> result = new ArrayList<>();
    for (SSTable table : ssTables) {
      if (table.getLevel() == level) {
        result.add(table);
      }
    }
    return result;
  }

  private static int countAtLevel(List<SSTable> ssTables, int level) {
    int count = 0;
    for (SSTable table : ssTables) {
      if (table.getLevel() == level) {
        count++;
      }
    }
    return count;
  }

  private static long totalSizeBytes(List<SSTable> tables) {
    long total = 0;
    for (SSTable table : tables) {
      total += table.getSizeBytes();
    }
    return total;
  }
}
