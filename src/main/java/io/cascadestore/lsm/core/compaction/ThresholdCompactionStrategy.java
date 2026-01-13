package io.cascadestore.lsm.core.compaction;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThresholdCompactionStrategy implements CompactionStrategy {
  private static final Logger logger = LoggerFactory.getLogger(ThresholdCompactionStrategy.class);

  private final CascadeConfig config;

  public ThresholdCompactionStrategy(CascadeConfig config) {
    this.config = config;
  }

  @Override
  public boolean shouldCompact(List<SSTable> ssTables) {
    return ssTables.size() >= config.compactionThreshold();
  }

  @Override
  public List<SSTable> selectTableToCompact(List<SSTable> ssTables) {
    // Group SSTables by level
    Map<Integer, List<SSTable>> sstablesByLevel = new HashMap<>();
    for (SSTable ssTable : ssTables) {
      int level = ssTable.getLevel();
      sstablesByLevel.computeIfAbsent(level, k -> new ArrayList<>()).add(ssTable);
    }

    // Find the level with the most SSTables
    int levelToCompact = 0;
    int maxSSTables = 0;
    for (Map.Entry<Integer, List<SSTable>> entry : sstablesByLevel.entrySet()) {
      if (entry.getValue().size() > maxSSTables) {
        maxSSTables = entry.getValue().size();
        levelToCompact = entry.getKey();
      }
    }

    // Skip if the level doesn't have enough SSTables to compact
    List<SSTable> tablesToCompact = sstablesByLevel.get(levelToCompact);
    if (tablesToCompact == null || tablesToCompact.size() < 2) {
      logger.info("Skipping compaction, not enough SSTables at any level");
      return new ArrayList<>();
    }

    logger.info(
        String.format(
            "Selected %d SSTables at level %d for compaction",
            tablesToCompact.size(), levelToCompact));

    return tablesToCompact;
  }

  @Override
  public int getCompactionOutputLevel(List<SSTable> tablesToCompact) {
    // If there are no tables to compact, return level 0
    if (tablesToCompact.isEmpty()) {
      return 0;
    }

    // Get the level of the first table (they should all be at the same level)
    int currentLevel = tablesToCompact.get(0).getLevel();

    // The new level is the current level + 1
    return currentLevel + 1;
  }

  @Override
  public String getName() {
    return "ThresholdCompactionStrategy";
  }
}
