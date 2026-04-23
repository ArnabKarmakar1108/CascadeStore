package io.cascadestore.lsm.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import org.junit.jupiter.api.Test;

class CascadeConfigTest {

  private static CascadeConfig config(double compactionIntervalMinutes) {
    return new CascadeConfig(
        1024 * 1024, "/tmp/data", 4, compactionIntervalMinutes, 1, 10, CompactionStrategyType.THRESHOLD);
  }

  @Test
  void compactionIntervalTreatsValuesAtOrAboveOneAsMinutes() {
    assertEquals(1800L, config(30).compactionInterval().period());
    assertEquals(600L, config(10).compactionInterval().period());
  }

  @Test
  void compactionIntervalTreatsValuesBelowOneAsSeconds() {
    assertEquals(1L, config(0.17).compactionInterval().period());
    assertEquals(1L, config(0.5).compactionInterval().period());
  }
}
