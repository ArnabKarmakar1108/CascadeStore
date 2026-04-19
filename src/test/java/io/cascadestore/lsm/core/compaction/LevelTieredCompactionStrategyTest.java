package io.cascadestore.lsm.core.compaction;

import static org.junit.jupiter.api.Assertions.*;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LevelTieredCompactionStrategyTest {

  @TempDir Path tempDir;

  private CascadeConfig config;
  private LevelTieredCompactionStrategy strategy;
  private List<SSTable> ssTables;
  private AtomicLong sequenceNumber;

  @BeforeEach
  void setUp() {
    config =
        new CascadeConfig(
            1024 * 1024,
            tempDir.toString(),
            4,
            30,
            1,
            10,
            CompactionStrategyType.LEVEL_TIERED);
    strategy =
        new LevelTieredCompactionStrategy(
            config,
            4,
            4,
            2_000,
            10,
            7);
    ssTables = new ArrayList<>();
    sequenceNumber = new AtomicLong(0);
  }

  @AfterEach
  void tearDown() {
    for (SSTable ssTable : ssTables) {
      try {
        ssTable.close();
        ssTable.delete();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test
  void shouldNotCompactWhenL0BelowTriggerAndLevelsWithinBudget() throws IOException {
    createL0Tables(3, "a");
    assertFalse(strategy.shouldCompact(ssTables));
  }

  @Test
  void shouldCompactWhenL0AtTrigger() throws IOException {
    createL0Tables(4, "a");
    assertTrue(strategy.shouldCompact(ssTables));
  }

  @Test
  void selectL0TablesOldestFirstUpToCap() throws IOException {
    createL0Tables(6, "a");

    List<SSTable> selected = strategy.selectTableToCompact(ssTables);

    assertEquals(4, selected.size());
    assertTrue(selected.stream().allMatch(t -> t.getLevel() == 0));
    assertEquals(0, selected.get(0).getSequenceNumber());
    assertEquals(3, selected.get(3).getSequenceNumber());
    assertEquals(1, strategy.getCompactionOutputLevel(selected));
  }

  @Test
  void shouldCompactWhenLevelExceedsByteBudget() throws IOException {
    createTableAtLevel(1, "a", 30);
    createTableAtLevel(1, "b", 30);

    assertTrue(strategy.shouldCompact(ssTables));
  }

  @Test
  void selectLevelCompactionIncludesOverlappingNextLevel() throws IOException {
    createTableAtLevel(1, "a", 40);
    createTableAtLevel(2, "a", 10);
    createTableAtLevel(2, "m", 10);

    List<SSTable> selected = strategy.selectTableToCompact(ssTables);

    assertEquals(2, selected.size());
    assertEquals(1, selected.get(0).getLevel());
    assertTrue(selected.stream().anyMatch(t -> t.getLevel() == 2 && overlapsPrefix(t, "a")));
    assertFalse(selected.stream().anyMatch(t -> overlapsPrefix(t, "m")));
    assertEquals(2, strategy.getCompactionOutputLevel(selected));
  }

  @Test
  void disjointRangesAtSameLevelDoNotFalsePositiveOverlap() throws IOException {
    SSTable left = createTableAtLevel(1, "a", 20);
    SSTable right = createTableAtLevel(1, "m", 20);

    assertFalse(left.overlaps(right));
  }

  @Test
  void levelBudgetScalesWithMultiplier() {
    assertEquals(2_000, strategy.levelBudgetBytes(1));
    assertEquals(20_000, strategy.levelBudgetBytes(2));
    assertEquals(200_000, strategy.levelBudgetBytes(3));
  }

  @Test
  void getCompactionOutputLevelRespectsMaxLevels() throws IOException {
    LevelTieredCompactionStrategy capped =
        new LevelTieredCompactionStrategy(config, 4, 4, 2_000, 10, 2);
    createTableAtLevel(1, "a", 40);

    List<SSTable> selected = capped.selectTableToCompact(ssTables);

    assertFalse(selected.isEmpty());
    assertEquals(2, capped.getCompactionOutputLevel(selected));
  }

  @Test
  void emptySelectionReturnsZeroOutputLevel() {
    assertEquals(0, strategy.getCompactionOutputLevel(List.of()));
    assertTrue(strategy.selectTableToCompact(List.of()).isEmpty());
  }

  @Test
  void l0CompactionTakesPriorityOverLevelBudget() throws IOException {
    createL0Tables(4, "a");
    createTableAtLevel(1, "z", 50);

    List<SSTable> selected = strategy.selectTableToCompact(ssTables);

    assertTrue(selected.stream().allMatch(t -> t.getLevel() == 0));
  }

  private void createL0Tables(int count, String prefix) throws IOException {
    for (int i = 0; i < count; i++) {
      createTableAtLevel(0, prefix, 5);
    }
  }

  private SSTable createTableAtLevel(int level, String prefix, int entryCount)
      throws IOException {
    MemTable memTable = new MemTable(config.memTableMaxSizeBytes());
    for (int j = 0; j < entryCount; j++) {
      String key = prefix + "-key-" + j;
      String value = "value-" + prefix + "-" + j + "-" + "x".repeat(40);
      memTable.put(key.getBytes(), value.getBytes(), 0);
    }
    SSTable ssTable =
        new SSTable(memTable, config.dataDirectory(), level, sequenceNumber.getAndIncrement());
    ssTables.add(ssTable);
    memTable.close();
    return ssTable;
  }

  private static boolean overlapsPrefix(SSTable table, String prefix) {
    byte[] min = table.getMinKey();
    return min != null && new String(min).startsWith(prefix);
  }
}
