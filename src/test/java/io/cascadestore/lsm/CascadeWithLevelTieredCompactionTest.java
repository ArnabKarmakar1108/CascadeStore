package io.cascadestore.lsm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CascadeWithLevelTieredCompactionTest {
  private CascadeStore store;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    store =
        new CascadeStore(
            32 * 1024,
            tempDir.toString(),
            4,
            CompactionStrategyType.LEVEL_TIERED);
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.shutdown();
    }
  }

  @Test
  void testLevelTieredCompaction() throws IOException, InterruptedException {
    for (int i = 0; i < 10; i++) {
      String key = "key-" + i;
      String value = "value-" + i;
      assertTrue(store.put(key.getBytes(), value.getBytes()));
      assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
    }

    for (int i = 0; i < 10; i++) {
      String key = "key-" + i;
      String value = "value-" + i;
      assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
    }
  }

  @Test
  void testUpdatesWithLevelTieredCompaction() throws IOException, InterruptedException {
    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String value = "initial-value-" + i;
      assertTrue(store.put(key.getBytes(), value.getBytes()));
    }

    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String value = "updated-value-" + i;
      assertTrue(store.put(key.getBytes(), value.getBytes()));
    }

    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String expectedValue = "updated-value-" + i;
      assertArrayEquals(expectedValue.getBytes(), store.get(key.getBytes()));
    }
  }
}
