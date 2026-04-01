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

class CascadeWithSizeTieredCompactionTest {
  private CascadeStore store;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    // Use a temporary directory for testing with a small MemTable size to force frequent flushing
    // and the SIZE_TIERED compaction strategy
    store =
        new CascadeStore(
            32 * 1024, // 32KB MemTable size
            tempDir.toString(),
            4, // Compact after 4 SSTables
            CompactionStrategyType.SIZE_TIERED);
  }

  @AfterEach
  void tearDown() {
    // Ensure proper cleanup
    if (store != null) {
      store.shutdown();
    }
  }

  @Test
  void testSizeTieredCompaction() throws IOException, InterruptedException {
    // Create a small set of key-value pairs
    for (int i = 0; i < 10; i++) {
      String key = "key-" + i;
      String value = "value-" + i;

      // Put the key-value pair
      assertTrue(store.put(key.getBytes(), value.getBytes()));

      // Verify that the key-value pair was stored correctly
      assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
    }

    // Verify that all keys can be retrieved
    for (int i = 0; i < 10; i++) {
      String key = "key-" + i;
      String value = "value-" + i;

      assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
    }
  }

  @Test
  void testUpdatesWithSizeTieredCompaction() throws IOException, InterruptedException {
    // Write initial data
    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String value = "initial-value-" + i;
      assertTrue(store.put(key.getBytes(), value.getBytes()));
    }

    // Update the keys with new values
    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String value = "updated-value-" + i;
      assertTrue(store.put(key.getBytes(), value.getBytes()));
    }

    // Verify that all keys have their latest values
    for (int i = 0; i < 10; i++) {
      String key = "update-key-" + i;
      String expectedValue = "updated-value-" + i;
      assertArrayEquals(
          expectedValue.getBytes(),
          store.get(key.getBytes()),
          "Key " + key + " should have its updated value");
    }
  }
}
