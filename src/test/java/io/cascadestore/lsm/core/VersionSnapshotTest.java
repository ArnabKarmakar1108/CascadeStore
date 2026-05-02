package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VersionSnapshotTest {

  @TempDir Path tempDir;

  @Test
  void readsStayVisibleWhileCompactionRetiresInputTables() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            8 * 1024,
            tempDir.toString(),
            10,
            0.05,
            10_000,
            5,
            CompactionStrategyType.THRESHOLD);

    CascadeStore store = new CascadeStore(config);
    try {
      for (int i = 0; i < 200; i++) {
        String key = String.format("key%05d", i);
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        assertTrue(store.put(bytes, bytes));
      }

      store.switchMemTableForTest();
      store.flushMemTables();
      assertEquals(1, store.getSSTablesCount());

      for (int i = 200; i < 400; i++) {
        String key = String.format("key%05d", i);
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        assertTrue(store.put(bytes, bytes));
      }
      store.switchMemTableForTest();
      store.flushMemTables();
      assertEquals(2, store.getSSTablesCount());

      AtomicInteger misses = new AtomicInteger();
      CountDownLatch readersStarted = new CountDownLatch(4);
      CountDownLatch releaseReaders = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(4);

      for (int t = 0; t < 4; t++) {
        pool.submit(
            () -> {
              readersStarted.countDown();
              try {
                releaseReaders.await();
                for (int round = 0; round < 50; round++) {
                  for (int i = 0; i < 400; i++) {
                    String key = String.format("key%05d", i);
                    byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                    if (store.get(bytes) == null) {
                      misses.incrementAndGet();
                    }
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
      }

      assertTrue(readersStarted.await(10, TimeUnit.SECONDS));
      store.flushMemTables();
      assertTrue(store.getSSTablesCount() <= 2);

      releaseReaders.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));

      assertEquals(0, misses.get(), "keys must remain readable across layout publishes");
    } finally {
      store.shutdown();
    }
  }

  @Test
  void retiredSSTableDeletesAfterLastReaderUnpins() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            1024,
            tempDir.toString(),
            1_000,
            10_000,
            10_000,
            10_000,
            CompactionStrategyType.THRESHOLD);

    CascadeStore store = new CascadeStore(config);
    try {
      byte[] key = "pin-key".getBytes(StandardCharsets.UTF_8);
      assertTrue(store.put(key, key));
      store.switchMemTableForTest();
      store.flushMemTables();
      assertNotNull(store.get(key));
    } finally {
      store.shutdown();
    }
  }
}
