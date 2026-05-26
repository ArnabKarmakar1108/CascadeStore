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

class StorageVersionRetainTest {

  @TempDir Path tempDir;

  @Test
  void readsSurviveLayoutPublishWhileRetained() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            8 * 1024,
            tempDir.toString(),
            2,
            0.05,
            10_000,
            5,
            CompactionStrategyType.LEVEL_TIERED);

    CascadeStore store = new CascadeStore(config);
    try {
      for (int i = 0; i < 500; i++) {
        byte[] key = ("retain-key-" + i).getBytes(StandardCharsets.UTF_8);
        assertTrue(store.put(key, key));
      }

      AtomicInteger decodeErrors = new AtomicInteger();
      CountDownLatch readersReady = new CountDownLatch(8);
      CountDownLatch startReads = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(8);

      for (int t = 0; t < 8; t++) {
        pool.submit(
            () -> {
              readersReady.countDown();
              try {
                startReads.await();
                for (int round = 0; round < 200; round++) {
                  for (int i = 0; i < 500; i++) {
                    byte[] key = ("retain-key-" + i).getBytes(StandardCharsets.UTF_8);
                    byte[] value = store.get(key);
                    if (value == null || value.length != key.length) {
                      decodeErrors.incrementAndGet();
                    }
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
      }

      assertTrue(readersReady.await(10, TimeUnit.SECONDS));
      startReads.countDown();

      for (int rotation = 0; rotation < 20; rotation++) {
        for (int i = 0; i < 100; i++) {
          byte[] key = ("rotate-" + rotation + "-" + i).getBytes(StandardCharsets.UTF_8);
          assertTrue(store.put(key, key));
        }
        store.switchMemTableForTest();
      }

      pool.shutdown();
      assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS));
      assertEquals(0, decodeErrors.get(), "reads must not observe torn SSTable bytes");
      assertNotNull(store.get("retain-key-0".getBytes(StandardCharsets.UTF_8)));
    } finally {
      store.shutdown();
    }
  }
}
