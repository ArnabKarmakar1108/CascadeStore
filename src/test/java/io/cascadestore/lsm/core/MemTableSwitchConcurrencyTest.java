package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class MemTableSwitchConcurrencyTest {

  @TempDir Path tempDir;

  @Test
  void concurrentPutsSurviveMemTableRotation() throws Exception {
    // Small memtable forces many rotations; without single-switch guarding this test flakes.
    CascadeConfig config =
        new CascadeConfig(
            8 * 1024,
            tempDir.toString(),
            1_000,
            10_000,
            10_000,
            10_000,
            CompactionStrategyType.THRESHOLD);

    CascadeStore store = new CascadeStore(config);
    try {
      int threadCount = 8;
      int keysPerThread = 200;
      AtomicInteger failedPuts = new AtomicInteger();
      ExecutorService writers = Executors.newFixedThreadPool(threadCount);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threadCount);

      for (int thread = 0; thread < threadCount; thread++) {
        final int writerId = thread;
        writers.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < keysPerThread; i++) {
                  String key = String.format("t%02d-k%05d", writerId, i);
                  byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                  if (!store.put(bytes, bytes)) {
                    failedPuts.incrementAndGet();
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }

      start.countDown();
      assertTrue(done.await(120, TimeUnit.SECONDS));
      writers.shutdownNow();

      assertEquals(0, failedPuts.get(), "all concurrent puts must succeed");

      AtomicInteger misses = new AtomicInteger();
      for (int thread = 0; thread < threadCount; thread++) {
        for (int i = 0; i < keysPerThread; i++) {
          String key = String.format("t%02d-k%05d", thread, i);
          byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
          if (store.get(bytes) == null) {
            misses.incrementAndGet();
          }
        }
      }

      assertEquals(0, misses.get(), "all keys must remain readable after concurrent rotations");
    } finally {
      store.shutdown();
    }
  }
}
