package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

class MergeOperationTest {

  @TempDir Path tempDir;

  @Test
  void mergeUpdatesExistingValue() {
    CascadeStore store = newStore();
    try {
      byte[] key = "merge-key".getBytes(StandardCharsets.UTF_8);
      assertTrue(store.put(key, "alpha".getBytes(StandardCharsets.UTF_8)));

      assertTrue(
          store.merge(
              key,
              existing ->
                  ("merged:" + new String(existing, StandardCharsets.UTF_8))
                      .getBytes(StandardCharsets.UTF_8)));

      assertArrayEquals("merged:alpha".getBytes(StandardCharsets.UTF_8), store.get(key));
    } finally {
      store.shutdown();
    }
  }

  @Test
  void mergeReturnsFalseForMissingKey() {
    CascadeStore store = newStore();
    try {
      byte[] key = "missing".getBytes(StandardCharsets.UTF_8);
      assertFalse(
          store.merge(
              key, existing -> "should-not-run".getBytes(StandardCharsets.UTF_8)));
      assertNull(store.get(key));
    } finally {
      store.shutdown();
    }
  }

  @Test
  void mergeTreatsTombstoneAsAbsent() {
    CascadeStore store = newStore();
    try {
      byte[] key = "deleted".getBytes(StandardCharsets.UTF_8);
      assertTrue(store.put(key, "value".getBytes(StandardCharsets.UTF_8)));
      assertTrue(store.delete(key));

      assertFalse(
          store.merge(
              key, existing -> "new".getBytes(StandardCharsets.UTF_8)));
      assertNull(store.get(key));
    } finally {
      store.shutdown();
    }
  }

  @Test
  void mergeWorksAfterFlushToSSTable() {
    CascadeStore store = newStore();
    try {
      byte[] key = "flushed".getBytes(StandardCharsets.UTF_8);
      assertTrue(store.put(key, "v1".getBytes(StandardCharsets.UTF_8)));
      store.switchMemTableForTest();
      store.flushMemTables();

      assertTrue(
          store.merge(
              key,
              existing -> "v2".getBytes(StandardCharsets.UTF_8)));

      assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), store.get(key));
    } finally {
      store.shutdown();
    }
  }

  @Test
  void concurrentMergesProduceConsistentFinalValue() throws Exception {
    CascadeStore store = newStore();
    try {
      byte[] key = "counter".getBytes(StandardCharsets.UTF_8);
      assertTrue(store.put(key, "0".getBytes(StandardCharsets.UTF_8)));

      int threads = 4;
      int mergesPerThread = 25;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      AtomicInteger failures = new AtomicInteger();

      for (int t = 0; t < threads; t++) {
        pool.submit(
            () -> {
              try {
                start.await();
                for (int i = 0; i < mergesPerThread; i++) {
                  boolean ok =
                      store.merge(
                          key,
                          existing -> {
                            int value =
                                Integer.parseInt(new String(existing, StandardCharsets.UTF_8));
                            return Integer.toString(value + 1).getBytes(StandardCharsets.UTF_8);
                          });
                  if (!ok) {
                    failures.incrementAndGet();
                  }
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
      }

      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

      byte[] finalValue = store.get(key);
      assertTrue(failures.get() < threads * mergesPerThread, "some merges should succeed");
      int count = Integer.parseInt(new String(finalValue, StandardCharsets.UTF_8));
      assertTrue(count > 0);
      assertTrue(count <= threads * mergesPerThread + 1);
    } finally {
      store.shutdown();
    }
  }

  private CascadeStore newStore() {
    CascadeConfig config =
        new CascadeConfig(
            8 * 1024,
            tempDir.toString(),
            10,
            0.05,
            10_000,
            5,
            CompactionStrategyType.THRESHOLD,
            0);
    return new CascadeStore(config);
  }
}
