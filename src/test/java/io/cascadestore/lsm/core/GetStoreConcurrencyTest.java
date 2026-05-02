package io.cascadestore.lsm.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

class GetStoreConcurrencyTest {

  @TempDir Path tempDir;

  @Test
  void concurrentReadOnlyAfterFlush() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            32 * 1024,
            tempDir.resolve("read-only").toString(),
            1_000,
            10_000,
            10_000,
            10_000,
            CompactionStrategyType.THRESHOLD);

    CascadeStore store = new CascadeStore(config);
    try {
      int keyCount = 500;
      for (int i = 0; i < keyCount; i++) {
        String key = String.format("key%05d", i);
        store.put(key.getBytes(StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8));
      }
      store.flushMemTables();

      AtomicInteger misses = new AtomicInteger();
      ExecutorService readers = Executors.newFixedThreadPool(4);
      CountDownLatch done = new CountDownLatch(4);
      for (int thread = 0; thread < 4; thread++) {
        readers.submit(
            () -> {
              try {
                for (int round = 0; round < 100; round++) {
                  for (int i = 0; i < keyCount; i++) {
                    String key = String.format("key%05d", i);
                    if (store.get(key.getBytes(StandardCharsets.UTF_8)) == null) {
                      misses.incrementAndGet();
                    }
                  }
                }
              } finally {
                done.countDown();
              }
            });
      }

      assertEquals(true, done.await(60, TimeUnit.SECONDS));
      readers.shutdownNow();
      assertEquals(0, misses.get());
    } finally {
      store.shutdown();
    }
  }

  @Test
  void concurrentReadsRemainVisibleWhileMemTablesFlush() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            32 * 1024,
            tempDir.toString(),
            1_000,
            10_000,
            10_000,
            10_000,
            CompactionStrategyType.THRESHOLD);

    CascadeStore store = new CascadeStore(config);
    try {
      int keyCount = 500;
      for (int i = 0; i < keyCount; i++) {
        String key = String.format("key%05d", i);
        store.put(key.getBytes(StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8));
      }

      AtomicInteger misses = new AtomicInteger();
      ExecutorService readers = Executors.newFixedThreadPool(4);
      ExecutorService writers = Executors.newFixedThreadPool(2);
      CountDownLatch done = new CountDownLatch(6);

      for (int thread = 0; thread < 4; thread++) {
        readers.submit(
            () -> {
              try {
                for (int round = 0; round < 50; round++) {
                  for (int i = 0; i < keyCount; i++) {
                    String key = String.format("key%05d", i);
                    if (store.get(key.getBytes(StandardCharsets.UTF_8)) == null) {
                      misses.incrementAndGet();
                    }
                  }
                }
              } finally {
                done.countDown();
              }
            });
      }

      for (int thread = 0; thread < 2; thread++) {
        final int writerId = thread;
        writers.submit(
            () -> {
              try {
                for (int round = 0; round < 50; round++) {
                  String key = String.format("writer%02d-%05d", writerId, round);
                  store.put(key.getBytes(StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8));
                }
              } finally {
                done.countDown();
              }
            });
      }

      assertEquals(true, done.await(120, TimeUnit.SECONDS));
      readers.shutdownNow();
      writers.shutdownNow();
      assertEquals(0, misses.get(), "keys must stay visible while immutable memtables flush");
    } finally {
      store.shutdown();
    }
  }
}
