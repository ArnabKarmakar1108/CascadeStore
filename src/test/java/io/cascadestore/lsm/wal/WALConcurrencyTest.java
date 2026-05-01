package io.cascadestore.lsm.wal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WALConcurrencyTest {

  @TempDir java.nio.file.Path tempDir;

  private WAL wal;

  @AfterEach
  void tearDown() throws IOException {
    if (wal != null) {
      wal.close();
    }
  }

  @Test
  void concurrentAppendAndTruncateDoesNotThrow() throws Exception {
    wal = new WALImpl(tempDir.toString());
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);

    for (int i = 0; i < 4; i++) {
      final int thread = i;
      executor.submit(
          () -> {
            try {
              start.await();
              for (int j = 0; j < 500; j++) {
                byte[] key = ("key-" + thread + "-" + j).getBytes();
                byte[] value = ("value-" + j).getBytes();
                wal.appendPutRecord(key, value, 0);
                if (j % 25 == 0) {
                  wal.deleteAllLogs();
                }
              }
            } catch (Exception e) {
              failed.set(true);
              throw new RuntimeException(e);
            }
          });
    }

    start.countDown();
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    assertDoesNotThrow(() -> {});
    assertTrue(!failed.get(), "concurrent WAL append/truncate failed");
  }
}
