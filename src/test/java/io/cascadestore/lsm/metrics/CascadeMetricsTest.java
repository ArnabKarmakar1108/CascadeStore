package io.cascadestore.lsm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.cascadestore.lsm.core.store.CascadeStore;
import io.cascadestore.lsm.metrics.AmplificationSnapshot;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CascadeMetricsTest {

  private CascadeStore store;

  @TempDir Path tempDir;

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.shutdown();
    }
  }

  @Test
  void metricsDisabledByDefault() {
    store = new CascadeStore(1024 * 1024, tempDir.toString(), 4);
    assertFalse(store.metrics().isEnabled());
    assertEquals(-1, store.metricsPort());
  }

  @Test
  void snapshotReportsCounterTotals() {
    CascadeMetrics metrics = CascadeMetrics.create();
    metrics.recordRead(3, 2, 1);
    metrics.recordUserWriteBytes(100);
    metrics.recordFlush(1_000_000, 400);

    AmplificationSnapshot snapshot = metrics.snapshot(250, 4);
    assertEquals(1, snapshot.readOperations());
    assertEquals(3, snapshot.sstableLookups());
    assertEquals(2, snapshot.bloomProbes());
    assertEquals(1, snapshot.bloomNegatives());
    assertEquals(100, snapshot.userWriteBytes());
    assertEquals(400, snapshot.sstableBytesWritten());
    assertEquals(0, snapshot.compactions());
    assertEquals(250, snapshot.liveSstableDataBytes());
    assertEquals(4, snapshot.liveSstableCount());
    assertEquals(3.0, snapshot.readAmplification(), 0.0001);
    assertEquals(2.0, snapshot.filesProbedAmplification(), 0.0001);
  }
}
