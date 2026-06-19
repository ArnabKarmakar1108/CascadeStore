package io.cascadestore.lsm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import io.cascadestore.lsm.metrics.AmplificationSnapshot;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

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
  void exposesPrometheusEndpointWhenEnabled() throws Exception {
    // Bind an OS-assigned ephemeral port (0) so the server itself claims a free port atomically.
    // Pre-selecting a port and rebinding races with other processes under CI load.
    CascadeConfig config =
        new CascadeConfig(1024 * 1024, tempDir.toString(), 4, 30, 1, 10, CompactionStrategyType.THRESHOLD)
            .withMetricsEnabled(true)
            .withMetricsPort(0);

    store = new CascadeStore(config);
    assertTrue(store.metrics().isEnabled());
    int port = store.metricsPort();
    assertTrue(port > 0, "metrics server should bind an ephemeral port");

    byte[] key = "metrics-key".getBytes();
    byte[] value = "metrics-value".getBytes();
    assertTrue(store.put(key, value));
    assertArrayEquals(value, store.get(key));

    String body = scrapeMetrics(port);
    assertTrue(body.contains("cascadestore_read_operations_total"));
    assertTrue(body.contains("cascadestore_user_write_bytes_total"));
    assertTrue(body.contains("cascadestore_memtable_bytes"));
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

  private static void assertArrayEquals(byte[] expected, byte[] actual) {
    assertEquals(new String(expected, StandardCharsets.UTF_8), new String(actual, StandardCharsets.UTF_8));
  }

  private static String scrapeMetrics(int port) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:" + port + "/metrics").openConnection();
    connection.setConnectTimeout(2_000);
    connection.setReadTimeout(2_000);
    connection.connect();
    try (var input = connection.getInputStream()) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
