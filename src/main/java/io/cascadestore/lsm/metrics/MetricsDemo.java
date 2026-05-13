package io.cascadestore.lsm.metrics;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/** Runs CascadeStore with metrics enabled and a steady read/write workload. */
public final class MetricsDemo {

  private MetricsDemo() {}

  public static void main(String[] args) throws Exception {
    int metricsPort = intProperty("METRICS_PORT", 9090);
    String dataDir = stringProperty("DATA_DIR", "./data/metrics-demo");
    int memTableKb = intProperty("MEMTABLE_KB", 256);
    int flushSeconds = intProperty("FLUSH_INTERVAL_SECONDS", 3);
    int compactionThreshold = intProperty("COMPACTION_THRESHOLD", 4);

    Path dataPath = Path.of(dataDir);
    if (Files.exists(dataPath)) {
      deleteRecursively(dataPath);
    }
    Files.createDirectories(dataPath);

    CascadeConfig config =
        new CascadeConfig(
                memTableKb * 1024,
                dataDir,
                compactionThreshold,
                0.5,
                1,
                flushSeconds,
                CompactionStrategyType.THRESHOLD,
                8 * 1024 * 1024)
            .withMetricsEnabled(true)
            .withMetricsPort(metricsPort);

    AtomicBoolean running = new AtomicBoolean(true);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

    System.out.println("Starting CascadeStore with metrics on port " + metricsPort);
    System.out.println("Data directory: " + dataDir);
    System.out.println("Dashboard: http://127.0.0.1:" + metricsPort + "/");
    System.out.println("Scrape:   http://127.0.0.1:" + metricsPort + "/metrics");
    System.out.println("Press Ctrl+C to stop.");

    CascadeStore store = new CascadeStore(config);
    int keyCount = 5_000;
    for (int i = 0; i < keyCount; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
      store.put(key, value);
    }

    long iteration = 0;
    while (running.get()) {
      int index = ThreadLocalRandom.current().nextInt(keyCount);
      byte[] key = ("key-" + index).getBytes(StandardCharsets.UTF_8);
      if (iteration % 3 == 0) {
        byte[] value = ("updated-" + iteration).getBytes(StandardCharsets.UTF_8);
        store.put(key, value);
      } else {
        store.get(key);
      }
      iteration++;

      if (iteration % 10_000 == 0) {
        System.out.printf(
            "Workload iteration %d (metrics: http://127.0.0.1:%d/metrics)%n", iteration, metricsPort);
      }

      Thread.sleep(1);
    }

    store.shutdown();
  }

  private static int intProperty(String name, int defaultValue) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  private static String stringProperty(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isEmpty() ? defaultValue : value;
  }

  private static void deleteRecursively(Path root) throws Exception {
    if (!Files.exists(root)) {
      return;
    }
    try (var walk = Files.walk(root)) {
      walk.sorted((a, b) -> b.compareTo(a))
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
