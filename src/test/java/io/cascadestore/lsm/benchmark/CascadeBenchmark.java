package io.cascadestore.lsm.benchmark;

import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.core.store.CascadeStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class CascadeBenchmark {

  private CascadeStore store;
  private Path tempDir;
  private byte[][] keys;
  private byte[][] values;
  private Random random;

  private static final int DATA_SIZE = 10_000;
  private static final int KEY_SIZE = 16;
  private static final int VALUE_SIZE = 100;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // Create a temporary directory for the CascadeStore
    tempDir = Files.createTempDirectory("lsm-benchmark");

    // Initialize the CascadeStore with 1MB MemTable size and compaction threshold of 4
    store = new CascadeStore(1024 * 1024, tempDir.toString(), 4);

    // Initialize random number generator
    random = new Random(42); // Fixed seed for reproducibility

    // Generate random keys and values
    keys = new byte[DATA_SIZE][];
    values = new byte[DATA_SIZE][];

    for (int i = 0; i < DATA_SIZE; i++) {
      keys[i] = generateRandomBytes(KEY_SIZE);
      values[i] = generateRandomBytes(VALUE_SIZE);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // Shutdown the store
    if (store != null) {
      store.shutdown();
    }

    // Clean up the temporary directory
    try {
      Files.walk(tempDir)
          .sorted((a, b) -> -a.compareTo(b)) // Reverse order to delete files before directories
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  System.err.println("Failed to delete: " + path);
                }
              });
    } catch (IOException e) {
      System.err.println("Failed to clean up temporary directory: " + e.getMessage());
    }
  }

  @Benchmark
  public void benchmarkPut(Blackhole blackhole) {
    int index = random.nextInt(DATA_SIZE);
    boolean result = store.put(keys[index], values[index]);
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkGet(Blackhole blackhole) {
    int index = random.nextInt(DATA_SIZE);
    byte[] result = store.get(keys[index]);
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkDelete(Blackhole blackhole) {
    int index = random.nextInt(DATA_SIZE);
    boolean result = store.delete(keys[index]);
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkGetRange(Blackhole blackhole) {
    int startIndex = random.nextInt(DATA_SIZE - 100);
    int endIndex = startIndex + 100;

    Map<byte[], byte[]> result = store.getRange(keys[startIndex], keys[endIndex]);
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkGetIterator(Blackhole blackhole) {
    int startIndex = random.nextInt(DATA_SIZE - 100);
    int endIndex = startIndex + 100;

    try (KeyValueIterator iterator = store.getIterator(keys[startIndex], keys[endIndex])) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        blackhole.consume(entry);
      }
    }
  }

  @Benchmark
  public void benchmarkSequentialPut(Blackhole blackhole) {
    for (int i = 0; i < 100; i++) {
      byte[] key = generateRandomBytes(KEY_SIZE);
      byte[] value = generateRandomBytes(VALUE_SIZE);
      boolean result = store.put(key, value);
      blackhole.consume(result);
    }
  }

  @Benchmark
  public void benchmarkPutWithTTL(Blackhole blackhole) {
    int index = random.nextInt(DATA_SIZE);
    boolean result = store.put(keys[index], values[index], 60); // 60 seconds TTL
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkContainsKey(Blackhole blackhole) {
    int index = random.nextInt(DATA_SIZE);
    boolean result = store.containsKey(keys[index]);
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkListKeys(Blackhole blackhole) {
    List<byte[]> result = store.listKeys();
    blackhole.consume(result);
  }

  @Benchmark
  public void benchmarkSize(Blackhole blackhole) {
    int result = store.size();
    blackhole.consume(result);
  }

  @Benchmark
  @Fork(
      value = 1,
      jvmArgs = {}) // Use a separate fork to avoid affecting other benchmarks
  public void benchmarkClear(Blackhole blackhole) {
    // Add some data first to ensure there's something to clear
    for (int i = 0; i < 10; i++) {
      byte[] key = generateRandomBytes(KEY_SIZE);
      byte[] value = generateRandomBytes(VALUE_SIZE);
      store.put(key, value);
    }

    // Clear the store
    store.clear();

    // Consume the result of size to verify the clear worked
    int size = store.size();
    blackhole.consume(size);
  }

  private byte[] generateRandomBytes(int size) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return bytes;
  }

  @Benchmark
  public void benchmarkECommerceReadHeavyWorkload(Blackhole blackhole) {
    // 90% reads, 10% writes
    boolean isRead = random.nextInt(100) < 90;

    if (isRead) {
      // Read operation - simulate product browsing
      int index = random.nextInt(DATA_SIZE);
      byte[] result = store.get(keys[index]);
      blackhole.consume(result);

      // Occasionally perform a range query (related products)
      if (random.nextInt(10) == 0) {
        int startIndex = Math.max(0, index - 5);
        int endIndex = Math.min(DATA_SIZE - 1, index + 5);

        Map<byte[], byte[]> relatedProducts = store.getRange(keys[startIndex], keys[endIndex]);
        blackhole.consume(relatedProducts);
      }
    } else {
      // Write operation - simulate purchase or cart update
      int index = random.nextInt(DATA_SIZE);
      byte[] key = generateRandomBytes(KEY_SIZE);
      byte[] value = generateRandomBytes(VALUE_SIZE);
      boolean result = store.put(key, value);
      blackhole.consume(result);
    }
  }

  @Benchmark
  public void benchmarkBurstTraffic(Blackhole blackhole) {
    // Perform a burst of 20 operations in quick succession
    for (int i = 0; i < 20; i++) {
      int index = random.nextInt(DATA_SIZE);

      // 80% reads, 20% writes during burst
      boolean isRead = random.nextInt(100) < 80;

      if (isRead) {
        byte[] result = store.get(keys[index]);
        blackhole.consume(result);
      } else {
        byte[] key = generateRandomBytes(KEY_SIZE);
        byte[] value = generateRandomBytes(VALUE_SIZE);
        boolean result = store.put(key, value);
        blackhole.consume(result);
      }
    }
  }

  @Benchmark
  public void benchmarkHighContention(Blackhole blackhole) {
    // Use a small set of "hot" keys (10% of total)
    int hotKeyCount = DATA_SIZE / 10;
    int hotKeyIndex = random.nextInt(hotKeyCount);

    // 80% reads, 20% writes
    boolean isRead = random.nextInt(100) < 80;

    if (isRead) {
      byte[] result = store.get(keys[hotKeyIndex]);
      blackhole.consume(result);
    } else {
      boolean result = store.put(keys[hotKeyIndex], generateRandomBytes(VALUE_SIZE));
      blackhole.consume(result);
    }
  }

  @Benchmark
  public void benchmarkECommerceWorkload(Blackhole blackhole) {
    int operationType = random.nextInt(100);

    if (operationType < 70) {
      // 70% - Product browsing (get)
      int index = random.nextInt(DATA_SIZE);
      byte[] result = store.get(keys[index]);
      blackhole.consume(result);
    } else if (operationType < 85) {
      // 15% - Related product viewing (range query)
      int startIndex = random.nextInt(DATA_SIZE - 10);
      int endIndex = startIndex + 10;
      Map<byte[], byte[]> result = store.getRange(keys[startIndex], keys[endIndex]);
      blackhole.consume(result);
    } else if (operationType < 95) {
      // 10% - Adding to cart (put)
      int index = random.nextInt(DATA_SIZE);
      boolean result = store.put(generateRandomBytes(KEY_SIZE), generateRandomBytes(VALUE_SIZE));
      blackhole.consume(result);
    } else {
      // 5% - Removing from cart (delete)
      int index = random.nextInt(DATA_SIZE);
      boolean result = store.delete(keys[index]);
      blackhole.consume(result);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options options = new OptionsBuilder().include(CascadeBenchmark.class.getSimpleName()).build();
    new Runner(options).run();
  }
}
