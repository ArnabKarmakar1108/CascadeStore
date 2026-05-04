package io.cascadestore.lsm.core.backgroundservice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class FlushServiceTest {

  @TempDir Path tempDir;

  private List<MemTable> immutableMemTables;
  private List<SSTable> ssTables;
  private CascadeConfig config;
  private AtomicLong sequenceNumber;
  private CompactionService mockCompactionService;
  private FlushService flushService;

  @BeforeEach
  void setUp() {
    immutableMemTables = new ArrayList<>();
    ssTables = new ArrayList<>();
    config =
        new CascadeConfig(
            1024 * 1024, // 1MB MemTable size
            tempDir.toString(),
            2, // Compact after 2 SSTables
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            CompactionStrategyType.THRESHOLD // Use threshold-based compaction for tests
            );
    sequenceNumber = new AtomicLong(0);

    // Create a mock CompactionService
    mockCompactionService = Mockito.mock(CompactionService.class);

    flushService =
        new FlushService(
            immutableMemTables, ssTables, config, sequenceNumber, mockCompactionService);
  }

  @AfterEach
  void tearDown() {
    flushService.shutdown();
    for (MemTable memTable : immutableMemTables) {
      memTable.close();
    }
    for (SSTable ssTable : ssTables) {
      try {
        ssTable.close();
        ssTable.delete();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test
  void testFlushServiceStart() {
    // Start the service
    flushService.start();

    // Verify that the service is running
    // This is a simple test to make sure the service starts without errors
    assertTrue(true);
  }

  @Test
  void testFlushServiceExecuteNowWithNoMemTables() {
    // Execute the flush service with no immutable MemTables
    flushService.executeNow();

    // Verify that no SSTables were created
    assertEquals(0, ssTables.size());

    // Verify that the compaction service was not called
    verify(mockCompactionService, never()).executeNow();
  }

  @Test
  void testFlushServiceExecuteNow() throws IOException {
    // Create some immutable MemTables
    MemTable memTable1 = new MemTable(config.memTableMaxSizeBytes());
    memTable1.put("key1".getBytes(), "value1".getBytes(), 0);
    memTable1.put("key2".getBytes(), "value2".getBytes(), 0);
    memTable1.makeImmutable();

    MemTable memTable2 = new MemTable(config.memTableMaxSizeBytes());
    memTable2.put("key3".getBytes(), "value3".getBytes(), 0);
    memTable2.put("key4".getBytes(), "value4".getBytes(), 0);
    memTable2.makeImmutable();

    // Add the MemTables to the list
    immutableMemTables.add(memTable1);
    immutableMemTables.add(memTable2);

    // Execute the flush service
    flushService.executeNow();

    // Verify that the MemTables were flushed to SSTables
    assertEquals(2, ssTables.size());

    // Verify that the immutable MemTables list is empty
    assertEquals(0, immutableMemTables.size());

    // Verify that the compaction service was called
    verify(mockCompactionService, times(1)).executeNow();

    // Verify that the SSTables contain the expected keys
    boolean key1Found = false;
    boolean key2Found = false;
    boolean key3Found = false;
    boolean key4Found = false;

    for (SSTable ssTable : ssTables) {
      if (ssTable.get("key1".getBytes()) != null) key1Found = true;
      if (ssTable.get("key2".getBytes()) != null) key2Found = true;
      if (ssTable.get("key3".getBytes()) != null) key3Found = true;
      if (ssTable.get("key4".getBytes()) != null) key4Found = true;
    }

    assertTrue(key1Found, "key1 should be found in an SSTable");
    assertTrue(key2Found, "key2 should be found in an SSTable");
    assertTrue(key3Found, "key3 should be found in an SSTable");
    assertTrue(key4Found, "key4 should be found in an SSTable");
  }

  @Test
  void testFlushServiceExecuteNowWithCompactionThreshold() throws IOException {
    // Create a config with a lower compaction threshold
    CascadeConfig lowThresholdConfig =
        new CascadeConfig(
            1024 * 1024, // 1MB MemTable size
            tempDir.toString(),
            1, // Compact after 1 SSTable
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            CompactionStrategyType.THRESHOLD // Use threshold-based compaction for tests
            );

    // Create a new flush service with the low threshold config
    FlushService lowThresholdFlushService =
        new FlushService(
            immutableMemTables,
            ssTables,
            lowThresholdConfig,
            sequenceNumber,
            mockCompactionService);

    // Create an immutable MemTable
    MemTable memTable = new MemTable(lowThresholdConfig.memTableMaxSizeBytes());
    memTable.put("key1".getBytes(), "value1".getBytes(), 0);
    memTable.makeImmutable();

    // Add the MemTable to the list
    immutableMemTables.add(memTable);

    // Execute the flush service
    lowThresholdFlushService.executeNow();

    // Verify that the MemTable was flushed to an SSTable
    assertEquals(1, ssTables.size());

    // Verify that the compaction service was called
    verify(mockCompactionService, times(1)).executeNow();

    // Clean up
    lowThresholdFlushService.shutdown();
  }

  @Test
  void concurrentFlushClaimsMemTableOnce() throws Exception {
    MemTable memTable = new MemTable(config.memTableMaxSizeBytes());
    memTable.put("key1".getBytes(), "value1".getBytes(), 0);
    memTable.makeImmutable();
    immutableMemTables.add(memTable);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch go = new CountDownLatch(1);

    for (int i = 0; i < 2; i++) {
      pool.submit(
          () -> {
            ready.countDown();
            try {
              go.await();
              flushService.executeNow();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
    }

    ready.await();
    go.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

    assertEquals(1, ssTables.size(), "Each MemTable must flush to exactly one SSTable");
    assertEquals(0, immutableMemTables.size());
    assertNotNull(ssTables.get(0).get("key1".getBytes()));
  }

  @Test
  void testFlushInvokesWalTruncationHook() throws IOException {
    AtomicBoolean hookCalled = new AtomicBoolean(false);
    FlushService hookFlushService =
        new FlushService(
            immutableMemTables,
            ssTables,
            config,
            sequenceNumber,
            mockCompactionService,
            () -> hookCalled.set(true));

    MemTable memTable = new MemTable(config.memTableMaxSizeBytes());
    memTable.put("key1".getBytes(), "value1".getBytes(), 0);
    memTable.makeImmutable();
    immutableMemTables.add(memTable);

    hookFlushService.executeNow();

    assertTrue(hookCalled.get());
    hookFlushService.shutdown();
  }
}
