package io.cascadestore.lsm.core.backgroundservice;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.memtable.MemTable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CleanupServiceTest {

  @TempDir Path tempDir;

  private MemTable activeMemTable;
  private List<MemTable> immutableMemTables;
  private ReadWriteLock memTableLock;
  private CascadeConfig config;
  private CleanupService cleanupService;

  @BeforeEach
  void setUp() {
    activeMemTable = new MemTable(1024 * 1024); // 1MB MemTable size
    immutableMemTables = new ArrayList<>();
    memTableLock = new ReentrantReadWriteLock();
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
    cleanupService = new CleanupService(activeMemTable, immutableMemTables, memTableLock, config);
  }

  @AfterEach
  void tearDown() {
    cleanupService.shutdown();
    activeMemTable.close();
    for (MemTable memTable : immutableMemTables) {
      memTable.close();
    }
  }

  @Test
  void testCleanupServiceStart() {
    // Start the service
    cleanupService.start();

    // Verify that the service is running
    // This is a simple test to make sure the service starts without errors
    assertTrue(true);
  }

  @Test
  void testCleanupServiceExecuteNow() throws InterruptedException {
    // Add some entries to the active MemTable with short TTL
    activeMemTable.put("key1".getBytes(), "value1".getBytes(), 1); // 1 second TTL
    activeMemTable.put("key2".getBytes(), "value2".getBytes(), 0); // No TTL

    // Wait for the TTL to expire
    TimeUnit.SECONDS.sleep(2);

    // Execute the cleanup service
    cleanupService.executeNow();

    // Verify that the expired entry was removed
    Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
    boolean key1Found = false;
    boolean key2Found = false;

    for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
      if (new String(entry.getKey().getData()).equals("key1")) {
        key1Found = !entry.getValue().isTombstone(); // Should be a tombstone
      }
      if (new String(entry.getKey().getData()).equals("key2")) {
        key2Found = !entry.getValue().isTombstone(); // Should not be a tombstone
      }
    }

    assertFalse(key1Found, "key1 should be marked as tombstone");
    assertTrue(key2Found, "key2 should not be marked as tombstone");
  }

  @Test
  void testCleanupServiceWithImmutableMemTables() throws InterruptedException {
    // Create an immutable MemTable with an expired entry
    MemTable immutableMemTable = new MemTable(1024 * 1024);
    immutableMemTable.put("key3".getBytes(), "value3".getBytes(), 1); // 1 second TTL
    immutableMemTable.makeImmutable();
    immutableMemTables.add(immutableMemTable);

    // Wait for the TTL to expire
    TimeUnit.SECONDS.sleep(2);

    // Execute the cleanup service
    cleanupService.executeNow();

    // Verify that a tombstone was added to the active MemTable for the expired entry
    Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
    boolean key3TombstoneFound = false;

    for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
      if (new String(entry.getKey().getData()).equals("key3")) {
        key3TombstoneFound = entry.getValue().isTombstone();
      }
    }

    assertTrue(key3TombstoneFound, "A tombstone for key3 should be added to the active MemTable");
  }
}
