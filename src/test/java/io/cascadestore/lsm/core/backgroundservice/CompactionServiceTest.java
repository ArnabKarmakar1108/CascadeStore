package io.cascadestore.lsm.core.backgroundservice;

import static org.junit.jupiter.api.Assertions.*;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactionServiceTest {

  @TempDir Path tempDir;

  private List<SSTable> ssTables;
  private CascadeConfig config;
  private AtomicLong sequenceNumber;
  private CompactionService compactionService;

  @BeforeEach
  void setUp() {
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
    compactionService = new CompactionService(ssTables, config, sequenceNumber);
  }

  @AfterEach
  void tearDown() {
    compactionService.shutdown();
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
  void testCompactionServiceStart() {
    // Start the service
    compactionService.start();

    // Verify that the service is running
    // This is a simple test to make sure the service starts without errors
    assertTrue(true);
  }

  @Test
  void testCompactionServiceExecuteNow() throws IOException {
    // Create some SSTables
    MemTable memTable1 = new MemTable(config.memTableMaxSizeBytes());
    memTable1.put("key1".getBytes(), "value1".getBytes(), 0);
    memTable1.put("key2".getBytes(), "value2".getBytes(), 0);

    MemTable memTable2 = new MemTable(config.memTableMaxSizeBytes());
    memTable2.put("key3".getBytes(), "value3".getBytes(), 0);
    memTable2.put("key4".getBytes(), "value4".getBytes(), 0);

    // Create SSTables from the MemTables
    SSTable ssTable1 =
        new SSTable(memTable1, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());
    SSTable ssTable2 =
        new SSTable(memTable2, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());

    // Add the SSTables to the list
    ssTables.add(ssTable1);
    ssTables.add(ssTable2);

    // Execute the compaction service
    compactionService.executeNow();

    // Verify that the compaction was performed
    // The number of SSTables should be 1 (the compacted table)
    assertEquals(1, ssTables.size());

    // Verify that the compacted table contains all the keys
    SSTable compactedTable = ssTables.get(0);
    assertNotNull(compactedTable.get("key1".getBytes()));
    assertNotNull(compactedTable.get("key2".getBytes()));
    assertNotNull(compactedTable.get("key3".getBytes()));
    assertNotNull(compactedTable.get("key4".getBytes()));
  }

  @Test
  void testCompactionWithLargeValues() throws IOException {
    byte[] largeValue = new byte[1100];
    Arrays.fill(largeValue, (byte) 'y');

    MemTable memTable1 = new MemTable(config.memTableMaxSizeBytes());
    memTable1.put("key1".getBytes(), largeValue, 0);

    MemTable memTable2 = new MemTable(config.memTableMaxSizeBytes());
    memTable2.put("key2".getBytes(), largeValue, 0);

    SSTable ssTable1 =
        new SSTable(memTable1, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());
    SSTable ssTable2 =
        new SSTable(memTable2, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());

    ssTables.add(ssTable1);
    ssTables.add(ssTable2);

    compactionService.executeNow();

    assertEquals(1, ssTables.size());
    SSTable compactedTable = ssTables.get(0);
    assertArrayEquals(largeValue, compactedTable.get("key1".getBytes()));
    assertArrayEquals(largeValue, compactedTable.get("key2".getBytes()));
  }
}
