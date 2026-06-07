package io.cascadestore.lsm.sstable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.core.store.CascadeStore;
import io.cascadestore.lsm.memtable.MemTable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SSTableLz4CompressionTest {

  @TempDir Path tempDir;

  @Test
  void flushedSSTableUsesLz4HeaderAndRoundTripsValues() throws Exception {
    MemTable memTable = new MemTable(1024 * 1024);
    byte[] key = "compression-key".getBytes(StandardCharsets.UTF_8);
    byte[] value = new byte[1024];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) ('a' + (i % 26));
    }
    assertTrue(memTable.put(key, value, 0));
    memTable.makeImmutable();

    SSTable table = new SSTable(memTable, tempDir.toString(), 0, 42);
    try {
      byte[] loaded = table.get(key);
      assertArrayEquals(value, loaded);

      byte[] header = Files.readAllBytes(tempDir.resolve("sst_L0_S42.data"));
      int magic =
          ((header[0] & 0xFF) << 24)
              | ((header[1] & 0xFF) << 16)
              | ((header[2] & 0xFF) << 8)
              | (header[3] & 0xFF);
      assertEqualsInt(SSTableDataFormat.MAGIC, magic);
    } finally {
      table.close();
      table.delete();
    }
  }

  @Test
  void storeSurvivesCompactionWithLz4Tables() throws Exception {
    CascadeConfig config =
        new CascadeConfig(
            4 * 1024,
            tempDir.toString(),
            2,
            0.05,
            10_000,
            5,
            CompactionStrategyType.LEVEL_TIERED);

    CascadeStore store = new CascadeStore(config);
    try {
      byte[] key = "lz4-compaction".getBytes(StandardCharsets.UTF_8);
      byte[] value = new byte[512];
      for (int i = 0; i < value.length; i++) {
        value[i] = (byte) (i & 0xFF);
      }
      assertTrue(store.put(key, value));
      store.switchMemTableForTest();
      store.flushMemTables();
      store.switchMemTableForTest();
      store.flushMemTables();
      assertNotNull(store.get(key));
    } finally {
      store.shutdown();
    }
  }

  private static void assertEqualsInt(int expected, int actual) {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }
}
