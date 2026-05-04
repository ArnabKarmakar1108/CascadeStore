package io.cascadestore.lsm.core.backgroundservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactionLayoutVersionTest {

  @TempDir Path tempDir;

  private CompactionService compactionService;

  @AfterEach
  void tearDown() {
    if (compactionService != null) {
      compactionService.shutdown();
    }
  }

  @Test
  void abortsCommitWhenLayoutVersionChangesDuringMerge() throws IOException {
    List<SSTable> ssTables = new ArrayList<>();
    CascadeConfig config =
        new CascadeConfig(
            1024 * 1024,
            tempDir.toString(),
            2,
            30,
            1,
            10,
            CompactionStrategyType.THRESHOLD);
    AtomicLong sequenceNumber = new AtomicLong(0);
    AtomicLong layoutVersion = new AtomicLong(1);

    compactionService =
        new CompactionService(
            ssTables,
            config,
            sequenceNumber,
            null,
            null,
            () -> {
              layoutVersion.incrementAndGet();
              return layoutVersion.get();
            });

    MemTable memTable1 = new MemTable(config.memTableMaxSizeBytes());
    memTable1.put("key1".getBytes(), "value1".getBytes(), 0);
    MemTable memTable2 = new MemTable(config.memTableMaxSizeBytes());
    memTable2.put("key2".getBytes(), "value2".getBytes(), 0);

    SSTable ssTable1 =
        new SSTable(memTable1, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());
    SSTable ssTable2 =
        new SSTable(memTable2, config.dataDirectory(), 0, sequenceNumber.getAndIncrement());
    ssTables.add(ssTable1);
    ssTables.add(ssTable2);

    compactionService.executeNow();

    assertEquals(2, ssTables.size());
    ssTable1.forceCloseAndDelete();
    ssTable2.forceCloseAndDelete();
  }
}
