package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.memtable.MemTable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTable {
  private static final Logger logger = LoggerFactory.getLogger(SSTable.class);

  // File paths
  private final Path dataFilePath;
  private final Path indexFilePath;
  private final Path filterFilePath;

  // File channels for data access
  private FileChannel dataChannel;
  private FileChannel indexChannel;

  // Sparse index for efficient lookups
  private final NavigableMap<ByteArrayWrapper, Long> sparseIndex;

  // Bloom filter for efficient negative lookups
  private BloomFilter bloomFilter;

  // Metadata
  private final long creationTime;
  private final int level;
  private final long sequenceNumber;

  public SSTable(MemTable memTable, String directory, int level, long sequenceNumber)
      throws IOException {
    this.creationTime = System.currentTimeMillis();
    this.level = level;
    this.sequenceNumber = sequenceNumber;
    this.sparseIndex = new TreeMap<>();

    // Create directory if it doesn't exist
    File dir = new File(directory);
    if (!dir.exists()) {
      dir.mkdirs();
    }

    // Define file paths
    String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
    this.dataFilePath = Path.of(directory, filePrefix + ".data");
    this.indexFilePath = Path.of(directory, filePrefix + ".index");
    this.filterFilePath = Path.of(directory, filePrefix + ".filter");

    // Flush MemTable to disk
    flushToDisk(memTable);

    logger.info("Created SSTable: " + filePrefix);
  }

  private void flushToDisk(MemTable memTable) throws IOException {
    throw new UnsupportedOperationException(
        "flush path added in a follow-up commit");
  }

  // lookup path added in a follow-up commit
}
