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

  public SSTable(String directory, int level, long sequenceNumber) throws IOException {
    this.level = level;
    this.sequenceNumber = sequenceNumber;
    this.sparseIndex = new TreeMap<>();

    // Define file paths
    String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
    this.dataFilePath = Path.of(directory, filePrefix + ".data");
    this.indexFilePath = Path.of(directory, filePrefix + ".index");
    this.filterFilePath = Path.of(directory, filePrefix + ".filter");

    // Load metadata
    this.creationTime = new File(dataFilePath.toString()).lastModified();

    // Load from disk
    loadFromDisk();

    logger.info("Opened SSTable: " + filePrefix);
  }

  private void flushToDisk(MemTable memTable) throws IOException {
    logger.info("Flushing MemTable to disk as SSTable");

    // Create a bloom filter for efficient negative lookups
    BloomFilter filter = new BloomFilter(memTable.getEntries().size(), 0.01);

    // Create data and index files
    try (FileChannel writeDataChannel =
            FileChannel.open(
                dataFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        FileChannel writeIndexChannel =
            FileChannel.open(
                indexFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      // Write header to data file
      ByteBuffer headerBuffer = ByteBuffer.allocate(16);
      headerBuffer.putLong(creationTime);
      headerBuffer.putInt(level);
      headerBuffer.putInt(memTable.getEntries().size());
      headerBuffer.flip();
      writeDataChannel.write(headerBuffer);

      // Write entries to data file and build index
      long currentOffset = 16; // Start after header

      for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry :
          memTable.getEntries().entrySet()) {
        byte[] key = entry.getKey().getData();
        MemTable.ValueEntry valueEntry = entry.getValue();

        // Skip expired entries
        if (valueEntry.isExpired()) {
          continue;
        }

        // Add key to bloom filter
        filter.add(key);

        // Create SSTableEntry
        SSTableEntry sstEntry;
        if (valueEntry.isTombstone()) {
          sstEntry = SSTableEntry.tombstone(key, valueEntry.getExpirationTime());
        } else {
          sstEntry = SSTableEntry.of(key, valueEntry.getValue(), valueEntry.getExpirationTime());
        }

        // Write entry to data file
        ByteBuffer keyBuffer = ByteBuffer.allocate(4 + key.length);
        keyBuffer.putInt(key.length);
        keyBuffer.put(key);
        keyBuffer.flip();
        writeDataChannel.write(keyBuffer);

        // Write value or tombstone marker
        if (valueEntry.isTombstone()) {
          ByteBuffer tombstoneBuffer =
              ByteBuffer.allocate(12); // 4 bytes for int + 8 bytes for long
          tombstoneBuffer.putInt(0); // 0 length indicates tombstone
          tombstoneBuffer.putLong(valueEntry.getExpirationTime());
          tombstoneBuffer.flip();
          writeDataChannel.write(tombstoneBuffer);
        } else {
          byte[] value = valueEntry.getValue();
          ByteBuffer valueBuffer = ByteBuffer.allocate(4 + value.length + 8);
          valueBuffer.putInt(value.length);
          valueBuffer.put(value);
          valueBuffer.putLong(valueEntry.getExpirationTime());
          valueBuffer.flip();
          writeDataChannel.write(valueBuffer);
        }

        // Add to sparse index
        sparseIndex.put(entry.getKey(), currentOffset);

        // Update offset for next entry
        currentOffset = writeDataChannel.position();
      }

      // Write index to index file
      for (Map.Entry<ByteArrayWrapper, Long> indexEntry : sparseIndex.entrySet()) {
        byte[] key = indexEntry.getKey().getData();
        long offset = indexEntry.getValue();

        ByteBuffer indexBuffer = ByteBuffer.allocate(4 + key.length + 8);
        indexBuffer.putInt(key.length);
        indexBuffer.put(key);
        indexBuffer.putLong(offset);
        indexBuffer.flip();

        writeIndexChannel.write(indexBuffer);
      }
    }

    // Save bloom filter to file
    filter.save(filterFilePath.toString());

    // Load the bloom filter into memory
    this.bloomFilter = filter;

    // Open the data file for reading
    this.dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);

    // Open the index file for reading
    this.indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);

    logger.info("MemTable flushed to SSTable successfully");
  }

  private void loadFromDisk() throws IOException {
    logger.info("Loading SSTable from disk");

    // Load the bloom filter
    if (Files.exists(filterFilePath)) {
      this.bloomFilter = BloomFilter.load(filterFilePath.toString());
    } else {
      logger.warn("Bloom filter file not found: " + filterFilePath);
      this.bloomFilter = new BloomFilter(1000, 0.01); // Default filter
    }

    // Load the sparse index
    if (Files.exists(indexFilePath)) {
      try (FileChannel indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ)) {
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

        while (indexChannel.position() < indexChannel.size()) {
          // Read key length
          buffer.clear();
          buffer.limit(4);
          indexChannel.read(buffer);
          buffer.flip();
          int keyLength = buffer.getInt();

          // Read key
          buffer.clear();
          buffer.limit(keyLength);
          if (buffer.capacity() < keyLength) {
            buffer = ByteBuffer.allocate(keyLength);
          }
          indexChannel.read(buffer);
          buffer.flip();
          byte[] key = new byte[keyLength];
          buffer.get(key);

          // Read offset
          buffer.clear();
          buffer.limit(8);
          indexChannel.read(buffer);
          buffer.flip();
          long offset = buffer.getLong();

          // Add to sparse index
          sparseIndex.put(new ByteArrayWrapper(key), offset);
        }
      }
    } else {
      logger.warn("Index file not found: " + indexFilePath);
    }

    // Open the data file for reading
    if (Files.exists(dataFilePath)) {
      try {
        // Open the data file channel
        dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);

        // Read header
        ByteBuffer headerBuffer = ByteBuffer.allocate(16);
        dataChannel.read(headerBuffer, 0);
        headerBuffer.flip();

        long storedCreationTime = headerBuffer.getLong();
        int storedLevel = headerBuffer.getInt();
        int entryCount = headerBuffer.getInt();

        logger.info(
            "Loaded SSTable with "
                + entryCount
                + " entries, level "
                + storedLevel
                + ", creation time "
                + storedCreationTime);
      } catch (IOException e) {
        logger.error("Error opening data file", e);
        throw e;
      }
    } else {
      logger.warn("Data file not found: " + dataFilePath);
    }

    logger.info("SSTable loaded successfully");
  }


  // lookup path added in a follow-up commit
}
