package io.cascadestore.lsm.sstable.io;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTableEntry;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableIOImpl implements SSTableIO {
  private static final Logger logger = LoggerFactory.getLogger(SSTableIOImpl.class);

  private final String directory;
  private final int level;
  private final long sequenceNumber;

  private final Path dataFilePath;
  private final Path indexFilePath;
  private final Path filterFilePath;

  private FileChannel dataChannel;
  private FileChannel indexChannel;

  public SSTableIOImpl(String directory, int level, long sequenceNumber) throws IOException {
    this.directory = directory;
    this.level = level;
    this.sequenceNumber = sequenceNumber;

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
  }

  @Override
  public String getDirectory() {
    return directory;
  }

  @Override
  public int getLevel() {
    return level;
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public FileChannel getDataChannel() {
    return dataChannel;
  }

  @Override
  public FileChannel getIndexChannel() {
    return indexChannel;
  }

  @Override
  public void flushToDisk(MemTable memTable) throws IOException {
    logger.info("Flushing MemTable to disk as SSTable");

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
      headerBuffer.putLong(System.currentTimeMillis());
      headerBuffer.putInt(level);
      headerBuffer.putInt(memTable.getEntries().size());
      headerBuffer.flip();
      writeDataChannel.write(headerBuffer);

      // Write entries to data file and build index
      long currentOffset = 16; // Start after header

      for (var entry : memTable.getEntries().entrySet()) {
        byte[] key = entry.getKey().getData();
        MemTable.ValueEntry valueEntry = entry.getValue();

        // Skip expired entries
        if (valueEntry.isExpired()) {
          continue;
        }

        // Create SSTableEntry
        SSTableEntry sstEntry;
        if (valueEntry.isTombstone()) {
          sstEntry = SSTableEntry.tombstone(key, valueEntry.getExpirationTime());
        } else {
          sstEntry = SSTableEntry.of(key, valueEntry.getValue(), valueEntry.getExpirationTime());
        }

        // Write entry to data file
        ByteBuffer entryBuffer = writeEntryHeader(sstEntry);
        writeDataChannel.write(entryBuffer);

        // Add to index
        ByteBuffer indexBuffer = ByteBuffer.allocate(4 + key.length + 8);
        indexBuffer.putInt(key.length);
        indexBuffer.put(key);
        indexBuffer.putLong(currentOffset);
        indexBuffer.flip();
        writeIndexChannel.write(indexBuffer);

        // Update offset for next entry
        currentOffset = writeDataChannel.position();
      }
    }

    // Open the data file for reading
    this.dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);

    // Open the index file for reading
    this.indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);

    logger.info("MemTable flushed to SSTable successfully");
  }

  @Override
  public void loadFromDisk() throws IOException {
    logger.info("Loading SSTable from disk");

    // Open the data file for reading
    if (dataFilePath.toFile().exists()) {
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

    // Open the index file for reading
    if (indexFilePath.toFile().exists()) {
      try {
        indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
      } catch (IOException e) {
        logger.error("Error opening index file", e);
        throw e;
      }
    } else {
      logger.warn("Index file not found: " + indexFilePath);
    }

    logger.info("SSTable loaded successfully");
  }

  @Override
  public ByteBuffer writeEntryHeader(SSTableEntry entry) {
    byte[] key = entry.key();
    byte[] value = entry.value();

    if (entry.tombstone()) {
      // Tombstone entry
      ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + 4 + 8);
      buffer.putInt(key.length);
      buffer.put(key);
      buffer.putInt(0); // 0 length indicates tombstone
      buffer.putLong(entry.timestamp());
      buffer.flip();
      return buffer;
    } else {
      // Regular entry
      ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + 4 + value.length + 8);
      buffer.putInt(key.length);
      buffer.put(key);
      buffer.putInt(value.length);
      buffer.put(value);
      buffer.putLong(entry.timestamp());
      buffer.flip();
      return buffer;
    }
  }

  @Override
  public EntryHeader readEntryHeader(ByteBuffer buffer) {
    int keyLength = buffer.getInt();
    int valueLength = buffer.getInt();
    long timestamp = buffer.getLong();
    boolean tombstone = (valueLength == 0);

    return new EntryHeader(keyLength, valueLength, timestamp, tombstone);
  }

  @Override
  public boolean deleteFiles() {
    boolean success = true;

    // Delete data file
    File dataFile = dataFilePath.toFile();
    if (dataFile.exists() && !dataFile.delete()) {
      success = false;
    }

    // Delete index file
    File indexFile = indexFilePath.toFile();
    if (indexFile.exists() && !indexFile.delete()) {
      success = false;
    }

    // Delete filter file
    File filterFile = filterFilePath.toFile();
    if (filterFile.exists() && !filterFile.delete()) {
      success = false;
    }

    return success;
  }

  @Override
  public void close() throws IOException {
    if (dataChannel != null && dataChannel.isOpen()) {
      dataChannel.close();
    }

    if (indexChannel != null && indexChannel.isOpen()) {
      indexChannel.close();
    }
  }
}
