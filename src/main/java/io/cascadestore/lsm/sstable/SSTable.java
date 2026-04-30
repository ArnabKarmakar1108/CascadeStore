package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.io.BlockCache;
import io.cascadestore.lsm.io.BufferedDataReader;
import io.cascadestore.lsm.io.MappedDataFile;
import io.cascadestore.lsm.io.ReadBuffers;
import io.cascadestore.lsm.io.ValueBufferPool;
import io.cascadestore.lsm.sstable.index.SparseIndex;
import io.cascadestore.lsm.sstable.index.SparseIndexPolicy;
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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private MappedDataFile mappedDataFile;
  private final ThreadLocal<BufferedDataReader> threadLocalDataReader = new ThreadLocal<>();

  // Sparse index for efficient lookups
  private SparseIndex sparseIndex;

  // Bloom filter for efficient negative lookups
  private BloomFilter bloomFilter;

  // Metadata
  private final long creationTime;
  private final int level;
  private final long sequenceNumber;
  private int entryCount;

  private final AtomicInteger pinCount = new AtomicInteger(0);
  private volatile boolean retired;
  private final BlockCache blockCache;

  public SSTable(MemTable memTable, String directory, int level, long sequenceNumber)
      throws IOException {
    this(memTable, directory, level, sequenceNumber, null);
  }

  public SSTable(
      MemTable memTable, String directory, int level, long sequenceNumber, BlockCache blockCache)
      throws IOException {
    this.blockCache = blockCache;
    this.creationTime = System.currentTimeMillis();
    this.level = level;
    this.sequenceNumber = sequenceNumber;
    this.sparseIndex = SparseIndex.empty();

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

    try {
      flushToDisk(memTable);
      logger.info("Created SSTable: {}", filePrefix);
    } catch (IOException e) {
      deleteFiles(directory, level, sequenceNumber);
      throw e;
    }
  }

  public SSTable(String directory, int level, long sequenceNumber) throws IOException {
    this(directory, level, sequenceNumber, null);
  }

  public SSTable(String directory, int level, long sequenceNumber, BlockCache blockCache)
      throws IOException {
    this.blockCache = blockCache;
    this.level = level;
    this.sequenceNumber = sequenceNumber;
    this.sparseIndex = SparseIndex.empty();

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
    BloomFilter filter = new BloomFilter(memTable.getEntries().size(), BloomFilter.DEFAULT_FALSE_POSITIVE_RATE);

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
      int writtenEntries = 0;
      ByteBuffer headerBuffer = ByteBuffer.allocate(16);
      headerBuffer.putLong(creationTime);
      headerBuffer.putInt(level);
      headerBuffer.putInt(0);
      headerBuffer.flip();
      writeDataChannel.write(headerBuffer);

      // Write entries to data file and build sparse index
      long currentOffset = 16; // Start after header
      long lastIndexedOffset = -1;
      ByteArrayWrapper lastIndexedKey = null;
      long lastEntryOffset = -1;
      TreeMap<ByteArrayWrapper, Long> indexBuilder = new TreeMap<>();

      for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry :
          memTable.getEntries().entrySet()) {
        byte[] key = entry.getKey().getData();
        MemTable.ValueEntry valueEntry = entry.getValue();

        if (valueEntry.isExpired()) {
          continue;
        }

        final byte[] value;
        if (valueEntry.isTombstone()) {
          value = null;
        } else {
          value = valueEntry.getValue();
          if (value == null) {
            logger.warn(
                "Skipping corrupt entry during flush: non-tombstone key has no value (key length {})",
                key.length);
            continue;
          }
        }

        filter.add(key);

        ByteBuffer keyBuffer = ByteBuffer.allocate(4 + key.length);
        keyBuffer.putInt(key.length);
        keyBuffer.put(key);
        keyBuffer.flip();
        writeDataChannel.write(keyBuffer);

        if (valueEntry.isTombstone()) {
          ByteBuffer tombstoneBuffer = ByteBuffer.allocate(12);
          tombstoneBuffer.putInt(0);
          tombstoneBuffer.putLong(valueEntry.getExpirationTime());
          tombstoneBuffer.flip();
          writeDataChannel.write(tombstoneBuffer);
        } else {
          ByteBuffer valueBuffer = ByteBuffer.allocate(4 + value.length + 8);
          valueBuffer.putInt(value.length);
          valueBuffer.put(value);
          valueBuffer.putLong(valueEntry.getExpirationTime());
          valueBuffer.flip();
          writeDataChannel.write(valueBuffer);
        }

        lastIndexedKey = entry.getKey();
        lastEntryOffset = currentOffset;
        writtenEntries++;

        if (SparseIndexPolicy.shouldAddIndexEntry(currentOffset, lastIndexedOffset)) {
          indexBuilder.put(entry.getKey(), currentOffset);
          lastIndexedOffset = currentOffset;
        }

        currentOffset = writeDataChannel.position();
      }

      if (lastIndexedKey != null && lastEntryOffset != lastIndexedOffset) {
        indexBuilder.put(lastIndexedKey, lastEntryOffset);
      }

      sparseIndex = SparseIndex.from(indexBuilder);
      entryCount = writtenEntries;
      writeDataChannel.position(0);
      headerBuffer.clear();
      headerBuffer.putLong(creationTime);
      headerBuffer.putInt(level);
      headerBuffer.putInt(writtenEntries);
      headerBuffer.flip();
      writeDataChannel.write(headerBuffer, 0);

      // Write index to index file
      for (int i = 0; i < sparseIndex.size(); i++) {
        byte[] key = sparseIndex.keyAt(i);
        long offset = sparseIndex.offsetAt(i);

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
    this.mappedDataFile = MappedDataFile.tryMap(dataChannel);

    // Open the index file for reading
    this.indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);

    logger.info("MemTable flushed to SSTable successfully");
  }

  private void loadFromDisk() throws IOException {
    logger.info("Loading SSTable from disk");

    loadSparseIndexFromDisk();

    int entryCount = 0;
    if (Files.exists(dataFilePath)) {
      dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
      mappedDataFile = MappedDataFile.tryMap(dataChannel);
      entryCount = readDataFileHeader();
    } else {
      logger.warn("Data file not found: {}", dataFilePath);
    }

    if (Files.exists(filterFilePath)) {
      this.bloomFilter = BloomFilter.load(filterFilePath.toString());
    } else {
      int expectedEntries = Math.max(entryCount, sparseIndex.size());
      logger.warn(
          "Bloom filter file not found: {}, rebuilding in-memory filter for {} entries",
          filterFilePath,
          expectedEntries);
      this.bloomFilter = new BloomFilter(Math.max(expectedEntries, 1), BloomFilter.DEFAULT_FALSE_POSITIVE_RATE);
      if (dataChannel != null) {
        rebuildBloomFilterFromData();
      }
    }

    logger.info("SSTable loaded successfully");
  }

  private void loadSparseIndexFromDisk() throws IOException {
    if (!Files.exists(indexFilePath)) {
      logger.warn("Index file not found: {}", indexFilePath);
      sparseIndex = SparseIndex.empty();
      return;
    }

    TreeMap<ByteArrayWrapper, Long> indexBuilder = new TreeMap<>();
    try (FileChannel indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate(1024);

      while (indexChannel.position() < indexChannel.size()) {
        buffer.clear();
        buffer.limit(4);
        indexChannel.read(buffer);
        buffer.flip();
        int keyLength = buffer.getInt();

        buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
        buffer.limit(keyLength);
        indexChannel.read(buffer);
        buffer.flip();
        byte[] key = new byte[keyLength];
        buffer.get(key);

        buffer.clear();
        buffer.limit(8);
        indexChannel.read(buffer);
        buffer.flip();
        long offset = buffer.getLong();

        indexBuilder.put(new ByteArrayWrapper(key), offset);
      }
    }

    sparseIndex = SparseIndex.from(indexBuilder);
  }

  private int readDataFileHeader() throws IOException {
    ByteBuffer headerBuffer = ByteBuffer.allocate(16);
    dataChannel.read(headerBuffer, 0);
    headerBuffer.flip();

    long storedCreationTime = headerBuffer.getLong();
    int storedLevel = headerBuffer.getInt();
    int storedEntryCount = headerBuffer.getInt();

    entryCount = storedEntryCount;
    logger.info(
        "Loaded SSTable with {} entries, level {}, creation time {}",
        entryCount,
        storedLevel,
        storedCreationTime);
    return storedEntryCount;
  }

  private void rebuildBloomFilterFromData() {
    for (byte[] key : listKeys()) {
      bloomFilter.add(key);
    }
  }

  public byte[] get(byte[] key) {
    if (key == null || key.length == 0 || dataChannel == null || retired) {
      return null;
    }

    // First check the bloom filter for a quick negative
    if (bloomFilter != null && !bloomFilter.mightContain(key)) {
      return null; // Definitely not in the set
    }

    try {
      long startOffset = sparseIndex.floorOffset(key);
      if (startOffset < 0) {
        startOffset = 16;
      }

      BufferedDataReader reader = openDataReader();
      reader.prefetch(startOffset);
      return findKeyInDataFile(key, startOffset, reader);
    } catch (IOException e) {
      logger.error("Error reading from SSTable", e);
      return null;
    }
  }

  /** Returns true when the key maps to a live (non-tombstone) value in this SSTable. */
  public boolean containsKey(byte[] key) {
    if (key == null || key.length == 0 || dataChannel == null || retired) {
      return false;
    }

    if (bloomFilter != null && !bloomFilter.mightContain(key)) {
      return false;
    }

    try {
      long startOffset = sparseIndex.floorOffset(key);
      if (startOffset < 0) {
        startOffset = 16;
      }

      BufferedDataReader reader = openDataReader();
      reader.prefetch(startOffset);
      return findKeyPresenceInDataFile(key, startOffset, reader);
    } catch (IOException e) {
      logger.error("Error checking key in SSTable", e);
      return false;
    }
  }

  /** Smallest key in this SSTable, derived from the sparse index. */
  public byte[] getMinKey() {
    return sparseIndex.minKey();
  }

  /** Largest key in this SSTable, derived from the sparse index. */
  public byte[] getMaxKey() {
    return sparseIndex.maxKey();
  }

  /** Returns true when this table's key range intersects {@code other}'s range. */
  public boolean overlaps(SSTable other) {
    if (other == null) {
      return false;
    }
    return keyRangesOverlap(
        getMinKey(), getMaxKey(), other.getMinKey(), other.getMaxKey());
  }

  public static boolean keyRangesOverlap(
      byte[] minA, byte[] maxA, byte[] minB, byte[] maxB) {
    if (minA == null || maxA == null || minB == null || maxB == null) {
      return false;
    }
    ByteArrayWrapper minAw = new ByteArrayWrapper(minA);
    ByteArrayWrapper maxAw = new ByteArrayWrapper(maxA);
    ByteArrayWrapper minBw = new ByteArrayWrapper(minB);
    ByteArrayWrapper maxBw = new ByteArrayWrapper(maxB);
    return minAw.compareTo(maxBw) <= 0 && minBw.compareTo(maxAw) <= 0;
  }

  private byte[] findKeyInDataFile(byte[] key, long startPosition, BufferedDataReader reader)
      throws IOException {
    reader.seek(startPosition);
    while (reader.position() < reader.size()) {
      int keyLength = reader.readInt();
      if (reader.bytesEqual(keyLength, key)) {
        int valueLength = reader.readInt();
        if (valueLength == 0) {
          reader.skip(8);
          return null;
        }
        byte[] value = ValueBufferPool.readCopy(reader, valueLength);
        reader.skip(8);
        return value;
      }

      int valueLength = reader.readInt();
      if (valueLength == 0) {
        reader.skip(8);
        continue;
      }
      reader.skip(valueLength + 8L);
    }
    return null;
  }

  private boolean findKeyPresenceInDataFile(
      byte[] key, long startPosition, BufferedDataReader reader) throws IOException {
    reader.seek(startPosition);
    while (reader.position() < reader.size()) {
      int keyLength = reader.readInt();
      if (reader.bytesEqual(keyLength, key)) {
        int valueLength = reader.readInt();
        if (valueLength == 0) {
          reader.skip(8);
          return false;
        }
        reader.skip(valueLength + 8L);
        return true;
      }

      int valueLength = reader.readInt();
      if (valueLength == 0) {
        reader.skip(8);
        continue;
      }
      reader.skip(valueLength + 8L);
    }
    return false;
  }

  private BufferedDataReader openDataReader() throws IOException {
    if (dataChannel == null) {
      throw new IOException("SSTable data channel is not open");
    }

    BufferedDataReader reader = threadLocalDataReader.get();
    if (reader == null) {
      reader =
          new BufferedDataReader(
              dataChannel,
              BufferedDataReader.DEFAULT_BUFFER_SIZE,
              blockCache,
              sequenceNumber,
              mappedDataFile);
      threadLocalDataReader.set(reader);
    }
    return reader;
  }

  private void scanDataFile(DataFileEntryConsumer consumer) throws IOException {
    BufferedDataReader reader = openDataReader();
    reader.seek(16);
    while (reader.position() < reader.size()) {
      int keyLength = reader.readInt();
      byte[] key = reader.readBytes(keyLength);
      int valueLength = reader.readInt();

      if (valueLength == 0) {
        reader.skip(8);
        continue;
      }

      byte[] value = ValueBufferPool.readCopy(reader, valueLength);
      reader.skip(8);
      consumer.accept(key, value);
    }
  }

  @FunctionalInterface
  private interface DataFileEntryConsumer {
    void accept(byte[] key, byte[] value) throws IOException;
  }

  public boolean mightContain(byte[] key) {
    return !retired && bloomFilter != null && bloomFilter.mightContain(key);
  }

  /** Retain this table for an in-flight read or compaction merge. */
  public void pin() {
    pinCount.incrementAndGet();
  }

  /** Release a pin acquired via {@link #pin()}. */
  public void unpin() {
    if (pinCount.decrementAndGet() == 0) {
      tryFinalizeRetired();
    }
  }

  /** Mark removed from the live set; on-disk files are deleted once unpinned. */
  public void retire() {
    retired = true;
    if (blockCache != null) {
      blockCache.invalidateSstable(sequenceNumber);
    }
    tryFinalizeRetired();
  }

  public boolean isRetired() {
    return retired;
  }

  int getPinCountForTest() {
    return pinCount.get();
  }

  private synchronized void tryFinalizeRetired() {
    if (!retired || pinCount.get() > 0) {
      return;
    }
    closeResources();
    deleteFiles(dataFilePath.getParent().toString(), level, sequenceNumber);
  }

  private void closeResources() {
    try {
      if (dataChannel != null && dataChannel.isOpen()) {
        dataChannel.close();
      }
      if (indexChannel != null && indexChannel.isOpen()) {
        indexChannel.close();
      }
      if (bloomFilter != null) {
        bloomFilter.close();
      }
      if (mappedDataFile != null) {
        mappedDataFile.close();
        mappedDataFile = null;
      }
      threadLocalDataReader.remove();

      logger.info("SSTable closed: " + dataFilePath);
    } catch (IOException e) {
      logger.warn("Error closing SSTable resources", e);
    }
  }

  public int getLevel() {
    return level;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getSizeBytes() {
    long size = 0;

    try {
      File dataFile = dataFilePath.toFile();
      File indexFile = indexFilePath.toFile();
      File filterFile = filterFilePath.toFile();

      if (dataFile.exists()) size += dataFile.length();
      if (indexFile.exists()) size += indexFile.length();
      if (filterFile.exists()) size += filterFile.length();
    } catch (Exception e) {
      logger.warn("Error getting SSTable size", e);
    }

    return size;
  }

  public void close() {
    closeResources();
  }

  public boolean delete() {
    retire();
    return true;
  }

  /** Closes and deletes on-disk files immediately (shutdown / test cleanup). */
  public void forceCloseAndDelete() {
    retired = true;
    if (blockCache != null) {
      blockCache.invalidateSstable(sequenceNumber);
    }
    closeResources();
    deleteFiles(dataFilePath.getParent().toString(), level, sequenceNumber);
  }

  /** Deletes on-disk SSTable component files without opening the table. */
  public static boolean deleteFiles(String directory, int level, long sequenceNumber) {
    String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
    boolean success = true;

    for (String suffix : new String[] {".data", ".index", ".filter"}) {
      try {
        if (!Files.deleteIfExists(Path.of(directory, filePrefix + suffix))) {
          // missing file is fine
        }
      } catch (IOException e) {
        logger.warn("Failed to delete SSTable file: {}{}", filePrefix, suffix, e);
        success = false;
      }
    }

    return success;
  }

  public List<byte[]> listKeys() {
    List<byte[]> keys = new ArrayList<>();

    try {
      if (dataChannel != null && dataChannel.isOpen()) {
        scanDataFile((key, value) -> keys.add(key));
      }
      return keys;
    } catch (Exception e) {
      logger.error("Error listing keys from SSTable", e);
      return keys;
    }
  }

  public int countEntries() {
    return entryCount;
  }

  public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
    Map<byte[], byte[]> result =
        new TreeMap<>(
            (a, b) -> {
              ByteArrayWrapper wrapperA = new ByteArrayWrapper(a);
              ByteArrayWrapper wrapperB = new ByteArrayWrapper(b);
              return wrapperA.compareTo(wrapperB);
            });

    try {
      ByteArrayWrapper startWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
      ByteArrayWrapper endWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;

      if (dataChannel != null && dataChannel.isOpen()) {
        scanDataFile(
            (key, value) -> {
              ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
              if (startWrapper != null && keyWrapper.compareTo(startWrapper) < 0) {
                return;
              }
              if (endWrapper != null && keyWrapper.compareTo(endWrapper) >= 0) {
                return;
              }
              result.put(key, value);
            });
      }

      return result;
    } catch (Exception e) {
      logger.error("Error getting range from SSTable", e);
      return result;
    }
  }
}
