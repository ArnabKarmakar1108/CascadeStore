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
  private SSTableDataFormat dataFormat = SSTableDataFormat.legacy();

  private final AtomicInteger pinCount = new AtomicInteger(0);
  private volatile boolean retired;
  private final BlockCache blockCache;
  private final boolean writeLz4Enabled;

  public SSTable(MemTable memTable, String directory, int level, long sequenceNumber)
      throws IOException {
    this(memTable, directory, level, sequenceNumber, null, true);
  }

  public SSTable(
      MemTable memTable, String directory, int level, long sequenceNumber, BlockCache blockCache)
      throws IOException {
    this(memTable, directory, level, sequenceNumber, blockCache, true);
  }

  public SSTable(
      MemTable memTable,
      String directory,
      int level,
      long sequenceNumber,
      BlockCache blockCache,
      boolean writeLz4Enabled)
      throws IOException {
    this.blockCache = blockCache;
    this.writeLz4Enabled = writeLz4Enabled;
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

  /**
   * Builds an SSTable by streaming pre-sorted records from {@code source} (used by compaction). The
   * source is responsible for de-duplication (newest-wins) and any tombstone dropping.
   */
  public SSTable(
      String directory,
      int level,
      long sequenceNumber,
      BlockCache blockCache,
      SortedRecordSource source,
      int estimatedEntries)
      throws IOException {
    this(directory, level, sequenceNumber, blockCache, source, estimatedEntries, true);
  }

  public SSTable(
      String directory,
      int level,
      long sequenceNumber,
      BlockCache blockCache,
      SortedRecordSource source,
      int estimatedEntries,
      boolean writeLz4Enabled)
      throws IOException {
    this.blockCache = blockCache;
    this.writeLz4Enabled = writeLz4Enabled;
    this.creationTime = System.currentTimeMillis();
    this.level = level;
    this.sequenceNumber = sequenceNumber;
    this.sparseIndex = SparseIndex.empty();

    File dir = new File(directory);
    if (!dir.exists()) {
      dir.mkdirs();
    }

    String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
    this.dataFilePath = Path.of(directory, filePrefix + ".data");
    this.indexFilePath = Path.of(directory, filePrefix + ".index");
    this.filterFilePath = Path.of(directory, filePrefix + ".filter");

    try {
      writeToDisk(source, estimatedEntries);
      logger.info("Created merged SSTable: {} ({} entries)", filePrefix, entryCount);
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
    this.writeLz4Enabled = true;
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
    if (!memTable.isImmutable()) {
      memTable.makeImmutable();
    }
    try (SortedRecordSource source = new MemTableRecordSource(memTable)) {
      writeToDisk(source, memTable.getEntries().size());
    }
    logger.info("MemTable flushed to SSTable successfully");
  }

  /**
   * Streams sorted records from {@code source} directly to the on-disk data/index/filter files.
   * Shared by memtable flush and compaction merge so the record format stays identical.
   */
  private void writeToDisk(SortedRecordSource source, int estimatedEntries) throws IOException {
    BloomFilter filter =
        new BloomFilter(Math.max(estimatedEntries, 1), BloomFilter.DEFAULT_FALSE_POSITIVE_RATE);

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

      dataFormat = writeLz4Enabled ? SSTableDataFormat.lz4() : SSTableDataFormat.legacy();
      dataFormat.writeHeader(writeDataChannel, creationTime, level);

      int writtenEntries = 0;
      long currentOffset = dataFormat.dataStartOffset();
      long lastIndexedOffset = -1;
      ByteArrayWrapper lastIndexedKey = null;
      long lastEntryOffset = -1;
      TreeMap<ByteArrayWrapper, Long> indexBuilder = new TreeMap<>();

      while (source.advance()) {
        byte[] key = source.key();
        byte[] value = source.value();

        filter.add(key);

        ByteBuffer keyBuffer = ByteBuffer.allocate(4 + key.length);
        keyBuffer.putInt(key.length);
        keyBuffer.put(key);
        keyBuffer.flip();
        writeDataChannel.write(keyBuffer);

        dataFormat.writeValueRecord(writeDataChannel, value, source.expirationTime());

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        lastIndexedKey = keyWrapper;
        lastEntryOffset = currentOffset;
        writtenEntries++;

        if (SparseIndexPolicy.shouldAddIndexEntry(currentOffset, lastIndexedOffset)) {
          indexBuilder.put(keyWrapper, currentOffset);
          lastIndexedOffset = currentOffset;
        }

        currentOffset = writeDataChannel.position();
      }

      if (lastIndexedKey != null && lastEntryOffset != lastIndexedOffset) {
        indexBuilder.put(lastIndexedKey, lastEntryOffset);
      }

      sparseIndex = SparseIndex.from(indexBuilder);
      entryCount = writtenEntries;
      dataFormat.patchEntryCount(writeDataChannel, writtenEntries);

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

    filter.save(filterFilePath.toString());
    this.bloomFilter = filter;

    this.dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
    this.mappedDataFile = MappedDataFile.tryMap(dataChannel);
    this.indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
  }

  /** Adapts an immutable memtable's entries to the streaming writer, skipping expired/corrupt rows. */
  private static final class MemTableRecordSource implements SortedRecordSource {
    private final java.util.Iterator<Map.Entry<ByteArrayWrapper, MemTable.ValueEntry>> iterator;
    private byte[] key;
    private byte[] value;
    private long expirationTime;

    private MemTableRecordSource(MemTable memTable) {
      this.iterator = memTable.getEntries().entrySet().iterator();
    }

    @Override
    public boolean advance() {
      while (iterator.hasNext()) {
        Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry = iterator.next();
        MemTable.ValueEntry valueEntry = entry.getValue();
        if (valueEntry.isExpired()) {
          continue;
        }
        if (valueEntry.isTombstone()) {
          value = null;
        } else {
          byte[] v = valueEntry.getValue();
          if (v == null) {
            logger.warn(
                "Skipping corrupt entry during flush: non-tombstone key has no value (key length {})",
                entry.getKey().getData().length);
            continue;
          }
          value = v;
        }
        key = entry.getKey().getData();
        expirationTime = valueEntry.getExpirationTime();
        return true;
      }
      return false;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] value() {
      return value;
    }

    @Override
    public long expirationTime() {
      return expirationTime;
    }
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
    dataFormat = SSTableDataFormat.readHeader(dataChannel);
    entryCount = dataFormat.readEntryCount(dataChannel);
    logger.info(
        "Loaded SSTable with {} entries, level {}, compression={}",
        entryCount,
        level,
        dataFormat.lz4Values() ? "LZ4" : "none");
    return entryCount;
  }

  private void rebuildBloomFilterFromData() {
    for (byte[] key : listKeys()) {
      bloomFilter.add(key);
    }
  }

  public byte[] get(byte[] key) {
    // NOTE: a retired table may still be pinned by an in-flight reader's storage
    // version; its files are not deleted until the pin count reaches zero, so it must
    // keep serving reads. Only bail when the data channel is genuinely unavailable.
    if (key == null || key.length == 0 || dataChannel == null) {
      return null;
    }

    // First check the bloom filter for a quick negative
    if (bloomFilter != null && !bloomFilter.mightContain(key)) {
      return null; // Definitely not in the set
    }

    try {
      long startOffset = sparseIndex.floorOffset(key);
      if (startOffset < 0) {
        startOffset = dataFormat.dataStartOffset();
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
    // See get(): retired-but-pinned tables must keep serving reads.
    if (key == null || key.length == 0 || dataChannel == null) {
      return false;
    }

    if (bloomFilter != null && !bloomFilter.mightContain(key)) {
      return false;
    }

    try {
      long startOffset = sparseIndex.floorOffset(key);
      if (startOffset < 0) {
        startOffset = dataFormat.dataStartOffset();
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
        return dataFormat.readValue(reader);
      }

      dataFormat.skipValue(reader);
    }
    return null;
  }

  private boolean findKeyPresenceInDataFile(
      byte[] key, long startPosition, BufferedDataReader reader) throws IOException {
    reader.seek(startPosition);
    while (reader.position() < reader.size()) {
      int keyLength = reader.readInt();
      if (reader.bytesEqual(keyLength, key)) {
        return dataFormat.readValuePresence(reader);
      }

      dataFormat.skipValue(reader);
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

  /**
   * Streaming, sorted record source consumed by {@link #writeToDisk}. {@code value() == null}
   * denotes a tombstone.
   */
  public interface SortedRecordSource extends AutoCloseable {
    boolean advance() throws IOException;

    byte[] key();

    byte[] value();

    long expirationTime();

    @Override
    default void close() throws IOException {}
  }

  /**
   * Sequential cursor over this table's sorted data file, including tombstones and expiration
   * timestamps. Intended for single-threaded compaction use; each cursor owns its own reader so it
   * does not contend with concurrent point reads. The table must stay pinned for the cursor's life.
   */
  public RecordCursor openRecordCursor() throws IOException {
    if (dataChannel == null) {
      throw new IOException("SSTable data channel is not open: " + dataFilePath);
    }
    return new RecordCursor();
  }

  public final class RecordCursor implements AutoCloseable {
    private final BufferedDataReader reader;
    private byte[] key;
    private byte[] value;
    private long expirationTime;

    private RecordCursor() throws IOException {
      this.reader =
          new BufferedDataReader(
              dataChannel,
              BufferedDataReader.DEFAULT_BUFFER_SIZE,
              blockCache,
              sequenceNumber,
              mappedDataFile);
      this.reader.seek(dataFormat.dataStartOffset());
    }

    public boolean advance() throws IOException {
      if (reader.position() >= reader.size()) {
        key = null;
        value = null;
        return false;
      }
      int keyLength = reader.readInt();
      key = reader.readBytes(keyLength);
      SSTableDataFormat.ValueRecord record = dataFormat.readRecord(reader);
      value = record.value();
      expirationTime = record.expirationTime();
      return true;
    }

    public byte[] key() {
      return key;
    }

    /** Decoded value, or {@code null} for a tombstone. */
    public byte[] value() {
      return value;
    }

    public long expirationTime() {
      return expirationTime;
    }

    public long sourceSequence() {
      return sequenceNumber;
    }

    @Override
    public void close() {
      reader.close();
    }
  }

  private void scanDataFile(DataFileEntryConsumer consumer) throws IOException {
    BufferedDataReader reader = openDataReader();
    reader.seek(dataFormat.dataStartOffset());
    while (reader.position() < reader.size()) {
      int keyLength = reader.readInt();
      byte[] key = reader.readBytes(keyLength);
      byte[] value = dataFormat.readValue(reader);
      if (value == null) {
        continue;
      }
      consumer.accept(key, value);
    }
  }

  @FunctionalInterface
  private interface DataFileEntryConsumer {
    void accept(byte[] key, byte[] value) throws IOException;
  }

  public boolean mightContain(byte[] key) {
    // Retired-but-pinned tables must still be probed; skipping them would hide
    // live keys from readers holding an older storage version.
    return bloomFilter != null && bloomFilter.mightContain(key);
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
    return getDataFileSizeBytes() + getIndexFileSizeBytes() + getFilterFileSizeBytes();
  }

  public long getDataFileSizeBytes() {
    try {
      File dataFile = dataFilePath.toFile();
      return dataFile.exists() ? dataFile.length() : 0L;
    } catch (Exception e) {
      logger.warn("Error getting SSTable data file size", e);
      return 0L;
    }
  }

  private long getIndexFileSizeBytes() {
    try {
      File indexFile = indexFilePath.toFile();
      return indexFile.exists() ? indexFile.length() : 0L;
    } catch (Exception e) {
      logger.warn("Error getting SSTable index file size", e);
      return 0L;
    }
  }

  private long getFilterFileSizeBytes() {
    try {
      File filterFile = filterFilePath.toFile();
      return filterFile.exists() ? filterFile.length() : 0L;
    } catch (Exception e) {
      logger.warn("Error getting SSTable filter file size", e);
      return 0L;
    }
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
