package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.api.Storage;
import io.cascadestore.lsm.api.ValueMerger;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.backgroundservice.CleanupService;
import io.cascadestore.lsm.core.backgroundservice.CompactionService;
import io.cascadestore.lsm.core.backgroundservice.FlushService;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
import io.cascadestore.lsm.io.BlockCache;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import io.cascadestore.lsm.wal.WAL;
import io.cascadestore.lsm.wal.WALImpl;
import io.cascadestore.lsm.wal.record.DeleteRecord;
import io.cascadestore.lsm.wal.record.PutRecord;
import io.cascadestore.lsm.wal.record.Record;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CascadeStore implements Storage {
  private static final Logger logger = LoggerFactory.getLogger(CascadeStore.class);

  // Configuration parameters
  private final CascadeConfig config;

  // Core components
  private MemTable activeMemTable;
  private final List<SSTable> ssTables;
  private final List<MemTable> immutableMemTables;
  private WAL wal;

  // Concurrency control
  private final ReadWriteLock memTableLock;
  private final AtomicLong sequenceNumber;
  private final AtomicLong layoutVersionId;
  private volatile StorageVersion storageVersion;
  private final AtomicBoolean recovering;

  // Background services
  private final BlockCache blockCache;
  private final CompactionService compactionService;
  private final CleanupService cleanupService;
  private final FlushService flushService;

  // Operation stores
  private PutStore putStore;
  private GetStore getStore;
  private DeleteStore deleteStore;
  private MergeStore mergeStore;

  public CascadeStore() {
    this(CascadeConfig.getDefault());
  }

  public CascadeStore(int memTableMaxSizeBytes, String dataDirectory, int compactionThreshold) {
    this(
        new CascadeConfig(
            memTableMaxSizeBytes,
            dataDirectory,
            compactionThreshold,
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            CompactionStrategyType.THRESHOLD // Default to threshold-based compaction
            ));
  }

  public CascadeStore(
      int memTableMaxSizeBytes,
      String dataDirectory,
      int compactionThreshold,
      CompactionStrategyType compactionStrategyType) {
    this(
        new CascadeConfig(
            memTableMaxSizeBytes,
            dataDirectory,
            compactionThreshold,
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            compactionStrategyType));
  }

  public CascadeStore(CascadeConfig config) {
    this.config = config;

    // Initialize components
    this.activeMemTable = new MemTable(config.memTableMaxSizeBytes());
    this.ssTables = new ArrayList<>();
    this.immutableMemTables = new ArrayList<>();

    // Initialize concurrency control
    this.memTableLock = new ReentrantReadWriteLock();
    this.sequenceNumber = new AtomicLong(0);
    this.layoutVersionId = new AtomicLong(0);
    this.storageVersion = StorageVersion.empty(0);
    this.recovering = new AtomicBoolean(false);

    // Create data directory if it doesn't exist
    File dir = new File(config.dataDirectory());
    if (!dir.exists()) {
      dir.mkdirs();
    }

    this.blockCache = BlockCache.create(config.blockCacheSizeBytes());

    // Load existing SSTables from disk
    loadSSTables();

    // Create WAL directory
    String walDirectory = Paths.get(config.dataDirectory(), "wal").toString();
    try {
      Files.createDirectories(Paths.get(walDirectory));

      // Initialize WAL
      this.wal = new WALImpl(walDirectory);

      // Recover from WAL if it exists
      recover();
    } catch (IOException e) {
      logger.error("Error initializing WAL", e);
    }

    // Initialize background services
    this.compactionService =
        new CompactionService(
            ssTables,
            config,
            sequenceNumber,
            this::publishStorageLayout,
            blockCache,
            layoutVersionId::get);
    this.cleanupService =
        new CleanupService(activeMemTable, immutableMemTables, memTableLock, config);
    this.flushService =
        new FlushService(
            immutableMemTables,
            ssTables,
            config,
            sequenceNumber,
            compactionService,
            this::truncateWalIfAllDataFlushed,
            this::publishStorageLayout,
            blockCache);

    // Start background services
    compactionService.start();
    cleanupService.start();
    flushService.start();

    // Initialize operation stores
    putStore = new PutStore(activeMemTable, memTableLock, wal, recovering);

    getStore =
        new GetStore(
            activeMemTable,
            storageVersion,
            memTableLock,
            config.parallelBloomEnabled(),
            config.parallelBloomMinTables());

    deleteStore = new DeleteStore(activeMemTable, memTableLock, wal, recovering, getStore);

    mergeStore = new MergeStore(activeMemTable, memTableLock, wal, recovering, getStore);

    publishStorageLayout();

    logger.info("CascadeStore initialized with data directory: " + config.dataDirectory());
  }

  /** Publishes an immutable snapshot of immutable-memtable and SSTable tiers for readers. */
  void publishStorageLayout() {
    List<MemTable> immutableCopy;
    synchronized (immutableMemTables) {
      immutableCopy = new ArrayList<>(immutableMemTables);
    }
    List<SSTable> ssTableCopy;
    synchronized (ssTables) {
      ssTableCopy = new ArrayList<>(ssTables);
    }
    StorageVersion previous = storageVersion;
    storageVersion =
        new StorageVersion(layoutVersionId.incrementAndGet(), immutableCopy, ssTableCopy);
    if (previous != null) {
      previous.release();
    }
    getStore.updateDependencies(activeMemTable, storageVersion, memTableLock);
  }


  // Initialization and Recovery
  private void loadSSTables() {
    File dataDir = new File(config.dataDirectory());
    File[] files = dataDir.listFiles((dir, name) -> name.endsWith(".data"));

    if (files == null || files.length == 0) {
      logger.info("No SSTable files found in directory: " + config.dataDirectory());
      return;
    }

    logger.info("Loading " + files.length + " SSTable files from disk");

    for (File file : files) {
      String fileName = file.getName();
      // Parse the level and sequence number from the file name
      // Format: sst_L<level>_S<sequenceNumber>.data
      if (fileName.startsWith("sst_L")) {
        try {
          int levelStart = fileName.indexOf('L') + 1;
          int levelEnd = fileName.indexOf('_', levelStart);
          int level = Integer.parseInt(fileName.substring(levelStart, levelEnd));

          int seqStart = fileName.indexOf('S') + 1;
          int seqEnd = fileName.indexOf('.', seqStart);
          long seq = Long.parseLong(fileName.substring(seqStart, seqEnd));

          // Update the sequence number to be greater than the highest in the SSTables
          sequenceNumber.updateAndGet(current -> Math.max(current, seq + 1));

          // Load the SSTable
          SSTable ssTable = new SSTable(config.dataDirectory(), level, seq, blockCache);
          ssTables.add(ssTable);

          logger.info("Loaded SSTable: " + fileName);
        } catch (Exception e) {
          logger.error("Error loading SSTable: " + fileName, e);
        }
      }
    }

    // Sort SSTables by sequence number (newest first)
    ssTables.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));
    logger.info("Loaded " + ssTables.size() + " SSTables from disk");
  }

  private void recover() {
    try {
      // Set recovering flag to true
      recovering.set(true);

      // Read all records from the WAL
      List<Record> records = wal.readRecords();

      if (records.isEmpty()) {
        logger.info("No WAL records to recover");
        return;
      }

      logger.info("Recovering {} records from WAL", records.size());

      // Sort records by sequence number to ensure correct order
      records.sort((r1, r2) -> Long.compare(r1.getSequenceNumber(), r2.getSequenceNumber()));

      // Update sequence number to be greater than the highest in the WAL
      if (!records.isEmpty()) {
        long maxSeqNum = records.get(records.size() - 1).getSequenceNumber();
        sequenceNumber.set(maxSeqNum + 1);
      }

      // Replay records
      for (Record record : records) {
        if (record instanceof PutRecord putRecord) {
          activeMemTable.put(putRecord.getKey(), putRecord.getValue(), putRecord.getTtlSeconds());
        } else if (record instanceof DeleteRecord) {
          activeMemTable.delete(record.getKey());
        }
      }

      logger.info("Recovery completed");
    } catch (IOException e) {
      logger.error("Error recovering from WAL", e);
    } finally {
      // Reset recovering flag
      recovering.set(false);
    }
  }

  /**
   * Deletes WAL segments once every memtable entry has been flushed to SSTables. Safe to call after
   * a flush batch when no immutable memtables remain and the active memtable is empty.
   */
  private void truncateWalIfAllDataFlushed() {
    if (wal == null) {
      return;
    }

    synchronized (immutableMemTables) {
      if (!immutableMemTables.isEmpty()) {
        return;
      }
    }

    memTableLock.readLock().lock();
    try {
      if (!activeMemTable.getEntries().isEmpty()) {
        return;
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    truncateWal();
  }

  private void truncateWal() {
    if (wal == null) {
      return;
    }
    try {
      wal.sync();
      wal.deleteAllLogs();
      logger.info("WAL truncated; memtable state is durable in SSTables");
    } catch (IOException e) {
      logger.warn("Failed to truncate WAL", e);
    }
  }


  // MemTable Management and Maintenance
  private void switchMemTable() {
    boolean shouldFlush = false;
    memTableLock.writeLock().lock();
    try {
      // Another writer may have already rotated the active MemTable.
      if (!activeMemTable.isFull()) {
        return;
      }

      // Make the current MemTable immutable
      activeMemTable.makeImmutable();

      // Add it to the list of immutable MemTables
      synchronized (immutableMemTables) {
        immutableMemTables.add(activeMemTable);
      }

      // Create a new active MemTable
      activeMemTable = new MemTable(config.memTableMaxSizeBytes());

      // Update the GetStore and PutStore with the new state
      getStore.updateDependencies(activeMemTable, storageVersion, memTableLock);
      putStore.updateDependencies(activeMemTable, memTableLock, wal, recovering);
      deleteStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
      mergeStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);

      try {
        wal.sync();
      } catch (IOException e) {
        logger.warn("Failed to sync WAL during MemTable switch", e);
      }

      logger.info("Switched to new MemTable, old one scheduled for flushing");

      publishStorageLayout();
      shouldFlush = true;
    } finally {
      memTableLock.writeLock().unlock();
    }

    if (shouldFlush) {
      flushService.executeNow();
    }
  }

  // Core Operations (put, get, delete)
  @Override
  public boolean put(byte[] key, byte[] value, long ttlSeconds) {
    for (int attempt = 0; attempt < 32; attempt++) {
      int result = putStore.put(key, value, ttlSeconds);
      if (result == PutStore.RESULT_SUCCESS) {
        return true;
      }
      if (result != PutStore.RESULT_MEMTABLE_FULL) {
        return false;
      }
      switchMemTable();
      putStore.updateDependencies(activeMemTable, memTableLock, wal, recovering);
    }
    return false;
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    return put(key, value, 0); // 0 means no expiration
  }

  @Override
  public boolean merge(byte[] key, ValueMerger merger) {
    for (int attempt = 0; attempt < 32; attempt++) {
      int result = mergeStore.merge(key, merger);
      if (result == MergeStore.RESULT_SUCCESS) {
        return true;
      }
      if (result == MergeStore.RESULT_KEY_NOT_FOUND) {
        return false;
      }
      if (result != MergeStore.RESULT_MEMTABLE_FULL) {
        return false;
      }
      switchMemTable();
      mergeStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
    }
    return false;
  }

  @Override
  public byte[] get(byte[] key) {
    return getStore.lookup(key);
  }

  @Override
  public boolean delete(byte[] key) {
    for (int attempt = 0; attempt < 32; attempt++) {
      int result = deleteStore.delete(key);
      if (result == DeleteStore.RESULT_SUCCESS) {
        return true;
      }
      if (result != DeleteStore.RESULT_MEMTABLE_FULL) {
        return false;
      }
      switchMemTable();
      deleteStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
    }
    return false;
  }


  // Query Operations (listKeys, containsKey, size, getRange, getIterator)
  @Override
  public List<byte[]> listKeys() {
    List<byte[]> allKeys = new ArrayList<>();

    // Collect keys from active MemTable
    memTableLock.readLock().lock();
    try {
      Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
      for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
        if (!entry.getValue().isExpired() && !entry.getValue().isTombstone()) {
          allKeys.add(entry.getKey().getData());
        }
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    // Collect keys from immutable MemTables
    synchronized (immutableMemTables) {
      for (MemTable memTable : immutableMemTables) {
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
          if (!entry.getValue().isExpired() && !entry.getValue().isTombstone()) {
            allKeys.add(entry.getKey().getData());
          }
        }
      }
    }

    // Collect keys from SSTables
    synchronized (ssTables) {
      for (SSTable ssTable : ssTables) {
        List<byte[]> tableKeys = ssTable.listKeys();
        for (byte[] key : tableKeys) {
          // We need to check if the key is already in the list to avoid duplicates
          // This is inefficient for large datasets, but it's a simple implementation
          boolean isDuplicate = false;
          for (byte[] existingKey : allKeys) {
            if (Arrays.equals(key, existingKey)) {
              isDuplicate = true;
              break;
            }
          }
          if (!isDuplicate) {
            allKeys.add(key);
          }
        }
      }
    }

    return allKeys;
  }

  @Override
  public boolean containsKey(byte[] key) {
    if (key == null || key.length == 0) {
      return false;
    }

    return getStore.lookup(key) != null;
  }

  @Override
  public int size() {
    int totalSize = 0;

    // Count entries in active MemTable
    memTableLock.readLock().lock();
    try {
      Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
      for (MemTable.ValueEntry entry : entries.values()) {
        if (!entry.isExpired() && !entry.isTombstone()) {
          totalSize++;
        }
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    // Count entries in immutable MemTables
    synchronized (immutableMemTables) {
      for (MemTable memTable : immutableMemTables) {
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
        for (MemTable.ValueEntry entry : entries.values()) {
          if (!entry.isExpired() && !entry.isTombstone()) {
            totalSize++;
          }
        }
      }
    }

    // Count entries in SSTables
    // This is an approximation as it doesn't account for duplicates across SSTables
    // or entries that might be overridden by newer entries in MemTables
    synchronized (ssTables) {
      for (SSTable ssTable : ssTables) {
        totalSize += ssTable.countEntries();
      }
    }

    return totalSize;
  }


  // Lifecycle Operations (clear, shutdown)
  @Override
  public void clear() {
    // Clear active MemTable
    memTableLock.writeLock().lock();
    try {
      activeMemTable.close();
      activeMemTable = new MemTable(config.memTableMaxSizeBytes());
    } finally {
      memTableLock.writeLock().unlock();
    }

    // Clear immutable MemTables
    synchronized (immutableMemTables) {
      for (MemTable memTable : immutableMemTables) {
        memTable.close();
      }
      immutableMemTables.clear();
    }

    // Clear SSTables
    synchronized (ssTables) {
      for (SSTable ssTable : ssTables) {
        ssTable.forceCloseAndDelete();
      }
      ssTables.clear();
    }

    // Reset sequence number
    sequenceNumber.set(0);
    layoutVersionId.set(0);

    // Delete all WAL files
    try {
      wal.deleteAllLogs();
    } catch (IOException e) {
      logger.error("Error deleting WAL files", e);
    }

    // Update the GetStore and PutStore with the new state
    getStore.updateDependencies(activeMemTable, storageVersion, memTableLock);
    putStore.updateDependencies(activeMemTable, memTableLock, wal, recovering);
    deleteStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
    mergeStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
    publishStorageLayout();

    logger.info("CascadeStore cleared");
  }

  @Override
  public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
    // Use a custom Map implementation that can handle byte array keys
    Map<byte[], byte[]> result =
        new HashMap<byte[], byte[]>() {
          @Override
          public byte[] get(Object key) {
            if (!(key instanceof byte[] keyBytes)) {
              return null;
            }
            for (Entry<byte[], byte[]> entry : entrySet()) {
              if (Arrays.equals(entry.getKey(), keyBytes)) {
                return entry.getValue();
              }
            }

            return null;
          }
        };

    // Use the iterator for efficient range scanning
    try (KeyValueIterator iterator = getIterator(startKey, endKey)) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        result.put(entry.getKey(), entry.getValue());
      }
    } catch (Exception e) {
      logger.error("Error during range query", e);
    }
    return result;
  }

  @Override
  public KeyValueIterator getIterator(byte[] startKey, byte[] endKey) {
    return new CascadeIterator(startKey, endKey);
  }

  public int getActiveMemTableEntryCount() {
    return activeMemTable.getEntries().size();
  }

  public int getImmutableMemTablesCount() {
    synchronized (immutableMemTables) {
      return immutableMemTables.size();
    }
  }

  public int getSSTablesCount() {
    synchronized (ssTables) {
      return ssTables.size();
    }
  }

  public void flushMemTables() {
    if (logger.isDebugEnabled()) {
      synchronized (immutableMemTables) {
        logger.debug(
            "flushMemTables: {} immutable MemTables pending", immutableMemTables.size());
      }
    }

    flushService.executeNow();

    if (logger.isDebugEnabled()) {
      synchronized (ssTables) {
        logger.debug("flushMemTables: {} SSTables after flush", ssTables.size());
        for (SSTable ssTable : ssTables) {
          logger.debug("flushMemTables: SSTable seq={}", ssTable.getSequenceNumber());
        }
      }
    }
  }

  public void switchMemTableForTest() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "switchMemTableForTest: active MemTable size={} bytes, entries={}",
          activeMemTable.getSizeBytes(),
          activeMemTable.getEntries().size());
    }

    memTableLock.writeLock().lock();
    try {
      activeMemTable.makeImmutable();

      synchronized (immutableMemTables) {
        immutableMemTables.add(activeMemTable);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "switchMemTableForTest: {} immutable MemTables queued",
              immutableMemTables.size());
        }
      }

      activeMemTable = new MemTable(config.memTableMaxSizeBytes());

      getStore.updateDependencies(activeMemTable, storageVersion, memTableLock);
      putStore.updateDependencies(activeMemTable, memTableLock, wal, recovering);
      deleteStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
      mergeStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);

      publishStorageLayout();

      logger.info("Switched to new MemTable for testing");
    } finally {
      memTableLock.writeLock().unlock();
    }
  }


  // Iterator Implementation
  private class CascadeIterator implements KeyValueIterator {
    private final ByteArrayWrapper startKeyWrapper;
    private final ByteArrayWrapper endKeyWrapper;
    private final List<Map.Entry<ByteArrayWrapper, byte[]>> entries;
    private int currentIndex = 0;

    public CascadeIterator(byte[] startKey, byte[] endKey) {
      this.startKeyWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
      this.endKeyWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;
      this.entries = new ArrayList<>();

      // Collect entries from MemTables and SSTables
      collectEntries();

      // Sort entries by key
      Collections.sort(entries, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
    }

    private void collectEntries() {
      // Collect entries from active MemTable
      memTableLock.readLock().lock();
      try {
        Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = activeMemTable.getEntries();
        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
          ByteArrayWrapper key = entry.getKey();

          // Check if key is in range
          if (isKeyInRange(key)) {
            MemTable.ValueEntry valueEntry = entry.getValue();

            // Skip expired or tombstone entries
            if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
              entries.add(new AbstractMap.SimpleEntry<>(key, valueEntry.getValue()));
            }
          }
        }
      } finally {
        memTableLock.readLock().unlock();
      }

      // Collect entries from immutable MemTables
      synchronized (immutableMemTables) {
        for (MemTable memTable : immutableMemTables) {
          Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = memTable.getEntries();
          for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
            ByteArrayWrapper key = entry.getKey();

            // Check if key is in range
            if (isKeyInRange(key)) {
              MemTable.ValueEntry valueEntry = entry.getValue();

              // Skip expired or tombstone entries
              if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
                entries.add(new AbstractMap.SimpleEntry<>(key, valueEntry.getValue()));
              }
            }
          }
        }
      }

      // Collect entries from SSTables
      synchronized (ssTables) {
        for (SSTable ssTable : ssTables) {
          // Get entries in the specified range from the SSTable
          byte[] startKeyBytes = startKeyWrapper != null ? startKeyWrapper.getData() : null;
          byte[] endKeyBytes = endKeyWrapper != null ? endKeyWrapper.getData() : null;

          Map<byte[], byte[]> rangeEntries = ssTable.getRange(startKeyBytes, endKeyBytes);

          for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();

            // Skip entries that are already in the list (from MemTables)
            boolean isDuplicate = false;
            for (Map.Entry<ByteArrayWrapper, byte[]> existingEntry : entries) {
              if (Arrays.equals(key, existingEntry.getKey().getData())) {
                isDuplicate = true;
                break;
              }
            }

            if (!isDuplicate) {
              entries.add(new AbstractMap.SimpleEntry<>(new ByteArrayWrapper(key), value));
            }
          }
        }
      }
    }

    private boolean isKeyInRange(ByteArrayWrapper key) {
      if (startKeyWrapper != null && key.compareTo(startKeyWrapper) < 0) {
        return false;
      }
      if (endKeyWrapper != null && key.compareTo(endKeyWrapper) >= 0) {
        return false;
      }
      return true;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < entries.size();
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements in the iterator");
      }

      Map.Entry<ByteArrayWrapper, byte[]> entry = entries.get(currentIndex++);
      return new AbstractMap.SimpleEntry<>(entry.getKey().getData(), entry.getValue());
    }

    @Override
    public byte[] peekNextKey() {
      if (!hasNext()) {
        return null;
      }

      return entries.get(currentIndex).getKey().getData();
    }

    @Override
    public void close() {
      // No resources to release
    }
  }

  @Override
  public void shutdown() {
    try {
      // Flush any pending data
      flushService.executeNow();

      // Make active MemTable immutable and flush it
      memTableLock.writeLock().lock();
      try {
        activeMemTable.makeImmutable();
        synchronized (immutableMemTables) {
          immutableMemTables.add(activeMemTable);
        }
      } finally {
        memTableLock.writeLock().unlock();
      }

      flushService.executeNow();

      truncateWal();

      // Shutdown background services
      flushService.shutdown();
      compactionService.shutdown();
      cleanupService.shutdown();

      if (!flushService.awaitTermination(5, TimeUnit.SECONDS)) {
        flushService.shutdownNow();
      }
      if (!compactionService.awaitTermination(5, TimeUnit.SECONDS)) {
        compactionService.shutdownNow();
      }
      if (!cleanupService.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupService.shutdownNow();
      }

      // Close all SSTables
      StorageVersion version = storageVersion;
      if (version != null) {
        version.release();
      }
      synchronized (ssTables) {
        for (SSTable ssTable : ssTables) {
          ssTable.close();
        }
      }

      // Close the WAL
      try {
        if (wal != null) {
          wal.close();
        }
      } catch (IOException e) {
        logger.warn("Error closing WAL", e);
      }

      logger.info("CascadeStore shutdown completed");
    } catch (InterruptedException e) {
      flushService.shutdownNow();
      compactionService.shutdownNow();
      cleanupService.shutdownNow();
      Thread.currentThread().interrupt();
      logger.warn("CascadeStore shutdown interrupted", e);
    }
  }
}
