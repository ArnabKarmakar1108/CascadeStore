package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.api.Storage;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.core.compaction.CompactionStrategyType;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CascadeStore implements Storage {
  private static final Logger logger = LoggerFactory.getLogger(CascadeStore.class);

  private final CascadeConfig config;
  private MemTable activeMemTable;
  private final List<SSTable> ssTables;
  private final List<MemTable> immutableMemTables;
  private WAL wal;
  private final ReadWriteLock memTableLock;
  private final AtomicLong sequenceNumber;
  private final AtomicBoolean recovering;
  private PutStore putStore;
  private GetStore getStore;

  public CascadeStore() {
    this(CascadeConfig.getDefault());
  }

  public CascadeStore(int memTableMaxSizeBytes, String dataDirectory, int compactionThreshold) {
    this(
        new CascadeConfig(
            memTableMaxSizeBytes,
            dataDirectory,
            compactionThreshold,
            30,
            1,
            10,
            CompactionStrategyType.THRESHOLD));
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
            30,
            1,
            10,
            compactionStrategyType));
  }

  public CascadeStore(CascadeConfig config) {
    this.config = config;
    this.activeMemTable = new MemTable(config.memTableMaxSizeBytes());
    this.ssTables = new ArrayList<>();
    this.immutableMemTables = new ArrayList<>();
    this.memTableLock = new ReentrantReadWriteLock();
    this.sequenceNumber = new AtomicLong(0);
    this.recovering = new AtomicBoolean(false);

    File dir = new File(config.dataDirectory());
    if (!dir.exists()) {
      dir.mkdirs();
    }

    loadSSTables();

    String walDirectory = Paths.get(config.dataDirectory(), "wal").toString();
    try {
      Files.createDirectories(Paths.get(walDirectory));
      this.wal = new WALImpl(walDirectory);
      recover();
    } catch (IOException e) {
      logger.error("Error initializing WAL", e);
    }

    putStore = new PutStore(activeMemTable, memTableLock, wal, recovering);
    getStore = new GetStore(activeMemTable, immutableMemTables, ssTables, memTableLock);

    logger.info("CascadeStore initialized with data directory: " + config.dataDirectory());
  }

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
      if (fileName.startsWith("sst_L")) {
        try {
          int levelStart = fileName.indexOf('L') + 1;
          int levelEnd = fileName.indexOf('_', levelStart);
          int level = Integer.parseInt(fileName.substring(levelStart, levelEnd));

          int seqStart = fileName.indexOf('S') + 1;
          int seqEnd = fileName.indexOf('.', seqStart);
          long seq = Long.parseLong(fileName.substring(seqStart, seqEnd));

          sequenceNumber.updateAndGet(current -> Math.max(current, seq + 1));

          SSTable ssTable = new SSTable(config.dataDirectory(), level, seq);
          ssTables.add(ssTable);

          logger.info("Loaded SSTable: " + fileName);
        } catch (Exception e) {
          logger.error("Error loading SSTable: " + fileName, e);
        }
      }
    }

    ssTables.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));
    logger.info("Loaded " + ssTables.size() + " SSTables from disk");
  }

  private void recover() {
    try {
      recovering.set(true);
      List<Record> records = wal.readRecords();

      if (records.isEmpty()) {
        logger.info("No WAL records to recover");
        return;
      }

      logger.info("Recovering %d records from WAL", records.size());
      records.sort((r1, r2) -> Long.compare(r1.getSequenceNumber(), r2.getSequenceNumber()));

      if (!records.isEmpty()) {
        long maxSeqNum = records.get(records.size() - 1).getSequenceNumber();
        sequenceNumber.set(maxSeqNum + 1);
      }

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
      recovering.set(false);
    }
  }

  @Override
  public boolean put(byte[] key, byte[] value, long ttlSeconds) {
    throw new UnsupportedOperationException("put() added in a follow-up commit");
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("put() added in a follow-up commit");
  }

  @Override
  public byte[] get(byte[] key) {
    throw new UnsupportedOperationException("get() added in a follow-up commit");
  }

  @Override
  public boolean delete(byte[] key) {
    throw new UnsupportedOperationException("delete() added in a follow-up commit");
  }

  @Override
  public List<byte[]> listKeys() {
    throw new UnsupportedOperationException("listKeys() added in a follow-up commit");
  }

  @Override
  public boolean containsKey(byte[] key) {
    throw new UnsupportedOperationException("containsKey() added in a follow-up commit");
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("size() added in a follow-up commit");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("clear() added in a follow-up commit");
  }

  @Override
  public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
    throw new UnsupportedOperationException("getRange() added in a follow-up commit");
  }

  @Override
  public KeyValueIterator getIterator(byte[] startKey, byte[] endKey) {
    throw new UnsupportedOperationException("getIterator() added in a follow-up commit");
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("shutdown() added in a follow-up commit");
  }
}
