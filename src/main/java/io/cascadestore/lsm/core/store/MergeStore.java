package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.api.ValueMerger;
import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.wal.WAL;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MergeStore {
  private static final Logger logger = LoggerFactory.getLogger(MergeStore.class);

  public static final int RESULT_SUCCESS = 0;
  public static final int RESULT_INVALID_INPUT = 1;
  public static final int RESULT_KEY_NOT_FOUND = 2;
  public static final int RESULT_WAL_ERROR = 3;
  public static final int RESULT_MEMTABLE_FULL = 4;

  private volatile MemTable activeMemTable;
  private ReadWriteLock memTableLock;
  private WAL wal;
  private AtomicBoolean recovering;
  private GetStore getStore;

  private IOException lastException;

  public MergeStore(
      MemTable activeMemTable,
      ReadWriteLock memTableLock,
      WAL wal,
      AtomicBoolean recovering,
      GetStore getStore) {
    if (activeMemTable == null) {
      throw new IllegalArgumentException("activeMemTable cannot be null");
    }
    if (memTableLock == null) {
      throw new IllegalArgumentException("memTableLock cannot be null");
    }
    if (wal == null) {
      throw new IllegalArgumentException("wal cannot be null");
    }
    if (recovering == null) {
      throw new IllegalArgumentException("recovering cannot be null");
    }
    if (getStore == null) {
      throw new IllegalArgumentException("getStore cannot be null");
    }

    this.activeMemTable = activeMemTable;
    this.memTableLock = memTableLock;
    this.wal = wal;
    this.recovering = recovering;
    this.getStore = getStore;
  }

  public void updateDependencies(
      MemTable activeMemTable,
      ReadWriteLock memTableLock,
      WAL wal,
      AtomicBoolean recovering,
      GetStore getStore) {
    if (activeMemTable == null) {
      throw new IllegalArgumentException("activeMemTable cannot be null");
    }
    if (memTableLock == null) {
      throw new IllegalArgumentException("memTableLock cannot be null");
    }
    if (wal == null) {
      throw new IllegalArgumentException("wal cannot be null");
    }
    if (recovering == null) {
      throw new IllegalArgumentException("recovering cannot be null");
    }
    if (getStore == null) {
      throw new IllegalArgumentException("getStore cannot be null");
    }

    this.activeMemTable = activeMemTable;
    this.memTableLock = memTableLock;
    this.wal = wal;
    this.recovering = recovering;
    this.getStore = getStore;
  }

  public IOException getLastException() {
    return lastException;
  }

  public int merge(byte[] key, ValueMerger merger) {
    lastException = null;

    if (key == null || key.length == 0 || merger == null) {
      return RESULT_INVALID_INPUT;
    }

    byte[] existing = getStore.lookup(key);
    if (existing == null) {
      return RESULT_KEY_NOT_FOUND;
    }

    byte[] merged;
    try {
      merged = merger.merge(existing);
    } catch (RuntimeException e) {
      logger.error("Merge callback failed for key", e);
      return RESULT_INVALID_INPUT;
    }

    if (merged == null) {
      return RESULT_KEY_NOT_FOUND;
    }

    try {
      if (!recovering.get()) {
        wal.appendPutRecord(key, merged, 0);
      }
      return putInMemTable(key, merged, 0);
    } catch (IOException e) {
      logger.error("Error writing merged value to WAL", e);
      lastException = e;
      return RESULT_WAL_ERROR;
    }
  }

  private int putInMemTable(byte[] key, byte[] value, long ttlSeconds) {
    boolean success = false;
    boolean needSwitch = false;

    memTableLock.readLock().lock();
    try {
      success = activeMemTable.put(key, value, ttlSeconds);
      if (!success && activeMemTable.isFull()) {
        needSwitch = true;
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    if (needSwitch) {
      return RESULT_MEMTABLE_FULL;
    }
    return success ? RESULT_SUCCESS : RESULT_INVALID_INPUT;
  }
}
