package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GetStore {
  private static final Logger logger = LoggerFactory.getLogger(GetStore.class);

  public static final int RESULT_SUCCESS = 0;
  public static final int RESULT_INVALID_INPUT = 1;
  public static final int RESULT_KEY_NOT_FOUND = 2;

  // Dependencies
  private MemTable activeMemTable;
  private List<MemTable> immutableMemTables;
  private List<SSTable> ssTables;
  private ReadWriteLock memTableLock;

  // For successful get operations
  private byte[] retrievedValue;

  public GetStore(
      MemTable activeMemTable,
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      ReadWriteLock memTableLock) {
    if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
    if (immutableMemTables == null)
      throw new IllegalArgumentException("immutableMemTables cannot be null");
    if (ssTables == null) throw new IllegalArgumentException("ssTables cannot be null");
    if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");

    this.activeMemTable = activeMemTable;
    this.immutableMemTables = immutableMemTables;
    this.ssTables = ssTables;
    this.memTableLock = memTableLock;
  }

  public void updateDependencies(
      MemTable activeMemTable,
      List<MemTable> immutableMemTables,
      List<SSTable> ssTables,
      ReadWriteLock memTableLock) {
    if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
    if (immutableMemTables == null)
      throw new IllegalArgumentException("immutableMemTables cannot be null");
    if (ssTables == null) throw new IllegalArgumentException("ssTables cannot be null");
    if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");

    this.activeMemTable = activeMemTable;
    this.immutableMemTables = immutableMemTables;
    this.ssTables = ssTables;
    this.memTableLock = memTableLock;
  }

  public byte[] getRetrievedValue() {
    return retrievedValue;
  }

  public int get(byte[] key) {
    // Reset retrieved value
    retrievedValue = null;

    // Validate input
    if (key == null || key.length == 0) {
      return RESULT_INVALID_INPUT;
    }

    // Search in memory first (active and immutable MemTables)
    byte[] result = getFromMemTables(key);

    // If not found in memory, search in SSTables
    if (result == null) {
      result = getFromSSTables(key);
    }

    if (result != null) {
      retrievedValue = result;
      return RESULT_SUCCESS;
    } else {
      return RESULT_KEY_NOT_FOUND;
    }
  }

  private byte[] getFromMemTables(byte[] key) {
    // First, check the active MemTable
    byte[] result = null;

    memTableLock.readLock().lock();
    try {
      result = activeMemTable.get(key);
    } finally {
      memTableLock.readLock().unlock();
    }

    // If not found, check immutable MemTables (newest to oldest)
    if (result == null) {
      synchronized (immutableMemTables) {
        for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
          MemTable memTable = immutableMemTables.get(i);
          result = memTable.get(key);
          if (result != null) {
            break;
          }
        }
      }
    }
    return result;
  }

  private byte[] getFromSSTables(byte[] key) {
    synchronized (ssTables) {
      // Search SSTables from newest to oldest
      for (int i = ssTables.size() - 1; i >= 0; i--) {
        SSTable ssTable = ssTables.get(i);
        // Use bloom filter for efficient negative lookups
        if (ssTable.mightContain(key)) {
          byte[] result = ssTable.get(key);
          if (result != null) {
            return result;
          }
        }
      }
    }
    return null;
  }
}
