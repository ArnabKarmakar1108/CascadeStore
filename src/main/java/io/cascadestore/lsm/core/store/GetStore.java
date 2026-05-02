package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public final class GetStore {
  public static final int RESULT_SUCCESS = 0;
  public static final int RESULT_INVALID_INPUT = 1;
  public static final int RESULT_KEY_NOT_FOUND = 2;

  private volatile MemTable activeMemTable;
  private volatile StorageVersion storageVersion;
  private ReadWriteLock memTableLock;
  private final boolean parallelBloomEnabled;
  private final int parallelBloomMinTables;

  public GetStore(
      MemTable activeMemTable, StorageVersion storageVersion, ReadWriteLock memTableLock) {
    this(activeMemTable, storageVersion, memTableLock, true, 3);
  }

  public GetStore(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      boolean parallelBloomEnabled,
      int parallelBloomMinTables) {
    if (activeMemTable == null) {
      throw new IllegalArgumentException("activeMemTable cannot be null");
    }
    if (storageVersion == null) {
      throw new IllegalArgumentException("storageVersion cannot be null");
    }
    if (memTableLock == null) {
      throw new IllegalArgumentException("memTableLock cannot be null");
    }
    this.activeMemTable = activeMemTable;
    this.storageVersion = storageVersion;
    this.memTableLock = memTableLock;
    this.parallelBloomEnabled = parallelBloomEnabled;
    this.parallelBloomMinTables = parallelBloomMinTables;
  }

  public void updateDependencies(
      MemTable activeMemTable, StorageVersion storageVersion, ReadWriteLock memTableLock) {
    if (activeMemTable == null) {
      throw new IllegalArgumentException("activeMemTable cannot be null");
    }
    if (storageVersion == null) {
      throw new IllegalArgumentException("storageVersion cannot be null");
    }
    if (memTableLock == null) {
      throw new IllegalArgumentException("memTableLock cannot be null");
    }
    this.activeMemTable = activeMemTable;
    this.storageVersion = storageVersion;
    this.memTableLock = memTableLock;
  }

  /** Returns the stored value, or {@code null} when the key is absent or invalid. */
  public byte[] lookup(byte[] key) {
    if (key == null || key.length == 0) {
      return null;
    }

    MemTable active;
    StorageVersion version;
    memTableLock.readLock().lock();
    try {
      active = activeMemTable;
      version = storageVersion;
    } finally {
      memTableLock.readLock().unlock();
    }

    byte[] result = active.get(key);
    if (result != null) {
      return result;
    }
    if (active.shadows(key)) {
      return null;
    }

    return lookupImmutableAndSSTables(key, version);
  }

  public int get(byte[] key) {
    if (key == null || key.length == 0) {
      return RESULT_INVALID_INPUT;
    }
    return lookup(key) != null ? RESULT_SUCCESS : RESULT_KEY_NOT_FOUND;
  }

  /** Checks key presence without loading the value from SSTables. */
  public int exists(byte[] key) {
    if (key == null || key.length == 0) {
      return RESULT_INVALID_INPUT;
    }

    MemTable active;
    StorageVersion version;
    memTableLock.readLock().lock();
    try {
      active = activeMemTable;
      version = storageVersion;
    } finally {
      memTableLock.readLock().unlock();
    }

    if (active.containsKey(key)) {
      return RESULT_SUCCESS;
    }
    if (active.shadows(key)) {
      return RESULT_KEY_NOT_FOUND;
    }

    return existsInImmutableAndSSTables(key, version)
        ? RESULT_SUCCESS
        : RESULT_KEY_NOT_FOUND;
  }

  private boolean existsInImmutableAndSSTables(byte[] key, StorageVersion version) {
    List<MemTable> immutableMemTables = version.immutableMemTables();
    for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
      MemTable memTable = immutableMemTables.get(i);
      if (memTable.shadows(key)) {
        return memTable.containsKey(key);
      }
    }

    List<SSTable> ssTables = version.ssTables();
    boolean[] bloomCandidates =
        BloomProbe.probeCandidates(
            ssTables, key, parallelBloomEnabled, parallelBloomMinTables);
    for (int i = ssTables.size() - 1; i >= 0; i--) {
      if (!bloomCandidates[i]) {
        continue;
      }
      if (ssTables.get(i).containsKey(key)) {
        return true;
      }
    }
    return false;
  }

  private byte[] lookupImmutableAndSSTables(byte[] key, StorageVersion version) {
    List<MemTable> immutableMemTables = version.immutableMemTables();
    for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
      MemTable memTable = immutableMemTables.get(i);
      if (memTable.shadows(key)) {
        return memTable.get(key);
      }
    }

    List<SSTable> ssTables = version.ssTables();
    boolean[] bloomCandidates =
        BloomProbe.probeCandidates(
            ssTables, key, parallelBloomEnabled, parallelBloomMinTables);
    for (int i = ssTables.size() - 1; i >= 0; i--) {
      if (!bloomCandidates[i]) {
        continue;
      }
      SSTable ssTable = ssTables.get(i);
      byte[] result = ssTable.get(key);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
