package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.metrics.CascadeMetrics;
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
  private final Object storageVersionGate;
  private final boolean parallelBloomEnabled;
  private final int parallelBloomMinTables;
  private final CascadeMetrics metrics;

  public GetStore(
      MemTable activeMemTable, StorageVersion storageVersion, ReadWriteLock memTableLock) {
    this(activeMemTable, storageVersion, memTableLock, new Object(), true, 3, CascadeMetrics.noop());
  }

  public GetStore(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      Object storageVersionGate) {
    this(
        activeMemTable,
        storageVersion,
        memTableLock,
        storageVersionGate,
        true,
        3,
        CascadeMetrics.noop());
  }

  public GetStore(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      boolean parallelBloomEnabled,
      int parallelBloomMinTables) {
    this(
        activeMemTable,
        storageVersion,
        memTableLock,
        new Object(),
        parallelBloomEnabled,
        parallelBloomMinTables,
        CascadeMetrics.noop());
  }

  public GetStore(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      Object storageVersionGate,
      boolean parallelBloomEnabled,
      int parallelBloomMinTables) {
    this(
        activeMemTable,
        storageVersion,
        memTableLock,
        storageVersionGate,
        parallelBloomEnabled,
        parallelBloomMinTables,
        CascadeMetrics.noop());
  }

  public GetStore(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      Object storageVersionGate,
      boolean parallelBloomEnabled,
      int parallelBloomMinTables,
      CascadeMetrics metrics) {
    if (activeMemTable == null) {
      throw new IllegalArgumentException("activeMemTable cannot be null");
    }
    if (storageVersion == null) {
      throw new IllegalArgumentException("storageVersion cannot be null");
    }
    if (memTableLock == null) {
      throw new IllegalArgumentException("memTableLock cannot be null");
    }
    if (storageVersionGate == null) {
      throw new IllegalArgumentException("storageVersionGate cannot be null");
    }
    this.activeMemTable = activeMemTable;
    this.storageVersion = storageVersion;
    this.memTableLock = memTableLock;
    this.storageVersionGate = storageVersionGate;
    this.parallelBloomEnabled = parallelBloomEnabled;
    this.parallelBloomMinTables = parallelBloomMinTables;
    this.metrics = metrics != null ? metrics : CascadeMetrics.noop();
  }

  public void updateDependencies(
      MemTable activeMemTable, StorageVersion storageVersion, ReadWriteLock memTableLock) {
    updateDependencies(
        activeMemTable, storageVersion, memTableLock, parallelBloomEnabled, parallelBloomMinTables, metrics);
  }

  public void updateDependencies(
      MemTable activeMemTable,
      StorageVersion storageVersion,
      ReadWriteLock memTableLock,
      boolean parallelBloomEnabled,
      int parallelBloomMinTables,
      CascadeMetrics metrics) {
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

    PinnedSnapshot snapshot = pinSnapshot();
    try {
      byte[] result = snapshot.active().get(key);
      if (result != null) {
        metrics.recordRead(0, 0, 0);
        return result;
      }
      if (snapshot.active().shadows(key)) {
        metrics.recordRead(0, 0, 0);
        return null;
      }

      return lookupImmutableAndSSTables(key, snapshot.version(), true);
    } finally {
      snapshot.release();
    }
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

    PinnedSnapshot snapshot = pinSnapshot();
    try {
      if (snapshot.active().containsKey(key)) {
        return RESULT_SUCCESS;
      }
      if (snapshot.active().shadows(key)) {
        return RESULT_KEY_NOT_FOUND;
      }

      return existsInImmutableAndSSTables(key, snapshot.version())
          ? RESULT_SUCCESS
          : RESULT_KEY_NOT_FOUND;
    } finally {
      snapshot.release();
    }
  }

  private PinnedSnapshot pinSnapshot() {
    memTableLock.readLock().lock();
    try {
      synchronized (storageVersionGate) {
        StorageVersion version = storageVersion;
        version.retain();
        MemTable active = activeMemTable;
        active.pin();
        return new PinnedSnapshot(active, version);
      }
    } finally {
      memTableLock.readLock().unlock();
    }
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
    ReadStats stats = probeSSTables(ssTables, key);
    for (int i = ssTables.size() - 1; i >= 0; i--) {
      if (!stats.bloomCandidates[i]) {
        continue;
      }
      stats.sstableLookups++;
      if (ssTables.get(i).containsKey(key)) {
        return true;
      }
    }
    return false;
  }

  private byte[] lookupImmutableAndSSTables(byte[] key, StorageVersion version, boolean countRead) {
    List<MemTable> immutableMemTables = version.immutableMemTables();
    for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
      MemTable memTable = immutableMemTables.get(i);
      if (memTable.shadows(key)) {
        return memTable.get(key);
      }
    }

    List<SSTable> ssTables = version.ssTables();
    ReadStats stats = probeSSTables(ssTables, key);
    for (int i = ssTables.size() - 1; i >= 0; i--) {
      if (!stats.bloomCandidates[i]) {
        continue;
      }
      SSTable ssTable = ssTables.get(i);
      stats.sstableLookups++;
      byte[] result = ssTable.get(key);
      if (result != null) {
        if (countRead) {
          metrics.recordRead(stats.sstableLookups, stats.bloomProbes, stats.bloomNegatives);
        }
        return result;
      }
    }
    if (countRead) {
      metrics.recordRead(stats.sstableLookups, stats.bloomProbes, stats.bloomNegatives);
    }
    return null;
  }

  private ReadStats probeSSTables(List<SSTable> ssTables, byte[] key) {
    boolean[] bloomCandidates =
        BloomProbe.probeCandidates(
            ssTables, key, parallelBloomEnabled, parallelBloomMinTables);
    int bloomNegatives = 0;
    for (boolean candidate : bloomCandidates) {
      if (!candidate) {
        bloomNegatives++;
      }
    }
    return new ReadStats(bloomCandidates, ssTables.size(), bloomNegatives, 0);
  }

  private static final class PinnedSnapshot {
    private final MemTable active;
    private final StorageVersion version;

    private PinnedSnapshot(MemTable active, StorageVersion version) {
      this.active = active;
      this.version = version;
    }

    private MemTable active() {
      return active;
    }

    private StorageVersion version() {
      return version;
    }

    private void release() {
      version.release();
      active.unpin();
    }
  }

  private static final class ReadStats {
    private final boolean[] bloomCandidates;
    private final int bloomProbes;
    private final int bloomNegatives;
    private int sstableLookups;

    private ReadStats(
        boolean[] bloomCandidates, int bloomProbes, int bloomNegatives, int sstableLookups) {
      this.bloomCandidates = bloomCandidates;
      this.bloomProbes = bloomProbes;
      this.bloomNegatives = bloomNegatives;
      this.sstableLookups = sstableLookups;
    }
  }
}
