package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Immutable snapshot of memtable/SSTable tiers for lock-free reads after publish. */
public final class StorageVersion {

  private final long versionId;
  private final List<MemTable> immutableMemTables;
  private final List<SSTable> ssTables;
  private final AtomicInteger refCount = new AtomicInteger(1);

  public StorageVersion(long versionId, List<MemTable> immutableMemTables, List<SSTable> ssTables) {
    this.versionId = versionId;
    this.immutableMemTables = List.copyOf(immutableMemTables);
    this.ssTables = List.copyOf(ssTables);
    for (MemTable memTable : this.immutableMemTables) {
      memTable.pin();
    }
    for (SSTable ssTable : this.ssTables) {
      ssTable.pin();
    }
  }

  public static StorageVersion empty(long versionId) {
    return new StorageVersion(versionId, Collections.emptyList(), Collections.emptyList());
  }

  public long versionId() {
    return versionId;
  }

  public List<MemTable> immutableMemTables() {
    return immutableMemTables;
  }

  public List<SSTable> ssTables() {
    return ssTables;
  }

  /** Retains this snapshot for an in-flight read. */
  public void retain() {
    refCount.incrementAndGet();
  }

  /**
   * Releases a retain or the initial publish reference. SSTable pins are dropped only when the
   * reference count reaches zero.
   */
  public void release() {
    if (refCount.decrementAndGet() == 0) {
      for (MemTable memTable : immutableMemTables) {
        memTable.unpin();
      }
      for (SSTable ssTable : ssTables) {
        ssTable.unpin();
      }
    }
  }
}
