package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.Collections;
import java.util.List;

/** Immutable snapshot of memtable/SSTable tiers for lock-free reads after publish. */
public final class StorageVersion {

  private final long versionId;
  private final List<MemTable> immutableMemTables;
  private final List<SSTable> ssTables;

  public StorageVersion(long versionId, List<MemTable> immutableMemTables, List<SSTable> ssTables) {
    this.versionId = versionId;
    this.immutableMemTables = List.copyOf(immutableMemTables);
    this.ssTables = List.copyOf(ssTables);
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

  /** Releases SSTable pins acquired when this version was published. */
  public void release() {
    for (SSTable ssTable : ssTables) {
      ssTable.unpin();
    }
  }
}
