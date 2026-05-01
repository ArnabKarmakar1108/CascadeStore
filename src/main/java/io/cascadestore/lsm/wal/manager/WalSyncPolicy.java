package io.cascadestore.lsm.wal.manager;

/** Batched WAL durability: fsync after a volume of writes instead of every record. */
public final class WalSyncPolicy {

  /** Default group-commit size before forcing the WAL to disk. */
  public static final long DEFAULT_SYNC_BATCH_BYTES = 1024 * 1024;

  private WalSyncPolicy() {}
}
