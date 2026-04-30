package io.cascadestore.lsm.sstable.index;

/** Controls how often SSTable data-file offsets are recorded in the on-disk index. */
public final class SparseIndexPolicy {

  /** Index one key roughly every 16 KiB of data file bytes. */
  public static final int INDEX_BLOCK_SIZE_BYTES = 16 * 1024;

  private SparseIndexPolicy() {}

  public static boolean shouldAddIndexEntry(long entryOffset, long lastIndexedOffset) {
    return lastIndexedOffset < 0
        || entryOffset - lastIndexedOffset >= INDEX_BLOCK_SIZE_BYTES;
  }
}
