package io.cascadestore.lsm.core.backgroundservice;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.config.CascadeConfig;
import io.cascadestore.lsm.memtable.MemTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

public class CleanupService extends AbstractBackgroundService {

  private final MemTable activeMemTable;
  private final List<MemTable> immutableMemTables;
  private final ReadWriteLock memTableLock;
  private final CascadeConfig config;

  public CleanupService(
      MemTable activeMemTable,
      List<MemTable> immutableMemTables,
      ReadWriteLock memTableLock,
      CascadeConfig config) {
    super("TTL-Cleanup");
    this.activeMemTable = activeMemTable;
    this.immutableMemTables = immutableMemTables;
    this.memTableLock = memTableLock;
    this.config = config;
  }

  @Override
  public void start() {
    scheduleTask(1, config.cleanupIntervalMinutes(), TimeUnit.MINUTES);
  }

  @Override
  protected void doExecute() {
    try {
      List<byte[]> keysToRemove = collectExpiredKeys();
      if (keysToRemove.isEmpty()) {
        logger.debug("Expired entries cleanup completed (none found)");
        return;
      }

      memTableLock.writeLock().lock();
      try {
        for (byte[] key : keysToRemove) {
          activeMemTable.delete(key);
          logger.debug("Added tombstone for expired entry with key: {}", Arrays.toString(key));
        }
      } finally {
        memTableLock.writeLock().unlock();
      }

      logger.debug("Expired entries cleanup completed ({} tombstones)", keysToRemove.size());
    } catch (Exception e) {
      logger.warn("Error during expired entries cleanup", e);
    }
  }

  private List<byte[]> collectExpiredKeys() {
    List<byte[]> keysToRemove = new ArrayList<>();

    memTableLock.readLock().lock();
    try {
      if (!activeMemTable.isRetired()) {
        activeMemTable.pin();
        try {
          collectExpiredKeys(activeMemTable.getEntries(), keysToRemove);
        } finally {
          activeMemTable.unpin();
        }
      }
    } finally {
      memTableLock.readLock().unlock();
    }

    // Hold the immutable list monitor and pin each table so a concurrent flush cannot retire and
    // free off-heap entry buffers while we scan for TTL expiry.
    synchronized (immutableMemTables) {
      for (MemTable memTable : immutableMemTables) {
        if (memTable.isRetired()) {
          continue;
        }
        memTable.pin();
        try {
          collectExpiredKeys(memTable.getEntries(), keysToRemove);
        } finally {
          memTable.unpin();
        }
      }
    }

    return keysToRemove;
  }

  private static void collectExpiredKeys(
      Map<ByteArrayWrapper, MemTable.ValueEntry> entries, List<byte[]> keysToRemove) {
    for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
      if (entry.getValue().isExpired()) {
        keysToRemove.add(entry.getKey().getData());
      }
    }
  }
}
