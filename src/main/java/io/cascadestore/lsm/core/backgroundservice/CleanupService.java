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
      long now = System.currentTimeMillis();
      List<byte[]> keysToRemove = new ArrayList<>();

      // Check active MemTable for expired entries
      memTableLock.readLock().lock();
      try {
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
          if (entry.getValue().isExpired()) {
            keysToRemove.add(entry.getKey().getData());
          }
        }
      } finally {
        memTableLock.readLock().unlock();
      }

      // Remove expired entries from active MemTable
      if (!keysToRemove.isEmpty()) {
        memTableLock.writeLock().lock();
        try {
          for (byte[] key : keysToRemove) {
            // Add a tombstone marker to indicate the key is deleted
            activeMemTable.delete(key);
            logger.debug("Removed expired entry with key: " + Arrays.toString(key));
          }
        } finally {
          memTableLock.writeLock().unlock();
        }
      }

      // Check immutable MemTables for expired entries
      // We don't modify immutable MemTables, but we can add tombstone markers to the active
      // MemTable
      synchronized (immutableMemTables) {
        for (MemTable memTable : immutableMemTables) {
          keysToRemove.clear();
          Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
          for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
            if (entry.getValue().isExpired()) {
              keysToRemove.add(entry.getKey().getData());
            }
          }

          // Add tombstone markers for expired entries
          if (!keysToRemove.isEmpty()) {
            memTableLock.writeLock().lock();
            try {
              for (byte[] key : keysToRemove) {
                // Add a tombstone marker to the active MemTable
                activeMemTable.delete(key);
                logger.debug("Added tombstone for expired entry with key: " + Arrays.toString(key));
              }
            } finally {
              memTableLock.writeLock().unlock();
            }
          }
        }
      }

      // We don't need to check SSTables for expired entries here
      // Expired entries in SSTables will be filtered out during reads and removed during compaction

      logger.debug("Expired entries cleanup completed");
    } catch (Exception e) {
      logger.warn("Error during expired entries cleanup", e);
    }
  }
}
