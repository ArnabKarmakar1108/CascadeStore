package io.cascadestore.lsm.io;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Per-store LRU cache of fixed-size SSTable data windows keyed by {@code (sstableId,
 * blockStartOffset)}. Read-write locking allows concurrent cache hits under write exclusion.
 */
public final class BlockCache {

  public static final int DEFAULT_SIZE_BYTES = 128 * 1024 * 1024;

  private static final record Key(long sstableId, long blockOffset) {}

  private final int maxBytes;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final LinkedHashMap<Key, byte[]> entries;
  private int currentBytes;

  public static BlockCache create(int maxBytes) {
    return maxBytes > 0 ? new BlockCache(maxBytes) : null;
  }

  private BlockCache(int maxBytes) {
    this.maxBytes = maxBytes;
    this.entries = new LinkedHashMap<>(64, 0.75f, true);
  }

  public boolean isEnabled() {
    return maxBytes > 0;
  }

  public byte[] get(long sstableId, long blockOffset) {
    lock.readLock().lock();
    try {
      return entries.get(new Key(sstableId, blockOffset));
    } finally {
      lock.readLock().unlock();
    }
  }

  public void put(long sstableId, long blockOffset, byte[] data) {
    if (data == null || data.length == 0) {
      return;
    }
    byte[] copy = Arrays.copyOf(data, data.length);
    lock.writeLock().lock();
    try {
      Key key = new Key(sstableId, blockOffset);
      byte[] existing = entries.remove(key);
      if (existing != null) {
        currentBytes -= existing.length;
      }
      entries.put(key, copy);
      currentBytes += copy.length;
      evictIfNeeded();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void invalidateSstable(long sstableId) {
    lock.writeLock().lock();
    try {
      Iterator<Map.Entry<Key, byte[]>> iterator = entries.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Key, byte[]> entry = iterator.next();
        if (entry.getKey().sstableId == sstableId) {
          currentBytes -= entry.getValue().length;
          iterator.remove();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  int sizeBytesForTest() {
    lock.readLock().lock();
    try {
      return currentBytes;
    } finally {
      lock.readLock().unlock();
    }
  }

  int entryCountForTest() {
    lock.readLock().lock();
    try {
      return entries.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  private void evictIfNeeded() {
    while (currentBytes > maxBytes && !entries.isEmpty()) {
      Iterator<Map.Entry<Key, byte[]>> iterator = entries.entrySet().iterator();
      Map.Entry<Key, byte[]> eldest = iterator.next();
      currentBytes -= eldest.getValue().length;
      iterator.remove();
    }
  }
}
