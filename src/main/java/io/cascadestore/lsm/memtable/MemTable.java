package io.cascadestore.lsm.memtable;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.memory.DirectBufferAllocator;
import io.cascadestore.lsm.memory.OffHeapAllocator;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTable {
  private static final Logger logger = LoggerFactory.getLogger(MemTable.class);

  private final NavigableMap<ByteArrayWrapper, ValueEntry> entries;
  private final OffHeapAllocator allocator;
  private final AtomicLong sizeBytes;
  private final AtomicLong maxWalSequence;
  private final long maxSizeBytes;
  private volatile boolean immutable;
  private volatile boolean atCapacity;
  private final AtomicInteger pinCount = new AtomicInteger(0);
  private volatile boolean forceClosed;
  private volatile boolean retired;
  private boolean allocatorClosed;

  public static class ValueEntry {
    private static final int HEADER_SIZE = 16;
    private static final int EXPIRATION_TIME_OFFSET = 0;
    private static final int TOMBSTONE_OFFSET = 8;
    private static final int VALUE_LENGTH_OFFSET = 12;

    private final ByteBuffer buffer;
    private final boolean hasTombstone;

    public ValueEntry(
        byte[] value, long ttlSeconds, boolean tombstone, OffHeapAllocator allocator) {
      this.hasTombstone = tombstone;
      long expirationTime = ttlSeconds > 0 ? System.currentTimeMillis() + (ttlSeconds * 1000) : 0;

      if (tombstone) {
        this.buffer = allocator.allocate(HEADER_SIZE);
        buffer.putLong(EXPIRATION_TIME_OFFSET, expirationTime);
        buffer.put(TOMBSTONE_OFFSET, (byte) 1);
        buffer.putInt(VALUE_LENGTH_OFFSET, 0);
      } else {
        int valueLength = value != null ? value.length : 0;
        this.buffer = allocator.allocate(HEADER_SIZE + valueLength);
        buffer.putLong(EXPIRATION_TIME_OFFSET, expirationTime);
        buffer.put(TOMBSTONE_OFFSET, (byte) 0);
        buffer.putInt(VALUE_LENGTH_OFFSET, valueLength);
        if (valueLength > 0) {
          buffer.put(HEADER_SIZE, value, 0, valueLength);
        }
      }
    }

    public byte[] getValue() {
      if (isTombstone()) {
        return null;
      }

      int valueLength = buffer.getInt(VALUE_LENGTH_OFFSET);
      if (valueLength <= 0) {
        return null;
      }

      if (HEADER_SIZE + valueLength > buffer.capacity()) {
        return null;
      }

      byte[] result = new byte[valueLength];
      buffer.get(HEADER_SIZE, result, 0, valueLength);
      return result;
    }

    public long getExpirationTime() {
      return buffer.getLong(EXPIRATION_TIME_OFFSET);
    }

    public boolean isExpired() {
      long expirationTime = getExpirationTime();
      return expirationTime > 0 && System.currentTimeMillis() > expirationTime;
    }

    public boolean isTombstone() {
      return hasTombstone || buffer.get(TOMBSTONE_OFFSET) != 0;
    }

    public long getSizeBytes() {
      return buffer.capacity();
    }
  }

  public MemTable() {
    this(10 * 1024 * 1024);
  }

  public MemTable(long maxSizeBytes) {
    this.entries = new ConcurrentSkipListMap<>();
    this.allocator = new DirectBufferAllocator();
    this.sizeBytes = new AtomicLong(0);
    this.maxWalSequence = new AtomicLong(-1);
    this.maxSizeBytes = maxSizeBytes;
    this.immutable = false;

    logger.info("MemTable created with max size: {} bytes", maxSizeBytes);
  }

  public boolean put(byte[] key, byte[] value, long ttlSeconds) {
    if (immutable) {
      return false;
    }

    if (key == null || key.length == 0 || value == null) {
      return false;
    }

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    ValueEntry newEntry = new ValueEntry(value, ttlSeconds, false, allocator);

    long entrySize = key.length + newEntry.getSizeBytes();

    if (sizeBytes.get() + entrySize > maxSizeBytes) {
      atCapacity = true;
      return false;
    }

    ValueEntry oldEntry = entries.put(keyWrapper, newEntry);

    if (oldEntry != null) {
      sizeBytes.addAndGet(entrySize - oldEntry.getSizeBytes());
    } else {
      sizeBytes.addAndGet(entrySize);
    }
    return true;
  }

  public boolean delete(byte[] key) {
    if (immutable) {
      return false;
    }

    if (key == null || key.length == 0) {
      return false;
    }

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    ValueEntry tombstone = new ValueEntry(null, 0, true, allocator);

    ValueEntry oldEntry = entries.put(keyWrapper, tombstone);

    if (oldEntry != null) {
      sizeBytes.addAndGet(-oldEntry.getSizeBytes());
    }
    return true;
  }

  public byte[] get(byte[] key) {
    if (key == null || key.length == 0) {
      return null;
    }

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    ValueEntry entry = entries.get(keyWrapper);

    if (entry == null || entry.isExpired() || entry.isTombstone()) {
      return null;
    }
    return entry.getValue();
  }

  public boolean containsKey(byte[] key) {
    if (key == null || key.length == 0) {
      return false;
    }

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    ValueEntry entry = entries.get(keyWrapper);

    return entry != null && !entry.isExpired() && !entry.isTombstone();
  }

  /**
   * Returns true when this memtable holds a non-expired entry for the key, including tombstones that
   * shadow older tiers.
   */
  public boolean shadows(byte[] key) {
    if (key == null || key.length == 0) {
      return false;
    }

    ValueEntry entry = entries.get(new ByteArrayWrapper(key));
    return entry != null && !entry.isExpired();
  }

  public Map<ByteArrayWrapper, ValueEntry> getEntries() {
    return entries;
  }

  public long getSizeBytes() {
    return sizeBytes.get();
  }

  public void noteWalSequence(long walSequence) {
    if (walSequence < 0) {
      return;
    }
    maxWalSequence.accumulateAndGet(walSequence, Math::max);
  }

  public long maxWalSequence() {
    return maxWalSequence.get();
  }

  public boolean isFull() {
    return atCapacity || sizeBytes.get() >= maxSizeBytes;
  }

  public void makeImmutable() {
    immutable = true;
    logger.info("MemTable made immutable with size: {} bytes", sizeBytes.get());
  }

  public boolean isImmutable() {
    return immutable;
  }

  /** Retain this memtable for an in-flight read or storage-version snapshot. */
  public void pin() {
    if (forceClosed) {
      throw new IllegalStateException("MemTable is closed");
    }
    pinCount.incrementAndGet();
  }

  /**
   * Release a pin acquired via {@link #pin()}. Off-heap storage is freed here when this memtable has
   * been retired (flushed) and the last pin drops, giving deterministic reclamation instead of
   * relying on GC to run the buffer cleaners.
   */
  public void unpin() {
    if (pinCount.decrementAndGet() == 0 && retired) {
      closeAllocator();
    }
  }

  /**
   * Marks this memtable as flushed and no longer part of the live set. Its off-heap buffers are
   * freed immediately if unpinned, otherwise by the final {@link #unpin()}.
   */
  public void retire() {
    retired = true;
    if (pinCount.get() == 0) {
      closeAllocator();
    }
  }

  /** Returns true once this memtable has been flushed and removed from the live immutable list. */
  public boolean isRetired() {
    return retired;
  }

  int getPinCountForTest() {
    return pinCount.get();
  }

  /** Force-closes off-heap storage (shutdown / clear); callers must ensure no readers remain. */
  public void close() {
    forceClosed = true;
    retired = true;
    closeAllocator();
  }

  private synchronized void closeAllocator() {
    if (allocatorClosed) {
      return;
    }
    allocatorClosed = true;
    allocator.close();
    logger.info("MemTable closed");
  }
}
