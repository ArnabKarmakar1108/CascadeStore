package io.cascadestore.lsm.api;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;

public interface CascadeIterator extends KeyValueIterator {

  class MemTableIterator implements CascadeIterator {
    private final List<Map.Entry<ByteArrayWrapper, byte[]>> entries;
    private int currentIndex = 0;

    public MemTableIterator(
        MemTable memTable, byte[] startKey, byte[] endKey, ReadWriteLock memTableLock) {
      ByteArrayWrapper startKeyWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
      ByteArrayWrapper endKeyWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;
      this.entries = new ArrayList<>();

      // Collect entries from MemTable
      memTableLock.readLock().lock();
      try {
        Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = memTable.getEntries();
        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
          ByteArrayWrapper key = entry.getKey();

          // Check if key is in range
          if (isKeyInRange(key, startKeyWrapper, endKeyWrapper)) {
            MemTable.ValueEntry valueEntry = entry.getValue();

            // Skip expired or tombstone entries
            if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
              entries.add(Map.entry(key, valueEntry.getValue()));
            }
          }
        }
      } finally {
        memTableLock.readLock().unlock();
      }

      // Sort entries by key
      Collections.sort(entries, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
    }

    @Override
    public boolean hasNext() {
      return currentIndex < entries.size();
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements in the iterator");
      }

      Map.Entry<ByteArrayWrapper, byte[]> entry = entries.get(currentIndex++);
      return new KeyValueEntry(entry.getKey().getData(), entry.getValue());
    }

    @Override
    public byte[] peekNextKey() {
      if (!hasNext()) {
        return null;
      }

      return entries.get(currentIndex).getKey().getData();
    }

    @Override
    public void close() {
      // No resources to release
    }

    private boolean isKeyInRange(
        ByteArrayWrapper key, ByteArrayWrapper startKey, ByteArrayWrapper endKey) {
      if (startKey != null && key.compareTo(startKey) < 0) {
        return false;
      }
      if (endKey != null && key.compareTo(endKey) >= 0) {
        return false;
      }
      return true;
    }
  }

  class SSTableIterator implements CascadeIterator {
    private final List<Map.Entry<byte[], byte[]>> entries;
    private int currentIndex = 0;

    public SSTableIterator(SSTable ssTable, byte[] startKey, byte[] endKey) {
      // Get entries in the specified range from the SSTable
      Map<byte[], byte[]> rangeEntries = ssTable.getRange(startKey, endKey);

      // Convert to a list for iteration
      this.entries = new ArrayList<>();
      for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
        entries.add(new KeyValueEntry(entry.getKey(), entry.getValue()));
      }

      // Sort entries by key
      Collections.sort(
          entries,
          (e1, e2) -> {
            ByteArrayWrapper w1 = new ByteArrayWrapper(e1.getKey());
            ByteArrayWrapper w2 = new ByteArrayWrapper(e2.getKey());
            return w1.compareTo(w2);
          });
    }

    @Override
    public boolean hasNext() {
      return currentIndex < entries.size();
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements in the iterator");
      }

      return entries.get(currentIndex++);
    }

    @Override
    public byte[] peekNextKey() {
      if (!hasNext()) {
        return null;
      }

      return entries.get(currentIndex).getKey();
    }

    @Override
    public void close() {
      // No resources to release
    }
  }

  class MergedIterator implements CascadeIterator {
    private final List<CascadeIterator> iterators;
    private final ByteArrayWrapper startKey;
    private final ByteArrayWrapper endKey;
    private Map.Entry<byte[], byte[]> nextEntry;

    public MergedIterator(List<CascadeIterator> iterators, byte[] startKey, byte[] endKey) {
      this.iterators = new ArrayList<>(iterators);
      this.startKey = startKey != null ? new ByteArrayWrapper(startKey) : null;
      this.endKey = endKey != null ? new ByteArrayWrapper(endKey) : null;

      // Initialize by finding the first entry
      findNextEntry();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more elements in the iterator");
      }

      Map.Entry<byte[], byte[]> current = nextEntry;
      findNextEntry();
      return current;
    }

    @Override
    public byte[] peekNextKey() {
      return hasNext() ? nextEntry.getKey() : null;
    }

    @Override
    public void close() {
      for (CascadeIterator iterator : iterators) {
        iterator.close();
      }
    }

    private void findNextEntry() {
      nextEntry = null;

      // Find the iterator with the smallest next key
      ByteArrayWrapper smallestKey = null;
      CascadeIterator iteratorWithSmallestKey = null;

      for (CascadeIterator iterator : iterators) {
        if (iterator.hasNext()) {
          byte[] key = iterator.peekNextKey();
          ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

          // Check if key is in range
          if (isKeyInRange(keyWrapper)) {
            if (smallestKey == null || keyWrapper.compareTo(smallestKey) < 0) {
              smallestKey = keyWrapper;
              iteratorWithSmallestKey = iterator;
            }
          }
        }
      }

      // If we found an iterator with a valid next key, get the next entry
      if (iteratorWithSmallestKey != null) {
        nextEntry = iteratorWithSmallestKey.next();

        // Skip entries with the same key from other iterators
        // (newer entries from earlier iterators take precedence)
        for (CascadeIterator iterator : iterators) {
          if (iterator != iteratorWithSmallestKey && iterator.hasNext()) {
            byte[] key = iterator.peekNextKey();
            if (Arrays.equals(key, nextEntry.getKey())) {
              iterator.next(); // Skip this entry
            }
          }
        }
      }
    }

    private boolean isKeyInRange(ByteArrayWrapper key) {
      if (startKey != null && key.compareTo(startKey) < 0) {
        return false;
      }
      if (endKey != null && key.compareTo(endKey) >= 0) {
        return false;
      }
      return true;
    }
  }
}
