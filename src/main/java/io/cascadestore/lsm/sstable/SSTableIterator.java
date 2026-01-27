package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.api.KeyValueIterator;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

public sealed interface SSTableIterator extends KeyValueIterator
    permits SSTableIterator.InMemoryIterator, SSTableIterator.FileIterator {

  SSTableEntry currentEntry() throws NoSuchElementException;

  final class InMemoryIterator implements SSTableIterator {
    private final SSTableEntry[] entries;
    private int currentIndex = 0;

    public InMemoryIterator(SSTableEntry[] entries) {
      if (entries == null) {
        throw new NullPointerException("Entries array cannot be null");
      }
      this.entries = entries;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < entries.length;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more entries");
      }
      SSTableEntry entry = entries[currentIndex++];
      return new AbstractMap.SimpleEntry<>(entry.key(), entry.value());
    }

    @Override
    public byte[] peekNextKey() {
      if (!hasNext()) {
        return null;
      }
      return entries[currentIndex].key();
    }

    @Override
    public void close() {
      // Nothing to close for in-memory iterator
    }

    @Override
    public SSTableEntry currentEntry() throws NoSuchElementException {
      if (currentIndex <= 0 || currentIndex > entries.length) {
        throw new NoSuchElementException("No current entry");
      }
      return entries[currentIndex - 1];
    }
  }

  final class FileIterator implements SSTableIterator {
    private final SSTable ssTable;
    private final byte[] startKey;
    private final byte[] endKey;
    private SSTableEntry currentEntry;
    private boolean hasNext;

    public FileIterator(SSTable ssTable, byte[] startKey, byte[] endKey) throws IOException {
      this.ssTable = ssTable;
      this.startKey = startKey;
      this.endKey = endKey;

      // Initialize the iterator
      // This is a placeholder implementation
      // The actual implementation would read the first entry from the file
      this.hasNext = false;
      this.currentEntry = null;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public Map.Entry<byte[], byte[]> next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more entries");
      }

      SSTableEntry entry = currentEntry;

      // Read the next entry
      // This is a placeholder implementation
      // The actual implementation would read the next entry from the file
      hasNext = false;
      currentEntry = null;
      return new AbstractMap.SimpleEntry<>(entry.key(), entry.value());
    }

    @Override
    public byte[] peekNextKey() {
      if (!hasNext()) {
        return null;
      }
      return currentEntry.key();
    }

    @Override
    public void close() {
      // Close any resources
      // This is a placeholder implementation
      // The actual implementation would close any open file handles
    }

    @Override
    public SSTableEntry currentEntry() throws NoSuchElementException {
      if (currentEntry == null) {
        throw new NoSuchElementException("No current entry");
      }
      return currentEntry;
    }
  }
}
