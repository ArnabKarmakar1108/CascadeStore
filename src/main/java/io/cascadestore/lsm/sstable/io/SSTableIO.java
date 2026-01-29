package io.cascadestore.lsm.sstable.io;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.SSTableEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface SSTableIO extends AutoCloseable {

  String getDirectory();

  int getLevel();

  long getSequenceNumber();

  FileChannel getDataChannel();

  FileChannel getIndexChannel();

  void flushToDisk(MemTable memTable) throws IOException;

  void loadFromDisk() throws IOException;

  ByteBuffer writeEntryHeader(SSTableEntry entry);

  EntryHeader readEntryHeader(ByteBuffer buffer);

  boolean deleteFiles();

  @Override
  void close() throws IOException;

  record EntryHeader(int keyLength, int valueLength, long timestamp, boolean tombstone) {}
}
