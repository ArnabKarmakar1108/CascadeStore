package io.cascadestore.lsm.sstable.data;

import io.cascadestore.lsm.sstable.SSTableEntry;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface DataFileManager extends AutoCloseable {

  Path getDataFilePath();

  FileChannel getDataChannel();

  long writeEntry(SSTableEntry entry) throws IOException;

  SSTableEntry readEntry(long offset) throws IOException;

  byte[] findKeyInDataFile(byte[] key, long startPosition) throws IOException;

  Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) throws IOException;

  List<byte[]> listKeys() throws IOException;

  int countEntries() throws IOException;

  @Override
  void close() throws IOException;
}
