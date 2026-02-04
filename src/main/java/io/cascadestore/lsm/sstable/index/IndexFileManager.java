package io.cascadestore.lsm.sstable.index;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;

public interface IndexFileManager extends AutoCloseable {

  Path getIndexFilePath();

  FileChannel getIndexChannel();

  void addIndexEntry(byte[] key, long offset) throws IOException;

  void writeIndex() throws IOException;

  void loadIndex() throws IOException;

  NavigableMap<ByteArrayWrapper, Long> getSparseIndex();

  Map.Entry<ByteArrayWrapper, Long> findClosestKey(byte[] key);

  @Override
  void close() throws IOException;
}
