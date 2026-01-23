package io.cascadestore.lsm.sstable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface SSTableInterface extends AutoCloseable {

  byte[] get(byte[] key);

  boolean mightContain(byte[] key);

  int getLevel();

  long getSequenceNumber();

  long getCreationTime();

  long getSizeBytes();

  boolean delete();

  List<byte[]> listKeys();

  int countEntries();

  Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey);

  @Override
  void close() throws IOException;
}
