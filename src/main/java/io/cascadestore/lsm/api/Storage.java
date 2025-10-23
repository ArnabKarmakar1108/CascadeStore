package io.cascadestore.lsm.api;

import java.util.List;
import java.util.Map;

public interface Storage {

  boolean put(byte[] key, byte[] value);

  boolean put(byte[] key, byte[] value, long ttlSeconds);

  byte[] get(byte[] key);

  boolean delete(byte[] key);

  List<byte[]> listKeys();

  boolean containsKey(byte[] key);

  int size();

  void clear();

  Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey);

  KeyValueIterator getIterator(byte[] startKey, byte[] endKey);

  void shutdown();
}
