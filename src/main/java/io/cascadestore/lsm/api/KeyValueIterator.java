package io.cascadestore.lsm.api;

import java.util.Iterator;
import java.util.Map;

public interface KeyValueIterator extends Iterator<Map.Entry<byte[], byte[]>>, AutoCloseable {

  byte[] peekNextKey();

  @Override
  void close();
}
