package io.cascadestore.lsm.api;

import java.util.List;
import java.util.Map;

public interface Storage {

  boolean put(byte[] key, byte[] value);

  boolean put(byte[] key, byte[] value, long ttlSeconds);

  /**
   * Reads the current value for {@code key}, applies {@code merger}, then persists the result.
   *
   * @return {@code true} when the key existed and the merged value was written; {@code false} when
   *     the key is absent (including tombstones) or the merger returns {@code null}
   */
  boolean merge(byte[] key, ValueMerger merger);

  /**
   * Applies {@code merger} to {@code existingValue} and persists the result without re-reading the
   * key from storage. Callers must supply bytes that were read immediately before this call (e.g.
   * YCSB read-modify-write).
   */
  boolean merge(byte[] key, byte[] existingValue, ValueMerger merger);

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
