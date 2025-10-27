package io.cascadestore.lsm.api;

import java.util.Arrays;
import java.util.Map;

public record KeyValueEntry(byte[] key, byte[] value) implements Map.Entry<byte[], byte[]> {

  public KeyValueEntry {
    if (key != null) {
      key = key.clone();
    }
    if (value != null) {
      value = value.clone();
    }
  }

  @Override
  public byte[] getKey() {
    return key != null ? key.clone() : null;
  }

  @Override
  public byte[] getValue() {
    return value != null ? value.clone() : null;
  }

  @Override
  public byte[] setValue(byte[] value) {
    throw new UnsupportedOperationException("KeyValueEntry is immutable");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyValueEntry that = (KeyValueEntry) o;
    return Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "KeyValueEntry["
        + "key="
        + Arrays.toString(key)
        + ", value="
        + Arrays.toString(value)
        + ']';
  }
}
