package io.cascadestore.lsm.sstable;

import java.util.Arrays;

public record SSTableEntry(byte[] key, byte[] value, long timestamp, boolean tombstone) {

  public SSTableEntry {
    // Make defensive copies of the byte arrays
    if (key != null) {
      key = key.clone();
    }
    if (value != null) {
      value = value.clone();
    }
  }

  public static SSTableEntry of(byte[] key, byte[] value, long timestamp) {
    return new SSTableEntry(key, value, timestamp, false);
  }

  public static SSTableEntry tombstone(byte[] key, long timestamp) {
    return new SSTableEntry(key, null, timestamp, true);
  }

  public boolean isNewerThan(SSTableEntry other) {
    return this.timestamp > other.timestamp;
  }

  @Override
  public byte[] key() {
    return key != null ? key.clone() : null;
  }

  @Override
  public byte[] value() {
    return value != null ? value.clone() : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SSTableEntry that = (SSTableEntry) o;
    return timestamp == that.timestamp
        && tombstone == that.tombstone
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (tombstone ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SSTableEntry["
        + "key="
        + Arrays.toString(key)
        + ", value="
        + Arrays.toString(value)
        + ", timestamp="
        + timestamp
        + ", tombstone="
        + tombstone
        + ']';
  }
}
