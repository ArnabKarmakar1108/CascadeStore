package io.cascadestore.lsm.wal.record;

import java.util.Arrays;

public class PutRecord implements Record {
  private final long sequenceNumber;
  private final byte[] key;
  private final byte[] value;
  private final long ttlSeconds;

  public PutRecord(long sequenceNumber, byte[] key, byte[] value, long ttlSeconds) {
    this.sequenceNumber = sequenceNumber;
    this.key = key.clone(); // Defensive copy
    this.value = value.clone(); // Defensive copy
    this.ttlSeconds = ttlSeconds;
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public byte[] getKey() {
    return key.clone(); // Return a copy to prevent modification
  }

  public byte[] getValue() {
    return value.clone(); // Return a copy to prevent modification
  }

  public long getTtlSeconds() {
    return ttlSeconds;
  }

  @Override
  public String toString() {
    return "PutRecord{"
        + "sequenceNumber="
        + sequenceNumber
        + ", key="
        + Arrays.toString(key)
        + ", value="
        + Arrays.toString(value)
        + ", ttlSeconds="
        + ttlSeconds
        + '}';
  }
}
