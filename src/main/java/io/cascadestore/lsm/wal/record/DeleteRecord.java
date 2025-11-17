package io.cascadestore.lsm.wal.record;

import java.util.Arrays;

public class DeleteRecord implements Record {
  private final long sequenceNumber;
  private final byte[] key;

  public DeleteRecord(long sequenceNumber, byte[] key) {
    this.sequenceNumber = sequenceNumber;
    this.key = key.clone(); // Defensive copy
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public byte[] getKey() {
    return key.clone(); // Return a copy to prevent modification
  }

  @Override
  public String toString() {
    return "DeleteRecord{"
        + "sequenceNumber="
        + sequenceNumber
        + ", key="
        + Arrays.toString(key)
        + '}';
  }
}
