package io.cascadestore.lsm.wal.record;

public interface Record {
  long getSequenceNumber();

  byte[] getKey();
}
