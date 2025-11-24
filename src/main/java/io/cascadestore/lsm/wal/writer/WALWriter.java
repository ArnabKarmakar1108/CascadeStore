package io.cascadestore.lsm.wal.writer;

import java.io.IOException;

public interface WALWriter {

  long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException;

  long appendDeleteRecord(byte[] key) throws IOException;
}
