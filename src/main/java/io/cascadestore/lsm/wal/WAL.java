package io.cascadestore.lsm.wal;

import io.cascadestore.lsm.wal.record.Record;
import java.io.IOException;
import java.util.List;

public interface WAL extends AutoCloseable {

  long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException;

  long appendDeleteRecord(byte[] key) throws IOException;

  List<Record> readRecords() throws IOException;

  void deleteAllLogs() throws IOException;

  @Override
  void close() throws IOException;
}
