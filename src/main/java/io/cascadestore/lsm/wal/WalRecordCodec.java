package io.cascadestore.lsm.wal;

import io.cascadestore.lsm.wal.record.DeleteRecord;
import io.cascadestore.lsm.wal.record.PutRecord;
import io.cascadestore.lsm.wal.record.Record;
import java.nio.ByteBuffer;

/** Binary WAL record encoding shared by writers and compaction/purge logic. */
public final class WalRecordCodec {

  private static final byte PUT_RECORD = 1;
  private static final byte DELETE_RECORD = 2;

  private WalRecordCodec() {}

  public static int encodedSize(Record record) {
    if (record instanceof PutRecord putRecord) {
      return 1 + 8 + 4 + putRecord.getKey().length + 4 + putRecord.getValue().length + 8;
    }
    if (record instanceof DeleteRecord) {
      return 1 + 8 + 4 + record.getKey().length;
    }
    throw new IllegalArgumentException("Unsupported record type: " + record.getClass());
  }

  public static void encode(Record record, ByteBuffer buffer) {
    if (record instanceof PutRecord putRecord) {
      byte[] key = putRecord.getKey();
      byte[] value = putRecord.getValue();
      buffer.put(PUT_RECORD);
      buffer.putLong(record.getSequenceNumber());
      buffer.putInt(key.length);
      buffer.put(key);
      buffer.putInt(value.length);
      buffer.put(value);
      buffer.putLong(putRecord.getTtlSeconds());
      return;
    }

    if (record instanceof DeleteRecord) {
      byte[] key = record.getKey();
      buffer.put(DELETE_RECORD);
      buffer.putLong(record.getSequenceNumber());
      buffer.putInt(key.length);
      buffer.put(key);
      return;
    }

    throw new IllegalArgumentException("Unsupported record type: " + record.getClass());
  }
}
