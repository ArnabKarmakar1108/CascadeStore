package io.cascadestore.lsm.wal.writer;

import io.cascadestore.lsm.io.RecordBufferPool;
import io.cascadestore.lsm.wal.file.WALFile;
import io.cascadestore.lsm.wal.manager.WALManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALWriterImpl implements WALWriter {
  private static final Logger logger = LoggerFactory.getLogger(WALWriterImpl.class);

  // Constants for record types
  private static final byte PUT_RECORD = 1;
  private static final byte DELETE_RECORD = 2;

  private final WALManager walManager;

  public WALWriterImpl(WALManager walManager) {
    this.walManager = walManager;
  }

  @Override
  public long appendPutRecord(byte[] key, byte[] value, long ttlSeconds)
      throws IOException {
    if (key == null || key.length == 0 || value == null) {
      throw new IllegalArgumentException("Key and value cannot be null or empty");
    }

    WALFile currentFile = walManager.getCurrentFile();
    if (currentFile == null) {
      throw new IOException("WAL is not open for writes");
    }

    // Check if we need to rotate the log
    if (currentFile.size() >= walManager.getMaxLogSizeBytes()) {
      walManager.rotateLog();
      currentFile = walManager.getCurrentFile();
      if (currentFile == null) {
        throw new IOException("WAL is not open for writes");
      }
    }

    return writePutRecord(currentFile, key, value, ttlSeconds);
  }

  public long appendPutRecordWithoutRotation(byte[] key, byte[] value, long ttlSeconds)
      throws IOException {
    if (key == null || key.length == 0 || value == null) {
      throw new IllegalArgumentException("Key and value cannot be null or empty");
    }

    WALFile currentFile = walManager.getCurrentFile();
    if (currentFile == null) {
      throw new IOException("WAL is not open for writes");
    }

    return writePutRecord(currentFile, key, value, ttlSeconds);
  }

  private long writePutRecord(WALFile currentFile, byte[] key, byte[] value, long ttlSeconds)
      throws IOException {

    // Get the next sequence number
    long seqNum = walManager.getNextSequenceNumber();

    // Calculate the record size
    int recordSize = 1 + 8 + 4 + key.length + 4 + value.length + 8;

    // Create a buffer for the record
    ByteBuffer buffer = RecordBufferPool.acquire(recordSize);

    // Write the record type
    buffer.put(PUT_RECORD);

    // Write the sequence number
    buffer.putLong(seqNum);

    // Write the key length and key
    buffer.putInt(key.length);
    buffer.put(key);

    // Write the value length and value
    buffer.putInt(value.length);
    buffer.put(value);

    // Write the TTL
    buffer.putLong(ttlSeconds);

    // Flip the buffer for writing
    buffer.flip();

    // Write the buffer to the log
    currentFile.write(buffer);
    walManager.noteBytesWritten(recordSize);

    logger.debug("Appended put record with sequence number: " + seqNum);

    return seqNum;
  }

  @Override
  public long appendDeleteRecord(byte[] key) throws IOException {
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Key cannot be null or empty");
    }

    WALFile currentFile = walManager.getCurrentFile();
    if (currentFile == null) {
      throw new IOException("WAL is not open for writes");
    }

    // Check if we need to rotate the log
    if (currentFile.size() >= walManager.getMaxLogSizeBytes()) {
      walManager.rotateLog();
      currentFile = walManager.getCurrentFile();
      if (currentFile == null) {
        throw new IOException("WAL is not open for writes");
      }
    }

    return writeDeleteRecord(currentFile, key);
  }

  public long appendDeleteRecordWithoutRotation(byte[] key) throws IOException {
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Key cannot be null or empty");
    }

    WALFile currentFile = walManager.getCurrentFile();
    if (currentFile == null) {
      throw new IOException("WAL is not open for writes");
    }

    return writeDeleteRecord(currentFile, key);
  }

  private long writeDeleteRecord(WALFile currentFile, byte[] key) throws IOException {
    long seqNum = walManager.getNextSequenceNumber();
    int recordSize = 1 + 8 + 4 + key.length;

    ByteBuffer buffer = RecordBufferPool.acquire(recordSize);
    buffer.put(DELETE_RECORD);
    buffer.putLong(seqNum);
    buffer.putInt(key.length);
    buffer.put(key);
    buffer.flip();

    currentFile.write(buffer);
    walManager.noteBytesWritten(recordSize);

    logger.debug("Appended delete record with sequence number: " + seqNum);
    return seqNum;
  }
}
