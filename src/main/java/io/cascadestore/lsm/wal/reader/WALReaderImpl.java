package io.cascadestore.lsm.wal.reader;

import io.cascadestore.lsm.io.ReadBuffers;
import io.cascadestore.lsm.wal.manager.WALManager;
import io.cascadestore.lsm.wal.record.DeleteRecord;
import io.cascadestore.lsm.wal.record.PutRecord;
import io.cascadestore.lsm.wal.record.Record;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALReaderImpl implements WALReader {
  private static final Logger logger = LoggerFactory.getLogger(WALReaderImpl.class);

  // Constants for record types
  private static final byte PUT_RECORD = 1;
  private static final byte DELETE_RECORD = 2;

  private final WALManager walManager;

  public WALReaderImpl(WALManager walManager) {
    this.walManager = walManager;
  }

  @Override
  public List<Record> readRecords() throws IOException {
    List<Record> records = new ArrayList<>();

    // Find all log files
    List<Path> logFiles = walManager.findLogFiles();

    // Read records from each log file
    for (Path logPath : logFiles) {
      records.addAll(readRecordsFromFile(logPath.toString()));
    }

    return records;
  }

  @Override
  public List<Record> readRecordsFromFile(String filePath) throws IOException {
    List<Record> records = new ArrayList<>();

    try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

      while (channel.position() < channel.size()) {
        // Read the record type
        buffer.clear();
        buffer.limit(1);
        channel.read(buffer);
        buffer.flip();
        byte recordType = buffer.get();

        // Read the sequence number
        buffer.clear();
        buffer.limit(8);
        channel.read(buffer);
        buffer.flip();
        long seqNum = buffer.getLong();

        // Read the key length
        buffer.clear();
        buffer.limit(4);
        channel.read(buffer);
        buffer.flip();
        int keyLength = buffer.getInt();

        // Read the key
        byte[] key = new byte[keyLength];
        buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
        buffer.limit(keyLength);
        channel.read(buffer);
        buffer.flip();
        buffer.get(key);

        if (recordType == PUT_RECORD) {
          // Read the value length
          buffer = ReadBuffers.ensureCapacity(buffer, 4);
          buffer.limit(4);
          channel.read(buffer);
          buffer.flip();
          int valueLength = buffer.getInt();

          // Read the value
          byte[] value = new byte[valueLength];
          buffer = ReadBuffers.ensureCapacity(buffer, valueLength);
          buffer.limit(valueLength);
          channel.read(buffer);
          buffer.flip();
          buffer.get(value);

          // Read the TTL
          buffer = ReadBuffers.ensureCapacity(buffer, 8);
          buffer.limit(8);
          channel.read(buffer);
          buffer.flip();
          long ttlSeconds = buffer.getLong();

          // Create a put record
          records.add(new PutRecord(seqNum, key, value, ttlSeconds));
        } else if (recordType == DELETE_RECORD) {
          // Create a delete record
          records.add(new DeleteRecord(seqNum, key));
        } else {
          throw new IOException("Unknown record type: " + recordType);
        }
      }
    } catch (IOException e) {
      logger.error("Error reading records from file: " + filePath, e);
      throw e;
    }

    return records;
  }
}
