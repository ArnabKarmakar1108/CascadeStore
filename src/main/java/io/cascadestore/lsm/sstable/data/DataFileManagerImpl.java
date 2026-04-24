package io.cascadestore.lsm.sstable.data;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.io.ReadBuffers;
import io.cascadestore.lsm.sstable.SSTableEntry;
import io.cascadestore.lsm.sstable.io.SSTableIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFileManagerImpl implements DataFileManager {
  private static final Logger logger = LoggerFactory.getLogger(DataFileManagerImpl.class);

  private final SSTableIO io;

  public DataFileManagerImpl(SSTableIO io) {
    this.io = io;
  }

  @Override
  public Path getDataFilePath() {
    return Path.of(
        io.getDirectory(),
        String.format("sst_L%d_S%d.data", io.getLevel(), io.getSequenceNumber()));
  }

  @Override
  public FileChannel getDataChannel() {
    return io.getDataChannel();
  }

  @Override
  public long writeEntry(SSTableEntry entry) throws IOException {
    ByteBuffer buffer = io.writeEntryHeader(entry);
    return getDataChannel().write(buffer);
  }

  @Override
  public SSTableEntry readEntry(long offset) throws IOException {
    FileChannel channel = getDataChannel();

    // Read key length
    ByteBuffer keyLengthBuffer = ByteBuffer.allocate(4);
    channel.read(keyLengthBuffer, offset);
    keyLengthBuffer.flip();
    int keyLength = keyLengthBuffer.getInt();

    // Read key
    ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
    channel.read(keyBuffer, offset + 4);
    keyBuffer.flip();
    byte[] key = new byte[keyLength];
    keyBuffer.get(key);

    // Read value length
    ByteBuffer valueLengthBuffer = ByteBuffer.allocate(4);
    channel.read(valueLengthBuffer, offset + 4 + keyLength);
    valueLengthBuffer.flip();
    int valueLength = valueLengthBuffer.getInt();

    // Check if this is a tombstone
    if (valueLength == 0) {
      // Read timestamp
      ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
      channel.read(timestampBuffer, offset + 4 + keyLength + 4);
      timestampBuffer.flip();
      long timestamp = timestampBuffer.getLong();

      return SSTableEntry.tombstone(key, timestamp);
    } else {
      // Read value
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
      channel.read(valueBuffer, offset + 4 + keyLength + 4);
      valueBuffer.flip();
      byte[] value = new byte[valueLength];
      valueBuffer.get(value);

      // Read timestamp
      ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
      channel.read(timestampBuffer, offset + 4 + keyLength + 4 + valueLength);
      timestampBuffer.flip();
      long timestamp = timestampBuffer.getLong();

      return SSTableEntry.of(key, value, timestamp);
    }
  }

  @Override
  public byte[] findKeyInDataFile(byte[] key, long startPosition) throws IOException {
    FileChannel channel = getDataChannel();
    ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
    long position = startPosition;

    while (position < channel.size()) {
      // Read key length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int keyLength = buffer.getInt();
      position += 4;

      // Read key
      buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
      buffer.limit(keyLength);
      channel.read(buffer, position);
      buffer.flip();
      byte[] entryKey = new byte[keyLength];
      buffer.get(entryKey);
      position += keyLength;

      // Read value length
      buffer = ReadBuffers.ensureCapacity(buffer, 4);
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int valueLength = buffer.getInt();
      position += 4;

      // Check if this is a tombstone
      if (valueLength == 0) {
        // Skip the timestamp (8 bytes)
        position += 8;

        // If this is the key we're looking for, it's been deleted
        if (Arrays.equals(key, entryKey)) {
          return null;
        }

        continue;
      }

      // Read value
      buffer = ReadBuffers.ensureCapacity(buffer, valueLength);
      buffer.limit(valueLength);
      channel.read(buffer, position);
      buffer.flip();
      byte[] value = new byte[valueLength];
      buffer.get(value);
      position += valueLength;

      // Skip the timestamp (8 bytes)
      position += 8;

      // If this is the key we're looking for, return the value
      if (Arrays.equals(key, entryKey)) {
        return value;
      }
    }

    // Key not found
    return null;
  }

  @Override
  public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) throws IOException {
    Map<byte[], byte[]> result =
        new TreeMap<>(
            (a, b) -> {
              ByteArrayWrapper wrapperA = new ByteArrayWrapper(a);
              ByteArrayWrapper wrapperB = new ByteArrayWrapper(b);
              return wrapperA.compareTo(wrapperB);
            });

    FileChannel channel = getDataChannel();
    ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
    long position = 16; // Start after the header

    while (position < channel.size()) {
      // Read key length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int keyLength = buffer.getInt();
      position += 4;

      // Read key
      buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
      buffer.limit(keyLength);
      channel.read(buffer, position);
      buffer.flip();
      byte[] key = new byte[keyLength];
      buffer.get(key);
      position += keyLength;

      // Check if the key is in the range
      ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
      boolean inRange = true;
      if (startKey != null && keyWrapper.compareTo(new ByteArrayWrapper(startKey)) < 0) {
        inRange = false; // Key is before the start of the range
      }
      if (endKey != null && keyWrapper.compareTo(new ByteArrayWrapper(endKey)) >= 0) {
        inRange = false; // Key is at or after the end of the range
      }

      // Read value length
      buffer = ReadBuffers.ensureCapacity(buffer, 4);
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int valueLength = buffer.getInt();
      position += 4;

      // Skip tombstones
      if (valueLength == 0) {
        // Skip the timestamp (8 bytes)
        position += 8;
        continue;
      }

      // If the key is in the range, add it to the result
      if (inRange) {
        // Read value
        buffer = ReadBuffers.ensureCapacity(buffer, valueLength);
        buffer.limit(valueLength);
        channel.read(buffer, position);
        buffer.flip();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        result.put(key, value);
      }

      // Skip the value (if we didn't read it) and timestamp
      position += valueLength + 8;
    }

    return result;
  }

  @Override
  public List<byte[]> listKeys() throws IOException {
    List<byte[]> keys = new ArrayList<>();

    FileChannel channel = getDataChannel();
    ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
    long position = 16; // Start after the header

    while (position < channel.size()) {
      // Read key length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int keyLength = buffer.getInt();
      position += 4;

      // Read key
      buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
      buffer.limit(keyLength);
      channel.read(buffer, position);
      buffer.flip();
      byte[] key = new byte[keyLength];
      buffer.get(key);
      position += keyLength;

      // Read value length
      buffer = ReadBuffers.ensureCapacity(buffer, 4);
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int valueLength = buffer.getInt();
      position += 4;

      // Skip tombstones
      if (valueLength == 0) {
        // Skip the timestamp (8 bytes)
        position += 8;
        continue;
      }

      // Add the key to the list
      keys.add(key);

      // Skip the value and timestamp
      position += valueLength + 8;
    }

    return keys;
  }

  @Override
  public int countEntries() throws IOException {
    int count = 0;

    FileChannel channel = getDataChannel();
    ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
    long position = 16; // Start after the header

    while (position < channel.size()) {
      // Read key length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int keyLength = buffer.getInt();
      position += 4;

      // Skip key
      position += keyLength;

      // Read value length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer, position);
      buffer.flip();
      int valueLength = buffer.getInt();
      position += 4;

      // Skip tombstones
      if (valueLength == 0) {
        // Skip the timestamp (8 bytes)
        position += 8;
        continue;
      }

      // Increment count
      count++;

      // Skip the value and timestamp
      position += valueLength + 8;
    }

    return count;
  }

  @Override
  public void close() throws IOException {
    // The SSTableIO will close the data channel
  }
}
