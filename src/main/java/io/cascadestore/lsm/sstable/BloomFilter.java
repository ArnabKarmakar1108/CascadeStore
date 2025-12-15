package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.memory.DirectBufferAllocator;
import io.cascadestore.lsm.memory.OffHeapAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomFilter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);

  private final ByteBuffer bitBuffer;
  private final int numHashFunctions;
  private final OffHeapAllocator allocator;
  private final int bitArraySize;
  private boolean closed;

  public BloomFilter(int expectedEntries, double falsePositiveRate) {
    int numBits = optimalNumOfBits(expectedEntries, falsePositiveRate);
    this.bitArraySize = (numBits + 7) / 8;
    this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
    this.allocator = new DirectBufferAllocator();
    this.bitBuffer = allocator.allocate(bitArraySize);
    zeroBuffer();
  }

  public BloomFilter(byte[] bits, int numHashFunctions) {
    this.bitArraySize = bits.length;
    this.numHashFunctions = numHashFunctions;
    this.allocator = new DirectBufferAllocator();
    this.bitBuffer = allocator.allocate(bitArraySize);
    for (int i = 0; i < bits.length; i++) {
      bitBuffer.put(i, bits[i]);
    }
  }

  private void zeroBuffer() {
    for (int i = 0; i < bitArraySize; i++) {
      bitBuffer.put(i, (byte) 0);
    }
  }

  public void add(byte[] key) {
    if (key == null) {
      return;
    }

    for (int i = 0; i < numHashFunctions; i++) {
      int hash = hash(key, i);
      int bitIndex = (hash & 0x7FFFFFFF) % (bitArraySize * 8);
      int byteIndex = bitIndex / 8;
      int bitOffset = bitIndex % 8;

      byte currentByte = bitBuffer.get(byteIndex);
      byte newByte = (byte) (currentByte | (1 << bitOffset));
      bitBuffer.put(byteIndex, newByte);
    }
  }

  public boolean mightContain(byte[] key) {
    if (key == null) {
      return false;
    }

    for (int i = 0; i < numHashFunctions; i++) {
      int hash = hash(key, i);
      int bitIndex = (hash & 0x7FFFFFFF) % (bitArraySize * 8);
      int byteIndex = bitIndex / 8;
      int bitOffset = bitIndex % 8;

      byte currentByte = bitBuffer.get(byteIndex);
      if ((currentByte & (1 << bitOffset)) == 0) {
        return false;
      }
    }
    return true;
  }

  public void save(String filePath) throws IOException {
    try (FileChannel channel =
        FileChannel.open(
            Path.of(filePath),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.putInt(numHashFunctions);
      buffer.flip();
      channel.write(buffer);

      byte[] bits = new byte[bitArraySize];
      for (int i = 0; i < bitArraySize; i++) {
        bits[i] = bitBuffer.get(i);
      }
      channel.write(ByteBuffer.wrap(bits));
    }
  }

  public static BloomFilter load(String filePath) throws IOException {
    try (FileChannel channel = FileChannel.open(Path.of(filePath), StandardOpenOption.READ)) {

      ByteBuffer buffer = ByteBuffer.allocate(4);
      channel.read(buffer);
      buffer.flip();
      int numHashFunctions = buffer.getInt();

      long fileSize = channel.size();
      int bitsSize = (int) (fileSize - 4);
      byte[] bits = new byte[bitsSize];
      channel.read(ByteBuffer.wrap(bits), 4);

      return new BloomFilter(bits, numHashFunctions);
    }
  }

  private int hash(byte[] key, int seed) {
    int h = seed;
    for (byte b : key) {
      h = 31 * h + b;
    }
    return h;
  }

  private int optimalNumOfBits(int n, double p) {
    return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  private int optimalNumOfHashFunctions(int n, int m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    allocator.close();
  }
}
