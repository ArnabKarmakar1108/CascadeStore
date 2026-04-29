package io.cascadestore.lsm.io;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/** Read-only mmap view of an SSTable data file (when it fits in {@code Integer.MAX_VALUE} bytes). */
public final class MappedDataFile implements AutoCloseable {

  private final MappedByteBuffer buffer;
  private final long size;

  private MappedDataFile(MappedByteBuffer buffer, long size) {
    this.buffer = buffer;
    this.size = size;
  }

  public static MappedDataFile tryMap(FileChannel channel) throws IOException {
    if (channel == null) {
      return null;
    }
    long fileSize = channel.size();
    if (fileSize <= 0 || fileSize > Integer.MAX_VALUE) {
      return null;
    }
    MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
    return new MappedDataFile(mapped, fileSize);
  }

  public long size() {
    return size;
  }

  public int getInt(long position) {
    return buffer.getInt((int) position);
  }

  public void getBytes(long position, byte[] target, int offset, int length) {
    int pos = (int) position;
    buffer.position(pos);
    buffer.get(target, offset, length);
  }

  public boolean bytesEqual(long position, int length, byte[] expected) {
    if (expected.length != length) {
      return false;
    }
    int pos = (int) position;
    for (int i = 0; i < length; i++) {
      if (buffer.get(pos + i) != expected[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    // MappedByteBuffer is released when the channel closes and the buffer is GC'd.
  }
}
