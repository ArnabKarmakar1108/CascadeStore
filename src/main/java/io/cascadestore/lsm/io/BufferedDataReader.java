package io.cascadestore.lsm.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Sequential reader over a {@link FileChannel} or mmap-backed SSTable data file. Refills a fixed-size
 * window to avoid one syscall per on-disk field when scanning, optionally prefetches the next
 * window, and uses mmap when the file fits in address space.
 */
public final class BufferedDataReader implements AutoCloseable {

  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  private final FileChannel channel;
  private final int bufferSize;
  private final long fileSize;
  private final BlockCache blockCache;
  private final long sstableId;
  private final MappedDataFile mapped;
  private ByteBuffer buffer;
  private long bufferStart = -1;
  private long bufferEnd = -1;
  private long mmapPosition;
  private ByteBuffer prefetchBuffer;
  private long prefetchStart = -1;
  private long prefetchEnd = -1;

  public BufferedDataReader(FileChannel channel) throws IOException {
    this(channel, DEFAULT_BUFFER_SIZE, null, 0L);
  }

  public BufferedDataReader(FileChannel channel, int bufferSize) throws IOException {
    this(channel, bufferSize, null, 0L);
  }

  public BufferedDataReader(
      FileChannel channel, int bufferSize, BlockCache blockCache, long sstableId)
      throws IOException {
    this(channel, bufferSize, blockCache, sstableId, MappedDataFile.tryMap(channel));
  }

  public BufferedDataReader(
      FileChannel channel,
      int bufferSize,
      BlockCache blockCache,
      long sstableId,
      MappedDataFile mapped)
      throws IOException {
    if (channel == null) {
      throw new IllegalArgumentException("channel cannot be null");
    }
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("bufferSize must be positive");
    }
    this.channel = channel;
    this.bufferSize = bufferSize;
    this.fileSize = channel.size();
    this.blockCache = blockCache;
    this.sstableId = sstableId;
    this.mapped = mapped;
    this.buffer = ByteBuffer.allocate(bufferSize);
    if (mapped != null) {
      this.prefetchBuffer = null;
    } else {
      this.prefetchBuffer = ByteBuffer.allocate(bufferSize);
    }
  }

  public boolean isMapped() {
    return mapped != null;
  }

  public long size() {
    return fileSize;
  }

  public long position() {
    if (mapped != null) {
      return mmapPosition;
    }
    if (bufferStart < 0) {
      return 0;
    }
    return bufferStart + buffer.position();
  }

  public void seek(long absolutePosition) throws IOException {
    if (absolutePosition < 0 || absolutePosition > fileSize) {
      throw new IOException("Seek out of range: " + absolutePosition);
    }
    if (mapped != null) {
      mmapPosition = absolutePosition;
      return;
    }
    if (absolutePosition >= bufferStart && absolutePosition < bufferEnd) {
      buffer.position((int) (absolutePosition - bufferStart));
      return;
    }
    if (absolutePosition >= prefetchStart && absolutePosition < prefetchEnd) {
      swapPrefetchIntoActive();
      buffer.position((int) (absolutePosition - bufferStart));
      return;
    }
    loadBuffer(absolutePosition);
  }

  /** Warms the reader window at {@code absolutePosition} (channel mode only). */
  public void prefetch(long absolutePosition) throws IOException {
    if (mapped != null || absolutePosition < 0 || absolutePosition >= fileSize) {
      return;
    }
    if (absolutePosition >= bufferStart && absolutePosition < bufferEnd) {
      schedulePrefetchAfter(bufferEnd);
      return;
    }
    if (absolutePosition >= prefetchStart && absolutePosition < prefetchEnd) {
      return;
    }
    loadBuffer(absolutePosition);
  }

  public int readInt() throws IOException {
    if (mapped != null) {
      int value = mapped.getInt(mmapPosition);
      mmapPosition += Integer.BYTES;
      return value;
    }
    ensureRemaining(Integer.BYTES);
    return buffer.getInt();
  }

  public long readLong() throws IOException {
    if (mapped != null) {
      long value = mapped.getLong(mmapPosition);
      mmapPosition += Long.BYTES;
      return value;
    }
    ensureRemaining(Long.BYTES);
    return buffer.getLong();
  }

  public byte[] readBytes(int length) throws IOException {
    return ValueBufferPool.readCopy(this, length);
  }

  /** Compares the next {@code length} bytes to {@code expected} without allocating. */
  public boolean bytesEqual(int length, byte[] expected) throws IOException {
    if (mapped != null) {
      if (expected.length != length) {
        mmapPosition += length;
        return false;
      }
      boolean matches = mapped.bytesEqual(mmapPosition, length, expected);
      mmapPosition += length;
      return matches;
    }
    if (expected.length != length) {
      skip(length);
      return false;
    }
    for (int i = 0; i < length; i++) {
      ensureRemaining(1);
      if (buffer.get() != expected[i]) {
        skip(length - i - 1L);
        return false;
      }
    }
    return true;
  }

  public void readFully(byte[] target) throws IOException {
    readFully(target, 0, target.length);
  }

  public void readFully(byte[] target, int offset, int length) throws IOException {
    if (mapped != null) {
      mapped.getBytes(mmapPosition, target, offset, length);
      mmapPosition += length;
      return;
    }
    int copied = 0;
    while (copied < length) {
      ensureRemaining(1);
      int toCopy = Math.min(buffer.remaining(), length - copied);
      buffer.get(target, offset + copied, toCopy);
      copied += toCopy;
    }
  }

  public void skip(long bytes) throws IOException {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }
    seek(position() + bytes);
  }

  private void ensureRemaining(int needed) throws IOException {
    if (buffer.remaining() >= needed) {
      return;
    }

    long pos = position();
    loadBuffer(pos);
    if (pos + needed > fileSize) {
      throw new IOException("Unexpected end of file at position " + pos);
    }
    if (buffer.remaining() < needed) {
      loadBuffer(pos);
    }
    if (buffer.remaining() < needed) {
      throw new IOException("Record spans buffer window at position " + pos);
    }
  }

  private void loadBuffer(long absolutePosition) throws IOException {
    if (blockCache != null && blockCache.isEnabled()) {
      byte[] cached = blockCache.get(sstableId, absolutePosition);
      if (cached != null) {
        buffer.clear();
        buffer.put(cached);
        buffer.flip();
        bufferStart = absolutePosition;
        bufferEnd = absolutePosition + cached.length;
        schedulePrefetchAfter(bufferEnd);
        return;
      }
      blockCache.recordMiss();
    }

    buffer.clear();
    bufferStart = absolutePosition;
    int toRead = (int) Math.min(bufferSize, fileSize - absolutePosition);
    if (toRead <= 0) {
      bufferEnd = absolutePosition;
      buffer.limit(0);
      return;
    }

    buffer.limit(toRead);
    int read = channel.read(buffer, absolutePosition);
    if (read <= 0) {
      bufferEnd = absolutePosition;
      buffer.limit(0);
      return;
    }

    buffer.flip();
    bufferEnd = absolutePosition + read;

    if (blockCache != null && blockCache.isEnabled() && read > 0) {
      byte[] copy = Arrays.copyOf(buffer.array(), buffer.limit());
      blockCache.put(sstableId, absolutePosition, copy);
    }

    schedulePrefetchAfter(bufferEnd);
  }

  private void schedulePrefetchAfter(long nextPosition) throws IOException {
    if (prefetchBuffer == null || nextPosition >= fileSize) {
      return;
    }
    if (nextPosition >= prefetchStart && nextPosition < prefetchEnd) {
      return;
    }

    prefetchBuffer.clear();
    prefetchStart = nextPosition;
    int toRead = (int) Math.min(bufferSize, fileSize - nextPosition);
    if (toRead <= 0) {
      prefetchEnd = nextPosition;
      prefetchBuffer.limit(0);
      return;
    }

    prefetchBuffer.limit(toRead);
    int read = channel.read(prefetchBuffer, nextPosition);
    if (read <= 0) {
      prefetchEnd = nextPosition;
      prefetchBuffer.limit(0);
      return;
    }

    prefetchBuffer.flip();
    prefetchEnd = nextPosition + read;
  }

  private void swapPrefetchIntoActive() {
    ByteBuffer active = buffer;
    long activeStart = bufferStart;
    long activeEnd = bufferEnd;

    buffer = prefetchBuffer;
    bufferStart = prefetchStart;
    bufferEnd = prefetchEnd;

    prefetchBuffer = active;
    prefetchStart = activeStart;
    prefetchEnd = activeEnd;
  }

  @Override
  public void close() {
    // Does not own the channel.
  }
}
