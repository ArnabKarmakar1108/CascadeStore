package io.cascadestore.lsm.io;

import java.nio.ByteBuffer;

/** Thread-local growable {@link ByteBuffer} for WAL / record encoding hot paths. */
public final class RecordBufferPool {

  private static final ThreadLocal<ByteBuffer> BUFFERS = ThreadLocal.withInitial(() -> ByteBuffer.allocate(256));

  private RecordBufferPool() {}

  public static ByteBuffer acquire(int minimumCapacity) {
    ByteBuffer buffer = BUFFERS.get();
    if (buffer.capacity() < minimumCapacity) {
      buffer = ByteBuffer.allocate(minimumCapacity);
      BUFFERS.set(buffer);
    } else {
      buffer.clear();
    }
    return buffer;
  }
}
