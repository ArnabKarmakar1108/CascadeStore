package io.cascadestore.lsm.io;

import java.nio.ByteBuffer;

/** Helpers for growing reusable read buffers when scanning on-disk records. */
public final class ReadBuffers {

  private ReadBuffers() {}

  /**
   * Returns a buffer with at least {@code required} bytes of capacity, cleared for writing.
   * Reallocates when the existing buffer is too small.
   */
  public static ByteBuffer ensureCapacity(ByteBuffer buffer, int required) {
    if (buffer.capacity() < required) {
      return ByteBuffer.allocate(required);
    }
    buffer.clear();
    return buffer;
  }
}
