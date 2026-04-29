package io.cascadestore.lsm.io;

import java.io.IOException;
import java.util.Arrays;

/** Thread-local growable scratch buffers for SSTable value reads. */
public final class ValueBufferPool {

  private static final ThreadLocal<byte[]> SCRATCH =
      ThreadLocal.withInitial(() -> new byte[1024]);

  private ValueBufferPool() {}

  /**
   * Reads {@code length} bytes from {@code reader} into a thread-local scratch buffer and returns an
   * owned copy for the caller.
   */
  public static byte[] readCopy(BufferedDataReader reader, int length) throws IOException {
    byte[] scratch = SCRATCH.get();
    if (scratch.length < length) {
      scratch = new byte[length];
      SCRATCH.set(scratch);
    }
    reader.readFully(scratch, 0, length);
    return Arrays.copyOf(scratch, length);
  }
}
