package io.cascadestore.lsm.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public final class DirectBufferAllocator implements OffHeapAllocator {

  private static final Object UNSAFE;
  private static final Method INVOKE_CLEANER;

  static {
    Object unsafe = null;
    Method invokeCleaner = null;
    try {
      Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      unsafe = theUnsafe.get(null);
      invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
    } catch (ReflectiveOperationException ignored) {
      // Unsafe unavailable; buffers will be reclaimed by GC as a fallback
    }
    UNSAFE = unsafe;
    INVOKE_CLEANER = invokeCleaner;
  }

  private final List<ByteBuffer> allocated = new ArrayList<>();
  private boolean closed;

  @Override
  public synchronized ByteBuffer allocate(int bytes) {
    if (closed) {
      throw new IllegalStateException("Allocator is closed");
    }
    ByteBuffer buffer = ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
    allocated.add(buffer);
    return buffer;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (ByteBuffer buffer : allocated) {
      freeDirectBuffer(buffer);
    }
    allocated.clear();
  }

  private static void freeDirectBuffer(ByteBuffer buffer) {
    if (!buffer.isDirect() || UNSAFE == null || INVOKE_CLEANER == null) {
      return;
    }
    try {
      INVOKE_CLEANER.invoke(UNSAFE, buffer);
    } catch (ReflectiveOperationException ignored) {
      // Fall back to GC reclamation if cleaner invocation fails
    }
  }
}
