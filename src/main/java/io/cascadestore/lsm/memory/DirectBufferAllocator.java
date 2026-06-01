package io.cascadestore.lsm.memory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public final class DirectBufferAllocator implements OffHeapAllocator {

  /**
   * Slab size for sub-allocation. Memtable entries are carved from these larger direct buffers so
   * writes cost a pointer bump instead of a native {@code malloc} + {@code Cleaner} registration per
   * entry. Values larger than a slab get a dedicated buffer.
   */
  static final int DEFAULT_SLAB_SIZE = 1 << 20; // 1 MiB

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

  private final int slabSize;
  // Every buffer we allocateDirect (slabs + oversized dedicated buffers). Only these carry a
  // Cleaner and get freed on close(); the per-entry slices we hand out do not.
  private final List<ByteBuffer> ownedBuffers = new ArrayList<>();
  private ByteBuffer currentSlab;
  private boolean closed;

  public DirectBufferAllocator() {
    this(DEFAULT_SLAB_SIZE);
  }

  DirectBufferAllocator(int slabSize) {
    if (slabSize <= 0) {
      throw new IllegalArgumentException("slabSize must be positive");
    }
    this.slabSize = slabSize;
  }

  @Override
  public synchronized ByteBuffer allocate(int bytes) {
    if (closed) {
      throw new IllegalStateException("Allocator is closed");
    }
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }

    if (bytes > slabSize) {
      ByteBuffer dedicated = ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
      ownedBuffers.add(dedicated);
      return dedicated;
    }

    if (currentSlab == null || currentSlab.remaining() < bytes) {
      currentSlab = ByteBuffer.allocateDirect(slabSize).order(ByteOrder.nativeOrder());
      ownedBuffers.add(currentSlab);
    }

    int start = currentSlab.position();
    currentSlab.position(start + bytes);
    // Absolute slice (JDK 13+): independent position/limit, backed by the slab's memory.
    return currentSlab.slice(start, bytes).order(ByteOrder.nativeOrder());
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    currentSlab = null;
    for (ByteBuffer buffer : ownedBuffers) {
      freeDirectBuffer(buffer);
    }
    ownedBuffers.clear();
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
