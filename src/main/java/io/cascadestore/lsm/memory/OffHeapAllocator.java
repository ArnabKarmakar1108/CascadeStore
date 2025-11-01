package io.cascadestore.lsm.memory;

import java.nio.ByteBuffer;

public interface OffHeapAllocator extends AutoCloseable {

  ByteBuffer allocate(int bytes);

  @Override
  void close();
}
