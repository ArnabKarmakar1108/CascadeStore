package io.cascadestore.lsm.wal.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public interface WALFile extends AutoCloseable {

  Path getPath();

  long getSequenceNumber();

  int write(ByteBuffer buffer) throws IOException;

  int read(ByteBuffer buffer, long position) throws IOException;

  void force(boolean metaData) throws IOException;

  long size() throws IOException;

  FileChannel getChannel();

  @Override
  void close() throws IOException;
}
