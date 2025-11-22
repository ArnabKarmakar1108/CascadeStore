package io.cascadestore.lsm.wal.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALFileImpl implements WALFile {
  private static final Logger logger = LoggerFactory.getLogger(WALFileImpl.class);

  private final Path path;
  private final long sequenceNumber;
  private final FileChannel channel;

  public WALFileImpl(Path path, long sequenceNumber, StandardOpenOption... options)
      throws IOException {
    this.path = path;
    this.sequenceNumber = sequenceNumber;
    this.channel = FileChannel.open(path, options);
    logger.debug("Opened WAL file: " + path);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public int write(ByteBuffer buffer) throws IOException {
    return channel.write(buffer);
  }

  @Override
  public int read(ByteBuffer buffer, long position) throws IOException {
    return channel.read(buffer, position);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    channel.force(metaData);
  }

  @Override
  public long size() throws IOException {
    return channel.size();
  }

  @Override
  public FileChannel getChannel() {
    return channel;
  }

  @Override
  public void close() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
      logger.debug("Closed WAL file: " + path);
    }
  }

  @Override
  public String toString() {
    return "WALFile{" + "path=" + path + ", sequenceNumber=" + sequenceNumber + '}';
  }
}
