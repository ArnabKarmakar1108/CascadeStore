package io.cascadestore.lsm.sstable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableAdapter extends SSTable {
  private static final Logger logger = LoggerFactory.getLogger(SSTableAdapter.class);

  private final SSTableInterface delegate;

  public SSTableAdapter(SSTableInterface delegate) throws IOException {
    // Call the parent constructor with a dummy directory
    super(createDummyDirectory(), 0, 0);
    this.delegate = delegate;
  }

  private static String createDummyDirectory() {
    try {
      // Create a temporary directory for the dummy SSTable
      Path tempDir = Files.createTempDirectory("sstable-adapter");
      // Delete the temporary directory on exit
      tempDir.toFile().deleteOnExit();
      return tempDir.toString();
    } catch (IOException e) {
      throw new RuntimeException("Error creating dummy directory", e);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    return delegate.get(key);
  }

  @Override
  public boolean mightContain(byte[] key) {
    return delegate.mightContain(key);
  }

  @Override
  public int getLevel() {
    return delegate.getLevel();
  }

  @Override
  public long getSequenceNumber() {
    return delegate.getSequenceNumber();
  }

  @Override
  public long getCreationTime() {
    return delegate.getCreationTime();
  }

  @Override
  public long getSizeBytes() {
    return delegate.getSizeBytes();
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (IOException e) {
      logger.warn("Error closing SSTable", e);
    }
  }

  @Override
  public boolean delete() {
    return delegate.delete();
  }

  @Override
  public List<byte[]> listKeys() {
    return delegate.listKeys();
  }

  @Override
  public int countEntries() {
    return delegate.countEntries();
  }

  @Override
  public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
    return delegate.getRange(startKey, endKey);
  }

  public SSTableInterface getDelegate() {
    return delegate;
  }
}
