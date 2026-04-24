package io.cascadestore.lsm.sstable.index;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import io.cascadestore.lsm.io.ReadBuffers;
import io.cascadestore.lsm.sstable.io.SSTableIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexFileManagerImpl implements IndexFileManager {
  private static final Logger logger = LoggerFactory.getLogger(IndexFileManagerImpl.class);

  private final SSTableIO io;
  private final NavigableMap<ByteArrayWrapper, Long> sparseIndex;

  public IndexFileManagerImpl(SSTableIO io) {
    this.io = io;
    this.sparseIndex = new TreeMap<>();
  }

  @Override
  public Path getIndexFilePath() {
    return Path.of(
        io.getDirectory(),
        String.format("sst_L%d_S%d.index", io.getLevel(), io.getSequenceNumber()));
  }

  @Override
  public FileChannel getIndexChannel() {
    return io.getIndexChannel();
  }

  @Override
  public void addIndexEntry(byte[] key, long offset) throws IOException {
    sparseIndex.put(new ByteArrayWrapper(key), offset);
  }

  @Override
  public void writeIndex() throws IOException {
    FileChannel channel = getIndexChannel();

    // Clear the channel
    channel.truncate(0);

    // Write each entry
    for (var entry : sparseIndex.entrySet()) {
      byte[] key = entry.getKey().getData();
      long offset = entry.getValue();

      ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + 8);
      buffer.putInt(key.length);
      buffer.put(key);
      buffer.putLong(offset);
      buffer.flip();

      channel.write(buffer);
    }

    // Force the data to disk
    channel.force(true);
  }

  @Override
  public void loadIndex() throws IOException {
    FileChannel channel = getIndexChannel();

    if (channel == null) {
      logger.warn("Index channel is null, cannot load index");
      return;
    }

    // Clear the existing index
    sparseIndex.clear();

    // Read the index
    ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

    while (channel.position() < channel.size()) {
      // Read key length
      buffer.clear();
      buffer.limit(4);
      channel.read(buffer);
      buffer.flip();
      int keyLength = buffer.getInt();

      // Read key
      buffer = ReadBuffers.ensureCapacity(buffer, keyLength);
      buffer.limit(keyLength);
      channel.read(buffer);
      buffer.flip();
      byte[] key = new byte[keyLength];
      buffer.get(key);

      // Read offset
      buffer.clear();
      buffer.limit(8);
      channel.read(buffer);
      buffer.flip();
      long offset = buffer.getLong();

      // Add to sparse index
      sparseIndex.put(new ByteArrayWrapper(key), offset);
    }

    logger.info("Loaded " + sparseIndex.size() + " entries into sparse index");
  }

  @Override
  public NavigableMap<ByteArrayWrapper, Long> getSparseIndex() {
    return sparseIndex;
  }

  @Override
  public Map.Entry<ByteArrayWrapper, Long> findClosestKey(byte[] key) {
    if (key == null) {
      return null;
    }

    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
    return sparseIndex.floorEntry(keyWrapper);
  }

  @Override
  public void close() throws IOException {
    // The SSTableIO will close the index channel
  }
}
