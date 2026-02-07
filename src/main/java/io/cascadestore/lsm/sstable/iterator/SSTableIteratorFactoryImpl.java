package io.cascadestore.lsm.sstable.iterator;

import io.cascadestore.lsm.api.KeyValueIterator;
import io.cascadestore.lsm.sstable.SSTableEntry;
import io.cascadestore.lsm.sstable.SSTableIterator;
import io.cascadestore.lsm.sstable.data.DataFileManager;
import io.cascadestore.lsm.sstable.index.IndexFileManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableIteratorFactoryImpl implements SSTableIteratorFactory {
  private static final Logger logger = LoggerFactory.getLogger(SSTableIteratorFactoryImpl.class);

  private final DataFileManager dataFileManager;
  private final IndexFileManager indexFileManager;

  public SSTableIteratorFactoryImpl(
      DataFileManager dataFileManager, IndexFileManager indexFileManager) {
    this.dataFileManager = dataFileManager;
    this.indexFileManager = indexFileManager;
  }

  @Override
  public SSTableIterator createInMemoryIterator(SSTableEntry[] entries) {
    return new SSTableIterator.InMemoryIterator(entries);
  }

  @Override
  public SSTableIterator createFileIterator(byte[] startKey, byte[] endKey) throws IOException {
    // This is a placeholder implementation
    // The actual implementation would read entries from the file
    return new SSTableIterator.FileIterator(null, startKey, endKey);
  }

  @Override
  public KeyValueIterator createIterator(byte[] startKey, byte[] endKey) throws IOException {
    try {
      // For now, we'll use the FileIterator
      // In a more complete implementation, we might choose between different iterator types
      // based on the size of the range, the number of entries, etc.
      return createFileIterator(startKey, endKey);
    } catch (IOException e) {
      logger.error("Error creating iterator", e);
      throw e;
    }
  }
}
