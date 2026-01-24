package io.cascadestore.lsm.sstable;

import io.cascadestore.lsm.memtable.MemTable;
import io.cascadestore.lsm.sstable.data.DataFileManager;
import io.cascadestore.lsm.sstable.data.DataFileManagerImpl;
import io.cascadestore.lsm.sstable.filter.FilterManager;
import io.cascadestore.lsm.sstable.filter.FilterManagerImpl;
import io.cascadestore.lsm.sstable.index.IndexFileManager;
import io.cascadestore.lsm.sstable.index.IndexFileManagerImpl;
import io.cascadestore.lsm.sstable.io.SSTableIO;
import io.cascadestore.lsm.sstable.io.SSTableIOImpl;
import io.cascadestore.lsm.sstable.iterator.SSTableIteratorFactory;
import io.cascadestore.lsm.sstable.iterator.SSTableIteratorFactoryImpl;
import java.io.IOException;

public class SSTableFactory {

  public static SSTableInterface createFromMemTable(
      MemTable memTable, String directory, int level, long sequenceNumber) throws IOException {

    // Create the I/O manager
    SSTableIO io = new SSTableIOImpl(directory, level, sequenceNumber);

    // Create the data file manager
    DataFileManager dataFileManager = new DataFileManagerImpl(io);

    // Create the index file manager
    IndexFileManager indexFileManager = new IndexFileManagerImpl(io);

    // Create the filter manager
    FilterManager filterManager = new FilterManagerImpl(io, memTable.getEntries().size());

    // Create the iterator factory
    SSTableIteratorFactory iteratorFactory =
        new SSTableIteratorFactoryImpl(dataFileManager, indexFileManager);

    // Create the SSTable
    return new SSTableImpl(
        memTable, dataFileManager, indexFileManager, filterManager, io, iteratorFactory);
  }

  public static SSTableInterface openFromDisk(String directory, int level, long sequenceNumber)
      throws IOException {

    // Create the I/O manager
    SSTableIO io = new SSTableIOImpl(directory, level, sequenceNumber);

    // Create the data file manager
    DataFileManager dataFileManager = new DataFileManagerImpl(io);

    // Create the index file manager
    IndexFileManager indexFileManager = new IndexFileManagerImpl(io);

    // Create the filter manager
    FilterManager filterManager = new FilterManagerImpl(io);

    // Create the iterator factory
    SSTableIteratorFactory iteratorFactory =
        new SSTableIteratorFactoryImpl(dataFileManager, indexFileManager);

    // Create the SSTable
    return new SSTableImpl(dataFileManager, indexFileManager, filterManager, io, iteratorFactory);
  }

  public static SSTable createBackwardCompatible(
      MemTable memTable, String directory, int level, long sequenceNumber) throws IOException {

    if (memTable != null) {
      // Create a new SSTable from a MemTable
      SSTableInterface sstable = createFromMemTable(memTable, directory, level, sequenceNumber);
      return new SSTableAdapter(sstable);
    } else {
      // Open an existing SSTable from disk
      SSTableInterface sstable = openFromDisk(directory, level, sequenceNumber);
      return new SSTableAdapter(sstable);
    }
  }
}
