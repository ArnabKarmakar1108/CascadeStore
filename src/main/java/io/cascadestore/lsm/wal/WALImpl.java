package io.cascadestore.lsm.wal;

import io.cascadestore.lsm.wal.manager.WALManager;
import io.cascadestore.lsm.wal.manager.WALManagerImpl;
import io.cascadestore.lsm.wal.reader.WALReader;
import io.cascadestore.lsm.wal.reader.WALReaderImpl;
import io.cascadestore.lsm.wal.record.Record;
import io.cascadestore.lsm.wal.writer.WALWriter;
import io.cascadestore.lsm.wal.writer.WALWriterImpl;
import io.cascadestore.lsm.wal.file.WALFile;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALImpl implements WAL {
  private static final Logger logger = LoggerFactory.getLogger(WALImpl.class);

  private final WALManager manager;
  private final WALReader reader;
  private final WALWriterImpl writer;
  private final ReadWriteLock writeLock = new ReentrantReadWriteLock();
  private boolean closed;

  public WALImpl(String directory) throws IOException {
    this(directory, 64 * 1024 * 1024); // Default 64MB max log size
  }

  public WALImpl(String directory, long maxLogSizeBytes) throws IOException {
    this.manager = new WALManagerImpl(directory, maxLogSizeBytes);
    this.reader = new WALReaderImpl(manager);
    this.writer = new WALWriterImpl(manager);

    logger.info("WAL initialized in directory: " + directory);
  }

  @Override
  public long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException {
    writeLock.readLock().lock();
    try {
      if (hasCapacityForAppend()) {
        return writer.appendPutRecordWithoutRotation(key, value, ttlSeconds);
      }
    } finally {
      writeLock.readLock().unlock();
    }

    writeLock.writeLock().lock();
    try {
      return writer.appendPutRecord(key, value, ttlSeconds);
    } finally {
      writeLock.writeLock().unlock();
    }
  }

  @Override
  public long appendDeleteRecord(byte[] key) throws IOException {
    writeLock.readLock().lock();
    try {
      if (hasCapacityForAppend()) {
        return writer.appendDeleteRecordWithoutRotation(key);
      }
    } finally {
      writeLock.readLock().unlock();
    }

    writeLock.writeLock().lock();
    try {
      return writer.appendDeleteRecord(key);
    } finally {
      writeLock.writeLock().unlock();
    }
  }

  @Override
  public List<Record> readRecords() throws IOException {
    return reader.readRecords();
  }

  @Override
  public void deleteAllLogs() throws IOException {
    writeLock.writeLock().lock();
    try {
      manager.deleteAllLogs();
    } finally {
      writeLock.writeLock().unlock();
    }
  }

  @Override
  public void sync() throws IOException {
    writeLock.writeLock().lock();
    try {
      manager.sync();
    } finally {
      writeLock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    writeLock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      manager.sync();
      if (manager.getCurrentFile() != null) {
        manager.getCurrentFile().close();
      }
      closed = true;
    } finally {
      writeLock.writeLock().unlock();
    }
  }

  private boolean hasCapacityForAppend() throws IOException {
    WALFile currentFile = manager.getCurrentFile();
    return currentFile != null && currentFile.size() < manager.getMaxLogSizeBytes();
  }
}
