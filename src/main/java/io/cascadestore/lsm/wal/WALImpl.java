package io.cascadestore.lsm.wal;

import io.cascadestore.lsm.wal.manager.WALManager;
import io.cascadestore.lsm.wal.manager.WALManagerImpl;
import io.cascadestore.lsm.wal.reader.WALReader;
import io.cascadestore.lsm.wal.reader.WALReaderImpl;
import io.cascadestore.lsm.wal.record.Record;
import io.cascadestore.lsm.wal.writer.WALWriter;
import io.cascadestore.lsm.wal.writer.WALWriterImpl;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALImpl implements WAL {
  private static final Logger logger = LoggerFactory.getLogger(WALImpl.class);

  private final WALManager manager;
  private final WALReader reader;
  private final WALWriter writer;

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
    return writer.appendPutRecord(key, value, ttlSeconds);
  }

  @Override
  public long appendDeleteRecord(byte[] key) throws IOException {
    return writer.appendDeleteRecord(key);
  }

  @Override
  public List<Record> readRecords() throws IOException {
    return reader.readRecords();
  }

  @Override
  public void deleteAllLogs() throws IOException {
    manager.deleteAllLogs();
  }

  @Override
  public void close() throws IOException {
    if (manager.getCurrentFile() != null) {
      manager.getCurrentFile().close();
    }

    logger.info("WAL closed");
  }
}
