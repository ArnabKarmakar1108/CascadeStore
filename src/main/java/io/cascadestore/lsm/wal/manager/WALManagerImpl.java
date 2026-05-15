package io.cascadestore.lsm.wal.manager;

import io.cascadestore.lsm.metrics.CascadeMetrics;
import io.cascadestore.lsm.wal.WalRecordCodec;
import io.cascadestore.lsm.wal.file.WALFile;
import io.cascadestore.lsm.wal.file.WALFileImpl;
import io.cascadestore.lsm.wal.reader.WALReaderImpl;
import io.cascadestore.lsm.wal.record.Record;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALManagerImpl implements WALManager {
  private static final Logger logger = LoggerFactory.getLogger(WALManagerImpl.class);

  // Directory to store WAL files
  private final String directory;

  // Current WAL file
  private WALFile currentFile;

  // Sequence number for records
  private final AtomicLong sequenceNumber;

  // Maximum size of a WAL file before rotation
  private final long maxLogSizeBytes;

  private final long syncBatchBytes;

  private long bytesSinceLastSync;

  private final CascadeMetrics metrics;

  public WALManagerImpl(String directory) throws IOException {
    this(directory, 64 * 1024 * 1024, WalSyncPolicy.DEFAULT_SYNC_BATCH_BYTES, CascadeMetrics.noop());
  }

  public WALManagerImpl(String directory, long maxLogSizeBytes) throws IOException {
    this(directory, maxLogSizeBytes, WalSyncPolicy.DEFAULT_SYNC_BATCH_BYTES, CascadeMetrics.noop());
  }

  public WALManagerImpl(String directory, long maxLogSizeBytes, long syncBatchBytes)
      throws IOException {
    this(directory, maxLogSizeBytes, syncBatchBytes, CascadeMetrics.noop());
  }

  public WALManagerImpl(
      String directory, long maxLogSizeBytes, long syncBatchBytes, CascadeMetrics metrics)
      throws IOException {
    this.directory = directory;
    this.maxLogSizeBytes = maxLogSizeBytes;
    this.syncBatchBytes = syncBatchBytes;
    this.metrics = metrics != null ? metrics : CascadeMetrics.noop();
    this.sequenceNumber = new AtomicLong(0);

    // Create directory if it doesn't exist
    Files.createDirectories(Paths.get(directory));

    // Initialize the current log file
    initializeCurrentLog();
    recoverSequenceCounter();

    logger.info("WALManager initialized in directory: " + directory);
  }

  private void initializeCurrentLog() throws IOException {
    // Find the latest log file
    List<Path> logFiles = findLogFiles();

    if (logFiles.isEmpty()) {
      // Create a new log file
      createNewFile();
    } else {
      // Use the latest log file
      Path latestLogPath = logFiles.get(logFiles.size() - 1);

      // Update sequence number based on the log file name
      String fileName = latestLogPath.getFileName().toString();
      try {
        long fileSeqNum = Long.parseLong(fileName.substring(4, fileName.indexOf(".log")));
        sequenceNumber.set(fileSeqNum);

        // Open the latest log file
        currentFile =
            new WALFileImpl(
                latestLogPath, fileSeqNum, StandardOpenOption.READ, StandardOpenOption.WRITE);
      } catch (NumberFormatException | IndexOutOfBoundsException e) {
        logger.warn("Could not parse sequence number from log file name: " + fileName, e);
        // Create a new log file as a fallback
        createNewFile();
      }
    }
  }

  @Override
  public WALFile getCurrentFile() {
    return currentFile;
  }

  @Override
  public WALFile createNewFile() throws IOException {
    if (currentFile != null) {
      sync();
      currentFile.close();
      currentFile = null;
    }

    long seqNum = sequenceNumber.getAndIncrement();
    Path newLogPath = Paths.get(directory, String.format("wal_%020d.log", seqNum));

    // Create a new file
    currentFile =
        new WALFileImpl(
            newLogPath,
            seqNum,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ);

    logger.info("Created new WAL file: " + newLogPath);

    return currentFile;
  }

  @Override
  public List<Path> findLogFiles() throws IOException {
    List<Path> logFiles = new ArrayList<>();

    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(Paths.get(directory), "wal_*.log")) {
      for (Path path : stream) {
        logFiles.add(path);
      }
    }

    // Sort by sequence number (extracted from file name)
    logFiles.sort(
        (p1, p2) -> {
          String name1 = p1.getFileName().toString();
          String name2 = p2.getFileName().toString();

          try {
            long seq1 = Long.parseLong(name1.substring(4, name1.indexOf(".log")));
            long seq2 = Long.parseLong(name2.substring(4, name2.indexOf(".log")));
            return Long.compare(seq1, seq2);
          } catch (NumberFormatException | IndexOutOfBoundsException e) {
            logger.warn("Error parsing sequence numbers from log file names", e);
            return name1.compareTo(name2);
          }
        });

    return logFiles;
  }

  @Override
  public void rotateLog() throws IOException {
    createNewFile();

    logger.info("Rotated WAL to new file");
  }

  @Override
  public void noteBytesWritten(int bytes) throws IOException {
    metrics.recordWalBytesWritten(bytes);
    bytesSinceLastSync += bytes;
    if (bytesSinceLastSync >= syncBatchBytes) {
      sync();
    }
  }

  @Override
  public void sync() throws IOException {
    long start = System.nanoTime();
    if (currentFile != null) {
      currentFile.force(true);
    }
    bytesSinceLastSync = 0;
    metrics.recordWalFsync(System.nanoTime() - start);
  }

  @Override
  public void deleteAllLogs() throws IOException {
    sync();

    if (currentFile != null) {
      currentFile.close();
      currentFile = null;
    }

    // Find all log files
    List<Path> logFiles = findLogFiles();

    // Delete each log file
    for (Path logPath : logFiles) {
      try {
        Files.delete(logPath);
        logger.info("Deleted WAL file: " + logPath);
      } catch (IOException e) {
        logger.warn("Failed to delete WAL file: " + logPath, e);
      }
    }

    // Create a new log file
    createNewFile();

    logger.info("Deleted all WAL files");
  }

  @Override
  public void purgeThrough(long maxSequenceInclusive) throws IOException {
    if (maxSequenceInclusive < 0) {
      return;
    }

    sync();

    Path currentPath = currentFile != null ? currentFile.getPath() : null;
    if (currentFile != null) {
      currentFile.close();
      currentFile = null;
    }

    WALReaderImpl reader = new WALReaderImpl(this);
    List<Path> logFiles = findLogFiles();
    for (Path logPath : logFiles) {
      List<Record> records = reader.readRecordsFromFile(logPath.toString());
      if (records.isEmpty()) {
        Files.deleteIfExists(logPath);
        continue;
      }

      long maxInFile =
          records.stream().mapToLong(Record::getSequenceNumber).max().orElse(maxSequenceInclusive);
      if (maxInFile <= maxSequenceInclusive) {
        Files.deleteIfExists(logPath);
        logger.info("Purged WAL file {}", logPath);
        continue;
      }

      List<Record> retained = new ArrayList<>();
      for (Record record : records) {
        if (record.getSequenceNumber() > maxSequenceInclusive) {
          retained.add(record);
        }
      }

      if (retained.size() == records.size()) {
        continue;
      }

      rewriteLogFile(logPath, retained);
      logger.info("Rewrote WAL file {} retaining {} records", logPath, retained.size());
    }

    if (currentPath != null && Files.exists(currentPath)) {
      currentFile =
          new WALFileImpl(
              currentPath,
              parseWalFileSequence(currentPath),
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
    } else {
      initializeCurrentLog();
    }

    recoverSequenceCounter();
  }

  @Override
  public long recoverSequenceCounter() throws IOException {
    WALReaderImpl reader = new WALReaderImpl(this);
    long maxSequence = -1;
    for (Path logPath : findLogFiles()) {
      for (Record record : reader.readRecordsFromFile(logPath.toString())) {
        maxSequence = Math.max(maxSequence, record.getSequenceNumber());
      }
    }

    long nextSequence = maxSequence + 1;
    sequenceNumber.updateAndGet(current -> Math.max(current, nextSequence));
    return maxSequence;
  }

  private void rewriteLogFile(Path logPath, List<Record> records) throws IOException {
    Path tempPath = Paths.get(logPath + ".rewrite");
    Files.deleteIfExists(tempPath);

    try (WALFile walFile =
        new WALFileImpl(
            tempPath,
            parseWalFileSequence(logPath),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE)) {
      for (Record record : records) {
        int size = WalRecordCodec.encodedSize(record);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        WalRecordCodec.encode(record, buffer);
        buffer.flip();
        walFile.write(buffer);
      }
      walFile.force(true);
    }

    try {
      Files.move(tempPath, logPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (java.nio.file.AtomicMoveNotSupportedException e) {
      Files.move(tempPath, logPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static long parseWalFileSequence(Path logPath) {
    String fileName = logPath.getFileName().toString();
    return Long.parseLong(fileName.substring(4, fileName.indexOf(".log")));
  }

  long getSyncBatchBytes() {
    return syncBatchBytes;
  }

  @Override
  public String getDirectory() {
    return directory;
  }

  @Override
  public long getMaxLogSizeBytes() {
    return maxLogSizeBytes;
  }

  @Override
  public long getNextSequenceNumber() {
    return sequenceNumber.getAndIncrement();
  }
}
