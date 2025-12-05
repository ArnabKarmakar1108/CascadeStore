package io.cascadestore.lsm.wal.manager;

import io.cascadestore.lsm.wal.file.WALFile;
import io.cascadestore.lsm.wal.file.WALFileImpl;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  public WALManagerImpl(String directory) throws IOException {
    this(directory, 64 * 1024 * 1024); // Default 64MB max log size
  }

  public WALManagerImpl(String directory, long maxLogSizeBytes) throws IOException {
    this.directory = directory;
    this.maxLogSizeBytes = maxLogSizeBytes;
    this.sequenceNumber = new AtomicLong(0);

    // Create directory if it doesn't exist
    Files.createDirectories(Paths.get(directory));

    // Initialize the current log file
    initializeCurrentLog();

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
    long seqNum = sequenceNumber.getAndIncrement();
    Path newLogPath = Paths.get(directory, String.format("wal_%020d.log", seqNum));

    // Close the current file if it exists
    if (currentFile != null) {
      currentFile.close();
    }

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
    // Create a new log file
    createNewFile();

    logger.info("Rotated WAL to new file");
  }

  @Override
  public void deleteAllLogs() throws IOException {
    // Close the current log file
    if (currentFile != null) {
      currentFile.close();
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
