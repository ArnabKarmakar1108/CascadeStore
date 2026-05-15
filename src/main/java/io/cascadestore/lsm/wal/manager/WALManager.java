package io.cascadestore.lsm.wal.manager;

import io.cascadestore.lsm.wal.file.WALFile;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface WALManager {

  WALFile getCurrentFile();

  WALFile createNewFile() throws IOException;

  List<Path> findLogFiles() throws IOException;

  void rotateLog() throws IOException;

  void deleteAllLogs() throws IOException;

  void purgeThrough(long maxSequenceInclusive) throws IOException;

  long recoverSequenceCounter() throws IOException;

  String getDirectory();

  long getMaxLogSizeBytes();

  long getNextSequenceNumber();

  void noteBytesWritten(int bytes) throws IOException;

  void sync() throws IOException;
}
