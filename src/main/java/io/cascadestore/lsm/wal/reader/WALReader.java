package io.cascadestore.lsm.wal.reader;

import io.cascadestore.lsm.wal.record.Record;
import java.io.IOException;
import java.util.List;

public interface WALReader {

  List<Record> readRecords() throws IOException;

  List<Record> readRecordsFromFile(String filePath) throws IOException;
}
