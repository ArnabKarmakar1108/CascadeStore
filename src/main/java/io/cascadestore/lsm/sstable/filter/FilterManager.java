package io.cascadestore.lsm.sstable.filter;

import java.io.IOException;
import java.nio.file.Path;

public interface FilterManager {

  Path getFilterFilePath();

  void add(byte[] key);

  boolean mightContain(byte[] key);

  void save() throws IOException;

  void load() throws IOException;

  void create(int expectedEntries, double falsePositiveRate);
}
