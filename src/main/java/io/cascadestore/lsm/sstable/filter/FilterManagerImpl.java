package io.cascadestore.lsm.sstable.filter;

import io.cascadestore.lsm.sstable.BloomFilter;
import io.cascadestore.lsm.sstable.io.SSTableIO;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterManagerImpl implements FilterManager {
  private static final Logger logger = LoggerFactory.getLogger(FilterManagerImpl.class);

  private final SSTableIO io;
  private BloomFilter filter;

  public FilterManagerImpl(SSTableIO io) {
    this.io = io;
    this.filter = new BloomFilter(1000, 0.01); // Default filter
  }

  public FilterManagerImpl(SSTableIO io, int expectedEntries) {
    this.io = io;
    this.filter = new BloomFilter(expectedEntries, 0.01);
  }

  @Override
  public Path getFilterFilePath() {
    return Path.of(
        io.getDirectory(),
        String.format("sst_L%d_S%d.filter", io.getLevel(), io.getSequenceNumber()));
  }

  @Override
  public void add(byte[] key) {
    filter.add(key);
  }

  @Override
  public boolean mightContain(byte[] key) {
    return filter.mightContain(key);
  }

  @Override
  public void save() throws IOException {
    filter.save(getFilterFilePath().toString());
  }

  @Override
  public void load() throws IOException {
    try {
      filter = BloomFilter.load(getFilterFilePath().toString());
    } catch (IOException e) {
      logger.warn("Error loading Bloom filter, creating a new one", e);
      filter = new BloomFilter(1000, 0.01); // Default filter
    }
  }

  @Override
  public void create(int expectedEntries, double falsePositiveRate) {
    filter = new BloomFilter(expectedEntries, falsePositiveRate);
  }
}
