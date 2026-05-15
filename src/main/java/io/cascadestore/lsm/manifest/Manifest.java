package io.cascadestore.lsm.manifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** On-disk checkpoint describing durable SSTables and the WAL replay frontier. */
public final class Manifest {

  public static final int CURRENT_VERSION = 1;

  private final int version;
  private final long flushedWalSequence;
  private final List<SSTableRef> sstables;

  public Manifest(long flushedWalSequence, List<SSTableRef> sstables) {
    this(CURRENT_VERSION, flushedWalSequence, sstables);
  }

  public Manifest(int version, long flushedWalSequence, List<SSTableRef> sstables) {
    if (version <= 0) {
      throw new IllegalArgumentException("version must be positive");
    }
    if (flushedWalSequence < -1) {
      throw new IllegalArgumentException("flushedWalSequence must be >= -1");
    }
    this.version = version;
    this.flushedWalSequence = flushedWalSequence;
    this.sstables = List.copyOf(new ArrayList<>(sstables));
  }

  public int version() {
    return version;
  }

  public long flushedWalSequence() {
    return flushedWalSequence;
  }

  public List<SSTableRef> sstables() {
    return sstables;
  }

  public record SSTableRef(int level, long sequenceNumber) {
    public SSTableRef {
      if (level < 0) {
        throw new IllegalArgumentException("level must be non-negative");
      }
      if (sequenceNumber < 0) {
        throw new IllegalArgumentException("sequenceNumber must be non-negative");
      }
    }

    public String fileBaseName() {
      return String.format("sst_L%d_S%d", level, sequenceNumber);
    }
  }

  public static Manifest empty() {
    return new Manifest(-1, Collections.emptyList());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Manifest manifest)) {
      return false;
    }
    return version == manifest.version
        && flushedWalSequence == manifest.flushedWalSequence
        && sstables.equals(manifest.sstables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, flushedWalSequence, sstables);
  }
}
