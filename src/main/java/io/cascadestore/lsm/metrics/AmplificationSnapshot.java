package io.cascadestore.lsm.metrics;

/** Point-in-time counters for read/write/space amplification reporting. */
public record AmplificationSnapshot(
    long readOperations,
    long sstableLookups,
    long bloomProbes,
    long bloomNegatives,
    long userWriteBytes,
    long sstableBytesWritten,
    long compactions,
    long liveSstableDataBytes,
    long liveSstableCount) {

  public static final AmplificationSnapshot EMPTY =
      new AmplificationSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0);

  /**
   * Bloom-positive SSTable probes per read. This is the classic "files checked per read" metric and
   * diverges across strategies when live SSTable counts differ.
   */
  public double readAmplification() {
    return ratio(sstableLookups, readOperations);
  }

  /**
   * Bloom filter evaluations per read (includes tables skipped without a data lookup). Use this
   * when strategies converge to similar lookup counts but retain different file counts.
   */
  public double filesProbedAmplification() {
    return ratio(bloomProbes, readOperations);
  }

  public double bloomSkipRate() {
    return ratio(bloomNegatives, bloomProbes);
  }

  public double writeAmplification() {
    return ratio(sstableBytesWritten, userWriteBytes);
  }

  /** Live SSTable data bytes divided by cumulative user write bytes. */
  public double spaceAmplification() {
    return ratio(liveSstableDataBytes, userWriteBytes);
  }

  public AmplificationSnapshot plus(AmplificationSnapshot other) {
    if (other == null) {
      return this;
    }
    return new AmplificationSnapshot(
        readOperations + other.readOperations,
        sstableLookups + other.sstableLookups,
        bloomProbes + other.bloomProbes,
        bloomNegatives + other.bloomNegatives,
        userWriteBytes + other.userWriteBytes,
        sstableBytesWritten + other.sstableBytesWritten,
        compactions + other.compactions,
        liveSstableDataBytes + other.liveSstableDataBytes,
        liveSstableCount + other.liveSstableCount);
  }

  private static double ratio(long numerator, long denominator) {
    if (denominator <= 0) {
      return 0.0;
    }
    return (double) numerator / denominator;
  }
}
