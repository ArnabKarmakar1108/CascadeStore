package io.cascadestore.lsm.benchmark.ycsb;

import io.cascadestore.lsm.metrics.AmplificationSnapshot;
import java.io.PrintStream;

/** Emits YCSB-parseable amplification lines captured by benchmark result files. */
final class YcsbAmplificationReporter {

  private YcsbAmplificationReporter() {}

  static void printSnapshot(PrintStream out, AmplificationSnapshot snapshot) {
    if (snapshot == null) {
      return;
    }
    printCounter(out, "read_operations_total", snapshot.readOperations());
    printCounter(out, "sstable_lookups_total", snapshot.sstableLookups());
    printCounter(out, "bloom_probes_total", snapshot.bloomProbes());
    printCounter(out, "bloom_negatives_total", snapshot.bloomNegatives());
    printCounter(out, "user_write_bytes_total", snapshot.userWriteBytes());
    printCounter(out, "sstable_bytes_written_total", snapshot.sstableBytesWritten());
    printCounter(out, "compaction_total", snapshot.compactions());
    printCounter(out, "live_sstable_data_bytes", snapshot.liveSstableDataBytes());
    printCounter(out, "live_sstable_count", snapshot.liveSstableCount());
    printRatio(out, "read_amplification", snapshot.readAmplification());
    printRatio(out, "files_probed_amplification", snapshot.filesProbedAmplification());
    printRatio(out, "write_amplification", snapshot.writeAmplification());
    printRatio(out, "space_amplification", snapshot.spaceAmplification());
  }

  private static void printCounter(PrintStream out, String name, long value) {
    out.printf("[CASCADE_METRICS], %s, %d%n", name, value);
  }

  private static void printRatio(PrintStream out, String name, double value) {
    out.printf("[CASCADE_METRICS], %s, %.6f%n", name, value);
  }
}
