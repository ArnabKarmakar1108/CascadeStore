package io.cascadestore.lsm.core.store;

import io.cascadestore.lsm.sstable.SSTable;
import java.util.List;
import java.util.stream.IntStream;

/** CPU-only bloom-filter candidate selection over an SSTable snapshot. */
public final class BloomProbe {

  private BloomProbe() {}

  /**
   * Returns a per-table bloom result aligned with {@code ssTables} index order (newest-first list).
   */
  public static boolean[] probeCandidates(
      List<SSTable> ssTables, byte[] key, boolean parallelEnabled, int parallelMinTables) {
    int tableCount = ssTables.size();
    boolean[] candidates = new boolean[tableCount];
    if (tableCount == 0) {
      return candidates;
    }

    if (parallelEnabled && tableCount >= parallelMinTables) {
      IntStream.range(0, tableCount)
          .parallel()
          .forEach(i -> candidates[i] = ssTables.get(i).mightContain(key));
    } else {
      for (int i = 0; i < tableCount; i++) {
        candidates[i] = ssTables.get(i).mightContain(key);
      }
    }
    return candidates;
  }
}
