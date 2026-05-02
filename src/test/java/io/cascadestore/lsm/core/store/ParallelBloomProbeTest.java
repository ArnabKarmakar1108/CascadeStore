package io.cascadestore.lsm.core.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.when;

import io.cascadestore.lsm.sstable.SSTable;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ParallelBloomProbeTest {

  @Test
  void serialAndParallelProduceSameCandidates() {
    byte[] key = "probe-key".getBytes();
    List<SSTable> tables =
        List.of(
            mockTable(key, true),
            mockTable(key, false),
            mockTable(key, true),
            mockTable(key, false),
            mockTable(key, true),
            mockTable(key, true));

    boolean[] serial = BloomProbe.probeCandidates(tables, key, false, 4);
    boolean[] parallel = BloomProbe.probeCandidates(tables, key, true, 4);

    assertArrayEquals(serial, parallel);
    assertArrayEquals(new boolean[] {true, false, true, false, true, true}, serial);
  }

  @Test
  void parallelDisabledWhenBelowMinTables() {
    byte[] key = "k".getBytes();
    List<SSTable> tables = List.of(mockTable(key, true), mockTable(key, false));

    boolean[] result = BloomProbe.probeCandidates(tables, key, true, 4);
    assertArrayEquals(new boolean[] {true, false}, result);
  }

  private static SSTable mockTable(byte[] key, boolean mightContain) {
    SSTable table = Mockito.mock(SSTable.class);
    when(table.mightContain(key)).thenReturn(mightContain);
    return table;
  }
}
