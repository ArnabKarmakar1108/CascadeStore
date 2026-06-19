package io.cascadestore.lsm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AmplificationSnapshotTest {

  @Test
  void computesAmplificationRatios() {
    AmplificationSnapshot snapshot =
        new AmplificationSnapshot(100, 250, 400, 150, 1_000, 3_000, 5, 2_500, 8);

    assertEquals(2.5, snapshot.readAmplification(), 0.0001);
    assertEquals(4.0, snapshot.filesProbedAmplification(), 0.0001);
    assertEquals(0.375, snapshot.bloomSkipRate(), 0.0001);
    assertEquals(3.0, snapshot.writeAmplification(), 0.0001);
    assertEquals(2.5, snapshot.spaceAmplification(), 0.0001);
  }

  @Test
  void plusAggregatesCountersAcrossShards() {
    AmplificationSnapshot a = new AmplificationSnapshot(10, 20, 30, 10, 100, 200, 1, 50, 3);
    AmplificationSnapshot b = new AmplificationSnapshot(5, 10, 15, 5, 50, 100, 2, 25, 2);
    AmplificationSnapshot total = a.plus(b);

    assertEquals(15, total.readOperations());
    assertEquals(30, total.sstableLookups());
    assertEquals(45, total.bloomProbes());
    assertEquals(15, total.bloomNegatives());
    assertEquals(150, total.userWriteBytes());
    assertEquals(300, total.sstableBytesWritten());
    assertEquals(3, total.compactions());
    assertEquals(75, total.liveSstableDataBytes());
    assertEquals(5, total.liveSstableCount());
    assertEquals(2.0, total.readAmplification(), 0.0001);
    assertEquals(3.0, total.filesProbedAmplification(), 0.0001);
  }
}
