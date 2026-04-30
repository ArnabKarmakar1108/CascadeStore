package io.cascadestore.lsm.sstable.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class SparseIndexTest {

  @Test
  void floorOffsetFindsGreatestIndexedKeyLessThanOrEqualToTarget() {
    TreeMap<ByteArrayWrapper, Long> entries = new TreeMap<>();
    entries.put(new ByteArrayWrapper("a".getBytes()), 16L);
    entries.put(new ByteArrayWrapper("m".getBytes()), 100L);
    entries.put(new ByteArrayWrapper("z".getBytes()), 200L);
    SparseIndex index = SparseIndex.from(entries);

    assertEquals(16L, index.floorOffset("a".getBytes()));
    assertEquals(16L, index.floorOffset("b".getBytes()));
    assertEquals(100L, index.floorOffset("m".getBytes()));
    assertEquals(100L, index.floorOffset("n".getBytes()));
    assertEquals(200L, index.floorOffset("z".getBytes()));
    assertEquals(-1L, index.floorOffset("0".getBytes()));
  }

  @Test
  void preservesSortedOrderFromTreeMap() {
    TreeMap<ByteArrayWrapper, Long> entries = new TreeMap<>();
    entries.put(new ByteArrayWrapper("key1".getBytes()), 16L);
    entries.put(new ByteArrayWrapper("key2".getBytes()), 32L);
    entries.put(new ByteArrayWrapper("key3".getBytes()), 48L);
    SparseIndex index = SparseIndex.from(entries);

    assertEquals(3, index.size());
    assertEquals("key1", new String(index.minKey()));
    assertEquals("key3", new String(index.maxKey()));
    assertTrue(index.floorOffset("key2".getBytes()) >= 16L);
  }
}
