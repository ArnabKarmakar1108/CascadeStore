package io.cascadestore.lsm.sstable.index;

import io.cascadestore.lsm.api.ByteArrayWrapper;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Immutable sorted sparse index for SSTable lookups. Uses binary search instead of {@link
 * java.util.TreeMap#floorEntry(Object)} to resolve the nearest data-file offset ≤ a key.
 */
public final class SparseIndex {

  private final byte[][] keys;
  private final long[] offsets;

  private SparseIndex(byte[][] keys, long[] offsets) {
    this.keys = keys;
    this.offsets = offsets;
  }

  public static SparseIndex empty() {
    return new SparseIndex(new byte[0][], new long[0]);
  }

  public static SparseIndex from(NavigableMap<ByteArrayWrapper, Long> entries) {
    if (entries == null || entries.isEmpty()) {
      return empty();
    }
    int size = entries.size();
    byte[][] keys = new byte[size][];
    long[] offsets = new long[size];
    int index = 0;
    for (Map.Entry<ByteArrayWrapper, Long> entry : entries.entrySet()) {
      keys[index] = entry.getKey().getData();
      offsets[index] = entry.getValue();
      index++;
    }
    return new SparseIndex(keys, offsets);
  }

  public boolean isEmpty() {
    return keys.length == 0;
  }

  public int size() {
    return keys.length;
  }

  public byte[] keyAt(int index) {
    return keys[index];
  }

  public long offsetAt(int index) {
    return offsets[index];
  }

  public byte[] minKey() {
    return keys.length == 0 ? null : keys[0];
  }

  public byte[] maxKey() {
    return keys.length == 0 ? null : keys[keys.length - 1];
  }

  /**
   * Returns the data-file offset for the greatest indexed key ≤ {@code key}, or {@code -1} when
   * every indexed key is greater than {@code key}.
   */
  public long floorOffset(byte[] key) {
    if (key == null || keys.length == 0) {
      return -1;
    }

    int lo = 0;
    int hi = keys.length - 1;
    int floor = -1;

    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int cmp = compare(keys[mid], key);
      if (cmp <= 0) {
        floor = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }

    return floor >= 0 ? offsets[floor] : -1;
  }

  private static int compare(byte[] left, byte[] right) {
    int length = Math.min(left.length, right.length);
    for (int i = 0; i < length; i++) {
      int a = left[i] & 0xFF;
      int b = right[i] & 0xFF;
      if (a != b) {
        return a - b;
      }
    }
    return left.length - right.length;
  }
}
