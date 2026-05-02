package io.cascadestore.lsm.api;

/** Applies a read-modify-write transformation to an existing stored value. */
@FunctionalInterface
public interface ValueMerger {

  /**
   * Returns the new value bytes to store, or {@code null} to abort the merge (treated as key not
   * found).
   */
  byte[] merge(byte[] existingValue);
}
