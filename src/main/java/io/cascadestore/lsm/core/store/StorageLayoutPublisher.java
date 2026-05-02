package io.cascadestore.lsm.core.store;

/** Publishes a new {@link StorageVersion} after the SSTable / immutable-memtable layout changes. */
@FunctionalInterface
public interface StorageLayoutPublisher {
  void publishStorageLayout();
}
