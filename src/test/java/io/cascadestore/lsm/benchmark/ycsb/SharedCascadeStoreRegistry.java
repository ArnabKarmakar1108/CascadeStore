package io.cascadestore.lsm.benchmark.ycsb;

import io.cascadestore.lsm.core.store.CascadeStore;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Reference-counted {@link CascadeStore} instances keyed by benchmark configuration.
 *
 * YCSB creates one {@link CascadeStoreYcsbClient} per thread; this registry ensures all
 * threads in a run share a single embedded store for the same datadir/config.
 */
final class SharedCascadeStoreRegistry {

  private static final ConcurrentHashMap<String, SharedStore> STORES = new ConcurrentHashMap<>();

  private SharedCascadeStoreRegistry() {}

  static boolean isOpen(String key) {
    return STORES.containsKey(key);
  }

  static CascadeStore acquire(String key, Supplier<CascadeStore> creator) {
    while (true) {
      SharedStore existing = STORES.get(key);
      if (existing != null) {
        existing.refCount.incrementAndGet();
        return existing.store;
      }

      CascadeStore created = creator.get();
      SharedStore candidate = new SharedStore(created);
      SharedStore winner = STORES.putIfAbsent(key, candidate);
      if (winner == null) {
        return created;
      }

      created.shutdown();
    }
  }

  static void release(String key) {
    SharedStore shared = STORES.get(key);
    if (shared == null) {
      return;
    }

    if (shared.refCount.decrementAndGet() == 0) {
      STORES.remove(key, shared);
      shared.store.shutdown();
    }
  }

  static void resetForTests() {
    for (SharedStore shared : STORES.values()) {
      shared.store.shutdown();
    }
    STORES.clear();
  }

  private static final class SharedStore {
    private final CascadeStore store;
    private final AtomicInteger refCount = new AtomicInteger(1);

    private SharedStore(CascadeStore store) {
      this.store = store;
    }
  }
}
