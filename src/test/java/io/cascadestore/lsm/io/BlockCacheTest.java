package io.cascadestore.lsm.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class BlockCacheTest {

  @Test
  void storesAndReturnsBlockCopies() {
    BlockCache cache = BlockCache.create(64 * 1024);
    byte[] block = new byte[1024];
    block[0] = 42;

    cache.put(1L, 0L, block);
    byte[] hit = cache.get(1L, 0L);
    assertNotNull(hit);
    assertEquals(42, hit[0]);

    block[0] = 99;
    assertEquals(42, cache.get(1L, 0L)[0]);
  }

  @Test
  void evictsOldestWhenOverCapacity() {
    BlockCache cache = BlockCache.create(3000);
    cache.put(1L, 0L, new byte[1000]);
    cache.put(1L, 1000L, new byte[1000]);
    cache.put(1L, 2000L, new byte[1000]);
    assertEquals(3, cache.entryCountForTest());

    cache.put(1L, 3000L, new byte[1000]);
    assertEquals(3, cache.entryCountForTest());
    assertNull(cache.get(1L, 0L));
    assertNotNull(cache.get(1L, 3000L));
  }

  @Test
  void invalidateSstableDropsAllBlocksForTable() {
    BlockCache cache = BlockCache.create(64 * 1024);
    cache.put(7L, 0L, new byte[512]);
    cache.put(7L, 512L, new byte[512]);
    cache.put(8L, 0L, new byte[512]);

    cache.invalidateSstable(7L);

    assertNull(cache.get(7L, 0L));
    assertNull(cache.get(7L, 512L));
    assertNotNull(cache.get(8L, 0L));
    assertEquals(512, cache.sizeBytesForTest());
  }
}
