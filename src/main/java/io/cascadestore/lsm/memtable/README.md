# MemTable Package

The MemTable (`io.cascadestore.lsm.memtable`) is the hot write buffer in the LSM tree. Recent puts and deletes live here in key order until the table fills and flushes to an SSTable.

## MemTable

`MemTable` indexes keys in a `ConcurrentSkipListMap<ByteArrayWrapper, ValueEntry>` so concurrent writers and readers do not contend on a single lock. Payload bytes sit off-heap in direct `ByteBuffer` regions allocated through `DirectBufferAllocator`.

Capabilities:

- Sorted iteration and range views
- TTL via per-entry expiration timestamps
- Tombstone flag for deletes (value length zero, tombstone bit set)
- `AtomicLong` byte accounting for size-based rotation

### Lifecycle

| State | Behavior |
|-------|----------|
| **Active** | Accepts puts and deletes |
| **Immutable** | Read-only (`volatile boolean`); queued for flush |

When estimated size exceeds `memTableMaxSizeBytes` (default 10 MiB in `CascadeConfig`), `CascadeStore` marks the table immutable, syncs the WAL, allocates a fresh active MemTable, and schedules `FlushService` to write a level-0 SSTable.

## ValueEntry Layout

Each value is stored in a direct buffer with a fixed 16-byte header:

```
Offset  0–7   (8 bytes): expirationTime (long) — 0 means no TTL
Offset  8     (1 byte):  tombstone — 0x01 deleted, 0x00 live
Offset  9–11  (3 bytes): padding
Offset 12–15  (4 bytes): valueLength (int) — 0 for tombstones
Offset 16+    (N bytes): value bytes
```

Header fields use absolute `ByteBuffer` accessors (`putLong(offset, …)`, `getInt(offset, …)`), so concurrent readers can inspect entries without extra synchronization.

## DirectBufferAllocator

Off-heap allocation is centralized in `io.cascadestore.lsm.memory.DirectBufferAllocator`:

- Allocates with `ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder())`
- Tracks buffers for deterministic release on `close()`
- Prefers `sun.misc.Unsafe.invokeCleaner(buffer)`; falls back to GC if Unsafe is unavailable

Keeping values off-heap reduces young-generation churn when values are large or write throughput is high.

## Role in the LSM Tree

The MemTable is the fastest read layer (after a miss, lookups fall through to immutable MemTables and SSTables). Its sorted structure also feeds flush and compaction: entries are written sequentially to the `.data` file and sampled into the sparse index during SSTable creation.
