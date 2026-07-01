# CascadeStore — Performance & Correctness Optimizations

This document records **problems observed during benchmarking and development**, **why they hurt**, and **how each was fixed**. It complements `ARCHITECTURE.md` (reference) and `DATA_FLOW.md` (walkthrough).

Optimizations are grouped by subsystem. Several fixes address both throughput and correctness.

---

## 1. WAL group commit (batched `fsync`)

### Problem

Every `put` and `delete` called `FileChannel.force(true)` immediately after appending to the WAL. On Workload A at 1M scale this dominated latency: each YCSB update is a read + WAL append + memtable write, and a per-write `fsync` caps throughput at tens of ops/s regardless of CPU.

### Symptom

- 1M YCSB run phase stuck at ~15–50 ops/s with WAL replay on restart taking hours when the log was huge.
- `iostat` showed sustained fsync wait; CPU was mostly idle.

### Root cause

`WALWriterImpl` synced after every record. YCSB ~1 KB values mean thousands of syscalls per second per shard.

### Fix

**Group commit** via `WalSyncPolicy` and `WALManagerImpl.noteBytesWritten()`:

| Component | Role |
| --- | --- |
| `WalSyncPolicy.DEFAULT_SYNC_BATCH_BYTES` | `1 MiB` — fsync after this many bytes appended since last sync |
| `WALManagerImpl.noteBytesWritten(n)` | Accumulates bytes; calls `sync()` when batch threshold reached |
| `WALManagerImpl.sync()` | `force(true)` + reset counter |

**Forced sync still happens** at safety boundaries (not batched away):

- WAL file rotation (`createNewFile` / `rotateLog`)
- Memtable switch (`CascadeStore.switchMemTable` → `wal.sync()`)
- WAL truncation (`deleteAllLogs`)
- Store shutdown (`wal.close()`)

### Impact

Removes the per-write fsync bottleneck on the update path. Combined with other fixes, run-phase throughput improved from unusable (~50 ops/s) to benchmark-viable ranges on large datasets.

**See also:** `DATA_FLOW.md` §3.3, `ARCHITECTURE.md` §4.

---

## 2. Sparse SSTable index (16 KiB blocks)

### Problem

The flush loop indexed **every key** into an in-memory `TreeMap<ByteArrayWrapper, Long>`. At 1M keys with ~1 KB values, a single L0 SSTable held ~915k index entries — hundreds of MB of heap for the index alone, plus slow `floorEntry` and GC pressure.

### Symptom

- 1M benchmark JVM OOM or severe GC pauses with `-Xmx2G`.
- Index files grew linearly with key count (~20 bytes × N keys).

### Root cause

Dense indexing: one map entry per flushed key.

### Fix

`SparseIndexPolicy` indexes one key every **16 KiB** of `.data` file bytes, plus **always** the last key in the table:

```java
// SparseIndexPolicy.INDEX_BLOCK_SIZE_BYTES = 16 * 1024
public static boolean shouldAddIndexEntry(long entryOffset, long lastIndexedOffset) {
  return lastIndexedOffset < 0
      || entryOffset - lastIndexedOffset >= INDEX_BLOCK_SIZE_BYTES;
}
```

During flush, `SSTable` calls this when building `sparseIndex` and when writing `.index`. `countEntries()` comes from the data-file header instead of `sparseIndex.size()`.

Lookups still use `floorEntry(key)` then a **short forward scan** in the data file (typically a handful of records per ~1 KB YCSB value).

### Impact

- Index memory and file size shrink by ~10–20× for large tables.
- 100k validation: ~80 KB index per ~29 MB L0 file (vs. multi-MB dense index at 1M).
- Small extra CPU per lookup (scan within one index block); acceptable trade-off.

**See also:** `DATA_FLOW.md` §5.1, §6; `ARCHITECTURE.md` §5.

---

## 3. Buffered SSTable reads

### Problem

`SSTable.findKeyInDataFile` issued **3–4 `FileChannel.read()` calls per record** scanned (key length, key, value length, value). A sparse-index hit that scans ~10 records in a 1M-row L1 file could trigger **30–40 syscalls per lookup**.

### Symptom

High read latency on SSTable-heavy paths; single-thread 1M run phase CPU low but throughput poor.

### Root cause

Positional reads without read-ahead; syscall overhead dominates over memcpy.

### Fix

`BufferedDataReader` (`io.cascadestore.lsm.io`):

- Default **64 KiB** sliding window over the data `FileChannel`
- `seek(position)` refills window when needed
- `readInt()`, `readBytes(n)`, `skip(n)` parse from the window

`SSTable` uses a **`ThreadLocal<BufferedDataReader>`** per open table so concurrent readers on the same SSTable do not share buffer cursor state. `findKeyInDataFile`, `listKeys`, and `getRange` all scan via the buffered reader.

### Impact

Syscalls drop to ~1 per 64 KiB of sequential scan instead of per field. Expected **~1.5–3×** improvement on read-heavy phases; meaningful on 1M L1 lookups.

**See also:** `DATA_FLOW.md` §6; `ARCHITECTURE.md` §5.

---

## 4. Reusable read buffer growth (`ReadBuffers`)

### Problem

Several code paths allocated a new `ByteBuffer` on every grow during SSTable/WAL scans (`buffer = ByteBuffer.allocate(required)` in a loop).

### Symptom

Allocation churn and GC pressure during compaction, WAL replay, and index load.

### Fix

`ReadBuffers.ensureCapacity(buffer, required)` grows a reusable buffer in place when possible.

### Impact

Reduces short-lived allocations on metadata scans. Foundation for `BufferedDataReader`.

---

## 5. Flush visibility (no “claimed but not on disk” gap)

### Problem

`FlushService.claimImmutableMemTables()` **cleared** the immutable list before flushing. During flush, keys existed only in the claimed memtable — not in `immutableMemTables`, not yet in `ssTables`. Concurrent `get()` could return `NOT_FOUND` for keys mid-flush.

### Symptom

~0.02% `NOT_FOUND` on 100k YCSB (4 threads) — 22 errors out of 100k ops. Single-thread rerun had zero errors.

### Root cause

Visibility gap between removing a memtable from the read path and publishing the SSTable.

### Fix

1. **Do not remove** memtables from `immutableMemTables` until flush succeeds.
2. **Atomically publish** under nested locks:

```java
synchronized (immutableMemTables) {
  synchronized (ssTables) {
    ssTables.add(ssTable);
    immutableMemTables.remove(memTable);
  }
}
```

3. **`synchronized (flushMonitor)`** on `doExecute()` so two concurrent `executeNow()` calls cannot double-flush the same table.

### Impact

Keys remain visible in the immutable layer until the SSTable is searchable. Eliminates flush-window `NOT_FOUND`s.

**See also:** `DATA_FLOW.md` §5; `ARCHITECTURE.md` §7.

---

## 6. Unified read path (`GetStore.lookup`)

### Problem

Two separate bugs in concurrent reads:

**A. Split memtable / SSTable search** — `getFromMemTables()` released `memTableLock` before checking `ssTables`. A flush could move data between layers in that window.

**B. Shared `retrievedValue` field** — `GetStore` stored the result in an instance field; YCSB threads overwrote each other's values between `get()` returning `SUCCESS` and `CascadeStore` reading the bytes. This caused **false `NOT_FOUND`** (~5% error rate) independent of flush timing.

### Symptom

- Multi-threaded YCSB: `READ Return=NOT_FOUND` and `UPDATE Return=NOT_FOUND` with all keys loaded.
- `GetStoreConcurrencyTest` showed thousands of misses under concurrent readers.

### Fix

1. **`lookup(byte[] key)`** returns the value directly (or `null`) — no shared mutable result slot.
2. **Single critical section** under `memTableLock.readLock()`:
   - Read `activeMemTable`
   - Then `synchronized (immutableMemTables) { synchronized (ssTables) { ... } }`
3. **`volatile MemTable activeMemTable`** in `GetStore`, `PutStore`, `DeleteStore` for safe publication after memtable switch.

`CascadeStore.get()` is now `return getStore.lookup(key)`.

### Impact

100k validation: **zero errors** at 4 shards × 4 threads (~17.8k ops/s run phase).

**See also:** `DATA_FLOW.md` §6; `ARCHITECTURE.md` §8.

---

## 7. WAL truncation after durable flush

### Problem

After loading 1M keys, restart replayed the entire WAL because every write was still in the log — even when all data had been flushed to SSTables. Startup took hours.

### Symptom

`recover()` logged hundreds of thousands of records on run start; throughput collapsed.

### Fix

`CascadeStore.truncateWalIfAllDataFlushed()`:

- Called from `FlushService` after a successful flush batch (via hook)
- Called on `shutdown()` after final flush
- Preconditions: no immutable memtables, active memtable empty
- Then `wal.sync()` + `wal.deleteAllLogs()`

Run phase then logs `No WAL records to recover`.

### Impact

Load → run handoff is fast; recovery only replays data not yet in SSTables.

**See also:** `DATA_FLOW.md` §8.

---

## 8. YCSB client sharding (multi-core without store-wide locking)

### Problem

`SharedCascadeStoreRegistry` gave every YCSB thread one shared `CascadeStore`. `THREADS>1` increased lock contention on WAL, `ssTables`, and `GetStore` without adding parallelism.

### Symptom

Single-thread baseline ~5k ops/s at 100k; 4 threads on one store did not scale (and, before §6, produced errors).

### Fix

YCSB binding property **`cascadestore.shards`** (default `1`):

| Piece | Behavior |
| --- | --- |
| `CascadeStoreShardRouter` | `shard = hash(storageKey) % shardCount` |
| Datadir layout | `<datadir>/shard-0` … `shard-N-1` |
| Registry | One reference-counted `CascadeStore` per shard |
| `scan` | K-way merge across shard iterators |

Recommended: **`THREADS == SHARDS`** (e.g. 4 and 4) so each thread mostly hits a different LSM tree.

### Impact

100k validation: **~17.8k ops/s** run (4×4) vs **~5.6k** single-thread baseline — near-linear scaling when I/O keeps up.

**See also:** `DATA_FLOW.md` §13; `ARCHITECTURE.md` §11.

---

## 9. Flush integrity (P1 — rollback on failure)

### Problem

Failed flush could leave a sealed memtable removed from the immutable list without a corresponding SSTable, or a partial SSTable on disk.

### Fix

- `FlushService` requeues memtable on `IOException`; deletes partial SSTable files via `SSTable.deleteFiles`
- `FlushService.claimImmutableMemTables` replaced by snapshot + skip-if-already-flushed (see §5)
- `SSTable` constructor rolls back files on flush failure
- `FlushServiceTest.concurrentFlushClaimsMemTableOnce` guards double-flush

---

## 10. Bloom filter rebuild when `.filter` missing

### Problem

Opening an SSTable without a `.filter` file left `mightContain` unusable or forced full scans.

### Fix

`SSTable.loadFromDisk()` rebuilds an in-memory bloom from the data file when `.filter` is absent (logged as warning).

---

## 11. Benchmark operations lessons

| Lesson | Action |
| --- | --- |
| Stale JAR | `scripts/ycsb-env.sh` `ensure_built()` only builds if test-jar missing — run `mvn -DskipTests package` after code changes |
| 1M heap | Use `JAVA_TOOL_OPTIONS="-Xms4G -Xmx8G"` with sparse index; 2G is tight at 1M |
| Correctness before scale | Validate 100k with `SHARDS=4 THREADS=4` and confirm zero `NOT_FOUND` before 1M |
| Single-thread control | `THREADS=1 SHARDS=1` remains the apples-to-apples strategy comparison baseline |

---

---

## 13. Version snapshots (lock-free reads after publish)

### Problem

Before snapshots, `GetStore.lookup()` held `memTableLock.readLock()` for the **entire** lookup, including `SSTable.get()` disk I/O. `switchMemTable()` needs the **write** lock, so all readers blocked during memtable rotation. Nested `synchronized (immutableMemTables)` and `synchronized (ssTables)` were also held across disk reads.

At 1M scale with 4 YCSB threads per shard, this produced **~124 ms p99** read latency and run throughput stuck around **1.3k ops/s** despite sharding.

### Symptom

- Run phase ~20× slower than 250k at the same thread count.
- Huge gap between p95 (~200 µs) and p99 (~125 ms) — classic lock + I/O stall tail.

### Fix

**`StorageVersion`** — immutable snapshot of immutable memtables + SSTable list, published atomically:

1. `CascadeStore.publishStorageLayout()` copies tier lists under brief locks, then swaps a volatile `storageVersion`.
2. `GetStore.lookup()` copies `activeMemTable` + `storageVersion` under a **short** read lock, then searches immutable tiers and SSTables **without** holding `memTableLock` or `ssTables` during disk I/O.
3. **`SSTable.pin()` / `unpin()`** — readers pin SSTables during `get()` so compaction cannot delete files mid-read.
4. **`CompactionService`** pins inputs under lock, merges outside lock, commits + publishes, then retires inputs.

`MemTable.shadows(key)` stops lookups at active/immutable tombstones so reads do not fall through to older SSTable versions.

### Impact

1M THRESHOLD (4 shards × 4 threads, cache off): run **3,247 ops/s** (was **1,328 ops/s**), read p99 **116 µs** (was **124 ms**). Zero errors.

**See also:** `DATA_FLOW.md` (WAL replay path).

---

## 14. Native merge (single LSM walk per update)

### Problem

YCSB Workload A is 50% updates. The original binding did:

```java
byte[] existing = store.get(storageKey);   // full LSM read
byte[] merged = YcsbRecordCodec.merge(existing, values);
store.put(storageKey, merged);             // WAL + memtable write
```

Half of all operations performed **two** full tree traversals (read path + write path).

### Fix

**`Storage.merge(key, ValueMerger)`** + `MergeStore`:

1. One `GetStore.lookup()` to read the current value.
2. Apply `ValueMerger.merge(existing)` in-process.
3. Append to WAL and write to memtable — no second read.

`CascadeStoreYcsbClient.update()` calls `merge()` instead of `get` + `put`.

### Impact

Halves LSM traversals on the update half of Workload A. Combined with §13, contributed to the **~2.4×** run-throughput gain at 1M before Phase F (1,328 → 3,247 ops/s). Phase F (§17) added **~3–5×** on top of v2 matrix numbers at trial 1.

**See also:** `api/ValueMerger.java`; `DATA_FLOW.md`.

---

## 15. Block cache (optional SSTable data reuse)

### Problem

`BufferedDataReader` refills a 64 KiB thread-local window from disk on every miss. Zipfian workloads revisit keys in large SSTables (~256 MB per shard at 1M), so the same index/data regions are re-read repeatedly with no cross-request reuse.

### Fix

**`BlockCache`** (`io.cascadestore.lsm.io`) — per-store LRU keyed by `(sstableId, blockOffset)`:

| Piece | Behavior |
| --- | --- |
| `BlockCache.create(bytes)` | Returns `null` when `bytes == 0` (disabled) |
| `BufferedDataReader` | Checks cache on window refill; inserts on miss |
| `CascadeConfig.blockCacheSizeBytes` | Default 128 MiB; `0` disables |
| Compaction retire | `BlockCache.invalidate(sstableId)` drops entries for compacted inputs |

**Toggle:**

- Engine: `CascadeConfig.withBlockCacheSizeBytes(0)` or constructor arg `blockCacheSizeBytes=0`
- YCSB: `BLOCK_CACHE_MB=0` or `-p cascadestore.block.cache.mb=0`
- Multi-shard YCSB auto-scales default cache per shard (`default / shardCount`, floor 8 MiB) when property is omitted

### When to enable

- **Single-shard, modest SSTable size:** can help zipfian read-heavy phases.
- **Multi-shard 1M baseline:** cache off passed the ≥3k ops/s gate with 1.8% GC; enable only after profiling shows disk read count is still the bottleneck.

Row cache was removed — it invalidated on every update (50% of Workload A) and added JVM memory pressure at multi-shard scale without net benefit.

### Impact

Optional; not required for the 1M correctness/throughput gate. Use as a profiling-driven knob, not a default at 4-shard scale.

**See also:** `BlockCacheTest`; `ARCHITECTURE.md`.

---

## 17. Phase F — read path and binding (2026-07-25)

Incremental optimizations after Phases A–E. Measured at 1M Workload A, 4 shards × 4 threads, cache off; **trial 1** vs v2 matrix (2.4–2.8k run) shows **~3–5×** run throughput.

### Highest impact

| Change | What it does |
| --- | --- |
| **mmap data files** (`MappedDataFile`) | SSTables ≤ 2 GiB mapped read-only; hot reads avoid per-window `read()` syscalls |
| **Sparse index binary search** (`SparseIndex`) | `floorOffset(key)` on sorted `byte[][]` + `long[]` instead of `TreeMap.floorEntry` |
| **Scan skip + `bytesEqual`** | On key miss in `.data` scan, skip value bytes without allocating; compare keys in the buffered window |
| **Block prefetch** | Double-buffered read-ahead in `BufferedDataReader` after sparse-index seek |

### Medium impact

| Change | What it does |
| --- | --- |
| **Version-level SSTable pin** | Pin all tables in `StorageVersion` at publish; drop per-`get` pin/unpin |
| **YCSB single-field patch** (`YcsbRecordCodec.patchSingleField`) | Workload A updates one field without full record decode/re-encode |
| **Parallel bloom on by default** | `BloomProbe` parallel when ≥3 SSTables per shard |
| **WAL read-write lock** | Concurrent appends on read lock; exclusive lock only for rotate/truncate/sync |
| **Bloom FPR 0.5%** | Fewer false-positive SSTable probes (new filters only) |

### Not done (deferred)

Columnar / field-level storage (F2d), delta WAL (F2e), off-heap block cache (F6d).

### Benchmark note

Report **trial 1** when comparing releases; later trials in a back-to-back matrix run faster due to cumulative JVM/OS warmth.

**See also:** `benchmark/throughput-by-scale/RESULTS.md` for headline throughput numbers.

---

## 19. Concurrency correctness — storage version pinning (2026-07-31)

### Problem

Under concurrent reads + flush + compaction (compaction-stress YCSB), three races produced `BufferUnderflowException`, torn values, and transient `NOT_FOUND`:

1. **SSTable lifetime** — `publishStorageLayout()` released the previous `StorageVersion` while readers still held references to its SSTable list; input tables were closed/deleted mid-read.
2. **MemTable lifetime** — `FlushService` called `memTable.close()` after flush, freeing off-heap buffers while pinned `StorageVersion` snapshots still referenced the immutable memtable.
3. **Retired-but-pinned SSTables** — `SSTable.get()` / `mightContain()` returned early when `retired == true`, even though pin-counting guaranteed on-disk files were still valid for in-flight readers.

A fourth bug caused **torn mmap reads**: `MappedDataFile.getBytes()` mutated the shared `MappedByteBuffer.position`, which is not thread-safe across concurrent reader threads.

A fifth bug was a **publish race**: `getStore.updateDependencies()` ran outside `storageVersionGate`, so readers could pin a stale `(activeMemTable, storageVersion)` pair during layout publish.

### Fix

| Component | Change |
| --- | --- |
| `StorageVersion` | `retain()` / `release()` with ref-count; pins immutable memtables + SSTables at construction; unpins at ref-count 0 |
| `GetStore.pinSnapshot()` | Retains version + pins active memtable for the lookup; releases in `finally` |
| `CascadeStore.publishStorageLayout()` | Updates `GetStore` dependencies inside `storageVersionGate` before releasing previous version |
| `FlushService` | No longer calls `memTable.close()`; calls `memTable.retire()` after successful flush |
| `MemTable` | `pin()` / `unpin()` / `retire()` — off-heap freed when retired and last pin drops |
| `SSTable.get()` / `containsKey()` / `mightContain()` | Serve reads on retired-but-pinned tables (files deleted only when pin count hits 0) |
| `MappedDataFile.getBytes()` | Absolute bulk `buffer.get(position, …)` — no shared position mutation |

`StorageVersionRetainTest` exercises 8 reader threads × 20 memtable rotations under load.

### Impact

Eliminates compaction-stress `BufferUnderflowException` and the “one failure per workload” pattern. Required for fair RocksDB comparison runs at 250k with frequent compaction.

---

## 20. Streaming k-way compaction merge (2026-07-31)

### Problem

Compaction materialized **all** input SSTable entries into a `HashMap`, then a merge `MemTable`, then flushed to a new SSTable — **2–3× input size** on-heap per compaction (~500–750 MB for 250 MB inputs). A per-compaction `Executors.newCachedThreadPool()` was created and torn down on every run.

The merge also dropped tombstones (`value == null`) at **every** level. Dropping a tombstone mid-tree can resurrect a deleted key still living in a deeper SSTable.

### Fix

**Streaming k-way merge** (`CompactionService.KWayMergeSource`):

1. Open a sorted `SSTable.RecordCursor` per input table.
2. Min-heap merge by key (newest sequence wins on ties).
3. Stream merged records directly into `SSTable.writeToDisk()` via `SortedRecordSource` — **O(#tables)** memory, no intermediate `HashMap` or merge memtable.

**Bottom-level tombstone rule** — tombstones are dropped only when `outputLevel > maxSurvivingLevel` (no deeper table can hold an older value for the same key). Otherwise tombstones are retained in the output SSTable.

Shared write path: memtable flush and compaction merge both use `SSTable.writeToDisk(SortedRecordSource)`.

### Impact

Lower compaction heap/GC pressure under compaction-stress config (64 MB memtable, threshold 2). Correct delete semantics when deeper levels exist.

---

## 21. LZ4 per-value SSTable compression (2026-07-31)

### Problem

Uncompressed SSTables at 250k scale use ~360 MB per workload datadir. RocksDB defaults to Snappy; comparison fairness benefits from optional compression on the CascadeStore side.

### Fix

**SSTable data format v2** (`SSTableDataFormat`, magic `CASK`):

- Per-record LZ4 when value ≥ 64 bytes and compressed size saves ≥ 5%; otherwise **RAW** flag.
- Legacy v1 files (no magic header) remain readable.
- New flushes and compaction outputs use v2.

Dependency: `org.lz4:lz4-java`.

### Measured on YCSB-shaped data

| Mode | Savings |
| --- | --- |
| Per-value LZ4 | **0%** (random field values are incompressible) |
| Per-64 KiB block (not implemented) | ~4.7% |

On YCSB, LZ4 adds negligible CPU overhead (RAW fallback) and keeps disk footprint comparable to RocksDB/Snappy. Block-level compression deferred — marginal YCSB gain, high read-path rewrite risk.

---

## 22. DirectBufferAllocator slab / arena (2026-07-31)

### Problem

Each memtable entry called `ByteBuffer.allocateDirect()` — one native `malloc` + `Cleaner` registration per put. A 64 MB memtable with ~1 KB records triggered **~65,000** native allocations per rotation.

### Fix

**Slab allocator** (`DirectBufferAllocator`):

- Carve entries from **1 MiB** direct slabs via absolute `slice(start, length)` (JDK 13+).
- Values larger than a slab get a dedicated buffer.
- Only slab roots are tracked and freed on `close()`; per-entry slices share slab memory.

### Impact

~1000× fewer native allocations per memtable on the write path. Pairs with §19 memtable `retire()` for prompt off-heap reclaim when the last reader unpins a flushed table.

---

## 18. Optimization summary table

| Optimization | Primary win | Correctness or perf |
| --- | --- | --- |
| WAL group commit (1 MiB) | Update throughput | Perf |
| Sparse index (16 KiB) | Heap / index size at 1M | Perf |
| Buffered data reads (64 KiB) | SSTable lookup syscalls | Perf |
| `ReadBuffers` reuse | Allocation churn | Perf |
| Flush atomic publish | No mid-flush `NOT_FOUND` | Correctness |
| `GetStore.lookup()` | Thread-safe reads | Correctness |
| WAL truncation | Fast restart / run start | Perf |
| YCSB sharding | Multi-core throughput | Perf |
| Flush rollback | No data loss on flush fail | Correctness |
| Bloom rebuild | Open tables without `.filter` | Robustness |
| **Version snapshots** | **1M p99 tail / concurrency** | **Perf** |
| **Native merge** | **Half the LSM walks on updates** | **Perf** |
| **Block cache (optional)** | **Hot SSTable region reuse** | **Perf** |
| **Phase F: mmap + sparse index + scan skip** | **1M run 3–5× vs v2 matrix (trial 1)** | **Perf** |
| **Phase F: version pin + YCSB patch + parallel bloom** | **Lower per-op overhead on hot path** | **Perf** |
| **Storage version + memtable pinning** | **No torn reads / flush-window NOT_FOUND** | **Correctness** |
| **Retired-but-pinned SSTable reads** | **No transient NOT_FOUND during compaction** | **Correctness** |
| **mmap absolute bulk get** | **Thread-safe concurrent mmap reads** | **Correctness** |
| **Streaming k-way compaction** | **Lower compaction heap; O(tables) merge** | **Perf** |
| **Bottom-level tombstone drop** | **Correct deletes across levels** | **Correctness** |
| **LZ4 per-value SSTable (v2)** | **Fair vs RocksDB Snappy; RAW on YCSB** | **Perf / fairness** |
| **DirectBufferAllocator slabs** | **~1000× fewer native allocs per memtable** | **Perf** |
| **MemTable retire + pin reclaim** | **Deterministic off-heap free after flush** | **Perf / correctness** |
