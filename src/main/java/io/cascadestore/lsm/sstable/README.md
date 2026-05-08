# SSTable Package

Sorted String Tables (`io.cascadestore.lsm.sstable`) are immutable on-disk key-value files. Each table is split into three companion files for data, index, and bloom filtering.

## On-Disk Layout

File prefix: `sst_L<level>_S<seqNum>`

| File | Purpose |
|------|---------|
| `.data` | 16-byte header, then sorted key/value records |
| `.index` | Sparse key → byte offset map (loaded into `SparseIndex`) |
| `.filter` | Off-heap bloom bit array with 4-byte header (hash function count) |

Index entries are emitted according to `SparseIndexPolicy` (roughly one anchor every 16 KiB of data) so `floorEntry(key)` narrows the scan window without indexing every key.

## Package Structure

| Area | Key types |
|------|-----------|
| `sstable` | `SSTable` (primary implementation), `BloomFilter`, `SSTableIterator`, factory/adapter types |
| `sstable.data` | `DataFileManager` / `DataFileManagerImpl` |
| `sstable.index` | `IndexFileManager`, `SparseIndex`, `SparseIndexPolicy` |
| `sstable.filter` | `FilterManager` / `FilterManagerImpl` |
| `sstable.io` | `SSTableIO` flush/load/delete |
| `sstable.iterator` | `SSTableIteratorFactory` for file and in-memory iteration |

`SSTable` is what `CascadeStore` uses directly; `SSTableImpl` plus `SSTableFactory` provide a modular alternative behind `SSTableInterface`.

## Creating and Opening Tables

```java
// Flush from MemTable
SSTable table = new SSTable(memTable, directory, level, sequenceNumber, blockCache);

// Open existing files
SSTable table = new SSTable(directory, level, sequenceNumber, blockCache);

// Modular API
SSTableInterface mod = SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);
SSTableInterface open = SSTableFactory.openFromDisk(directory, level, sequenceNumber);
```

## Bloom Filter

- Off-heap bits via `DirectBufferAllocator`
- Default false-positive rate: **0.5%** (`BloomFilter.DEFAULT_FALSE_POSITIVE_RATE`)
- Hashing: `h = 31 * h + b` with seed = function index
- Sizing uses standard `m/n` and `k` formulas from the target entry count and FPR

A negative bloom result skips disk I/O; a positive result still requires an index-guided scan.

## Point Lookup Path

`SSTable.get(key)` (and `containsKey`) follows:

1. **Bloom** — `mightContain(key)`; return miss if definitely absent
2. **Sparse index** — `floorEntry(key)` for the starting byte offset
3. **Data scan** — sequential read from that offset:
   - Prefer **mmap** via `MappedDataFile` when mapping succeeds
   - Otherwise **BufferedDataReader** (64 KiB window, per-thread) with optional **BlockCache** block reuse
4. **Value copy** — `ValueBufferPool.readCopy` returns a heap `byte[]` to the caller

On key miss within the scan window, the reader does not advance past unrelated keys (avoids double-skipping).

## Pinning and Retirement

SSTables support reference counting through `pin()` / `unpin()`. `StorageVersion` pins every table in a published snapshot so compaction can retire files only after readers release them. `blockCache.invalidateSstable(id)` runs when a table is deleted.

## Modular Design Benefits

1. **Focused components** — data, index, filter, and I/O are independently replaceable
2. **Testability** — each manager can be unit-tested in isolation
3. **Format evolution** — new on-disk versions can be introduced behind interfaces without rewriting `CascadeStore`
