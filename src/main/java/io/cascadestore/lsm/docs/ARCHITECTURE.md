# CascadeStore Architecture

## 1. High-Level Architecture

CascadeStore is a Log-Structured Merge-Tree (LSM-tree) based key-value storage engine written in Java 17. It provides durable, high-throughput writes with eventual consistency reads across multiple storage layers.

### Write Path

```
Client.put(key, value, ttl)
  → CascadeStore.put()
    → PutStore.put()
      → WALImpl.appendPutRecord()         [fsync to disk]
      → MemTable.put()                    [off-heap ConcurrentSkipListMap]
        → if full: CascadeStore.switchMemTable()
          → mark current immutable
          → create new active MemTable
          → FlushService.executeNow()
            → SSTable(memTable, dir, level=0, seqNum)
              → writes .data + .index + .filter files
```



### Read Path

```
Client.get(key)
  → CascadeStore.get()
    → GetStore.get()
      → 1. Active MemTable              [readLock]
      → 2. Immutable MemTables          [newest first, synchronized]
      → 3. SSTables                     [newest first, synchronized]
           → BloomFilter.mightContain() [fast negative]
           → sparseIndex.floorEntry()   [O(log n) offset lookup]
           → sequential scan from offset
```



### Recovery Path

```
CascadeStore startup
  → loadSSTables()        [discover .data files by filename pattern]
  → WALImpl(walDir)
  → recover()
    → recovering = true   [suppress WAL writes]
    → wal.readRecords()   [all WAL files, sorted by seqNum]
    → replay PutRecords → MemTable.put()
    → replay DeleteRecords → MemTable.delete()
    → update global sequenceNumber
```

---



## 2. Package Structure

```
io.cascadestore.lsm
├── api/                  Public interfaces (Storage, iterators, ByteArrayWrapper)
├── config/               CascadeConfig record
├── memory/               Off-heap allocator (OffHeapAllocator, DirectBufferAllocator)
├── memtable/             MemTable with ConcurrentSkipListMap + off-heap ValueEntry
├── sstable/              On-disk SSTable, BloomFilter, sparse index
│   ├── data/             DataFileManager (read/write entries)
│   ├── index/            IndexFileManager (sparse index)
│   ├── filter/           FilterManager (bloom filter lifecycle)
│   ├── io/               SSTableIO (file-level flush/load/delete)
│   └── iterator/         SSTableIteratorFactory
├── wal/                  Write-Ahead Log facade
│   ├── file/             WALFile abstraction
│   ├── manager/          WAL lifecycle (create/rotate/discover files)
│   ├── reader/           WALReader (deserialization)
│   ├── writer/           WALWriter (serialization + fsync)
│   └── record/           PutRecord, DeleteRecord
└── core/
    ├── store/            CascadeStore, PutStore, GetStore, DeleteStore
    ├── backgroundservice/ FlushService, CompactionService, CleanupService
    └── compaction/       CompactionStrategy, Threshold, SizeTiered, LevelTiered
```

---



## 3. MemTable

**Class:** `io.cascadestore.lsm.memtable.MemTable`

A fast, sorted, in-memory write buffer using off-heap storage for values.

### Fields


| Field          | Type                                                  | Purpose                                |
| -------------- | ----------------------------------------------------- | -------------------------------------- |
| `entries`      | `ConcurrentSkipListMap<ByteArrayWrapper, ValueEntry>` | Sorted key-to-value map                |
| `allocator`    | `OffHeapAllocator`                                    | Manages off-heap ByteBuffer allocation |
| `sizeBytes`    | `AtomicLong`                                          | Tracks total memory usage              |
| `maxSizeBytes` | `long`                                                | Capacity limit (default 10MB)          |
| `immutable`    | `volatile boolean`                                    | Once set, rejects all writes           |




### ValueEntry — Off-Heap ByteBuffer Layout

```
Offset  0-7  (8 bytes): expirationTime (long) — 0 = no expiration
Offset  8    (1 byte):  tombstone flag — 0x01 = deleted, 0x00 = live
Offset  9-11 (3 bytes): padding
Offset 12-15 (4 bytes): valueLength (int) — 0 for tombstones
Offset 16+   (N bytes): value data
```

**HEADER_SIZE = 16 bytes.** All access uses absolute-position ByteBuffer methods (thread-safe for concurrent reads).

### DirectBufferAllocator

- `ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder())`
- Tracks all buffers in a synchronized list
- `close()` invokes `sun.misc.Unsafe.invokeCleaner(buffer)` for deterministic deallocation
- Falls back to GC if Unsafe unavailable



### Lifecycle

1. Created mutable with `maxSizeBytes` capacity
2. Accepts `put()` / `delete()` (delete inserts a tombstone)
3. Returns `false` from `put()` when full
4. `makeImmutable()` — volatile write; all subsequent writes rejected
5. `close()` — frees all off-heap memory

---



## 4. WAL (Write-Ahead Log)



### Binary Record Format

**Put Record:**

```
[1 byte: type=0x01][8 bytes: seqNum][4 bytes: keyLen][N bytes: key]
[4 bytes: valueLen][M bytes: value][8 bytes: ttlSeconds]
```

**Delete Record:**

```
[1 byte: type=0x02][8 bytes: seqNum][4 bytes: keyLen][N bytes: key]
```



### Components


| Class            | Responsibility                                                    |
| ---------------- | ----------------------------------------------------------------- |
| `WAL`            | Interface: append, read, delete                                   |
| `WALImpl`        | Facade composing Manager + Reader + Writer                        |
| `WALWriterImpl`  | Serializes records, writes via FileChannel, fsyncs. Synchronized. |
| `WALReaderImpl`  | Sequential binary read from WAL files                             |
| `WALManagerImpl` | File lifecycle: create, rotate at 64MB, discover, delete          |
| `WALFileImpl`    | FileChannel wrapper for a single WAL file                         |
| `PutRecord`      | Immutable record with key (cloned), value (cloned), ttl, seqNum   |
| `DeleteRecord`   | Immutable record with key (cloned), seqNum                        |




### File Management

- Directory: `<dataDirectory>/wal/`
- Naming: `wal_%020d.log` (zero-padded sequence number)
- Rotation: at 64MB file size
- Sequence numbers: global `AtomicLong`, monotonically increasing

---



## 5. SSTable



### On-Disk Format

Each SSTable produces three files: `sst_L<level>_S<seqNum>.{data,index,filter}`

**Data File (.data):**

```
[Header: 16 bytes]
  8 bytes: creationTime (long)
  4 bytes: level (int)
  4 bytes: entryCount (int)
[Entries...]
  4 bytes: keyLength
  N bytes: key
  4 bytes: valueLength (0 = tombstone)
  M bytes: value (if not tombstone)
  8 bytes: timestamp
```

**Index File (.index):**

```
[Entries — sparse index, one per key]
  4 bytes: keyLength
  N bytes: key
  8 bytes: offset into data file
```

**Filter File (.filter):**

```
4 bytes: numHashFunctions
remaining: bloom filter bit array
```



### Class Hierarchy


| Class              | Role                                                              |
| ------------------ | ----------------------------------------------------------------- |
| `SSTable`          | Concrete class used by CascadeStore; handles flush + load + query |
| `SSTableInterface` | Interface for modular implementation                              |
| `SSTableImpl`      | Modular implementation delegating to component managers           |
| `SSTableAdapter`   | Adapts SSTableInterface to extend SSTable (Adapter pattern)       |
| `SSTableFactory`   | Factory for creating SSTables (modular or backward-compatible)    |
| `SSTableEntry`     | Immutable record for a single entry                               |
| `SSTableIterator`  | Sealed interface with InMemory and File implementations           |




### BloomFilter

- Off-heap bit array via `DirectBufferAllocator`
- Hash: `h = 31 * h + b` with seed = function index
- Optimal bits: `-(n * ln(p)) / (ln(2)^2)`
- Optimal hash functions: `max(1, round(m/n * ln(2)))`



### Lookup Path (SSTable.get)

1. `bloomFilter.mightContain(key)` — false → key definitely absent
2. `sparseIndex.floorEntry(key)` — find closest offset
3. Sequential scan from that offset in .data file

### Key Bounds (compaction)

- `getMinKey()` / `getMaxKey()` — first and last keys from the sparse index
- `overlaps(other)` — true when key ranges intersect
- `keyRangesOverlap(minA, maxA, minB, maxB)` — static range overlap check used by level-tiered selection

---



## 6. Compaction



### CompactionStrategy Interface

```java
boolean shouldCompact(List<SSTable> ssTables);
List<SSTable> selectTableToCompact(List<SSTable> ssTables);
int getCompactionOutputLevel(List<SSTable> tablesToCompact);
String getName();
```



### ThresholdCompactionStrategy

- **Trigger:** `ssTables.size() >= compactionThreshold` (default 4)
- **Selection:** Groups by level, picks level with most SSTables (requires >= 2)
- **Output:** `currentLevel + 1`



### SizeTieredCompactionStrategy

- **Trigger:** Any size bucket has >= `minThreshold` (4) SSTables
- **Bucketing:** `bucketLow * avg <= size <= bucketHigh * avg` (0.5x to 1.5x)
- **Selection:** Largest qualifying bucket, capped at `maxThreshold` (32)
- **Output:** `max(levels in selected) + 1`



### LevelTieredCompactionStrategy

Leveled compaction with adjacent-level merges. Strategy parameters are set on the constructor (defaults shown below), not on `CascadeConfig`.

| Parameter | Default | Purpose |
| --------- | ------- | ------- |
| `l0CompactionTrigger` | 4 | L0 file count that triggers compaction |
| `maxL0CompactionFiles` | 4 | Max L0 SSTables picked per L0 job |
| `baseLevelSizeBytes` | 10 MB | Byte budget for L1 |
| `levelSizeMultiplier` | 10 | Each deeper level budget = previous × multiplier |
| `maxLevels` | 7 | Cap on level number |

- **Trigger (L0):** `count(L0) >= l0CompactionTrigger`
- **Trigger (L1+):** Total bytes at level *n* ≥ level budget for *n*
- **Selection (L0):** Up to `maxL0CompactionFiles` oldest L0 SSTables by sequence number, plus all overlapping L1 SSTables (expanded until stable)
- **Selection (L1+):** Largest SSTable at over-budget level *n*, plus all overlapping SSTables at level *n+1*
- **Output:** Any job that includes L0 inputs → level 1; pure L*n* jobs → level *n+1* (capped at `maxLevels`)
- **Scheduling:** One adjacent-level merge per compaction tick (no cascade loop in a single job)



### CompactionService Flow

1. `compactionStrategy.shouldCompact()` check
2. `selectTableToCompact()` picks tables
3. Create merge MemTable with capacity `max(memTableMaxSizeBytes × 10, inputBytes × 1.2)`
4. Parallel reads from selected SSTables (cached thread pool)
5. Merge: newer SSTables win (descending seqNum order); tombstones dropped
6. Load merged entries into MemTable; abort without deleting inputs if full
7. Write merged data into new SSTable at output level
8. Remove old SSTables from list, delete files
9. **Merge safety:** Abort without deleting inputs on MemTable overflow or `OutOfMemoryError`

---



## 7. Background Services


| Service             | Schedule    | Responsibility                                |
| ------------------- | ----------- | --------------------------------------------- |
| `FlushService`      | Every 10s   | Flush immutable MemTables to level-0 SSTables |
| `CompactionService` | Every 30min | Merge SSTables per configured strategy        |
| `CleanupService`    | Every 1min  | Remove expired (TTL) entries from MemTables   |


All extend `AbstractBackgroundService`:

- Single daemon thread per service (`CascadeStore-<name>`)
- `ScheduledExecutorService.scheduleAtFixedRate()`
- Graceful shutdown: 5s await → `shutdownNow()`

---



## 8. Store Layer



### CascadeStore

The main entry point implementing `Storage`. Owns all components.


| Field                | Type                     | Purpose              |
| -------------------- | ------------------------ | -------------------- |
| `activeMemTable`     | `MemTable`               | Current write target |
| `immutableMemTables` | `List<MemTable>`         | Pending flush        |
| `ssTables`           | `List<SSTable>`          | On-disk tables       |
| `wal`                | `WAL`                    | Durability           |
| `memTableLock`       | `ReentrantReadWriteLock` | Concurrency control  |
| `sequenceNumber`     | `AtomicLong`             | Global ordering      |
| `recovering`         | `AtomicBoolean`          | WAL replay mode      |




### Operation Delegates


| Class         | Path                                             |
| ------------- | ------------------------------------------------ |
| `PutStore`    | WAL append → MemTable insert                     |
| `GetStore`    | Active MemTable → Immutable MemTables → SSTables |
| `DeleteStore` | Existence check → WAL append → tombstone insert  |


---



## 9. Configuration (CascadeConfig)


| Field                       | Default     | Purpose                             |
| --------------------------- | ----------- | ----------------------------------- |
| `memTableMaxSizeBytes`      | 10MB        | MemTable capacity before flush      |
| `dataDirectory`             | `"./data"`  | Root for all persistent files       |
| `compactionThreshold`       | 4           | SSTable count triggering compaction |
| `compactionIntervalMinutes` | 30          | Background compaction interval      |
| `cleanupIntervalMinutes`    | 1           | TTL cleanup interval                |
| `flushIntervalSeconds`      | 10          | Background flush interval           |
| `compactionStrategyType`    | `THRESHOLD` | Compaction strategy                 |


Immutable Java record with `with*()` builder methods.

---



## 10. API Layer



### Storage Interface

```java
put(key, value) / put(key, value, ttl)
get(key)
delete(key)
listKeys() / containsKey(key) / size()
getRange(start, end)
getIterator(start, end)
clear() / shutdown()
```

