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
    └── compaction/       CompactionStrategy, Threshold, SizeTiered
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


<!-- remaining sections land in a follow-up commit -->
