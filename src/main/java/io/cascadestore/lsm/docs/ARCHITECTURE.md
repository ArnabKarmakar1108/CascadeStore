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

<!-- remaining sections land in a follow-up commit -->
