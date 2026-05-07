# CascadeStore — End-to-End Data Flow

This document traces a key/value through every stage of the engine:

```
client.put() → WAL → active MemTable → immutable MemTable → SSTable → compaction
```

and then explains the cross-cutting mechanics that make that pipeline correct: **how locks are acquired**, **how compaction actually merges tables**, and **how crash recovery replays the log**. It also catalogs the important classes, the byte-level memory layouts, the types used as map keys, and the data structures backing each critical component.

It is a companion to `ARCHITECTURE.md` (reference sheet) and `OPTIMIZATIONS.md` (problems and fixes). Where `ARCHITECTURE.md` is a reference sheet, this document is a walkthrough with code.

---

## 0. Orientation — the objects involved

`CascadeStore` is the top-level `Storage` implementation. It owns every mutable piece of engine state and wires the three operation delegates (`PutStore`, `GetStore`, `DeleteStore`) plus the three background services (`FlushService`, `CompactionService`, `CleanupService`).

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (fields)
private MemTable activeMemTable;                 // current write target
private final List<SSTable> ssTables;            // on-disk sorted tables (newest last / see note)
private final List<MemTable> immutableMemTables; // full memtables awaiting flush
private WAL wal;                                 // durability log

private final ReadWriteLock memTableLock;        // guards the *identity* of activeMemTable
private final AtomicLong sequenceNumber;         // global ordering for SSTable seq + recovery
private final AtomicBoolean recovering;          // suppresses WAL writes during replay
```

The state is layered exactly as an LSM-tree prescribes, newest to oldest:

1. `activeMemTable` — absorbs writes.
2. `immutableMemTables` — sealed memtables not yet on disk.
3. `ssTables` — on-disk, immutable, sorted files.

A read walks these layers top-to-bottom and stops at the first hit. A write only ever touches layer 1 (plus the WAL).

---

## 1. Building-block classes and the data structures they back

Before the flow, here are the small types that show up everywhere, and — critically — **why each was chosen as a key or container**.

### 1.1 `ByteArrayWrapper` — the universal map key

Raw `byte[]` cannot be used as a key in any JDK map: arrays use identity `equals`/`hashCode`, so two arrays with the same bytes are "different" keys. `ByteArrayWrapper` fixes that and adds unsigned lexicographic ordering.

```java
// src/main/java/io/cascadestore/lsm/api/ByteArrayWrapper.java
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
  private final byte[] data;

  @Override
  public int compareTo(ByteArrayWrapper other) {
    byte[] otherData = other.getData();
    int length = Math.min(data.length, otherData.length);
    for (int i = 0; i < length; i++) {
      int a = data[i] & 0xff;          // unsigned comparison
      int b = otherData[i] & 0xff;
      if (a != b) return a - b;
    }
    return data.length - otherData.length; // shorter key sorts first on prefix tie
  }

  @Override public boolean equals(Object obj) { /* Arrays.equals(data, other.data) */ }
  @Override public int hashCode()            { return Arrays.hashCode(data); }
}
```

Why it matters:

- **`equals`/`hashCode` by content** → usable as a `HashMap`/`ConcurrentSkipListMap` key.
- **`compareTo` with `& 0xff`** → keys sort by *unsigned* byte value, which is the order the SSTable sparse index and range scans rely on. Signed comparison would place bytes ≥ 0x80 before 0x00 and corrupt range semantics.

This type is the key in **every** ordered structure in the engine: the MemTable map, the SSTable sparse index, and the compaction merge map.

### 1.2 Data structures used for the critical components

| Component | Structure | Why this structure |
| --- | --- | --- |
| MemTable entries | `ConcurrentSkipListMap<ByteArrayWrapper, ValueEntry>` | Sorted (needed for ordered flush + range scans) **and** lock-free concurrent reads/writes. Sorted iteration is what lets a flush write keys to the SSTable in order. |
| MemTable value storage | Off-heap `ByteBuffer` per entry (`DirectBufferAllocator`) | Keeps large values off the Java heap → less GC pressure. |
| SSTable sparse index | `NavigableMap<ByteArrayWrapper, Long>` (`TreeMap`) | One entry per **16 KiB** of `.data` bytes (`SparseIndexPolicy`), plus the last key. `floorEntry` + short forward scan. |
| SSTable data scan | `BufferedDataReader` (64 KiB window, `ThreadLocal` per SSTable) | Amortizes `FileChannel.read` syscalls when scanning `.data`. |
| SSTable negative lookups | `BloomFilter` over an off-heap bit array | O(k) probabilistic "definitely absent" check to skip disk reads. |
| Compaction merge | `HashMap<ByteArrayWrapper, byte[]>` + descending-seq iteration | First-writer-wins dedup where "first" = newest table. |
| WAL sequence / SSTable seq | `AtomicLong` | Monotonic global ordering across threads without locks. |
| `immutableMemTables`, `ssTables` | `ArrayList` guarded by `synchronized (list)` | Small collections mutated infrequently; coarse lock is sufficient. |

### 1.3 WAL records — `PutRecord` / `DeleteRecord`

Both implement the sealed-style `Record` interface and make **defensive copies** of the key/value so a caller mutating its array can't corrupt a buffered log record.

```java
// src/main/java/io/cascadestore/lsm/wal/record/PutRecord.java
public PutRecord(long sequenceNumber, byte[] key, byte[] value, long ttlSeconds) {
  this.sequenceNumber = sequenceNumber;
  this.key = key.clone();     // defensive copy in
  this.value = value.clone();
  this.ttlSeconds = ttlSeconds;
}
@Override public byte[] getKey() { return key.clone(); } // defensive copy out
```

---

## 2. The MemTable and its off-heap `ValueEntry` layout

The MemTable is a sorted, concurrent, in-memory buffer. Its keys live on-heap (`ByteArrayWrapper`); its **values live off-heap** inside a single `ByteBuffer` per entry.

### 2.1 `ValueEntry` byte layout

Each value is stored in one direct buffer with a fixed 16-byte header followed by the value bytes:

```
Offset  0–7   (8 bytes): expirationTime (long)  — 0 means "never expires"
Offset  8     (1 byte):  tombstone flag          — 0x01 = deleted, 0x00 = live
Offset  9–11  (3 bytes): padding (alignment)
Offset 12–15  (4 bytes): valueLength (int)       — 0 for tombstones
Offset 16..N            : value bytes            — absent for tombstones
```

```java
// src/main/java/io/cascadestore/lsm/memtable/MemTable.java (ValueEntry)
private static final int HEADER_SIZE = 16;
private static final int EXPIRATION_TIME_OFFSET = 0;
private static final int TOMBSTONE_OFFSET = 8;
private static final int VALUE_LENGTH_OFFSET = 12;

public ValueEntry(byte[] value, long ttlSeconds, boolean tombstone, OffHeapAllocator allocator) {
  this.hasTombstone = tombstone;
  long expirationTime = ttlSeconds > 0 ? System.currentTimeMillis() + (ttlSeconds * 1000) : 0;
  if (tombstone) {
    this.buffer = allocator.allocate(HEADER_SIZE);
    buffer.putLong(EXPIRATION_TIME_OFFSET, expirationTime);
    buffer.put(TOMBSTONE_OFFSET, (byte) 1);
    buffer.putInt(VALUE_LENGTH_OFFSET, 0);
  } else {
    int valueLength = value != null ? value.length : 0;
    this.buffer = allocator.allocate(HEADER_SIZE + valueLength);
    buffer.putLong(EXPIRATION_TIME_OFFSET, expirationTime);
    buffer.put(TOMBSTONE_OFFSET, (byte) 0);
    buffer.putInt(VALUE_LENGTH_OFFSET, valueLength);
    if (valueLength > 0) {
      buffer.put(HEADER_SIZE, value, 0, valueLength); // absolute-index bulk put
    }
  }
}
```

Every read/write uses **absolute-index** `ByteBuffer` methods (`getInt(offset)`, `put(offset, ...)`), never the position-based ones. That is deliberate: `position()` is mutable shared state, and the same `ValueEntry` is read concurrently by reader threads and by the flush thread. Absolute indexing makes `getValue()` safe under concurrent reads:

```java
// src/main/java/io/cascadestore/lsm/memtable/MemTable.java (ValueEntry.getValue)
public byte[] getValue() {
  if (isTombstone()) return null;
  int valueLength = buffer.getInt(VALUE_LENGTH_OFFSET);
  if (valueLength <= 0) return null;
  byte[] result = new byte[valueLength];
  buffer.get(HEADER_SIZE, result, 0, valueLength); // no shared position mutation
  return result;
}
```

### 2.2 Off-heap allocation lifecycle

Buffers come from `DirectBufferAllocator`, which tracks every allocation and frees them deterministically on `close()` (rather than waiting for GC to run the buffer's `Cleaner`). Allocation and close are `synchronized` because a MemTable accepts concurrent `put`/`delete`.

```java
// src/main/java/io/cascadestore/lsm/memory/DirectBufferAllocator.java
public synchronized ByteBuffer allocate(int bytes) {
  if (closed) throw new IllegalStateException("Allocator is closed");
  ByteBuffer buffer = ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
  allocated.add(buffer);
  return buffer;
}

public synchronized void close() {
  if (closed) return;
  closed = true;
  for (ByteBuffer buffer : allocated) freeDirectBuffer(buffer); // Unsafe.invokeCleaner
  allocated.clear();
}
```

`nativeOrder()` matches the platform endianness (little-endian on x86-64), matching the layout the original off-heap implementation used and avoiding per-access byte swaps.

### 2.3 MemTable `put` — size accounting and the "full" signal

```java
// src/main/java/io/cascadestore/lsm/memtable/MemTable.java (put)
public boolean put(byte[] key, byte[] value, long ttlSeconds) {
  if (immutable) return false;                       // sealed table rejects writes
  if (key == null || key.length == 0 || value == null) return false;

  ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
  ValueEntry newEntry = new ValueEntry(value, ttlSeconds, false, allocator);
  long entrySize = key.length + newEntry.getSizeBytes();

  if (sizeBytes.get() + entrySize > maxSizeBytes) return false; // FULL → caller must switch

  ValueEntry oldEntry = entries.put(keyWrapper, newEntry);
  if (oldEntry != null) sizeBytes.addAndGet(entrySize - oldEntry.getSizeBytes());
  else                  sizeBytes.addAndGet(entrySize);
  return true;
}
```

Two distinct `false` cases matter to the caller:

- Rejected because **immutable** or **invalid input** → hard failure.
- Rejected because the table is **full** (`sizeBytes + entrySize > maxSizeBytes`) → caller triggers a memtable switch and retries. The store distinguishes these via `isFull()`.

`sizeBytes` is an `AtomicLong` tracking `key.length + buffer.capacity()` per entry so `isFull()` is a cheap read.

---

## 3. Write path: `client.put()` → WAL → MemTable

### 3.1 Entry point and the "full → switch → retry" loop

`CascadeStore.put` delegates to `PutStore`. The interesting part is the return-code protocol: `PutStore` never switches memtables itself (it doesn't own the `activeMemTable` reference); it *signals* fullness and lets `CascadeStore` perform the structural change under the write lock.

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (put)
public boolean put(byte[] key, byte[] value, long ttlSeconds) {
  int result = putStore.put(key, value, ttlSeconds);
  if (result == PutStore.RESULT_SUCCESS) {
    return true;
  } else if (result == PutStore.RESULT_MEMTABLE_FULL) {
    switchMemTable();                                       // seal + create new + flush
    putStore = new PutStore(activeMemTable, memTableLock, wal, recovering);
    int newResult = putStore.put(key, value, ttlSeconds);  // retry on fresh table
    return newResult == PutStore.RESULT_SUCCESS;
  } else {
    return false;
  }
}
```

### 3.2 WAL first, MemTable second (durability ordering)

`PutStore.put` writes to the WAL **before** touching the MemTable. This ordering is the entire point of a write-ahead log: if the process dies after the WAL append but before/after the memtable insert, recovery replays the record and the write is not lost.

```java
// src/main/java/io/cascadestore/lsm/core/store/PutStore.java (put)
public int put(byte[] key, byte[] value, long ttlSeconds) {
  lastException = null;
  if (key == null || key.length == 0 || value == null) return RESULT_INVALID_INPUT;
  try {
    if (!recovering.get()) {                 // during replay we must NOT re-log
      wal.appendPutRecord(key, value, ttlSeconds);
    }
    return putInMemTable(key, value, ttlSeconds);
  } catch (IOException e) {
    lastException = e;
    return RESULT_WAL_ERROR;
  }
}
```

Note the `recovering` guard: during crash recovery the same `put()` path is reused to rebuild the memtable, but re-appending to the WAL would duplicate history, so it is suppressed.

### 3.3 WAL append — serialization + batched fsync (group commit)

`WALWriterImpl` serializes each record into a `ByteBuffer`, writes it through the `FileChannel`, and calls `walManager.noteBytesWritten(recordSize)`. The manager **does not fsync every record** — it group-commits:

```java
// WalSyncPolicy.DEFAULT_SYNC_BATCH_BYTES = 1 * 1024 * 1024  (1 MiB)

// WALManagerImpl.noteBytesWritten
bytesSinceLastSync += bytes;
if (bytesSinceLastSync >= syncBatchBytes) {
  sync();  // currentFile.force(true); bytesSinceLastSync = 0;
}
```

**Forced `sync()` still runs** at safety boundaries:

| Event | Why |
| --- | --- |
| `syncBatchBytes` reached | Periodic durability checkpoint |
| `rotateLog()` / `createNewFile()` | Seal old segment before new file |
| `switchMemTable()` → `wal.sync()` | Memtable identity change |
| `truncateWal()` / `deleteAllLogs()` | Before deleting segments |
| Shutdown | Final durability |

The writer methods remain `synchronized`, so concurrent appends serialize at the log boundary.

```java
// WALWriterImpl.appendPutRecord (after write)
walManager.getCurrentFile().write(buffer);
walManager.noteBytesWritten(recordSize);   // may trigger sync() at 1 MiB
```

**On-disk WAL record formats:**

```
Put:    [1: type=0x01][8: seqNum][4: keyLen][keyLen: key][4: valLen][valLen: value][8: ttl]
Delete: [1: type=0x02][8: seqNum][4: keyLen][keyLen: key]
```

WAL files are named `wal_%020d.log` (zero-padded sequence) and rotate at 64 MB. Durability is `FileChannel.force(true)` via `WALManagerImpl.sync()`.

### 3.4 MemTable insert under the read lock

This is the subtle part of the concurrency model. Inserting into the active MemTable takes the **read** lock, not the write lock:

```java
// src/main/java/io/cascadestore/lsm/core/store/PutStore.java (putInMemTable)
private int putInMemTable(byte[] key, byte[] value, long ttlSeconds) {
  boolean success = false, needSwitch = false;
  memTableLock.readLock().lock();          // READ lock — see §4 for why
  try {
    success = activeMemTable.put(key, value, ttlSeconds);
    if (!success && activeMemTable.isFull()) needSwitch = true;
  } finally {
    memTableLock.readLock().unlock();
  }
  if (needSwitch) return RESULT_MEMTABLE_FULL;
  return success ? RESULT_SUCCESS : RESULT_INVALID_INPUT;
}
```

---

## 4. Locking model — exactly how locks are acquired

CascadeStore uses **one** `ReentrantReadWriteLock` (`memTableLock`) plus intrinsic `synchronized` blocks on the two lists and on the WAL writer. The read/write lock is used in an unusual but correct way.

### 4.1 What the read/write lock actually protects

`memTableLock` does **not** protect the *contents* of a memtable — `ConcurrentSkipListMap` already makes concurrent `put`/`get` safe. It protects the **reference** `activeMemTable` from being swapped out from under an in-flight operation.

- **Readers and writers of data** (`put`, `delete`, `get`, `listKeys`, `size`, iterator) take the **read lock**. Multiple can run concurrently; the concurrent map handles the actual data races. The read lock only guarantees "the `activeMemTable` I'm about to use won't be replaced mid-call."
- **Structural changes** (`switchMemTable`, `clear`, `shutdown`) take the **write lock**. These reassign `activeMemTable`, so they need exclusivity against everyone holding the reference.

This is why a plain `put` uses `readLock()` even though it mutates the map — the mutation safety comes from the data structure, and the lock is purely about reference stability.

### 4.2 The memtable switch under the write lock

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (switchMemTable)
private void switchMemTable() {
  memTableLock.writeLock().lock();                  // exclusive: no reader holds a stale ref
  try {
    activeMemTable.makeImmutable();                 // volatile write; further puts rejected
    synchronized (immutableMemTables) {             // second lock: the list itself
      immutableMemTables.add(activeMemTable);
    }
    activeMemTable = new MemTable(config.memTableMaxSizeBytes());
    getStore.updateDependencies(activeMemTable, immutableMemTables, ssTables, memTableLock);
    putStore.updateDependencies(activeMemTable, memTableLock, wal, recovering);
    deleteStore.updateDependencies(activeMemTable, memTableLock, wal, recovering, getStore);
    flushMemTables();                               // trigger flush of the sealed table
  } finally {
    memTableLock.writeLock().unlock();
  }
}
```

`makeImmutable()` is a `volatile boolean` write, so the seal is visible to all threads immediately; any late `put` on the old table returns `false` at the `if (immutable)` guard.

### 4.3 Lock ordering and nesting

The nesting is always **`memTableLock` → `synchronized(list)`**, never the reverse. `switchMemTable` holds the write lock and then briefly synchronizes on `immutableMemTables`. The list-level `synchronized` blocks (in `FlushService`, `GetStore`, `CleanupService`) are short and never try to acquire `memTableLock` while held, so no cycle exists.

### 4.4 The WAL writer lock

Orthogonal to `memTableLock`: `WALWriterImpl.appendPutRecord`/`appendDeleteRecord` are `synchronized` on the writer instance so record framing + `force()` are atomic. A `put` therefore acquires the WAL monitor first (in `PutStore.put`), releases it, then acquires the memtable read lock (in `putInMemTable`) — two independent critical sections, never held simultaneously.

### 4.5 Summary of every lock site

| Operation | Lock(s) | Mode |
| --- | --- | --- |
| `put` / `delete` data insert | `memTableLock` | read |
| WAL append | `WALWriterImpl` monitor | exclusive (`synchronized`) |
| `get` / `containsKey` | `memTableLock` read → `immutableMemTables` → `ssTables` (nested, single `lookup()` call) | read + per-list |
| `switchMemTable`, `clear`, `shutdown` seal | `memTableLock` | write |
| flush publish SSTable | `synchronized(immutableMemTables)` then `synchronized(ssTables)` | exclusive, nested |
| flush batch guard | `synchronized(flushMonitor)` on `FlushService.doExecute` | exclusive |
| compaction whole cycle | `synchronized(ssTables)` | exclusive |
| TTL cleanup scan → tombstone | `memTableLock` read (scan), then write (delete) | read then write |

---

## 5. Immutable MemTable → SSTable (FlushService)

`FlushService` runs on a scheduled daemon thread (default every 10s) and is also invoked synchronously via `executeNow()` right after a switch. It flushes sealed memtables to level-0 SSTables **without removing them from the read path until the SSTable is published**.

```java
// FlushService.doExecute — entire batch under flushMonitor
synchronized (flushMonitor) {
  synchronized (immutableMemTables) {
    tablesToFlush = new ArrayList<>(immutableMemTables);  // snapshot only; list NOT cleared
  }
  for (MemTable memTable : tablesToFlush) {
    if (!immutableMemTables.contains(memTable)) continue; // another batch finished it
    flushMemTable(memTable);
  }
  maybeTruncateWal();      // hook → CascadeStore.truncateWalIfAllDataFlushed()
  maybeTriggerCompaction();
}
```

```java
// flushMemTable — atomic publish (readers see key in immutable OR ssTables, never neither)
SSTable ssTable = new SSTable(memTable, config.dataDirectory(), 0, seqNum);
synchronized (immutableMemTables) {
  synchronized (ssTables) {
    ssTables.add(ssTable);
    immutableMemTables.remove(memTable);
  }
}
memTable.close();
```

**Why not claim-and-clear?** An earlier design removed memtables from `immutableMemTables` before writing the SSTable. Concurrent `get()` calls could miss keys during that window. The publish pattern keeps keys visible until the SSTable exists.

Note the coupling: a flush that pushes the SSTable count to the threshold **immediately kicks compaction**, so compaction is not solely time-driven.

### 5.1 SSTable on-disk format — and why it's three files

The `SSTable(memTable, dir, level, seq)` constructor writes three files that share a name stem, `sst_L<level>_S<seq>.{data,index,filter}`. They are separated because they are accessed at different times, at different granularities, and with different lifetimes in memory:

```
.data   ← the actual key/value entries (the large file; stays on disk, read on demand)
.index  ← key → byte-offset map        (small; loaded fully into RAM at open)
.filter ← the bloom bit array           (small; loaded fully into RAM at open)
```

**`.data`** — the entries themselves, written in sorted key order behind a fixed 16-byte header:

```
[Header 16 bytes]  8: creationTime (long) | 4: level (int) | 4: entryCount (int)
[Entry]* :         4: keyLen | keyLen: key | 4: valLen (0 ⇒ tombstone) | valLen: value | 8: timestamp
```

**`.index`** — sparse key → byte-offset map (`SparseIndexPolicy`: one entry per **16 KiB** of `.data`, plus the last key):

```
[Entry]* :         4: keyLen | keyLen: key | 8: offset-into-.data
```

**`.filter`** — the persisted Bloom filter:

```
4: numHashFunctions | remaining: bloom bit array
```

**Why length-prefixed, sorted, header-first.** Every variable-size field is preceded by its length, so a reader reads the length and then exactly that many bytes — no delimiters, no escaping. `valLen == 0` doubles as the on-disk tombstone marker, so no separate flag byte is needed. Entries are physically **sorted** (the memtable iterates in `ByteArrayWrapper` order), which is precisely what makes offset-seeking and range scans possible. The header stores `entryCount` up front so a reader knows the table size without scanning it.

**Why three separate files instead of one:**

| Concern | Why the split helps |
| --- | --- |
| **Residency** | `.index` and `.filter` are small and live in RAM; `.data` is large and stays on disk. Splitting means open-time loads only the small metadata, never the whole table. |
| **Access pattern** | A lookup consults `.filter` (RAM) → `.index` (RAM) → then **one** targeted seek into `.data` (disk). Inlining values into the index would bloat what must stay resident. |
| **Independent rebuild** | The filter/index can be reloaded or regenerated on their own — `loadFromDisk` even falls back to a default filter if `.filter` is missing. |
| **Lifecycle** | `delete()` simply removes the three files together; the split costs nothing on teardown. |

This is the LevelDB/RocksDB "data block vs. index block vs. filter block" idea, materialized as three files rather than blocks inside one.

The write loop iterates the memtable **in sorted order**, skips expired entries, feeds each key to the bloom filter, appends the entry to `.data`, and records sparse index offsets:

```java
// SSTable.flushToDisk — sparse index policy
long lastIndexedOffset = -1;
for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memTable.getEntries().entrySet()) {
    // ... write key/value to .data ...
    if (SparseIndexPolicy.shouldAddIndexEntry(currentOffset, lastIndexedOffset)) {
      sparseIndex.put(entry.getKey(), currentOffset);
      lastIndexedOffset = currentOffset;
    }
}
// always index the last key in the table
if (lastIndexedKey != null && lastEntryOffset != lastIndexedOffset) {
  sparseIndex.put(lastIndexedKey, lastEntryOffset);
}
entryCount = writtenEntries;  // stored in data-file header
```

After writing, it reopens `.data`/`.index` read-only channels and keeps the `sparseIndex` (a `NavigableMap`, concretely a `TreeMap`) and `BloomFilter` resident for lookups.

### 5.2 `BloomFilter` internals

A Bloom filter answers exactly one question: **"is this key *definitely not* in this SSTable?"** It can say "definitely no" or "maybe yes" — never "definitely yes." That one-sided guarantee is enough to skip a disk read for the majority of missing keys.

It is just a **bit array** (stored off-heap) plus **k independent hash functions**.

**Construction** — size the bit array and pick `k` from the standard optimal formulas, then allocate an all-zero off-heap bit array:

```java
// src/main/java/io/cascadestore/lsm/sstable/BloomFilter.java (constructor)
public BloomFilter(int expectedEntries, double falsePositiveRate) {
  int numBits = optimalNumOfBits(expectedEntries, falsePositiveRate);
  this.bitArraySize = (numBits + 7) / 8;                      // bits → bytes, rounded up
  this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
  this.allocator = new DirectBufferAllocator();
  this.bitBuffer = allocator.allocate(bitArraySize);
  zeroBuffer();
}
```

**Adding a key** sets `k` bits to 1:

```java
// src/main/java/io/cascadestore/lsm/sstable/BloomFilter.java (add)
for (int i = 0; i < numHashFunctions; i++) {
  int hash = hash(key, i);                                   // seed = i → a different hash per function
  int bitIndex  = (hash & 0x7FFFFFFF) % (bitArraySize * 8);  // clear sign bit, fold into range
  int byteIndex = bitIndex / 8;                              // which byte
  int bitOffset = bitIndex % 8;                              // which bit within that byte
  byte currentByte = bitBuffer.get(byteIndex);
  bitBuffer.put(byteIndex, (byte) (currentByte | (1 << bitOffset))); // set exactly that one bit
}
```

- `hash & 0x7FFFFFFF` clears the sign bit so the value is non-negative (otherwise `%` could return a negative index).
- `% (bitArraySize * 8)` folds the hash into a valid bit position.
- `currentByte | (1 << bitOffset)` flips one bit to 1, leaving the other seven untouched.

**Checking a key** recomputes the same `k` positions:

```java
// src/main/java/io/cascadestore/lsm/sstable/BloomFilter.java (mightContain)
for (int i = 0; i < numHashFunctions; i++) {
  int hash = hash(key, i);
  int bitIndex = (hash & 0x7FFFFFFF) % (bitArraySize * 8);
  byte currentByte = bitBuffer.get(bitIndex / 8);
  if ((currentByte & (1 << (bitIndex % 8))) == 0) {
    return false;                                            // a required bit is 0 → DEFINITELY absent
  }
}
return true;                                                 // all k bits are 1 → MAYBE present
```

**Why false positives but never false negatives:**

- *No false negatives* — once a key is added its `k` bits are 1 forever (bits only flip 0→1). A key that really was inserted always passes.
- *False positives happen* — bits are shared across keys. Two unrelated keys can collectively set all `k` bits that a third, never-added key happens to hash to, so `mightContain` returns `true` for it. The cost is a wasted disk lookup that finds nothing; correctness is preserved.

**The k "independent" hashes from one function** — there is a single multiplicative hash, seeded with the function index, so each of the `k` probes produces a different bit position for the same key:

```java
// src/main/java/io/cascadestore/lsm/sstable/BloomFilter.java (hash)
private int hash(byte[] key, int seed) {   // seed = 0,1,…,k-1
  int h = seed;
  for (byte b : key) h = 31 * h + b;       // classic polynomial hash (same shape as String.hashCode)
  return h;
}
```

**Sizing math** — given `n` expected entries and target false-positive rate `p`:

```java
// src/main/java/io/cascadestore/lsm/sstable/BloomFilter.java (sizing)
private int optimalNumOfBits(int n, double p)       { return (int)(-n * Math.log(p) / (Math.log(2)*Math.log(2))); }
private int optimalNumOfHashFunctions(int n, int m) { return Math.max(1, (int)Math.round((double)m/n*Math.log(2))); }
```

- bits: `m = -n·ln(p) / (ln 2)²`
- hash functions: `k = (m/n)·ln 2`

With the code's default `p = 0.01` (1%) this works out to ≈ **9.6 bits/key** and **k ≈ 7**. So a 10,000-entry SSTable gets a ~12 KB filter that wrongly says "maybe" about 1% of the time.

**Worked micro-example.** Take a 16-bit array and `k = 3`. Adding `"cat"` — suppose its three hashes land on bit indices **2, 9, 14** (indexing the array left-to-right from 0):

```
index:  0  1  2  3  4  5  6  7    8  9 10 11 12 13 14 15
value:  0  0  1  0  0  0  0  0    0  1  0  0  0  0  1  0
              ^                      ^                 ^
              add("cat") sets bits 2, 9, 14
```

> This ruler is the **logical** bit array (index 0 on the left), which is the easy way to reason about the filter. Physically the code packs each byte LSB-first via `1 << (index % 8)`, so the two stored bytes are `0x04` (bit 2) and `0x42` (bits 9 and 14) — i.e. `00000100 01000010` in normal MSB-first binary. Don't confuse the two orderings: only the bit *index* matters for correctness.

Now:

- `mightContain("dog")` with hashes → {2, 5, 9}: bit 5 is `0` → return **false**, no disk read.
- `mightContain("cat")` → {2, 9, 14}: all `1` → **true**, proceed to read `.data`.
- If some `"fox"` also hashed to {2, 9, 14}, it would return **true** despite never being added — a false positive.

This bloom check is the first gate in `SSTable.get` and in `GetStore.lookup`'s SSTable loop.

---

## 6. Read path — `GetStore.lookup()` with bloom + sparse index + buffered scan

`GetStore.lookup` is **thread-safe**: it returns the `byte[]` value directly (no shared `retrievedValue` field). The entire memtable + SSTable search runs under one `readLock` scope with nested list locks so flush cannot hide keys between layers.

```java
// GetStore.lookup
memTableLock.readLock().lock();
try {
  byte[] result = activeMemTable.get(key);
  if (result == null) {
    synchronized (immutableMemTables) {
      synchronized (ssTables) {
        // immutable memtables newest → oldest
        // then SSTables newest → oldest (bloom → get)
      }
    }
  }
  return result;
} finally {
  memTableLock.readLock().unlock();
}
```

`CascadeStore.get()` is simply `return getStore.lookup(key)`.

#### How the sparse index + sequential scan work

Within a single SSTable, `get` is a three-step funnel — bloom filter, then a sparse-index seek, then a short forward scan:

```java
// src/main/java/io/cascadestore/lsm/sstable/SSTable.java (get)
if (bloomFilter != null && !bloomFilter.mightContain(key)) return null; // definitely absent
Map.Entry<ByteArrayWrapper, Long> indexEntry = sparseIndex.floorEntry(new ByteArrayWrapper(key));
return (indexEntry == null)
    ? findKeyInDataFile(key, 16)                    // scan from just after the 16-byte header
    : findKeyInDataFile(key, indexEntry.getValue()); // scan from nearest indexed offset
```

**The sparse-index idea.** A sparse index stores a byte offset for only *some* keys. To locate key `K`:

1. `sparseIndex.floorEntry(K)` — the `NavigableMap` returns the entry with the **greatest indexed key ≤ K**, i.e. an offset *at or before* where `K` would live. If `K` sorts before every indexed key, `floorEntry` returns `null` and the scan starts at offset `16` (right after the header).
2. **Sequentially scan** `.data` forward from that offset. Because entries are physically stored in sorted order, `K` — if present — must lie at or after that point.

**The scan** uses `BufferedDataReader` (64 KiB default window, one per thread via `ThreadLocal`):

```java
// SSTable.findKeyInDataFile — simplified
BufferedDataReader reader = openDataReader();  // ThreadLocal
reader.seek(startPosition);
while (reader.position() < reader.size()) {
  int keyLength = reader.readInt();
  byte[] entryKey = reader.readBytes(keyLength);
  int valueLength = reader.readInt();
  // tombstone or value + timestamp skip ...
}
```

`BufferedDataReader` refills from `FileChannel.read(buffer, absolutePosition)` when the cursor leaves the current window. This replaces 3–4 syscalls per record with ~1 syscall per 64 KiB of sequential data.

**Sparse index at 16 KiB.** With ~1 KB YCSB values, each index block covers roughly 10–15 keys; `floorEntry` lands at or before the target, then the scan walks forward within that block. Index RAM at 1M keys drops from O(N) to O(N / block_size).

---

## 7. Compaction — exactly how it merges

`CompactionService` runs on its own daemon thread (default every 30 min) and is also triggered by `FlushService` when the SSTable count crosses the threshold. The entire cycle runs inside `synchronized (ssTables)`, so no reader/flusher mutates the list mid-merge.

### 7.1 The strategy interface

```java
// src/main/java/io/cascadestore/lsm/core/compaction/CompactionStrategy.java
boolean shouldCompact(List<SSTable> ssTables);
List<SSTable> selectTableToCompact(List<SSTable> ssTables);
int getCompactionOutputLevel(List<SSTable> tablesToCompact);
String getName();
```

The concrete strategy is chosen once from config via a `switch`:

```java
// src/main/java/io/cascadestore/lsm/core/backgroundservice/CompactionService.java (createCompactionStrategy)
return switch (strategyType) {
  case THRESHOLD  -> new ThresholdCompactionStrategy(config);
  case SIZE_TIERED -> new SizeTieredCompactionStrategy(config);
};
```

### 7.2 The merge algorithm (the important part)

```java
// src/main/java/io/cascadestore/lsm/core/backgroundservice/CompactionService.java (doExecute, condensed)
synchronized (ssTables) {
  if (!compactionStrategy.shouldCompact(ssTables)) return;
  List<SSTable> tablesToCompact = compactionStrategy.selectTableToCompact(ssTables);
  if (tablesToCompact.isEmpty()) return;

  MemTable mergedMemTable = new MemTable(config.memTableMaxSizeBytes() * 10); // roomy buffer

  // (a) newest first — this ordering is what makes "first writer wins" == "newest wins"
  tablesToCompact.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

  Map<ByteArrayWrapper, byte[]> mergedData = new HashMap<>();

  // (b) read every selected table's entries in parallel on a daemon cached pool
  ExecutorService executor = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "compaction-io"); t.setDaemon(true); return t;
  });
  List<CompletableFuture<List<Map.Entry<byte[],byte[]>>>> futures = new ArrayList<>();
  for (SSTable ssTable : tablesToCompact)
    futures.add(CompletableFuture.supplyAsync(() -> getEntriesFromSSTable(ssTable), executor));

  // (c) consume results in the SAME (newest-first) order; keep the first value seen per key
  for (int i = 0; i < tablesToCompact.size(); i++) {
    for (Map.Entry<byte[],byte[]> entry : futures.get(i).get()) {
      ByteArrayWrapper key = new ByteArrayWrapper(entry.getKey());
      if (!mergedData.containsKey(key)) {          // newest wins; older duplicates ignored
        byte[] value = entry.getValue();
        if (value != null) mergedData.put(key, value); // tombstones/nulls dropped
      }
    }
  }
  executor.shutdown();
  if (mergedData.isEmpty()) return;

  // (d) build a new SSTable at the strategy-chosen output level
  for (Map.Entry<ByteArrayWrapper, byte[]> e : mergedData.entrySet())
    mergedMemTable.put(e.getKey().getData(), e.getValue(), 0);

  int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
  long newSeq = sequenceNumber.getAndIncrement();
  SSTable compactedTable = new SSTable(mergedMemTable, config.dataDirectory(), outputLevel, newSeq);

  // (e) swap: add new table, delete + close old ones
  ssTables.add(compactedTable);
  for (SSTable old : tablesToCompact) { ssTables.remove(old); old.delete(); old.close(); }
}
```

Key mechanics:

- **Newest-wins dedup.** Tables are sorted by descending sequence number, entries are consumed in that order, and `mergedData.putIfAbsent`-style logic (`if (!containsKey)`) keeps the first — i.e. newest — value for each key.
- **Tombstone collapse.** A `null`/tombstone value is simply *not* inserted into `mergedData`, so a deleted key that is not shadowed by a newer live value disappears from the output table. (Note: because reads already stop at the newest layer, a tombstone only needs to survive long enough to shadow older tables; once all older copies are compacted together, the tombstone can be dropped.)
- **Parallel reads.** Each source table is scanned on a cached daemon thread pool; results are joined in deterministic order so correctness doesn't depend on completion timing.
- **`getEntriesFromSSTable`** uses `getRange(null, null)` (full scan) and filters out tombstones/nulls at the source.

### 7.3 Threshold strategy

Trigger on total count; compact the level with the most tables (needs ≥ 2); output to `level + 1`.

```java
// src/main/java/io/cascadestore/lsm/core/compaction/ThresholdCompactionStrategy.java
public boolean shouldCompact(List<SSTable> ssTables) { return ssTables.size() >= config.compactionThreshold(); }
public int getCompactionOutputLevel(List<SSTable> t)  { return t.isEmpty() ? 0 : t.get(0).getLevel() + 1; }
// selectTableToCompact: group by level, pick the level with the max #tables, require >= 2
```

### 7.4 Size-tiered strategy

Groups tables into size buckets (`bucketLow*avg ≤ size ≤ bucketHigh*avg`, i.e. 0.5×–1.5×) and compacts the largest bucket that has ≥ `minThreshold` (4) tables, capped at `maxThreshold` (32); output level is `max(level) + 1`.

```java
// src/main/java/io/cascadestore/lsm/core/compaction/SizeTieredCompactionStrategy.java
public boolean shouldCompact(List<SSTable> ssTables) {
  if (ssTables.size() < minThreshold) return false;
  for (List<SSTable> bucket : getBuckets(ssTables).values())
    if (bucket.size() >= minThreshold) return true;
  return false;
}
// getBuckets: sort by size, assign each table to a bucket whose running average
// is within [bucketLow, bucketHigh] of the table size, else start a new bucket.
```

### 7.5 `getCompactionOutputLevel` — what it does and why `+1`

```java
int getCompactionOutputLevel(List<SSTable> tablesToCompact);
```

This answers: **"when I merge these tables, what `level` should the resulting SSTable get?"** The returned int becomes part of the new file name (`sst_L<level>_S<seq>.data`) and is stored in its data-file header.

Levels are the "L" in LSM-tree — a hierarchy where **higher level = older, larger, more-compacted data**. Fresh flushes always land at **level 0**; compaction pushes merged data **down** to the next level. That cascading of data through levels is exactly what the engine is named after.

- **Threshold strategy** merges tables that are all at the same level, so output = `level + 1`:

```java
// src/main/java/io/cascadestore/lsm/core/compaction/ThresholdCompactionStrategy.java
public int getCompactionOutputLevel(List<SSTable> t) { return t.isEmpty() ? 0 : t.get(0).getLevel() + 1; }
```

- **Size-tiered strategy** selects by size bucket (the tables may span levels), so it uses `max(level) + 1`:

```java
// src/main/java/io/cascadestore/lsm/core/compaction/SizeTieredCompactionStrategy.java
public int getCompactionOutputLevel(List<SSTable> t) {
  if (t.isEmpty()) return 0;
  int maxLevel = t.stream().mapToInt(SSTable::getLevel).max().orElse(0);
  return maxLevel + 1;
}
```

**Why the `+1` matters.** It drives data downward and prevents an infinite compaction loop. If the merged output stayed at the same level, it would immediately re-qualify for compaction with its siblings. Bumping the level moves the result into a new tier, where it accumulates until *that* level fills up and is merged down again. Over time keys migrate L0 → L1 → L2 → …, each level holding fewer, larger, more-consolidated tables — the classic LSM shape that trades write amplification for bounded read/space amplification. (The threshold strategy's `selectTableToCompact` groups by level, so its "all tables at one level" assumption holds; size-tiered groups by size bucket, which is why it defensively takes `max(level)`.)

---

## 8. Crash recovery — replaying the WAL

Recovery runs during construction, **after** SSTables are discovered and the WAL is opened, and **before** background services start.

### 8.1 Startup sequence

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (constructor, ordering)
loadSSTables();                                  // 1. discover sst_L*_S*.data on disk
Files.createDirectories(Paths.get(walDirectory));
this.wal = new WALImpl(walDirectory);            // 2. open/rotate WAL
recover();                                       // 3. replay WAL into active memtable
// 4. only now create + start Flush/Compaction/Cleanup services
```

### 8.2 SSTable discovery

`loadSSTables` parses each `sst_L<level>_S<seq>.data` filename to recover its level and sequence number, advances the global `sequenceNumber` past the highest seen, opens each table (loads its bloom filter + sparse index), then sorts newest-first.

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (loadSSTables, core)
int level = Integer.parseInt(fileName.substring(levelStart, levelEnd));
long seq  = Long.parseLong(fileName.substring(seqStart, seqEnd));
sequenceNumber.updateAndGet(current -> Math.max(current, seq + 1)); // never reuse a seq
ssTables.add(new SSTable(config.dataDirectory(), level, seq));
...
ssTables.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));
```

### 8.3 The replay itself

```java
// src/main/java/io/cascadestore/lsm/core/store/CascadeStore.java (recover)
private void recover() {
  try {
    recovering.set(true);                        // suppress WAL re-writes during replay
    List<Record> records = wal.readRecords();    // read every wal_*.log, all records
    if (records.isEmpty()) { logger.info("No WAL records to recover"); return; }

    records.sort((r1, r2) -> Long.compare(r1.getSequenceNumber(), r2.getSequenceNumber())); // apply in order
    long maxSeqNum = records.get(records.size() - 1).getSequenceNumber();
    sequenceNumber.set(maxSeqNum + 1);           // continue numbering after the log

    for (Record record : records) {
      if (record instanceof PutRecord putRecord) {
        activeMemTable.put(putRecord.getKey(), putRecord.getValue(), putRecord.getTtlSeconds());
      } else if (record instanceof DeleteRecord) {
        activeMemTable.delete(record.getKey());  // re-insert tombstone
      }
    }
  } catch (IOException e) {
    logger.error("Error recovering from WAL", e);
  } finally {
    recovering.set(false);                       // normal WAL logging resumes
  }
}
```

Why this is correct:

- **Ordering.** Records are sorted by sequence number so replay applies mutations in their original order (a later delete correctly shadows an earlier put, etc.).
- **Idempotent-ish rebuild.** Replaying `put`/`delete` through the normal MemTable API reconstructs exactly the in-memory state that existed before the crash for anything not yet flushed to an SSTable.
- **No double logging.** `recovering.set(true)` makes `PutStore`/`DeleteStore` skip `wal.append*`, so replay doesn't append the history back onto the log.
- **Sequence continuity.** `sequenceNumber` is bumped past both the highest SSTable seq (in `loadSSTables`) and the highest WAL seq (here), so new writes never collide with recovered identifiers.

### 8.4 WAL truncation after durable flush

When all memtable state is on disk, the WAL is deleted so the next startup does not replay millions of records:

```java
// CascadeStore.truncateWalIfAllDataFlushed()
// Preconditions: immutableMemTables empty, activeMemTable empty
wal.sync();
wal.deleteAllLogs();   // removes wal_*.log, creates fresh empty segment
```

Triggered from `FlushService` (post-batch hook) and `shutdown()` (after final flush). YCSB run phase then logs `No WAL records to recover`.

### 8.5 Reading records back

`WALReaderImpl.readRecords` reads every WAL file (found and sorted by `WALManagerImpl.findLogFiles`) and deserializes each framed record using the exact inverse of the writer's layout, dispatching on the 1-byte type tag.

```java
// src/main/java/io/cascadestore/lsm/wal/reader/WALReaderImpl.java (per record)
byte recordType = ...;           // 0x01 put, 0x02 delete
long seqNum     = ...;           // 8 bytes
int keyLength   = ...;  byte[] key = ...;
if (recordType == PUT_RECORD) {
  int valueLength = ...;  byte[] value = ...;  long ttlSeconds = ...;
  records.add(new PutRecord(seqNum, key, value, ttlSeconds));
} else if (recordType == DELETE_RECORD) {
  records.add(new DeleteRecord(seqNum, key));
} else {
  throw new IOException("Unknown record type: " + recordType);
}
```

> **Durability note / current limitation.** The WAL is never truncated after a flush, so recovery replays the entire retained log history rather than only the un-flushed tail. This is safe (flushed keys are simply re-inserted into the fresh memtable) but means recovery time grows with total log size until logs are cleared via `clear()`/`deleteAllLogs()`.

---

## 9. Delete path & tombstones

Deletes are logically identical to puts but insert a **tombstone** `ValueEntry` (header-only, `tombstone = 0x01`). `DeleteStore` first confirms the key exists (via `GetStore`), then logs to the WAL, then inserts the tombstone under the read lock — same structure as `PutStore`.

```java
// src/main/java/io/cascadestore/lsm/core/store/DeleteStore.java (delete)
int getResult = getStore.get(key);
if (getResult != GetStore.RESULT_SUCCESS) return RESULT_KEY_NOT_FOUND; // nothing to delete
if (!recovering.get()) wal.appendDeleteRecord(key);
return deleteFromMemTable(key);                                        // read lock + memtable.delete
```

A tombstone shadows older live values in lower layers during reads, and is physically removed during compaction once no older copy of the key remains.

---

## 10. TTL cleanup

`CleanupService` (default every 1 min) scans memtables for entries whose `expirationTime` has passed and writes a tombstone into the **active** memtable for each. It never mutates immutable memtables or SSTables directly — expired entries there are filtered on read and dropped during flush/compaction (`SSTable.flushToDisk` skips `valueEntry.isExpired()`).

```java
// src/main/java/io/cascadestore/lsm/core/backgroundservice/CleanupService.java (pattern)
memTableLock.readLock().lock();                 // scan phase under read lock
try { /* collect keys where entry.isExpired() */ } finally { memTableLock.readLock().unlock(); }
if (!keysToRemove.isEmpty()) {
  memTableLock.writeLock().lock();              // mutate phase under write lock
  try { for (byte[] key : keysToRemove) activeMemTable.delete(key); }
  finally { memTableLock.writeLock().unlock(); }
}
```

This is the one place that intentionally acquires the read lock for scanning and then the write lock for mutation, in two separate critical sections.

---

## 11. End-to-end sequence (put of a key that fills the table)

1. `client.put(k, v, ttl)` → `CascadeStore.put` → `PutStore.put`.
2. `PutStore` appends a `PutRecord` to the WAL; `noteBytesWritten` may batch fsync up to 1 MiB (forced sync at switch/truncate).
3. `PutStore.putInMemTable` takes the **read** lock and calls `activeMemTable.put`.
4. MemTable allocates an off-heap `ValueEntry`, inserts into the `ConcurrentSkipListMap`, updates `sizeBytes`.
5. If the table is full, `put` returns `false`/`isFull()` → `PutStore` returns `RESULT_MEMTABLE_FULL`.
6. `CascadeStore.switchMemTable` takes the **write** lock, `wal.sync()`, seals the table, moves it to `immutableMemTables`, creates a fresh active table, and calls `flushMemTables()`.
7. `FlushService` writes the sealed table to `sst_L0_S<seq>.{data,index,filter}` (sparse index), atomically publishes to `ssTables`, removes from immutable list, may truncate WAL.
8. If SSTable count ≥ threshold, `CompactionService` merges tables newest-wins, drops tombstones, writes a higher-level table, and deletes the inputs.
9. A later `client.get(k)` calls `GetStore.lookup`: active → immutable → SSTables (bloom + sparse index + `BufferedDataReader` scan).
10. After a crash, construction discovers SSTables, opens the WAL, and `recover()` replays remaining records in sequence order to rebuild the active memtable.

---

## 12. Class reference index

| Class | Package | Role | Key data structure |
| --- | --- | --- | --- |
| `CascadeStore` | `core.store` | Top-level `Storage`; owns state, switching, lifecycle | `ReentrantReadWriteLock`, two `ArrayList`s, `AtomicLong` |
| `PutStore` / `GetStore` / `DeleteStore` | `core.store` | Operation delegates | `GetStore.lookup()` thread-safe |
| `BufferedDataReader` | `io` | 64 KiB SSTable data scan window | `ByteBuffer` + `FileChannel` |
| `SparseIndexPolicy` | `sstable.index` | 16 KiB sparse index spacing | — |
| `WalSyncPolicy` | `wal.manager` | 1 MiB WAL group-commit threshold | — |
| `MemTable` | `memtable` | Sorted concurrent write buffer | `ConcurrentSkipListMap<ByteArrayWrapper, ValueEntry>` |
| `MemTable.ValueEntry` | `memtable` | Off-heap value + metadata | direct `ByteBuffer` (16-byte header) |
| `DirectBufferAllocator` | `memory` | Off-heap allocation + deterministic free | `synchronized` `ArrayList<ByteBuffer>` |
| `ByteArrayWrapper` | `api` | Content-equality, unsigned-ordered key | `byte[]` |
| `WALImpl` / `WALWriterImpl` / `WALReaderImpl` / `WALManagerImpl` / `WALFileImpl` | `wal.*` | Durable append log (fsync, rotation, replay) | `FileChannel`, `AtomicLong` |
| `PutRecord` / `DeleteRecord` | `wal.record` | Immutable log records (defensive copies) | `byte[]` |
| `SSTable` | `sstable` | Immutable on-disk sorted table | `NavigableMap`/`TreeMap` sparse index + `BloomFilter` |
| `SSTableEntry` | `sstable` | Immutable single-entry record | `byte[]` (defensive copies) |
| `BloomFilter` | `sstable` | Probabilistic negative lookups | off-heap bit array (`ByteBuffer`) |

---

## 13. YCSB benchmark — multi-shard data flow

The production engine is a single `CascadeStore` per process. The **YCSB binding** (`CascadeStoreYcsbClient`, test-jar) adds optional sharding for multi-threaded benchmarks without internal store locking across cores.

### 13.1 Layout

```
cascadestore.datadir=/tmp/ycsb-data
cascadestore.shards=4

/tmp/ycsb-data/shard-0/   ← full CascadeStore (wal/, sst_*, memtables)
/tmp/ycsb-data/shard-1/
...
```

Each shard is an independent LSM tree: own WAL, memtables, SSTables, flush/compaction threads.

### 13.2 Request routing

```java
byte[] storageKey = (table + ":" + userKey).getBytes(UTF_8);
int shard = (Arrays.hashCode(storageKey) & 0x7FFFFFFF) % shardCount;
CascadeStore store = shards[shard];
```

| Operation | Routing |
| --- | --- |
| `insert` / `read` / `update` / `delete` | Hash to one shard |
| `scan` | Open iterator on **every** shard; k-way merge by key in client |

`SharedCascadeStoreRegistry` reference-counts one `CascadeStore` per `(config, shard)` so all YCSB threads in a JVM share the same shard pool.

### 13.3 Recommended benchmark settings

```bash
SHARDS=4 THREADS=4 \
JAVA_TOOL_OPTIONS="-Xms4G -Xmx8G" \
./scripts/run-ycsb.sh all workloada LEVEL_TIERED
```

- `THREADS` should match `SHARDS` to avoid multiple threads hammering one store.
- Run `mvn -DskipTests package` after code changes (`ycsb-env.sh` may not rebuild automatically).

### 13.4 What sharding does *not* do

- No cross-shard transactions or replication.
- No automatic rebalancing — hash partitioning is fixed at load time.
- Not used by the embedded `CascadeStore` API directly; only the YCSB adapter.

For problem/solution narrative and measured throughput, see `OPTIMIZATIONS.md`.
| `FlushService` / `CompactionService` / `CleanupService` | `core.backgroundservice` | Scheduled maintenance daemons | `ScheduledExecutorService` |
| `ThresholdCompactionStrategy` / `SizeTieredCompactionStrategy` | `core.compaction` | Pluggable merge policies | level/size grouping maps |
| `CascadeConfig` | `config` | Immutable configuration record | Java `record` |
