# WAL Package

The write-ahead log (`io.cascadestore.lsm.wal`) makes mutations durable before they are visible in the MemTable. After a crash, recovery replays log records to rebuild in-memory state.

## Layout

| Subpackage | Contents |
|------------|----------|
| `wal` | `WAL` interface and `WALImpl` facade |
| `wal.record` | `PutRecord`, `DeleteRecord` — immutable types with defensive byte copies |
| `wal.file` | `WALFileImpl` — `FileChannel` wrapper for one log segment |
| `wal.manager` | `WALManagerImpl` — create, rotate, discover, delete segments |
| `wal.reader` | `WALReaderImpl` — sequential binary deserialization |
| `wal.writer` | `WALWriterImpl` — serialization and durability policy |

## Durability Model

Writes are **group-committed** rather than fsynced per record:

- `WalSyncPolicy.DEFAULT_SYNC_BATCH_BYTES` (1 MiB) triggers `FileChannel.force(true)` after enough bytes are buffered
- Rotation, MemTable switch, and explicit `sync()` always force the log to disk
- `WALWriterImpl` serializes into pooled `ByteBuffer` instances via `RecordBufferPool`

`WALImpl` uses a `ReadWriteLock` so routine appends take the read lock on the fast path; file rotation acquires the write lock only when the active segment is full.

## Binary Record Format

**Put record**

```
[1 byte: type=0x01][8 bytes: seqNum][4 bytes: keyLen][key]
[4 bytes: valueLen][value][8 bytes: ttlSeconds]
```

**Delete record**

```
[1 byte: type=0x02][8 bytes: seqNum][4 bytes: keyLen][key]
```

## File Management

| Setting | Value |
|---------|-------|
| Directory | `<dataDirectory>/wal/` |
| Naming | `wal_%020d.log` (zero-padded sequence) |
| Rotation | Default 64 MiB per segment |
| Sequence | Global `AtomicLong`, monotonically increasing |

## Component Reference

| Class | Role |
|-------|------|
| `WAL` | Append put/delete, read all records, close |
| `WALImpl` | Composes manager, reader, writer; coordinates locking |
| `WALWriterImpl` | Buffer serialization, batched `force(true)` |
| `WALReaderImpl` | Sequential read across all segments |
| `WALManagerImpl` | Segment lifecycle and sync accounting |
| `WALFileImpl` | Single-segment channel I/O |
| `PutRecord` / `DeleteRecord` | Typed, immutable log entries |

## Usage

```java
WAL wal = new WALImpl("./data/wal");

wal.appendPutRecord("key".getBytes(), "value".getBytes(), 0);
wal.appendDeleteRecord("key".getBytes());

List<Record> records = wal.readRecords();  // recovery
wal.close();
```

## Design Notes

1. **Separation of concerns** — manager, reader, and writer are independently testable
2. **Throughput vs safety** — batched fsync amortizes syscall cost; rotation and MemTable switch still force durability boundaries
3. **Extensibility** — new record types can be added with a distinct type byte without changing existing readers
