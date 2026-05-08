# Core Package

The `io.cascadestore.lsm.core` package wires together the LSM engine: public storage API, read/write delegates, immutable layout snapshots, and scheduled background work.

## CascadeStore

`CascadeStore` implements `Storage` and is the main entry point. It owns:

- An **active MemTable** for incoming writes
- A **WAL** for crash recovery
- Lists of **immutable MemTables** and **SSTables** published through `StorageVersion`
- A shared **BlockCache** (optional; disabled when size is 0)
- Daemon-thread **background services** for flush, compaction, and TTL cleanup

Construction accepts either convenience arguments (MemTable size, data directory, compaction threshold) or a full `CascadeConfig`.

## Read / Write Delegates

Operations are split into focused stores so locking and I/O policies stay localized:

| Class | Responsibility |
|-------|----------------|
| `PutStore` | WAL append, MemTable insert, rotation when full |
| `GetStore` | Layered lookup: active MemTable → immutables → SSTables |
| `DeleteStore` | Tombstone write; rejects deletes for keys that do not exist |
| `MergeStore` | K-way merge of sorted sources for compaction and scans |

`GetStore` consults bloom filters (optionally in parallel when enough SSTables are open), then delegates to each `SSTable` for index-guided scans.

## StorageVersion

`StorageVersion` is an immutable snapshot of the immutable MemTable list and SSTable list at a point in time. When published:

1. Lists are defensively copied
2. Every SSTable in the snapshot is **pinned** so compaction cannot delete files mid-read
3. Readers swap the volatile `storageVersion` reference and work without holding the MemTable write lock across disk I/O

When a newer version replaces an old one, the previous version's `release()` unpins its SSTables.

## Concurrency Model

- `ReentrantReadWriteLock` (**memTableLock**) guards MemTable rotation and dependency updates
- Reads take the read lock only while checking the active MemTable; SSTable tiers come from the pinned `StorageVersion`
- WAL appends use a separate read/write lock so file rotation does not block unrelated appends on the fast path

## Background Services

All services extend `AbstractBackgroundService` and run on a shared daemon `ScheduledExecutorService`.

| Service | Trigger | Effect |
|---------|---------|--------|
| `FlushService` | MemTable rotation or periodic schedule | Writes immutable MemTable to a new L0 SSTable, updates layout |
| `CompactionService` | Periodic schedule + strategy hooks | Merges SSTables per the active compaction policy |
| `CleanupService` | Periodic schedule | Removes expired TTL entries |

Compaction strategy is selected at startup via `CompactionStrategyType`:

- **ThresholdCompactionStrategy** — file-count trigger per level
- **SizeTieredCompactionStrategy** — groups similar-sized inputs
- **LevelTieredCompactionStrategy** — L0 count plus per-level byte budgets with L1 overlap pulls

## Recovery

On startup, `CascadeStore` replays WAL records into the active MemTable, then loads existing SSTables from disk. A recovering flag suppresses side effects (such as spurious deletes) until replay completes.

## Related Documentation

- [ARCHITECTURE.md](../docs/ARCHITECTURE.md) — detailed write/read/recovery diagrams
- [DATA_FLOW.md](../docs/DATA_FLOW.md) — how records move between layers
- [OPTIMIZATIONS.md](../docs/OPTIMIZATIONS.md) — version pinning, parallel bloom, block cache
