# CascadeStore

[![Java Version](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.java.net/projects/jdk/17/)

CascadeStore is a Java 17 LSM-tree storage engine that keeps hot data in memory, persists mutations through a WAL, and serves reads from sorted on-disk tables. Direct off-heap `ByteBuffer` allocation and a `ConcurrentSkipListMap`-backed MemTable reduce GC overhead while preserving ordered iteration.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Performance](#performance)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)

## Overview

The project implements the classic LSM pattern: writes land in a mutable MemTable, are appended to a WAL, and are eventually flushed into immutable SSTables. Compaction merges overlapping tables so point lookups and range scans stay bounded as data grows. The design fits append-heavy workloads, event streams, and other write-biased use cases.

Primary components:
- In-memory MemTable for recent puts and deletes
- Immutable SSTables stored as separate data, index, and bloom-filter files
- WAL replay for crash recovery
- Scheduled flush, compaction, and TTL cleanup services

## Features

- **Write-optimized path**: Batches mutations in memory before sequential disk writes.
- **Ordered storage**: Keys remain sorted for efficient range scans and iterators.
- **Background compaction**: Threshold, size-tiered, or level-tiered policies merge SSTables automatically.
- **TTL entries**: Optional expiration timestamps on individual keys.
- **Tombstones**: Explicit delete markers propagate through flush and compaction.
- **Bloom filters**: Off-heap filters short-circuit negative lookups before disk I/O.
- **Direct memory**: Native-order direct buffers with explicit cleanup via `Unsafe.invokeCleaner()`.
- **Concurrency**: `ReentrantReadWriteLock` guards MemTable rotation; skip-list maps support concurrent access.
- **Parallel compaction reads**: A cached thread pool loads SSTable inputs in parallel during merges.

## Requirements

- JDK 17 or newer
- Apache Maven 3.6+ available on your `PATH` (no Maven Wrapper ships with this repo)

## Installation

### Maven dependency

Add the artifact to your application `pom.xml` when published locally or to a repository mirror:

```xml
<dependency>
    <groupId>io.cascadestore</groupId>
    <artifactId>cascade-store</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### Build from source

Clone the repository and build with a system Maven install:

```bash
git clone https://github.com/ArnabKarmakar1108/CascadeStore.git
cd CascadeStore
mvn clean install
```

You can also use the provided `Makefile`, which invokes `mvn` directly (`make test`, `make package`, etc.).

## Usage

### Basic operations

```java
// Default configuration
Storage storage = new CascadeStore();

// Custom MemTable size, data directory, and compaction threshold
Storage storage = new CascadeStore(
    10 * 1024 * 1024,  // 10 MB MemTable cap
    "./data",
    4                  // compact once four SSTables exist at a level
);

storage.put("key".getBytes(), "value".getBytes());

byte[] value = storage.get("key".getBytes());

storage.delete("key".getBytes());

boolean present = storage.containsKey("key".getBytes());

List<byte[]> keys = storage.listKeys();

int count = storage.size();

storage.clear();

storage.shutdown();
```

### Range scans

```java
byte[] startKey = "a".getBytes();
byte[] endKey = "z".getBytes();

Map<byte[], byte[]> slice = storage.getRange(startKey, endKey);

try (KeyValueIterator iterator = storage.getIterator(startKey, endKey)) {
    while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        // handle entry
    }
}
```

### Time-to-live

```java
// Expire after 60 seconds
storage.put("key".getBytes(), "value".getBytes(), 60);
```

### Compaction tuning

CascadeStore supports three compaction strategies via `CompactionStrategyType`:

- **THRESHOLD** — compact when a level accumulates enough SSTables (default).
- **SIZE_TIERED** — group similarly sized SSTables and merge them.
- **LEVEL_TIERED** — L0 count trigger plus per-level byte budgets; L0 jobs include overlapping L1 files so deeper levels stay non-overlapping.

```java
Storage storage = new CascadeStore(
    10 * 1024 * 1024,
    "./data",
    4,
    CompactionStrategyType.SIZE_TIERED
);

Storage levelTiered = new CascadeStore(
    10 * 1024 * 1024,
    "./data",
    4,
    CompactionStrategyType.LEVEL_TIERED
);

CascadeConfig config = new CascadeConfig(
    10 * 1024 * 1024,
    "./data",
    4,
    30,   // compaction check interval (minutes)
    1,    // TTL cleanup interval (minutes)
    10,   // flush interval (seconds)
    CompactionStrategyType.THRESHOLD
);
Storage tuned = new CascadeStore(config);
```

## Architecture

Package layout under `io.cascadestore.lsm`:

1. **core** — `CascadeStore` orchestrates reads, writes, MemTable rotation, and background services.
2. **memtable** — `MemTable` stores sorted, off-heap value entries until flush.
3. **sstable** — `SSTable` handles flush format, bloom filters, sparse indexes, and range access.
4. **wal** — append-only log types and I/O for durable recovery.

See `src/main/java/io/cascadestore/lsm/docs/ARCHITECTURE.md` for a deeper component breakdown.

## Performance

Benchmarks on modern hardware have reported roughly:

- ~1M writes/sec for sustained insert workloads
- ~500K point reads/sec with warm bloom filters
- Efficient range iteration for small to medium key spans

Real numbers depend on disk speed, MemTable sizing, compaction pressure, and key/value sizes.

## Contributing

Issues and pull requests are welcome. Open a discussion or PR on GitHub with a concise description of the change and any test coverage you added.

## Acknowledgements

- The LSM-tree model comes from ["The Log-Structured Merge-Tree (LSM-Tree)"](https://www.cs.umb.edu/~poneil/lsmtree.pdf) by Patrick O'Neil et al.
- Design cues were taken from LevelDB, RocksDB, Cassandra, and similar open-source LSM engines.
