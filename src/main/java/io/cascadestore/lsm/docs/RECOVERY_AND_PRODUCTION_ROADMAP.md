# Crash Recovery and Production Roadmap

Discussion document for CascadeStore’s current durability model, its limits, and a pragmatic path from POC to **production-grade single-node** storage — without building a full distributed database.

---

## 1. How crash recovery works today

### 1.1 Startup order

On `CascadeStore` construction:

```
1. loadSSTables()     — scan data dir for sst_L*_S*.data, open index + bloom, sort newest-first
2. WALImpl(wal/)      — discover or create WAL segment files
3. recover()          — replay WAL into the active MemTable
4. start background services (flush, compaction, TTL cleanup)
5. wire PutStore / GetStore / DeleteStore / MergeStore
```

There is **no separate manifest file**, **no checkpoint record**, and **no incremental snapshot** of MemTable state. Durability is **SSTables on disk + whatever remains in the WAL**.

### 1.2 WAL replay (the recovery mechanism)

`recover()` in `CascadeStore`:

1. Sets `recovering = true` so live puts/deletes **skip new WAL appends** during replay (MemTable updates still apply).
2. Calls `wal.readRecords()`, which reads **every** `wal_*.log` segment in order.
3. Sorts all records by **sequence number**.
4. Replays each record into the **active MemTable**:
   - `PutRecord` → `memTable.put(key, value, ttl)`
   - `DeleteRecord` → `memTable.delete(key)` (tombstone)
5. Sets `sequenceNumber` to `max(seq) + 1`.
6. Sets `recovering = false`.

So: **yes, the entire WAL is replayed** on every restart whenever log files exist. There is no “replay only since last checkpoint” logic.

### 1.3 What is *not* snapshotting

| Mechanism | Present? | Role |
|-----------|----------|------|
| MemTable flush → SSTable | Yes | Persists sorted runs to `.data` / `.index` / `.filter` |
| WAL truncation (`deleteAllLogs`) | Yes | **Only** form of “checkpoint” today |
| RocksDB-style MANIFEST | No | No single file listing live SSTables + flush seq |
| Checkpoint / snapshot file | No | No recorded “durability frontier” sequence number |
| Point-in-time MemTable snapshot | No | Recovery always rebuilds MemTable from WAL |

`StorageVersion` (Phase F) is a **runtime read snapshot** for concurrent lookups. It is unrelated to crash recovery.

### 1.4 WAL truncation — the only checkpoint-like behavior

WAL files are deleted only when **all** of the following hold (`truncateWalIfAllDataFlushed`):

- No immutable MemTables waiting to flush
- Active MemTable is **empty**

Typical cases:

| Scenario | SSTables | WAL after restart |
|----------|----------|-------------------|
| Clean shutdown after load-only workload (YCSB load → flush → empty active) | Hold all keys | **Empty** — nothing to replay |
| Steady writes, active MemTable non-empty | Hold flushed data + active holds recent writes | **Replay full WAL** into active MemTable |
| Crash mid-write | SSTables may be stale; WAL has durable records since last fsync | **Replay WAL**; MemTable wins on read over older SSTable versions |

Truncation is **all-or-nothing**: `wal.sync()` then `deleteAllLogs()` — not “truncate up to sequence N”.

### 1.5 Read path after recovery (why replay is still correct)

Lookups order:

1. Active MemTable (includes WAL replay)
2. Immutable MemTables
3. SSTables (newest sequence first)

If a key exists in both an SSTable and the replayed MemTable, the MemTable layer is consulted first, so the **latest WAL state wins**. Correctness does not require deleting overlapping keys from SSTables on recovery.

### 1.6 Recovery limitations (POC → production gaps)

| Limitation | Impact |
|------------|--------|
| **Full WAL scan on every startup** | Recovery time ∝ WAL bytes, even when most data is already in SSTables |
| **No flush frontier in metadata** | Cannot skip replay of records already persisted in SSTables |
| **Redundant work** | Re-applies puts for keys already on disk; wastes CPU and inflates active MemTable |
| **No CRC / torn-write detection per record** | Corrupt tail of WAL may fail recovery hard (`IOException`) |
| **No parallel recovery** | Single-threaded replay |
| **TTL on replay** | Replayed puts use stored TTL; expired entries depend on MemTable TTL logic at read time |

These are acceptable for benchmarks and demos; they are **not** acceptable for a production story without a planned fix (see §3.2).

### 1.7 Recovery flow (diagram)

```
                    ┌─────────────────┐
                    │  Process start  │
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │  loadSSTables()             │
              │  (all sst_L*_S*.data)       │
              └──────────────┬──────────────┘
                             │
              ┌──────────────▼──────────────┐
              │  WAL segments exist?        │
              └──────┬──────────────┬───────┘
                     │ no           │ yes
                     ▼              ▼
              ┌──────────┐   ┌─────────────────────┐
              │  Ready   │   │  readRecords()      │
              │  (empty  │   │  sort by seqNum     │
              │   WAL)   │   │  replay → MemTable  │
              └──────────┘   └──────────┬──────────┘
                                        │
                             ┌──────────▼──────────┐
                             │  recovering = false │
                             │  serve traffic      │
                             └─────────────────────┘

  Background (when safe):  active empty + no immutables
                             → wal.sync() + deleteAllLogs()
```

---

## 2. Your future plans — discussion

Source: `future_plans.txt`. Evaluated as **resume + production-grade single-node** work, not as a distributed database.

### 2.1 Observability (Prometheus metrics)

**Verdict: High value, do this.**

Suggested metrics (aligned with your list and what interviewers expect):

| Metric | Why it matters |
|--------|----------------|
| `memtable_bytes` / `memtable_entries` | Capacity planning, flush triggers |
| `flush_duration_seconds` | Latency SLO, stall detection |
| `compaction_duration_seconds` | Strategy comparison under stress |
| `bloom_filter_hit_ratio` | Validates read-path optimizations |
| `block_cache_hit_ratio` | Cache tuning (when enabled) |
| `read_amplification` (logical) | Keys / SSTables consulted per get |
| `write_amplification` (logical) | Bytes written / bytes ingested |
| `wal_fsync_latency_seconds` | Write path bottleneck |
| `sstable_count_by_level` | LSM health |
| `compaction_pending` | Backpressure signal |

**Implementation sketch (later):** Micrometer + Prometheus endpoint, or a small `MetricsRegistry` interface with a Prometheus adapter. Hook at `FlushService`, `CompactionService`, `GetStore`, `WALManagerImpl.sync()`, `BlockCache`.

**Resume angle:** *“Instrumented LSM engine with Prometheus metrics for flush/compaction latency, cache/bloom effectiveness, and amplification.”*

**Performance cost:** Negligible if counters are atomic and histograms sampled.

---

### 2.2 Compare against RocksDB / LevelDB

**Verdict: Valuable if framed correctly — not as “we win.”**

| Do | Don’t |
|----|-------|
| Same YCSB workload, same record size, same thread count | Claim parity with production-tuned RocksDB |
| Report throughput **and** p99 latency | Single headline ops/s only |
| Note embedded Java vs native C++ | Ignore hardware / JVM differences |
| Show **your** optimization delta (before/after Phase F) | Imply Cassandra-class maturity |

**Suggested artifact:** `benchmark/COMPARISON.md` with:

- Workload C @ 100k (read-heavy hero)
- Workload A @ 100k (balanced)
- Environment table (CPU, RAM, JDK, RocksDB version, `BLOCK_CACHE_MB`, etc.)
- One paragraph: *“CascadeStore is an educational/single-node embedded engine; RocksDB is the throughput ceiling reference.”*

**Resume angle:** *“Benchmarked against RocksDB/LevelDB on YCSB Workloads A/C under identical scale; documented trade-offs of embedded Java LSM vs native stores.”*

---

### 2.3 Async WAL replication (leader → followers, no consensus)

**Verdict: Reasonable **optional** stretch goal — **not** a distributed database.**

Your sketch:

```
Leader  ← all writes
   ↓ (async serialized WAL stream)
Follower 1, Follower 2  ← reads allowed
```

**What this gives you:**

- A credible **“replication”** bullet without Raft, elections, or split-brain handling
- Read scaling story (if followers apply WAL and serve reads)
- Interview talking point on **durability vs availability** trade-offs

**What it does *not* give you:**

- HA on leader failure (no automatic failover)
- Linearizable reads on followers (lagging replicas)
- Split-brain safety

**Performance impact:**

| Path | Effect |
|------|--------|
| Leader writes | Small CPU/network cost to ship WAL bytes async |
| Leader reads | Unchanged if reads stay local |
| Follower reads | Network not involved; local LSM after apply |
| Benchmarks on leader | Slight write throughput drop; document `REPLICATION=off` for peak numbers |

**Honest naming:** *“Async WAL shipping to standby nodes”* — not *“distributed CascadeStore.”*

**Scope control:**

1. Phase 1: TCP/gRPC stream of **WAL records** (after local fsync)
2. Phase 2: Follower replays into its own `CascadeStore` instance (separate data dir)
3. Phase 3: Optional read-only API on follower
4. **Stop** — no leader election

---

## 3. Recommended production-grade roadmap (no full distribution)

Ordered by **resume impact / effort**. None of this requires implementing now; this is the planning doc.

### Tier 1 — Document and measure (current foundation)

| Item | Status | Action |
|------|--------|--------|
| YCSB matrix A/B/C/F @ 10k + 100k | Done | Freeze “resume suite” in README |
| Phase F optimization story | Done | Before/after chart in README |
| Architecture + data flow docs | Done | Link from root README |
| Published benchmark results | Done | See `benchmark/README.md` |

### Tier 2 — Production credibility (single node)

| Item | Effort | Outcome |
|------|--------|---------|
| **Prometheus metrics** (§2.1) | Medium | Operable system narrative |
| **MANIFEST + flush sequence** | Medium–High | Proper WAL truncation frontier; faster recovery |
| **Compaction stress benchmark** | Low | Strategy comparison when L0 actually compacts |
| **RocksDB/LevelDB comparison doc** (§2.2) | Low–Medium | External reference credibility |
| **Recovery tests** | Low | Kill -9 during load/run; assert key counts |
| **CI** (GitHub Actions: `mvn test`) | Low | Professional hygiene |

### Tier 3 — Optional stretch (still not “distributed DB”)

| Item | Effort | Outcome |
|------|--------|---------|
| Async WAL replication (§2.3) | High | Replication story without consensus |
| JFR profiling guide + one blog section | Low | “How I found bottlenecks” |
| CSV export from benchmark scripts | Low | Plots for portfolio |

### Explicitly defer

| Item | Why |
|------|-----|
| Full distributed DB (Raft, sharding coordinator) | Scope explosion; hurts benchmarks; hard to defend in interviews |
| Columnar / delta WAL (F2d/e) | Low ROI for resume narrative |
| Beating RocksDB on throughput | Wrong goal |

---

## 4. Recovery improvements (when you move to Tier 2)

If production-grade recovery matters more than new features, prioritize in this order:

### 4.1 MANIFEST with flushed sequence number (recommended)

Persist a small `MANIFEST` file on each flush/compaction:

```
flushed_sequence_number=12847
sstables=[sst_L0_S12, sst_L1_S3, ...]
```

On recovery:

1. Load SSTables listed in MANIFEST (or scan + validate against MANIFEST).
2. Replay **only** WAL records with `seq > flushed_sequence_number`.

**Benefit:** Recovery time tied to **unflushed WAL**, not total historical WAL.

### 4.2 Per-segment WAL truncation

Instead of `deleteAllLogs()` only when active MemTable is empty, truncate segments whose max sequence ≤ flush frontier (RocksDB-style log recycling).

### 4.3 Checkpoint record in WAL

Embed occasional `CheckpointRecord(seq)` in the WAL itself so truncation can be crash-safe without a separate MANIFEST (simpler but less common in modern LSMs).

### 4.4 Recovery metrics

Expose `wal_replay_duration_seconds` and `wal_replay_records_total` — ties directly to observability tier.

---

## 5. How to talk about this on a resume / in interviews

**One-liner:**

> Built a durable single-node LSM in Java with WAL replay recovery, SSTable persistence, three compaction strategies, and YCSB validation (~100k read ops/s @ 100k keys); instrumented and benchmarked against RocksDB.

**If asked about recovery:**

> “On startup we load SSTables, then replay the full WAL into the MemTable. WAL is truncated only when all data is flushed and the active MemTable is empty — there’s no sequence-based checkpoint yet; that’s the next production hardening step.”

**If asked about distribution:**

> “I explored async WAL replication to read-only followers as a future extension, but intentionally stopped short of consensus — the core project stays an optimized embedded engine.”

---

## 6. Summary

| Question | Answer |
|----------|--------|
| Full WAL replay today? | **Yes** — all segments, all records, into active MemTable |
| Snapshotting / checkpointing? | **No** — only binary WAL delete when MemTable tiers are empty |
| Are future_plans.txt items reasonable? | **Yes** — metrics + comparison + optional async replication strengthen a **single-node production** story without building Cassandra |
| Will replication hurt benchmarks? | **Slightly on write path** if enabled; keep it optional and off for headline numbers |
| Best next doc/engineering focus? | Metrics + MANIFEST/flush frontier + frozen benchmark suite |

---

*Document purpose: planning and discussion only. No implementation implied.*
