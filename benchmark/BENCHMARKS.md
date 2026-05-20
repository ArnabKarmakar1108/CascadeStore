# CascadeStore YCSB Benchmarks

Living log of YCSB numbers for plots and strategy comparison.

**Default:** `THREADS=1` — embedded binding shares one `CascadeStore`; multi-thread adds lock
contention without scaling throughput.

Raw outputs: `benchmark/results/`. Summarize a run:

```bash
./scripts/collect-ycsb-metrics.sh benchmark/results/workloada-THRESHOLD-run-20260724-001315.txt
```

---

## Benchmark matrix (target)

Full comparison grid to populate over time:

| | THRESHOLD | SIZE_TIERED | LEVEL_TIERED |
|---|-----------|-------------|--------------|
| **Workload A** (50/50 R/U, zipfian) | ☑ | ☑ | ☑ |
| **Workload B** (95/5 R/U, zipfian) | ☑ | ☑ | ☑ |
| **Workload C** (100% read, latest) | ☑ | ☑ | ☑ |
| **Workload F** (50/50 RMW, zipfian) | ☑ | ☑ | ☑ |

@10k + @100k (Phase F, Jul 25). All four workloads benchmarked at standard scale.

**Scales:** 1k (smoke) → **10k** (dry) → **100k** (standard) → **250k** (pre-1M gate) → **1M** (large) → beyond TBD

| Scale | Records / ops | Workload file | Purpose |
|-------|---------------|---------------|---------|
| 1k | 1,000 | `workloada-dryrun` | Binding smoke test |
| 10k | 10,000 | `workloada-10k` | Fast validation before long runs |
| 100k | 100,000 | `workloada` | Primary comparison scale |
| 250k | 250,000 | `workloada` | Pre-1M validation (4 shards × 4 threads) |
| 1M | 1,000,000 | `workloada` | Large-scale throughput comparison |

**Run Workload A ladder (baseline only — 10k + 100k):**

```bash
./scripts/run-ycsb-scale-ladder.sh          # 10k + 100k
```

**Run Workload A @ 1M (baseline):**

```bash
THREADS=1 MEMTABLE_MB=256 COMPACTION_THRESHOLD=4 RECORDCOUNT=1000000 OPERATIONCOUNT=1000000 \
  ./scripts/run-ycsb-matrix.sh workloada 1000000 1000000
```

**Run workloads B/C/F (10k or 100k):**

```bash
./scripts/run-ycsb-workloads-bcf.sh 10000          # 10k smoke
RECORDCOUNT=100000 OPERATIONCOUNT=100000 TRIALS=3 \
  ./scripts/run-ycsb-workloads-bcf.sh 100000      # 100k standard
```

**Run full workload matrix (when ready):**

```bash
THREADS=1 MEMTABLE_MB=256 RECORDCOUNT=100000 OPERATIONCOUNT=100000 \
  ./scripts/run-ycsb-matrix.sh matrix
```

---

## Standard environment

| Setting | Baseline value |
|---------|----------------|
| YCSB | 0.17.0 |
| JVM | `-Xms2G -Xmx2G` |
| Threads | **1** |
| MemTable | 256 MB |
| Compaction threshold | **4** (default) |
| Compaction interval | 30 min (default) |
| Load → run | `reset.datadir=true` → `false` |

---

## Two benchmark profiles (do not mix)

| | **Baseline (scale ladder)** | **Compaction stress** |
|---|---------------------------|----------------------|
| Script | `run-ycsb-scale-ladder.sh`, `run-ycsb-matrix.sh` | `run-ycsb-compaction-matrix.sh` |
| Purpose | Compare throughput/latency across scales & strategies | Force L0 buildup and exercise STCS/LTCS/THRESHOLD compaction |
| MemTable | **256 MB** | **64 MB** |
| Compaction threshold | **4** (default) | **2** |
| Compaction interval | 30 min (default) | ~1 s |
| Flush interval | 10 s (default) | 5 s |

**Why 256 MB for 10k/100k/1M baseline runs (not 64 MB):**

- Default 16 MB MemTable cannot finish a 100k load (rotation bug fixed, but load still stalls around ~14k with tiny MemTable).
- 256 MB keeps flush count low during load so numbers reflect **strategy + scale**, not flush churn.
- **64 MB is only for the compaction-stress profile**, where we *want* more L0 files and periodic compaction attempts.

Pre-fix July 24 runs (buffer bug, accidental `COMPACTION_THRESHOLD=2`) are listed under [Runs to exclude](#runs-to-exclude).

---

## Workload matrix @ 10k (Phase F, 2026-07-25)

Single-thread, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, `BLOCK_CACHE_MB=0`, JVM `-Xms2G -Xmx2G`.
All loads: `INSERT Return=OK, 10000`. All runs: 100% OK. Zero errors in log. No compaction triggered (1–2 L0 SSTables).

Script: `./scripts/run-ycsb-workloads-bcf.sh 10000` (B/C/F) + `./scripts/run-ycsb-matrix.sh workloada-10k`.

### Workload A (50/50 read/update, zipfian)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 9,833 | 17,241 | 73 | 191 |
| SIZE_TIERED | 10,331 | **18,182** | 87 | 199 |
| LEVEL_TIERED | 10,741 | 17,483 | 81 | 230 |

Results: `workloada-10k-*-{load,run}-20260725-2236*.txt`, `2237*.txt`

### Workload B (95/5 read/update, zipfian)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 10,776 | **24,213** | 72 | 274 |
| SIZE_TIERED | 9,681 | **24,272** | 67 | 303 |
| LEVEL_TIERED | 8,097 | 16,949 | 68 | 303 |

Results: `workloadb-10k-*-{load,run}-20260725-2230*.txt`, `2231*.txt`

### Workload C (100% read, latest)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) |
|----------|-------------|-------------|---------------|
| THRESHOLD | 10,341 | 33,223 | 71 |
| SIZE_TIERED | 11,976 | **33,333** | 65 |
| LEVEL_TIERED | 11,587 | 31,056 | 71 |

Results: `workloadc-10k-*-{load,run}-20260725-2231*.txt`, `2232*.txt`

### Workload F (50/50 read / RMW, zipfian)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | RMW p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|--------------|
| THRESHOLD | 10,977 | 13,369 | 71 | 193 | 305 |
| SIZE_TIERED | 12,255 | **15,848** | 74 | 201 | 303 |
| LEVEL_TIERED | 10,277 | 11,848 | 72 | 180 | 282 |

Results: `workloadf-10k-*-{load,run}-20260725-2232*.txt`

Notes:
- Run throughput **~4×** vs pre–Phase F Workload A @ 10k (Jul 24: ~4.5k → now ~17–18k for 50/50 R/U).
- Workload C fastest (~33k ops/s); Workload F slowest (~12–16k) due to read-modify-write cost.
- THRESHOLD/STCS beat LTCS on B and F at 10k; strategy differences minimal on pure-read C.
- Supersedes Jul 24 Workload A @ 10k numbers (`workloada-10k-*-20260724-040*.txt`, pre–Phase F).

---

## Workload A @ 100k (Phase F, 2026-07-25)

Single-thread, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, `BLOCK_CACHE_MB=0`, JVM `-Xms2G -Xmx2G`.
3 trials, 30 s warmup. Script: `RECORDCOUNT=100000 OPERATIONCOUNT=100000 TRIALS=3 ./scripts/run-ycsb-matrix.sh workloada 100000 100000`.

All loads: `INSERT Return=OK, 100000`. All runs: 100% OK. Zero errors. No compaction triggered (2 L0 SSTables).

50/50 read/update, zipfian. Tables show **median** run/load throughput across 3 trials. Latencies from trial 1.

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 22,805 | 27,902 | 28 | 77 |
| SIZE_TIERED | 21,363 | **35,162** | 29 | 83 |
| LEVEL_TIERED | 23,223 | 34,771 | 39 | 92 |

Trial run throughputs: THRESHOLD `[39032, 27902, 26969]` · STCS `[35162, 33278, 35562]` · LTCS `[30713, 34771, 34916]`

Results: `workloada-*-{load,run}-20260725-2320*.txt` … `2326*.txt`  
Log: `/tmp/ycsb-a-100k.log`

Notes:
- Run throughput **~3–4×** vs Jul 24 post-opt A @ 100k (~8–10k → ~28–35k).
- Among Phase F workloads @ 100k: slower than B/C (more writes) but comparable to F (~28–35k vs ~31k) — expected for 50/50 R/U vs 95/5 or 100% read.
- STCS/LTCS ~25% ahead of THRESHOLD on run median; trial-1 cold effect on THRESHOLD (39k vs ~27k median).
- Supersedes Jul 24 Workload A @ 100k sections below.

---

## Workload B/C/F @ 100k (Phase F, 2026-07-25)

Single-thread, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, `BLOCK_CACHE_MB=0`, JVM `-Xms2G -Xmx2G`.
3 trials, 30 s warmup between cells. Script: `RECORDCOUNT=100000 OPERATIONCOUNT=100000 TRIALS=3 ./scripts/run-ycsb-workloads-bcf.sh 100000`.

All loads: `INSERT Return=OK, 100000`. All runs: 100% OK. Zero errors. No compaction triggered (2 L0 SSTables: load flush + run flush).

Tables show **median** run/load throughput across 3 trials. Latencies from trial 1. Trial-1 run throughputs listed for warmup comparison (trial 1 is cold; trials 2–3 benefit from matrix/OS cache warmth).

### Workload B (95/5 read/update, zipfian)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 22,983 | **64,392** | 33 | 290 |
| SIZE_TIERED | 23,889 | 66,489 | 26 | 199 |
| LEVEL_TIERED | 22,217 | 60,277 | 26 | 196 |

Trial run throughputs: THRESHOLD `[36928, 67431, 64392]` · STCS `[62539, 67024, 66489]` · LTCS `[62035, 53677, 60277]`

### Workload C (100% read, latest)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) |
|----------|-------------|-------------|---------------|
| THRESHOLD | 23,245 | 92,081 | 21 |
| SIZE_TIERED | 25,297 | 90,580 | 20 |
| LEVEL_TIERED | 24,307 | **101,010** | 23 |

Trial run throughputs: THRESHOLD `[101833, 92081, 88496]` · STCS `[104058, 90580, 90090]` · LTCS `[101010, 98232, 101215]`

### Workload F (50/50 read / RMW, zipfian)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | RMW p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|--------------|
| THRESHOLD | 24,950 | 31,837 | 46 | 78 | 113 |
| SIZE_TIERED | 24,814 | 30,618 | 46 | 79 | 116 |
| LEVEL_TIERED | 24,900 | **32,626** | 28 | 64 | 92 |

Trial run throughputs: THRESHOLD `[28249, 32123, 31837]` · STCS `[29326, 30618, 35461]` · LTCS `[33535, 32626, 30722]`

Results: `workload{b,c,f}-*-{load,run}-20260725-2257*.txt` … `2315*.txt`  
Log: `/tmp/ycsb-bcf-100k-v2.log` (or terminal output from corrected re-run)

Notes:
- Workload C fastest (~90–101k run median); F slowest (~31k) due to read-modify-write.
- STCS leads B run median (~66k); LTCS leads C and F at 100k (reverses 10k trend on B).
- Trial-1 B throughputs ~40–45% below median — cite **medians** for cross-scale comparison; trial 1 for cold-start behavior.
- Workload A @ 100k Phase F: ~28–35k run median (50/50 R/U) — between F (~31k) and B (~64k); see [Workload A @ 100k](#workload-a--100k-phase-f-2026-07-25).
- Earlier mistaken 1k run (script bug) documented under [Runs to exclude](#runs-to-exclude).

---

## Workload B/C/F @ 1k (upstream, mistaken — 2026-07-25)

**Do not use.** First `./scripts/run-ycsb-workloads-bcf.sh 100000` attempt used upstream 1k defaults due to a script bug (`run-ycsb-matrix.sh` cleared exported counts). Fixed and re-run @ 100k above.

Results (archive only): `workload{b,c,f}-*-{load,run}-20260725-2237*.txt` … `2253*.txt`

---

## Workload A @ 100k (2026-07-24, post Phase A/B/D) — superseded

**Superseded by [Workload A @ 100k (Phase F, 2026-07-25)](#workload-a--100k-phase-f-2026-07-25).** Historical reference only.

Single-thread, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms2G -Xmx2G`.
Phases A (version snapshots), B1 (block cache), B2 (row cache), D (native `merge`) applied.

All loads: `INSERT Return=OK, 100000`. All runs: 100% OK. Zero errors in log.

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) | Disk | L0 |
|----------|-------------|-------------|---------------|-----------------|-----------------|------|-----|
| THRESHOLD | 19,897 | 8,301 | 217 | 297 | 69 | 146 MB | 2 |
| SIZE_TIERED | 19,596 | 7,079 | 263 | 342 | 73 | 146 MB | 2 |
| LEVEL_TIERED | 17,176 | **10,244** | 216 | 280 | 78 | 146 MB | 2 |

Results: `workloada-*-{load,run}-20260724-2317*.txt`

Notes:
- Run throughput **+47–82%** vs pre-opt 100k baseline (4.4–5.6k → 7.1–10.2k ops/s).
- Load throughput **~7×** faster (~2.7k → ~18–20k ops/s).
- 2 L0 per strategy; compaction not triggered (threshold 4).
- No `NOT_FOUND`, `ERROR`, or exceptions in `/tmp/ycsb-100k-matrix-post-opt.log`.

## Workload A @ 100k (2026-07-24, post-fix) — superseded

**Superseded by [Workload A @ 100k (Phase F, 2026-07-25)](#workload-a--100k-phase-f-2026-07-25).** Historical reference only.

Single-thread, 256 MB MemTable, `COMPACTION_THRESHOLD=4`. All loads: `INSERT Return=OK, 100000`. All runs: 100% OK. Zero errors in log.

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) | Disk | L0 |
|----------|-------------|-------------|---------------|-----------------|-----------------|------|-----|
| THRESHOLD | 2,754 | 4,443 | 53 | 570 | 615 | 354 MB | 2 |
| SIZE_TIERED | 2,679 | 5,185 | 51 | 586 | 600 | 354 MB | 2 |
| LEVEL_TIERED | 2,689 | **5,640** | 53 | 535 | 632 | 353 MB | 2 |

Results: `workloada-*-{load,run}-20260724-040*.txt`

Notes:
- Clean baseline: 2 L0 each, compaction skipped. Run throughput ~4.4–5.6k ops/s (vs ~3–7 ops/s on broken pre-fix 1M attempt).
- LTCS leads run throughput; all strategies within ~25% of July 23 baseline.

### Prior 100k baseline (2026-07-23, threshold=4, 2 L0)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 2,982 | 5,527 | 43 | 702 |
| SIZE_TIERED | 2,378 | 5,764 | 51 | 555 |
| LEVEL_TIERED | 2,890 | **5,966** | 43 | 556 |

Results: `20260723-1134*`. Compaction not exercised (2 L0, 30 min interval).

---

## Workload A @ 250k (2026-07-25, post Phase A–E, 4 shards × 4 threads)

`SHARDS=4 THREADS=4`, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms4G -Xmx8G`.
All optimization phases applied (A–E).

All loads: `INSERT Return=OK, 250000`. All runs: 100% OK. Zero errors in log.

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) | Disk | L0/shard |
|----------|-------------|-------------|---------------|-----------------|-----------------|------|----------|
| THRESHOLD | 14,293 | 9,790 | 533 | 1,230 | 417 | 359 MB | 2 |
| SIZE_TIERED | 14,233 | **10,980** | 475 | 1,146 | 588 | 360 MB | 2 |
| LEVEL_TIERED | 13,653 | 8,853 | 654 | 1,411 | 447 | 360 MB | 2 |

Results: `workloada-*-{load,run}-20260725-011*.txt`

Notes:
- **Correctness gate passed:** zero `NOT_FOUND`/`ERROR`/exceptions; 250k inserts + 250k run ops all OK.
- 2 L0 per shard (8 SSTables total); compaction not triggered (threshold 4).
- GC time ~23–26% during run phase (G1 young gen).
- Run throughput lower than July 24 pre-opt 250k (~25–31k) — likely cache memory pressure (4×128 MiB block + 4×64 MiB row cache per JVM) and heavier GC; investigate before 1M.

## Workload A @ 250k (2026-07-24, post-fix, 4 shards × 4 threads)

`SHARDS=4 THREADS=4`, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms4G -Xmx8G`.
All loads: `INSERT Return=OK, 250000`. All runs: 100% OK. **Zero errors.**

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|-----------------|
| THRESHOLD | 10,473 | **31,129** | 220 | 375 | 748 |
| SIZE_TIERED | 18,693 | 25,301 | 208 | 398 | 327 |
| LEVEL_TIERED | 24,843 | 29,572 | 214 | 404 | 251 |

Results: `workloada-*-{load,run}-20260724-1118*.txt`, `1119*.txt`

Notes:
- THRESHOLD leads run throughput at this scale (likely fewer compaction merges during mixed workload).
- ~6× higher throughput vs single-thread 100k baseline — sharding + concurrency fix validated.
- Gate passed for 1M run.

**Run command:**

```bash
SHARDS=4 THREADS=4 MEMTABLE_MB=256 COMPACTION_THRESHOLD=4 \
JAVA_TOOL_OPTIONS="-Xms4G -Xmx8G" \
./scripts/run-ycsb-matrix.sh workloada 250000 250000
```

---

## Workload A @ 1M (2026-07-25, post Phase A–E + WAL fix, cache disabled)

`SHARDS=4 THREADS=4`, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms4G -Xmx8G`.
**Block cache = 0** (`BLOCK_CACHE_MB=0`). Matrix: `./scripts/run-ycsb-matrix.sh workloada 1000000 1000000`.

All loads: `INSERT Return=OK, 1000000`. All runs: 100% OK. **Zero errors.**

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) | GC % (run) | Disk |
|----------|-------------|-------------|---------------|-----------------|-----------------|------------|------|
| THRESHOLD | 32,571 | 2,369 | 122 | 786 | 178 | 1.9 | 1.4 GB |
| SIZE_TIERED | 26,489 | **2,800** | 126 | 369 | 186 | 2.1 | 1.4 GB |
| LEVEL_TIERED | **34,835** | 2,512 | 122 | 424 | 147 | 1.8 | 1.4 GB |

Results: `workloada-*-{load,run}-20260725-2024*.txt`, `2031*.txt`, `2038*.txt`

Notes:
- **Correctness:** WAL append/truncate race fixed before this run; no `NOT_FOUND` / `READ-FAILED`.
- **Compaction:** not triggered (3 L0 / shard, threshold 4); identical file layout across strategies (12 SSTables total).
- **Run variance:** single THRESHOLD run earlier same day hit **3,247** ops/s; matrix THRESHOLD **2,369** — ~27% spread, likely back-to-back runs + JVM/disk state.
- **vs pre-opt 1M (Jul 24):** 1,328–1,397 run → **1.7–2.1×** improvement; gate (≥3k) met in single run, borderline in matrix.
- SIZE_TIERED leads run phase in this matrix; LEVEL_TIERED leads load.

## Workload A @ 1M (2026-07-25, single THRESHOLD, cache disabled)

`SHARDS=4 THREADS=4`, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms4G -Xmx8G`.
**Block cache = 0** (`BLOCK_CACHE_MB=0`).

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | Insert p99 (µs) | GC % | Disk |
|----------|-------------|-------------|---------------|-----------------|-----------------|------|------|
| THRESHOLD | 32,647 | **3,247** | 116 | 349 | 162 | 1.8 | 1.4 GB |

Results: `workloada-THRESHOLD-{load,run}-20260725-1940*.txt`

---

## Workload A @ 1M (2026-07-25, Phase F, cache disabled)

`SHARDS=4 THREADS=4`, 256 MB MemTable, `COMPACTION_THRESHOLD=4`, JVM `-Xms4G -Xmx8G`.
**Block cache = 0** (`BLOCK_CACHE_MB=0`). **3 trials** per strategy (`TRIALS=3`, `WARMUP_SECONDS=30`).

Phases F1–F4 applied (mmap, sparse-index binary search, scan skip, YCSB patch merge, version-level SSTable pin, parallel bloom on, WAL read-write lock). See `OPTIMIZATIONS.md` §17.

### Run throughput — trial 1 (fairest compare to v2 matrix)

First cell per strategy; fresh datadir; ~2 h before later trials in the same session.

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) | GC % (run) | vs v2 run |
|----------|-------------|-------------|---------------|-----------------|------------|-----------|
| THRESHOLD | 28,069 | **8,291** | 77 | 883 | 1.2 | **3.5×** |
| SIZE_TIERED | 19,601 | **8,076** | 76 | 817 | — | **2.9×** |
| LEVEL_TIERED | 29,554 | **13,465** | 73 | 417 | — | **5.4×** |

Results: `workloada-*-{load,run}-20260725-2130*.txt`, `2133*.txt`, `2137*.txt`

### Run throughput — all trials (median)

Later trials in the same matrix session run faster (hot JVM / OS cache); use **trial 1** for cross-release comparison.

| Strategy | Trial 1 | Trial 2 | Trial 3 | **Median** |
|----------|---------|---------|---------|------------|
| THRESHOLD | 8,291 | 21,699 | 27,286 | **21,699** |
| SIZE_TIERED | 8,076 | — | 24,657 | **16,366**† |
| LEVEL_TIERED | 13,465 | 14,972 | 25,829 | **14,972** |

†SIZE_TIERED trial 2 run incomplete; median of trials 1 and 3.

Results: `workloada-*-{load,run}-20260725-213*.txt`, `214*.txt`

Notes:
- **Gate (≥3.5k run):** passed on **trial 1** for all strategies; median trials **15–22k** ops/s.
- **vs v2 matrix (Jul 25):** 2,369–2,800 run → **2.9–5.4×** on trial 1; runtime **~2 min → ~40 s** by trial 3.
- **Compaction:** not triggered (2 L0 / shard after load); gains are read/update path (Phase F), not compaction.
- **Correctness:** 8/9 run cells 100% OK (~500k read + ~500k update each); one SIZE_TIERED trial-2 run aborted early.
- **Strategy:** LEVEL_TIERED best on trial 1; all three cluster ~25–27k by trial 3.

**Run command:**

```bash
SHARDS=4 THREADS=4 MEMTABLE_MB=256 COMPACTION_THRESHOLD=4 BLOCK_CACHE_MB=0 \
RECORDCOUNT=1000000 OPERATIONCOUNT=1000000 \
JAVA_TOOL_OPTIONS="-Xms4G -Xmx8G" \
./scripts/run-ycsb-matrix.sh workloada 1000000 1000000
```

Log: `/tmp/ycsb-1m-matrix-nocache-v2.log`

---

## Workload A @ 1k (historical dry-run)

| Strategy | Load (ops/s) | Run (ops/s) | Read p99 (µs) | Update p99 (µs) |
|----------|-------------|-------------|---------------|-----------------|
| THRESHOLD | 1,653 | 2,110 | 168 | 775 |
| SIZE_TIERED | 1,626 | 1,715 | 165 | 2,061 |
| LEVEL_TIERED | 1,520 | 2,062 | 187 | 1,087 |

Results: `20260723-1055*`.

---

## Scale trends (Workload A, single-thread)

| Scale | Best run (ops/s) | Strategy | Observation |
|-------|------------------|----------|-------------|
| 1k | 2,110 | THRESHOLD | Smoke only |
| 10k | 4,699 | LEVEL_TIERED | Post-fix; ~2.2× vs 1k |
| 100k | 5,640 | LEVEL_TIERED | Post-fix baseline (t=1, 2 L0) |
| 250k | 31,129 | THRESHOLD | 4 shards × 4 threads; zero errors |
| 1M (v2) | 2,800 | SIZE_TIERED | 4×4, cache off, pre–Phase F |
| **1M (Phase F, trial 1)** | **13,465** | **LEVEL_TIERED** | 4×4, cache off; **3.5–5.4×** vs v2 |
| 1M (Phase F, median) | 21,699 | THRESHOLD | Same matrix; later trials hotter |

---

## Workloads B / C / F

_TBD._ Run via:

```bash
THREADS=1 MEMTABLE_MB=256 RECORDCOUNT=100000 OPERATIONCOUNT=100000 \
  ./scripts/run-ycsb-matrix.sh workloadb   # or workloadc, workloadf
```

| Workload | Mix | Status |
|----------|-----|--------|
| B | 95% read / 5% update | Planned |
| C | 100% read (latest) | Planned |
| F | 50% RMW / 50% update | Planned |

---

## Compaction stress profile

For strategy comparison **under compaction** (not baseline):

```bash
./scripts/run-ycsb-compaction-matrix.sh
```

| Setting | Value |
|---------|-------|
| MEMTABLE_MB | 64 |
| COMPACTION_THRESHOLD | 2 |
| COMPACTION_INTERVAL_MINUTES | 0.17 (~1 s) |
| FLUSH_INTERVAL_SECONDS | 5 |

_TBD — fill after dedicated run._

---

## Runs to exclude

| Run | Issue |
|-----|-------|
| `20260723-110*` | 16 MB MemTable; load stopped at ~14k |
| `20260723-111818`, `110620` | Incomplete load |
| `20260723-232830` – `233522` | 8 threads + compaction contention |
| `20260724-001*` (10k/100k) | Pre-fix buffer bug; accidental `COMPACTION_THRESHOLD=2` |
| `20260724-03*` (1M THRESHOLD) | Pre-fix: `newLimit > capacity` → compaction failed → ~10 L0, run ~3–7 ops/s; killed |
| `20260725-2237*` – `2253*` (B/C/F) | Mistaken 1k run: script bug dropped `recordcount`/`operationcount` overrides |

---

## Planned

- [x] Re-run 100k with explicit `COMPACTION_THRESHOLD=4` (clean baseline) — 2026-07-24 post-fix
- [x] Workload A @ 250k × 3 strategies (4 shards × 4 threads) — 2026-07-24 post-fix
- [x] Workload A @ 1M × 3 strategies (cache off, WAL fix) — 2026-07-25 matrix v2
- [x] Workload A @ 1M × 3 strategies (Phase F, 3 trials) — 2026-07-25
- [x] Workloads B, C, F @ 100k × 3 strategies — 2026-07-25 (Phase F, 3 trials)
- [ ] Compaction stress matrix results
- [ ] CSV export / plotting script from `collect-ycsb-metrics.sh`
