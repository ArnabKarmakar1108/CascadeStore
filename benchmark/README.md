# Benchmarks

Published YCSB results — each directory has CSV data, plots, and a `RESULTS.md`.

| Benchmark | What it measures | Directory |
|-----------|------------------|-----------|
| Strategy comparison | THRESHOLD vs STCS vs LTCS amplification & throughput | [`strategy-comparison/`](strategy-comparison/) |
| Engine comparison | CascadeStore vs RocksDB vs LevelDB @ 100k–500k | [`comparison/`](comparison/) |
| Throughput by scale | Headline 8×8 write/read curve, 100k–1M | [`throughput-by-scale/`](throughput-by-scale/) |
| Scaling matrix | Scale × shards × threads for workloads A/B/C/F | [`scaling-matrix/`](scaling-matrix/) |

## Raw data (audit / re-run)

| Run | Location |
|-----|----------|
| Scaling matrix | `benchmark/results/plan/scaling-matrix/` |
| Engine comparison | `benchmark/results/comparison-20260801/`, `comparison-leveldb-20260801/` |

Re-run scripts live in `scripts/` — see each `RESULTS.md` for commands.

## Regenerate plots

```bash
python3 benchmark/strategy-comparison/plots/plot_strategy_comparison.py
python3 benchmark/comparison/plots/plot_comparison_by_scale.py
python3 benchmark/throughput-by-scale/plots/plot_throughput_by_scale.py
python3 benchmark/scaling-matrix/plots/plot_scaling_matrix.py
```
