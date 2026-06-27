#!/usr/bin/env python3
"""Plot engine comparison throughput vs scale from comparison_by_scale.csv."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ENGINES = ["cascadestore", "rocksdb", "leveldb"]
LABELS = {
    "cascadestore": "CascadeStore",
    "rocksdb": "RocksDB",
    "leveldb": "LevelDB",
}
COLORS = {
    "cascadestore": "#4C72B0",
    "rocksdb": "#DD8452",
    "leveldb": "#55A868",
}
SCALES = [100_000, 250_000, 500_000]
WORKLOADS = {
    "A": "Workload A (50/50 read/update)",
    "B": "Workload B (95/5 read/update)",
    "C": "Workload C (100% read)",
    "F": "Workload F (50/50 read/RMW)",
}


def plot_workload(df: pd.DataFrame, workload: str, phase: str, outfile: Path) -> None:
    subset = df[(df["workload"] == workload) & (df["phase"] == phase)]
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for engine in ENGINES:
        rows = subset[subset["engine"] == engine].sort_values("scale")
        if rows.empty:
            continue
        ax.plot(
            rows["scale"],
            rows["throughput_ops"],
            marker="o",
            linewidth=2,
            label=LABELS[engine],
            color=COLORS[engine],
        )
    ax.set_xscale("log")
    ax.set_xticks(SCALES)
    ax.set_xticklabels(["100k", "250k", "500k"])
    ax.set_xlabel("Scale (records)")
    ax.set_ylabel("Throughput (ops/s)")
    phase_label = "load" if phase == "load" else "run"
    ax.set_title(f"{WORKLOADS[workload]} — {phase_label} throughput vs scale")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(outfile, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "comparison_by_scale.csv",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.csv)
    for workload in WORKLOADS:
        plot_workload(df, workload, "run", args.outdir / f"run_throughput_workload{workload.lower()}_vs_scale.png")
    print(f"Wrote charts to {args.outdir}")


if __name__ == "__main__":
    main()
