#!/usr/bin/env python3
"""Plot scaling matrix — throughput and latency vs scale per workload."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

WORKLOADS = {
    "workloada": "Workload A (50/50 read/update)",
    "workloadb": "Workload B (95/5 read/update)",
    "workloadc": "Workload C (100% read)",
    "workloadf": "Workload F (50/50 read/RMW)",
}
COMBOS = [("8", "8x8"), ("4", "4x4"), ("1", "1x1")]
COLORS = {"8": "#55A868", "4": "#DD8452", "1": "#4C72B0"}
SCALES = [100_000, 250_000, 500_000, 750_000, 1_000_000]


def scale_labels(scales: list[int]) -> list[str]:
    return [f"{int(s // 1000)}k" if s < 1_000_000 else "1M" for s in scales]


def plot_workload(df: pd.DataFrame, workload: str, title: str, outdir: Path) -> None:
    subset = df[(df["workload"] == workload) & (df["phase"] == "run")].copy()
    if subset.empty:
        return

    has_update = workload in {"workloada", "workloadb", "workloadf"}
    fig, (ax_tp, ax_lat) = plt.subplots(
        2,
        1,
        figsize=(10, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 2], "hspace": 0.08},
    )

    for shard, label in COMBOS:
        rows = subset[subset["shards"] == int(shard)].sort_values("scale")
        if rows.empty:
            continue
        color = COLORS[shard]
        ax_tp.plot(
            rows["scale"],
            rows["run_ops"],
            marker="o",
            linewidth=2.2,
            markersize=7,
            label=label,
            color=color,
            zorder=3 if shard == "8" else 2,
        )
        ax_lat.plot(
            rows["scale"],
            rows["read_p99_us"],
            marker="s",
            linewidth=1.8,
            markersize=5,
            linestyle="-",
            label=f"{label} read p99",
            color=color,
            alpha=0.9,
        )
        if has_update:
            ax_lat.plot(
                rows["scale"],
                rows["update_p99_us"],
                marker="^",
                linewidth=1.5,
                markersize=5,
                linestyle="--",
                label=f"{label} update p99",
                color=color,
                alpha=0.65,
            )

    ax_tp.set_xscale("log")
    ax_lat.set_xscale("log")
    ax_tp.set_xticks(SCALES)
    ax_lat.set_xticks(SCALES)
    ax_lat.set_xticklabels(scale_labels(SCALES))
    ax_tp.tick_params(labelbottom=False)
    ax_tp.set_ylabel("Run throughput (ops/s)")
    ax_lat.set_ylabel("Latency p99 (µs)")
    ax_lat.set_xlabel("Scale (records)")
    ax_tp.set_title(f"{title} — scaling matrix (LTCS)")
    ax_tp.grid(True, alpha=0.3)
    ax_lat.grid(True, alpha=0.3)
    ax_tp.legend(loc="upper left", fontsize=9)
    ax_lat.legend(loc="upper left", fontsize=7, ncol=2 if has_update else 1)
    fig.subplots_adjust(hspace=0.12)
    wl = workload.replace("workload", "")
    fig.savefig(outdir / f"run_throughput_workload{wl}_vs_scale.png", dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "track_c_matrix.csv",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.csv)
    for workload, title in WORKLOADS.items():
        plot_workload(df, workload, title, args.outdir)

    print(f"Wrote charts to {args.outdir}")


if __name__ == "__main__":
    main()
