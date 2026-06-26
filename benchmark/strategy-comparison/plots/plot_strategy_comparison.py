#!/usr/bin/env python3
"""Plot compaction strategy comparison charts from strategy_comparison_by_scale.csv."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

STRATEGIES = ["THRESHOLD", "SIZE_TIERED", "LEVEL_TIERED"]
LABELS = {
    "THRESHOLD": "Threshold",
    "SIZE_TIERED": "Size-tiered",
    "LEVEL_TIERED": "Level-tiered",
}
COLORS = {
    "THRESHOLD": "#4C72B0",
    "SIZE_TIERED": "#DD8452",
    "LEVEL_TIERED": "#55A868",
}
SCALES = [100_000, 250_000, 500_000, 750_000, 1_000_000]


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["scale_label"] = df["scale"].map(
        lambda s: f"{int(s // 1000)}k" if s < 1_000_000 else "1M"
    )
    return df


def plot_lines(
    df: pd.DataFrame,
    track: str,
    metric: str,
    title: str,
    ylabel: str,
    outfile: Path,
    strategies: list[str] | None = None,
) -> None:
    subset = df[df["track"] == track].copy()
    if strategies:
        subset = subset[subset["strategy"].isin(strategies)]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    for strategy in STRATEGIES:
        if strategies and strategy not in strategies:
            continue
        rows = subset[subset["strategy"] == strategy].sort_values("scale")
        if rows[metric].isna().all():
            continue
        ax.plot(
            rows["scale"],
            rows[metric],
            marker="o",
            label=LABELS[strategy],
            color=COLORS[strategy],
            linewidth=2,
            linestyle="-",
        )

    ax.set_xscale("log")
    ax.set_xticks(SCALES)
    ax.set_xticklabels(["100k", "250k", "500k", "750k", "1M"])
    ax.set_xlabel("Scale (records)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(outfile, dpi=150)
    plt.close(fig)


def plot_bars_500k(df: pd.DataFrame, track: str, metric: str, title: str, ylabel: str, outfile: Path) -> None:
    subset = df[(df["track"] == track) & (df["scale"] == 500_000)]

    fig, ax = plt.subplots(figsize=(6, 4))
    x = range(len(STRATEGIES))
    values = [subset[subset["strategy"] == s][metric].iloc[0] for s in STRATEGIES]
    ax.bar(x, values, color=[COLORS[s] for s in STRATEGIES])
    ax.set_xticks(list(x))
    ax.set_xticklabels([LABELS[s] for s in STRATEGIES])
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "strategy_comparison_by_scale.csv",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    df = load_csv(args.csv)

    # Expected trends only — exceptions stay as text in RESULTS.md
    plot_lines(
        df, "write", "write_amp",
        "Write amplification vs scale (Workload A)",
        "Write amplification",
        args.outdir / "write_amp_vs_scale.png",
    )
    plot_lines(
        df, "read", "files_probed_amp",
        "Read amplification vs scale (Workload C)",
        "Files probed per read",
        args.outdir / "read_amp_vs_scale.png",
    )
    plot_lines(
        df, "read", "throughput_ops",
        "Read throughput vs scale (Workload C)",
        "Run throughput (ops/s)",
        args.outdir / "read_throughput_vs_scale.png",
    )
    plot_lines(
        df, "space", "throughput_ops",
        "Read throughput vs scale (Workload F)",
        "Run throughput (ops/s)",
        args.outdir / "space_throughput_vs_scale.png",
    )
    plot_lines(
        df, "write", "throughput_ops",
        "Write throughput vs scale (Workload A)",
        "Load throughput (ops/s)",
        args.outdir / "write_throughput_vs_scale.png",
    )
    plot_lines(
        df, "space", "space_amp",
        "Space amplification vs scale (Workload F)",
        "Space amplification",
        args.outdir / "space_amp_vs_scale.png",
    )

    # Measured snapshot @ 500k
    plot_bars_500k(
        df, "write", "write_amp",
        "Write amplification @ 500k",
        "Write amplification",
        args.outdir / "write_amp_500k.png",
    )
    plot_bars_500k(
        df, "read", "files_probed_amp",
        "Files probed per read @ 500k",
        "Files probed per read",
        args.outdir / "read_amp_500k.png",
    )
    plot_bars_500k(
        df, "space", "space_amp",
        "Space amplification @ 500k",
        "Space amplification",
        args.outdir / "space_amp_500k.png",
    )

    print(f"Wrote charts to {args.outdir}")


if __name__ == "__main__":
    main()
