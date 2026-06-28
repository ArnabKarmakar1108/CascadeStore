#!/usr/bin/env python3
"""Plot throughput vs scale from throughput_by_scale.csv."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

SCALES = [100_000, 250_000, 500_000, 750_000, 1_000_000]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "throughput_by_scale.csv",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    args = parser.parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.csv)
    write = df[df["workload"] == "A"].sort_values("scale")
    read = df[df["workload"] == "C"].sort_values("scale")

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(
        write["scale"], write["throughput_ops"],
        marker="o", linewidth=2, color="#DD8452", label="Write (Workload A, size-tiered)",
    )
    ax.plot(
        read["scale"], read["throughput_ops"],
        marker="s", linewidth=2, color="#55A868", label="Read (Workload C, level-tiered)",
    )
    ax.set_xscale("log")
    ax.set_xticks(SCALES)
    ax.set_xticklabels(["100k", "250k", "500k", "750k", "1M"])
    ax.set_xlabel("Scale (records)")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title("Throughput vs scale")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.outdir / "throughput_vs_scale.png", dpi=150)
    plt.close(fig)
    print(f"Wrote charts to {args.outdir}")


if __name__ == "__main__":
    main()
