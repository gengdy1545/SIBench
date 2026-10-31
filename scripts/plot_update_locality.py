#!/usr/bin/env python3
"""Plot the CSVs written by `analyze-locality`; it does no Trace or key interpretation."""

import argparse
import gzip
import json
import pathlib
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=pathlib.Path)
    parser.add_argument("--out", type=pathlib.Path, default=None)
    args = parser.parse_args()
    run = args.run_directory
    out = args.out or run / "plots"
    out.mkdir(parents=True, exist_ok=True)
    summary = json.loads((run / "summary.json").read_text())
    windows = pd.read_csv(run / "windows.csv")
    heatmap = pd.read_csv(run / "heatmap.csv.gz", compression="gzip")
    windows["window"] = windows["window"].astype(int)
    heatmap["window"] = heatmap["window"].astype(int)
    heatmap["bin"] = heatmap["bin"].astype(int)
    l0_path = run / "l0-batches.csv"
    l0 = pd.read_csv(l0_path) if l0_path.exists() else None

    names = {
        (item["table_id"], item["index_id"]): item["name"]
        for item in summary["analysis"]["indexes"]
    }
    for (table, index), group in heatmap.groupby(["table", "index"], sort=True):
        name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
        pivot = group.pivot_table(
            index="window", columns="bin", values="count", aggfunc="sum", fill_value=0
        )
        if pivot.empty:
            continue
        fig, ax = plt.subplots(figsize=(11, max(3.2, 0.13 * len(pivot))))
        image = ax.imshow(
            pivot.to_numpy(),
            aspect="auto",
            origin="lower",
            interpolation="nearest",
            cmap="magma",
            norm=matplotlib.colors.LogNorm(vmin=1, vmax=max(2, int(pivot.to_numpy().max()))),
        )
        ax.set_title(f"{name}: update count by window and ordered block bin")
        ax.set_xlabel(f"block bin (0..{len(pivot.columns) - 1})")
        ax.set_ylabel("business-change window")
        fig.colorbar(image, ax=ax, label="modifications (log scale)")
        fig.tight_layout()
        fig.savefig(out / f"heatmap-{safe(name)}.png", dpi=130)
        fig.savefig(out / f"heatmap-{safe(name)}.svg")
        plt.close(fig)

    for (table, index), group in windows.groupby(["table", "index"], sort=True):
        name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
        for category, style in (("business", "-"), ("index", "--")):
            part = group[group["category"] == category].sort_values("window")
            if part.empty:
                continue
            axes[0].plot(
                part["window"],
                part["cumulative_block_ratio"],
                style,
                label=f"{category} cumulative blocks",
            )
            axes[1].plot(
                part["window"],
                part["hit_blocks"] / part["blocks"].where(part["blocks"] > 0),
                style,
                label=f"{category} window blocks",
            )
        axes[0].set_title(f"{name}: cumulative block coverage")
        axes[0].set_xlabel("window")
        axes[0].set_ylabel("fraction of baseline blocks")
        axes[0].set_ylim(0, 1.02)
        axes[0].legend()
        axes[1].set_title(f"{name}: blocks touched in each window")
        axes[1].set_xlabel("window")
        axes[1].set_ylabel("fraction of baseline blocks")
        axes[1].set_ylim(0, 1.02)
        axes[1].legend()
        fig.tight_layout()
        fig.savefig(out / f"coverage-{safe(name)}.png", dpi=130)
        fig.savefig(out / f"coverage-{safe(name)}.svg")
        plt.close(fig)

    # One compact source-wide comparison, independent of an index's absolute block count.
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for (table, index, category), part in windows.groupby(
        ["table", "index", "category"], sort=True
    ):
        if category != "business" or part.empty:
            continue
        name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
        ax.plot(part["window"], part["cumulative_block_ratio"], label=name)
    ax.set_title("Business changes: cumulative block coverage")
    ax.set_xlabel("window")
    ax.set_ylabel("fraction of baseline blocks")
    ax.set_ylim(0, 1.02)
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(out / "business-coverage.png", dpi=130)
    fig.savefig(out / "business-coverage.svg")
    plt.close(fig)

    if l0 is not None:
        for (table, index), part in l0.groupby(["table", "index"], sort=True):
            name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
            fig, ax = plt.subplots(figsize=(10, 3.8))
            ax.plot(part["batch"], part["hit_files"], label="files with actual hits")
            ax.plot(part["batch"], part["span_files"], label="files in min..max range")
            ax.plot(part["batch"], part["l1_files"], "--", label="all L1 files")
            ax.set_title(f"{name}: assumed L0 file overlap with static L1")
            ax.set_xlabel("assumed L0 batch")
            ax.set_ylabel("files")
            ax.legend(fontsize=8)
            fig.tight_layout()
            fig.savefig(out / f"l0-overlap-{safe(name)}.png", dpi=130)
            fig.savefig(out / f"l0-overlap-{safe(name)}.svg")
            plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())