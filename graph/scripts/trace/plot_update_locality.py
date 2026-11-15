#!/usr/bin/env python3
"""Plot the CSVs written by `analyze-locality`; it does no Trace or key interpretation."""

import argparse
import json
import pathlib
import re

from _common import save_figure
from graph.config import update_locality_config as style
from graph.config.global_config import apply_style

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import pandas as pd


def safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_directory", type=pathlib.Path)
    parser.add_argument(
        "--out", type=pathlib.Path,
        default=pathlib.Path("results/plots/trace/locality"),
        help="output directory (default: %(default)s); overwrite matching figures",
    )
    args = parser.parse_args()
    run = args.run_directory
    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    apply_style()
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
        fig, ax = plt.subplots(figsize=style.HEATMAP_FIGURE_SIZE)
        image = ax.imshow(
            pivot.to_numpy(),
            norm=LogNorm(vmin=1, vmax=max(2, int(pivot.to_numpy().max()))),
            **style.HEATMAP_STYLE,
        )
        ax.set_title(style.HEATMAP_TITLE.format(name=name))
        ax.set_xlabel(style.HEATMAP_X_LABEL.format(last_bin=len(pivot.columns) - 1))
        ax.set_ylabel(style.HEATMAP_Y_LABEL)
        fig.colorbar(image, ax=ax, label=style.HEATMAP_COLORBAR_LABEL)
        save_figure(fig, out / f"heatmap-{safe(name)}")
        plt.close(fig)

    for (table, index), group in windows.groupby(["table", "index"], sort=True):
        name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
        fig, axes = plt.subplots(1, 2, figsize=style.COVERAGE_FIGURE_SIZE)
        for category, line_style in style.COVERAGE_LINE_STYLES.items():
            part = group[group["category"] == category].sort_values("window")
            if part.empty:
                continue
            axes[0].plot(
                part["window"],
                part["cumulative_block_ratio"],
                line_style,
                label=style.COVERAGE_LABELS[0].format(category=category),
            )
            axes[1].plot(
                part["window"],
                part["hit_blocks"] / part["blocks"].where(part["blocks"] > 0),
                line_style,
                label=style.COVERAGE_LABELS[1].format(category=category),
            )
        for ax, title in zip(axes, style.COVERAGE_TITLES):
            ax.set_title(title.format(name=name))
            ax.set_xlabel(style.COVERAGE_X_LABEL)
            ax.set_ylabel(style.COVERAGE_Y_LABEL)
            ax.set_ylim(style.COVERAGE_Y_LIMITS)
            ax.legend(**style.COVERAGE_LEGEND_STYLE)
        save_figure(fig, out / f"coverage-{safe(name)}")
        plt.close(fig)

    # One compact source-wide comparison, independent of an index's absolute block count.
    fig, ax = plt.subplots(figsize=style.BUSINESS_FIGURE_SIZE)
    for (table, index, category), part in windows.groupby(
        ["table", "index", "category"], sort=True
    ):
        if category != "business" or part.empty:
            continue
        name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
        ax.plot(part["window"], part["cumulative_block_ratio"], label=name)
    ax.set_title(style.BUSINESS_TITLE)
    ax.set_xlabel(style.COVERAGE_X_LABEL)
    ax.set_ylabel(style.COVERAGE_Y_LABEL)
    ax.set_ylim(style.COVERAGE_Y_LIMITS)
    ax.legend(**style.BUSINESS_LEGEND_STYLE)
    save_figure(fig, out / "business-coverage")
    plt.close(fig)

    if l0 is not None:
        for (table, index), part in l0.groupby(["table", "index"], sort=True):
            name = names.get((int(table), int(index)), f"t{int(table)}-i{int(index)}")
            fig, ax = plt.subplots(figsize=style.L0_FIGURE_SIZE)
            for column, line_style in style.L0_SERIES.items():
                ax.plot(part["batch"], part[column], **line_style)
            ax.set_title(style.L0_TITLE.format(name=name))
            ax.set_xlabel(style.L0_X_LABEL)
            ax.set_ylabel(style.L0_Y_LABEL)
            ax.legend(**style.L0_LEGEND_STYLE)
            save_figure(fig, out / f"l0-overlap-{safe(name)}")
            plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
