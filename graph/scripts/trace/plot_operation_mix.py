#!/usr/bin/env python3
"""Plot horizontal 100% stacked operation bars from Trace manifest counts."""

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
from matplotlib.transforms import offset_copy
import numpy as np

from _common import ROOT, load_inputs, save_figure
from graph.config import operation_mix_config as style
from graph.config.global_config import COLOR_PALETTE, apply_style


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    config = load_inputs(style.TRACES)
    manifests = [str(t["directory"] / "manifest.json") for t in config["traces"]]
    with tempfile.TemporaryDirectory(prefix="sibench-operation-mix-") as temporary:
        report = Path(temporary) / "counts.json"
        subprocess.run([sys.executable, str(ROOT / "scripts/summarize_trace.py"),
                        *manifests, "--output", str(report)], cwd=ROOT, check=True)
        reports = json.loads(report.read_text())
    apply_style()
    fig, ax = plt.subplots(figsize=style.FIGURE_SIZE)
    kinds = ("get", "scan", "put", "delete")
    centers = np.arange(len(reports))
    rows = []
    operation_shares = []
    for trace, report in zip(config["traces"], reports):
        workload = report["phases"]["workload"]
        counts = [sum(workload[s]["operations"][k] for s in ("primary", "secondary")) for k in kinds]
        shares = [n / workload["total"] if workload["total"] else 0 for n in counts]
        operation_shares.append(shares)
        for kind, count, share in zip(kinds, counts, shares):
            rows.append((trace["name"], kind, count, share, report["manifest_sha256"]))
    left = np.zeros(len(reports))
    for i, kind in enumerate(kinds):
        shares = [values[i] for values in operation_shares]
        if not any(shares):
            continue
        ax.barh(centers, shares, left=left, height=style.BAR_HEIGHT,
               label=kind.title(), color=COLOR_PALETTE[i], **style.BAR_STYLE)
        for center, base, share in zip(centers, left, shares):
            if not share:
                continue
            label = f"{share * 100:.2f}".rstrip("0").rstrip(".") + "%" if share >= 0.0001 else "<0.01%"
            if share >= 0.06:
                ax.text(base + share / 2, center, label, ha="center", va="center", **style.PERCENT_LABEL_STYLE)
            else:
                ax.plot(base + share, center, color=COLOR_PALETTE[i], clip_on=False,
                        transform=offset_copy(ax.transData, fig=fig, x=3, units="points"),
                        **style.PERCENT_MARKER_STYLE)
                ax.annotate(label, (base + share, center), xytext=(6, 0), textcoords="offset points",
                            ha="left", va="center", annotation_clip=False, **style.PERCENT_LABEL_STYLE)
        left += shares
    ax.set_yticks(centers, [trace["label"] for trace in config["traces"]], **style.Y_TICK_LABEL_STYLE)
    ax.invert_yaxis()
    ax.set_xlabel(style.X_LABEL)
    ax.set_xlim(0, 1)
    ax.set_xticks(style.X_TICKS)
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(**style.GRID_STYLE)
    if ax.containers:
        ax.legend(**style.LEGEND_STYLE)
    out = config["plot_directory"] / "operation_mix"
    save_figure(fig, out)
    plt.close(fig)
    with out.with_suffix(".csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("trace", "operation", "count", "share", "manifest_sha256"))
        writer.writerows(rows)
    print(out)


if __name__ == "__main__":
    main()
