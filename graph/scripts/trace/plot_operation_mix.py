#!/usr/bin/env python3
"""Plot primary-only Get/Scan/Insert/Update shares from Trace manifest counts."""

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

from _common import ROOT, load_inputs, parse_arguments, save_figure
from graph.config import operation_mix_config as style
from graph.config.global_config import COLOR_PALETTE, apply_style


def main():
    arguments = parse_arguments(__doc__)
    config = load_inputs(arguments)
    manifests = [str(t["directory"] / "manifest.json") for t in config["traces"]]
    with tempfile.TemporaryDirectory(prefix="sibench-operation-mix-") as temporary:
        report = Path(temporary) / "counts.json"
        subprocess.run([sys.executable, str(ROOT / "scripts/summarize_trace.py"),
                        *manifests, "--output", str(report)], cwd=ROOT, check=True)
        reports = json.loads(report.read_text())
    kinds = ("get", "scan", "insert", "update")
    centers = np.arange(len(reports))
    rows = []
    operation_shares = []
    for trace, report in zip(config["traces"], reports):
        workload = report["phases"]["workload"]
        if workload["secondary"]["total"]:
            raise ValueError(f"Operation mix requires primary-only requests: {trace['name']}")
        operations = workload["primary"]["operations"]
        if operations["delete"]:
            raise ValueError(f"Operation mix does not include Delete requests: {trace['name']}")
        origins = workload["write_origins"]
        counts = [operations["get"], operations["scan"], origins["business_insert"], origins["business_update"]]
        if sum(counts[2:]) != operations["put"]:
            raise ValueError(f"Put origin count mismatch: {trace['name']}")
        shares = [n / workload["total"] if workload["total"] else 0 for n in counts]
        operation_shares.append(shares)
        for kind, count, share in zip(kinds, counts, shares):
            rows.append((trace["name"], kind, count, share, report["manifest_sha256"]))
    apply_style()
    fig, ax = plt.subplots(figsize=style.FIGURE_SIZE)
    minimum_percent = 10 ** -style.PERCENT_LABEL_DECIMALS
    order = [kinds.index(kind) for kind in style.OPERATION_ORDER]
    left = np.zeros(len(reports))
    for i in order:
        kind = kinds[i]
        shares = [values[i] for values in operation_shares]
        if not any(shares):
            continue
        ax.barh(centers, shares, left=left, height=style.BAR_HEIGHT,
               label=kind.title(), color=COLOR_PALETTE[i], **style.BAR_STYLE)
        for center, base, share in zip(centers, left, shares):
            if not share:
                continue
            label = f"{share * 100:.{style.PERCENT_LABEL_DECIMALS}f}"
            if "." in label:
                label = label.rstrip("0").rstrip(".")
            label = (label if share * 100 >= minimum_percent else f"<{minimum_percent:g}") + "%"
            if share >= style.MIN_INSIDE_LABEL_SHARE:
                ax.text(base + share / 2, center, label, ha="center", va="center", **style.PERCENT_LABEL_STYLE)
            else:
                small = [j for j in order if 0 < operation_shares[center][j] < style.MIN_INSIDE_LABEL_SHARE]
                offset = ((len(small) - 1) / 2 - small.index(i)) * (
                    style.PERCENT_LABEL_STYLE["fontsize"] + style.OUTSIDE_LABEL_GAP_PT)
                ax.plot(1, center, color=COLOR_PALETTE[i], clip_on=False,
                        transform=offset_copy(ax.transData, fig=fig, x=style.OUTSIDE_MARKER_OFFSET_PT,
                                              y=offset, units="points"),
                        **style.PERCENT_MARKER_STYLE)
                ax.annotate(label, (1, center), xytext=(style.OUTSIDE_LABEL_OFFSET_PT, offset), textcoords="offset points",
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
