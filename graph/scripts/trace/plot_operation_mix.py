#!/usr/bin/env python3
"""Plot grouped primitive-operation shares using recorded Trace manifest counts."""

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
import numpy as np

from _common import DEFAULT_CONFIG, ROOT, STYLE, load_config, save_figure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    config = load_config(args.config)
    manifests = [str(t["directory"] / "manifest.json") for t in config["traces"]]
    with tempfile.TemporaryDirectory(prefix="sibench-operation-mix-") as temporary:
        report = Path(temporary) / "counts.json"
        subprocess.run([sys.executable, str(ROOT / "scripts/summarize_trace.py"),
                        *manifests, "--output", str(report)], cwd=ROOT, check=True)
        reports = json.loads(report.read_text())
    plt.style.use(STYLE)
    fig, ax = plt.subplots(figsize=(11, 4.8))
    kinds = ("get", "scan", "put", "delete")
    centers = np.arange(len(kinds))
    width = 0.8 / len(reports)
    rows = []
    for i, (trace, report) in enumerate(zip(config["traces"], reports)):
        workload = report["phases"]["workload"]
        counts = [sum(workload[s]["operations"][k] for s in ("primary", "secondary")) for k in kinds]
        shares = [n / workload["total"] if workload["total"] else 0 for n in counts]
        ax.bar(centers + (i - (len(reports) - 1) / 2) * width, shares,
               width=width, label=trace["label"], edgecolor="white", linewidth=0.4,
               hatch="//" if i >= 8 else None)
        for kind, count, share in zip(kinds, counts, shares):
            rows.append((trace["name"], kind, count, share, report["manifest_sha256"]))
    ax.set_xticks(centers, [k.title() for k in kinds])
    ax.set_ylabel("Share of workload requests")
    ax.set_ylim(0, 1.05)
    ax.yaxis.set_major_formatter(PercentFormatter(1))
    fig.suptitle("Trace workload operation mix", fontsize=14)
    ax.grid(axis="y", alpha=0.2)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.02),
              ncol=min(8, len(reports)), fontsize=8)
    fig.text(0.5, 0.015, "Baseline excluded; every exported index request counts. Index scope follows each input.",
             ha="center", fontsize=8)
    fig.tight_layout(rect=(0, 0.05, 1, 0.92))
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
