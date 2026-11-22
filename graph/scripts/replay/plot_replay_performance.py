#!/usr/bin/env python3
"""Plot load and workload throughput and read/write latency per Trace and backend from replay reports."""

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "trace"))
from _common import ROOT, save_figure
from graph.config import global_config
from graph.config import replay_performance_config as style


def load_results(results, traces, backends, metrics):
    """Returns {(trace, backend): (values by metric name, report SHA-256)}; rejects missing or unpassed reports."""
    table = {}
    for trace in traces:
        for backend in backends:
            path = results / trace["name"] / backend["name"] / "report.json"
            if not path.is_file():
                raise FileNotFoundError(f"Missing replay report: {path}")
            data = path.read_bytes()
            report = json.loads(data)
            if report.get("status") != "passed":
                raise ValueError(f"Replay did not pass: {path}")
            values = {}
            for metric in metrics:
                node = report["result"]
                for key in metric["path"][:-1]:
                    node = node[key]
                # Latency summaries carry their sample count; zero samples has no percentile.
                if "count" in node:
                    count = node["count"]
                    if type(count) is not int or count < 0:
                        raise ValueError(f"Invalid latency sample count for {metric['name']}: {path}")
                    if count == 0:
                        values[metric["name"]] = math.nan
                        continue
                value = node[metric["path"][-1]]
                if (type(value) not in (int, float) or not math.isfinite(value) or value < 0
                        or (metric["scale"] == "log" and value == 0)):
                    raise ValueError(f"Invalid replay metric {metric['name']}: {path}")
                values[metric["name"]] = value / metric["divisor"]
            table[(trace["name"], backend["name"])] = (values, hashlib.sha256(data).hexdigest())
    return table


def plot(table, traces, backends, metrics):
    global_config.apply_style()
    fig, axes = plt.subplots(len(metrics), 1, figsize=style.FIGURE_SIZE, sharex=True, squeeze=False)
    axes = axes[:, 0]
    centers = np.arange(len(traces))
    width = style.GROUP_WIDTH / len(backends)
    for ax, metric in zip(axes, metrics):
        for i, backend in enumerate(backends):
            x = centers - style.GROUP_WIDTH / 2 + (i + 0.5) * width
            values = [table[(t["name"], backend["name"])][0][metric["name"]] for t in traces]
            ax.bar(x, values, width, label=backend["label"], hatch=backend["hatch"],
                   color=global_config.COLOR_PALETTE[i], **style.BAR_STYLE)
        ax.set_yscale(metric["scale"])
        if metric["scale"] == "log":
            ax.yaxis.set_major_locator(LogLocator(subs=style.LOG_TICK_SUBS))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
            ax.yaxis.set_minor_formatter(NullFormatter())
        for i, backend in enumerate(backends):
            x = centers - style.GROUP_WIDTH / 2 + (i + 0.5) * width
            for xi, t in zip(x, traces):
                if math.isnan(table[(t["name"], backend["name"])][0][metric["name"]]):
                    ax.text(xi, ax.get_ylim()[0], style.MISSING_LABEL, **style.MISSING_LABEL_STYLE)
        ax.set_ylabel(metric["label"])
        ax.grid(**style.GRID_STYLE)
    axes[-1].set_xticks(centers, [t["label"] for t in traces], **style.X_TICK_LABEL_STYLE)
    axes[0].legend(**style.LEGEND_STYLE)
    return fig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, help="sweep directory (default: RESULTS_DIRECTORY in replay_performance_config.py)")
    parser.add_argument("--out", type=Path, help="output directory (default: REPLAY_PLOT_DIRECTORY in global_config.py)")
    arguments = parser.parse_args()
    # Command-line paths are relative to the working directory; ROOT / absolute keeps them unchanged.
    results = ROOT / (arguments.results.resolve() if arguments.results else style.RESULTS_DIRECTORY)
    out = ROOT / (arguments.out.resolve() if arguments.out else global_config.REPLAY_PLOT_DIRECTORY) / style.OUTPUT_NAME
    table = load_results(results, style.TRACES, style.BACKENDS, style.METRICS)
    fig = plot(table, style.TRACES, style.BACKENDS, style.METRICS)
    save_figure(fig, out)
    plt.close(fig)
    with out.with_suffix(".csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("trace", "backend", "metric", "value", "report_sha256"))
        for (trace, backend), (values, digest) in table.items():
            for metric in style.METRICS:
                value = values[metric["name"]]
                writer.writerow((trace, backend, metric["name"], "" if math.isnan(value) else value * metric["divisor"], digest))
    print(out)


if __name__ == "__main__":
    main()
