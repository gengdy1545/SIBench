#!/usr/bin/env python3
"""Plot the cumulative share of each Trace's key universe that was read."""

import json
import os
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import EngFormatter, FuncFormatter
import numpy as np
import pandas as pd

from _common import load_analysis, load_inputs, parse_arguments, save_figure
from graph.config import access_coverage_config as style
from graph.config import global_config

READ_COLUMNS = ("get_hits", "scan_hits")
CHUNK_ROWS = 1_000_000


def first_reads(path, window_count, chunksize=CHUNK_ROWS):
    first_visits = np.zeros(window_count, dtype=np.int64)
    totals = np.zeros(len(READ_COLUMNS), dtype=np.int64)
    columns = ("window", "key", *READ_COLUMNS)
    previous_window = -1
    with tempfile.TemporaryFile() as keys, subprocess.Popen(
            ["sort", "-s", "-u", "-k1,1", "-S", "512M", "--parallel=4"],
            stdin=subprocess.PIPE, stdout=keys, env={**os.environ, "LC_ALL": "C"}) as sorter:
        with sorter.stdin as stream:
            for batch in pd.read_csv(path, usecols=columns, keep_default_na=False,
                                     dtype={c: str if c == "key" else np.int64 for c in columns},
                                     chunksize=chunksize):
                if batch.empty:
                    continue
                windows = batch["window"].to_numpy()
                if (windows[0] < max(0, previous_window) or windows[-1] >= window_count
                        or np.any(windows[1:] < windows[:-1])):
                    raise ValueError(f"Invalid window order or range in {path}")
                previous_window = windows[-1]
                reads = batch[list(READ_COLUMNS)].to_numpy()
                if (reads < 0).any():
                    raise ValueError(f"Negative read count in {path}")
                totals += reads.sum(axis=0)
                active = batch.loc[np.any(reads > 0, axis=1), ["key", "window"]]
                # Stable unique sort retains the earliest window for keys shared by chunks.
                active.drop_duplicates("key").to_csv(stream, sep=" ", index=False, header=False)
        if sorter.wait():
            raise RuntimeError(f"Could not sort first reads in {path}")
        if totals.any():
            keys.seek(0)
            for batch in pd.read_csv(keys, sep=" ", header=None, usecols=[1], dtype=np.int64,
                                     chunksize=chunksize):
                first_visits += np.bincount(batch[1], minlength=window_count)
    return first_visits, totals


def trace_coverage(directory, summary):
    first_visits = np.zeros(summary["windows"], dtype=np.int64)
    totals = np.zeros(len(READ_COLUMNS), dtype=np.int64)
    for table in summary["tables"]:
        path = directory / f"table-{table['id']}.csv.gz"
        print(f"Counting first reads in {path}", flush=True)
        visits, hits = first_reads(path, len(first_visits))
        first_visits += visits
        totals += hits
    expected = [summary["row_hits"][column[:-5]] for column in READ_COLUMNS]
    if totals.tolist() != expected:
        raise ValueError(f"CSV read counts differ from the analysis summary in {directory}")
    # Keep window endpoints even before the first read, including the final partial window.
    requests = np.minimum(np.arange(1, len(first_visits) + 1) * summary["window_requests"],
                          summary["workload_requests"])
    return requests, np.cumsum(first_visits)


def key_count(summary, receipt):
    if receipt.get("manifest_sha256") != summary["manifest_sha256"]:
        raise ValueError("Key counts do not match the access analysis manifest")
    baseline = receipt["baseline_distinct_pk"]
    total = receipt["total_distinct_pk"]
    if type(baseline) is not int or type(total) is not int or not 0 <= baseline <= total:
        raise ValueError("Invalid distinct key counts")
    if baseline != summary["baseline_rows"]:
        raise ValueError("Baseline key count differs from the access analysis")
    return total


def coverage_series(trace, summary, requests, distinct, total):
    if np.any((distinct < 0) | (distinct > total)):
        raise ValueError(f"Read key count outside the key universe for {trace['name']}")
    percent = distinct / total * 100 if total else np.full(len(distinct), np.nan)
    return pd.DataFrame({"trace": trace["name"], "label": trace["label"],
                         "cumulative_requests": requests, "distinct_pk": distinct,
                         "total_distinct_pk": total, "coverage_percent": percent,
                         "manifest_sha256": summary["manifest_sha256"]})


def plot_coverage(series, output):
    global_config.apply_style()
    figure, axis = plt.subplots(figsize=style.FIGURE_SIZE)
    curves = series.groupby("trace", sort=False)
    colors = plt.get_cmap(style.COLOR_MAP, len(curves))
    for index, (_, curve) in enumerate(curves):
        coverage = curve["coverage_percent"]
        axis.loglog(curve["cumulative_requests"], coverage.where(coverage > 0),
                    label=curve["label"].iat[0], color=colors(index),
                    linestyle=style.LINE_STYLES[index % len(style.LINE_STYLES)])

    axis.set(xlabel=style.X_LABEL, ylabel=style.Y_LABEL)
    axis.xaxis.set_major_formatter(EngFormatter())
    axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}%"))
    axis.legend(**style.LEGEND_STYLE)
    save_figure(figure, output)
    series.to_csv(output.with_suffix(".csv"), index=False)
    plt.close(figure)
    print(output, flush=True)


def main():
    config = load_inputs(style.TRACES, parse_arguments(__doc__, analysis_paths=True))
    counts_path = config["analysis_directory"] / "access_key_counts.json"
    if not counts_path.is_file():
        raise FileNotFoundError(f"Missing {counts_path}; run count-access-keys first")
    key_counts = json.loads(counts_path.read_text())
    series = []
    for trace in config["traces"]:
        directory, summary = load_analysis(config, trace)
        name = trace["name"]
        if name not in key_counts:
            raise ValueError(f"Missing key counts for {name}; run count-access-keys")
        total = key_count(summary, key_counts[name])
        requests, distinct = trace_coverage(directory, summary)
        series.append(coverage_series(trace, summary, requests, distinct, total))
    plot_coverage(pd.concat(series, ignore_index=True), config["plot_directory"] / "access_coverage")


if __name__ == "__main__":
    main()
