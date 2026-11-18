#!/usr/bin/env python3
"""Plot key-range hit shares and windowed key reuse for each table and operation."""

from collections import defaultdict
import json
import os
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from matplotlib.ticker import EngFormatter, FuncFormatter, PercentFormatter
import numpy as np
import pandas as pd

from _common import load_analysis, load_inputs, parse_arguments, save_figure
from graph.config import access_locality_config as style
from graph.config.global_config import apply_style

HIT_COLUMNS = ("get_hits", "scan_hits", "put_hits", "delete_hits")
OPERATIONS = ("get", "scan", "write")


def batches(path, columns, chunksize=1_000_000):
    return pd.read_csv(path, usecols=columns, keep_default_na=False, chunksize=chunksize,
                       dtype={column: str if column == "key" else np.int64 for column in columns})


def key_cuts(paths, key_column):
    if key_column == "pk":
        low, high = np.iinfo(np.int64).max, np.iinfo(np.int64).min
        for path in paths:
            for batch in batches(path, [key_column]):
                if len(batch):
                    low = min(low, int(batch[key_column].min()))
                    high = max(high, int(batch[key_column].max()))
        size = max(0, int(high) - int(low) + 1)
        count = min(style.KEY_BINS, size)
        return np.array([low + (i * size + count - 1) // count
                         for i in range(1, count)], dtype=np.int64)

    # Canonical hex keys can be sorted as whole lines in byte order.
    with tempfile.TemporaryFile() as keys, subprocess.Popen(
            ["sort", "-u", "-S", "512M", "--parallel=4"], stdin=subprocess.PIPE, stdout=keys,
            env={**os.environ, "LC_ALL": "C"}) as sorter:
        with sorter.stdin as data:
            for path in paths:
                for batch in batches(path, [key_column]):
                    if len(batch):
                        data.write(b"\n".join(batch[key_column].unique().astype("S")) + b"\n")
        if sorter.wait():
            raise RuntimeError("Could not sort the distinct primary keys")
        keys.seek(0)
        size = sum(block.count(b"\n") for block in iter(lambda: keys.read(1 << 20), b""))
        count = min(style.KEY_BINS, size)
        offsets = {(i * size + count - 1) // count for i in range(1, count)}
        keys.seek(0)
        return np.array([line.rstrip(b"\n") for i, line in enumerate(keys) if i in offsets], dtype="S")


def access_windows(path, key_column, chunksize):
    pending = []
    previous = -1
    for batch in batches(path, ("window", key_column, *HIT_COLUMNS), chunksize):
        windows = batch["window"].to_numpy()
        cuts = np.r_[0, np.flatnonzero(windows[1:] != windows[:-1]) + 1, len(windows)]
        for start, stop in zip(cuts[:-1], cuts[1:]):
            if start == stop:
                continue
            window = int(windows[start])
            if window != previous:
                if window <= previous:
                    raise ValueError(f"Invalid window order in {path}")
                if pending:
                    yield previous, pending[0] if len(pending) == 1 else pd.concat(pending)
                pending = []
                previous = window
            pending.append(batch.iloc[start:stop])
    if pending:
        yield previous, pending[0] if len(pending) == 1 else pd.concat(pending)


def summarize_table(path, key_column, cuts, window_count, windows_per_column, chunksize=1_000_000):
    display_columns = (window_count + windows_per_column - 1) // windows_per_column
    grid = np.zeros((display_columns, len(cuts) + 1, len(OPERATIONS)), dtype=np.int64)
    totals = np.zeros((window_count, len(OPERATIONS)), dtype=np.int64)
    repeats = np.zeros_like(totals)
    adjacent = np.zeros_like(totals)
    original_totals = np.zeros(len(HIT_COLUMNS), dtype=np.int64)
    previous_window = -2
    for window, rows in access_windows(path, key_column, chunksize):
        if window >= window_count:
            raise ValueError(f"Window outside analysis range in {path}: {window}")
        keys = rows[key_column].to_numpy(dtype="S" if key_column == "key" else np.int64)
        if np.any(keys[1:] <= keys[:-1]):
            raise ValueError(f"Expected unique ordered keys in window {window} of {path}")
        counts = rows[list(HIT_COLUMNS)].to_numpy()
        if (counts < 0).any():
            raise ValueError(f"Negative hit count in {path}")
        original_totals += counts.sum(axis=0)
        hits = np.column_stack((counts[:, 0], counts[:, 1], counts[:, 2] + counts[:, 3]))
        active = hits > 0
        totals[window] = hits.sum(axis=0)
        repeats[window] = totals[window] - active.sum(axis=0)
        indices = np.searchsorted(cuts, keys, side="right")
        np.add.at(grid[window // windows_per_column], indices, hits)
        if window == previous_window + 1:
            positions = np.searchsorted(previous_keys, keys)
            current = np.flatnonzero(positions < len(previous_keys))
            current = current[previous_keys[positions[current]] == keys[current]]
            adjacent[window] = (hits[current] * previous_active[positions[current]]).sum(axis=0)
        previous_window, previous_keys, previous_active = window, keys, active
    if not np.array_equal(grid.sum(axis=(0, 1)), totals.sum(axis=0)):
        raise ValueError(f"Heatmap hit totals differ from reuse totals in {path}")
    return grid, totals, repeats, adjacent, original_totals


def plot_operation(output, title, integer_keys, summary, grid, totals, repeats, adjacent):
    figure = plt.figure(figsize=style.FIGURE_SIZE)
    layout = figure.add_gridspec(2, 2, width_ratios=(1, 0.035))
    spatial = figure.add_subplot(layout[0, 0])
    scale = figure.add_subplot(layout[0, 1])
    temporal = figure.add_subplot(layout[1, 0], sharex=spatial)
    figure.suptitle(title)
    work = summary["workload_requests"]
    width = summary["window_requests"]
    heatmap_width = width * style.WINDOWS_PER_HEATMAP_COLUMN
    spatial.set_title(f"{heatmap_width:,} requests per column", loc="left")
    spatial.set_ylabel("Integer PK span" if integer_keys else "Observed PK rank")
    spatial.yaxis.set_major_formatter(PercentFormatter(1))
    spatial.set_ylim(0, 1)
    spatial.tick_params(labelbottom=False)
    temporal.set_ylabel("Share of hits")
    temporal.yaxis.set_major_formatter(PercentFormatter(1))
    temporal.set(ylim=(0, 1), xlim=(0, max(1, work)),
                 xlabel=style.X_LABEL.format(window_requests=width))
    temporal.xaxis.set_major_formatter(EngFormatter())
    if totals.any():
        x_edges = np.minimum(np.arange(len(grid) + 1) * heatmap_width, work)
        shares = grid / grid.sum(axis=1, keepdims=True).clip(1)
        image = spatial.pcolormesh(x_edges, np.linspace(0, 1, grid.shape[1] + 1),
                                   np.ma.masked_equal(shares.T, 0), rasterized=True,
                                   norm=LogNorm(*style.HEATMAP_SHARE_LIMITS))
        figure.colorbar(image, cax=scale, label="Share of window hits",
                        format=FuncFormatter(lambda value, _: f"{value * 100:g}%"))
        edges = np.minimum(np.arange(len(totals) + 1) * width, work)
        centers = (edges[:-1] + edges[1:]) / 2
        denominator = np.where(totals > 0, totals, np.nan)
        curves = (repeats / denominator, adjacent / denominator)
        curves[1][0] = np.nan
        for values, (label, options) in zip(curves, style.REUSE_LINES.items()):
            temporal.plot(centers, values, label=label, linewidth=0.9, marker=".", markersize=1, **options)
        temporal.legend(loc="lower left", bbox_to_anchor=(0, 1), borderaxespad=0, ncol=2)
    else:
        scale.set_axis_off()
        for axis in (spatial, temporal):
            axis.text(0.5, 0.5, "No hits", transform=axis.transAxes, **style.EMPTY_TEXT_STYLE)
    save_figure(figure, output)
    plt.close(figure)


def main():
    arguments = parse_arguments(__doc__, analysis_paths=True)
    config = load_inputs(style.TRACES, arguments)
    apply_style()
    groups = defaultdict(list)
    expected = {}
    actual = {}
    for trace in config["traces"]:
        directory, summary = load_analysis(config, trace)
        name = trace["name"]
        expected[name] = [summary["row_hits"][column[:-5]] for column in HIT_COLUMNS]
        actual[name] = np.zeros(len(HIT_COLUMNS), dtype=np.int64)
        for table in summary["tables"]:
            group = (summary["baseline_trace_sha256"], table["name"], table["key_type"])
            path = directory / f"table-{table['id']}.csv.gz"
            groups[group].append((trace, summary, table, path))

    for (_, table_name, key_type), sources in groups.items():
        print(f"Computing key intervals for {table_name}", flush=True)
        key_column = "pk" if key_type == "int" else "key"
        cuts = key_cuts([path for _, _, _, path in sources], key_column)
        for trace, summary, table, path in sources:
            grid, totals, repeats, adjacent, counts = summarize_table(
                path, key_column, cuts, summary["windows"], style.WINDOWS_PER_HEATMAP_COLUMN)
            actual[trace["name"]] += counts
            for index, operation in enumerate(OPERATIONS):
                output = config["plot_directory"] / trace["name"] / f"table-{table['id']}" / f"access_{operation}"
                title = f"{trace['label']} · {table_name} · {operation.title()}"
                plot_operation(output, title, key_type == "int", summary,
                               grid[:, :, index], totals[:, index], repeats[:, index], adjacent[:, index])
                print(output, flush=True)
    for name, counts in actual.items():
        if counts.tolist() != expected[name]:
            raise ValueError(f"CSV hit totals differ from the analysis summary for {name}")


if __name__ == "__main__":
    main()
