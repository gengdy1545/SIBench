#!/usr/bin/env python3
"""Plot the cumulative share of key hits ranked by exact PK frequency."""

from collections import Counter
import json
import os
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter
import numpy as np
import pandas as pd

from _common import load_analysis, load_inputs, parse_arguments, save_figure
from graph.config import access_concentration_config as style
from graph.config import global_config
from plot_access_coverage import key_count
from plot_access_locality import HIT_COLUMNS, batches

CHUNK_ROWS = 1_000_000


def table_frequencies(path, chunksize=CHUNK_ROWS):
    frequencies = Counter()
    totals = np.zeros(len(HIT_COLUMNS), dtype=np.int64)
    with tempfile.TemporaryFile() as keys, subprocess.Popen(
            ["sort", "-k1,1", "-S", "512M", "--parallel=4"], stdin=subprocess.PIPE,
            stdout=keys, env={**os.environ, "LC_ALL": "C"}) as sorter:
        with sorter.stdin as stream:
            for batch in batches(path, ("key", *HIT_COLUMNS), chunksize):
                counts = batch[list(HIT_COLUMNS)].to_numpy()
                if (counts < 0).any():
                    raise ValueError(f"Negative hit count in {path}")
                totals += counts.sum(axis=0)
                hits = counts.sum(axis=1)
                active = hits > 0
                indexed_hits = pd.Series(hits[active], index=batch["key"].to_numpy()[active])
                indexed_hits.groupby(level=0, sort=False).sum().to_csv(stream, sep=" ", header=False)
        if sorter.wait():
            raise RuntimeError(f"Could not sort key hits in {path}")
        if totals.any():
            keys.seek(0)
            tail_key, tail_hits = None, 0
            for batch in pd.read_csv(keys, sep=" ", names=["key", "hits"],
                                     dtype={"key": str, "hits": np.int64},
                                     keep_default_na=False, chunksize=chunksize):
                grouped = batch.groupby("key", sort=False)["hits"].sum()
                if grouped.index[0] == tail_key:
                    grouped.iloc[0] += tail_hits
                elif tail_key is not None:
                    frequencies[tail_hits] += 1
                # A key at the end of a sorted chunk may continue in the next chunk.
                tail_key, tail_hits = grouped.index[-1], int(grouped.iloc[-1])
                hits, counts = np.unique(grouped.to_numpy()[:-1], return_counts=True)
                frequencies.update({int(hit): int(count) for hit, count in zip(hits, counts)})
            frequencies[tail_hits] += 1
    return frequencies, totals


def trace_frequencies(directory, summary):
    frequencies = Counter()
    totals = np.zeros(len(HIT_COLUMNS), dtype=np.int64)
    for table in summary["tables"]:
        path = directory / f"table-{table['id']}.csv.gz"
        print(f"Counting per-PK hits in {path}", flush=True)
        table_counts, table_totals = table_frequencies(path)
        frequencies.update(table_counts)
        totals += table_totals
    expected = [summary["row_hits"][column[:-5]] for column in HIT_COLUMNS]
    if totals.tolist() != expected or sum(hits * count for hits, count in frequencies.items()) != sum(expected):
        raise ValueError(f"PK hit totals differ from the analysis summary in {directory}")
    return frequencies


def concentration_curve(frequencies, total_keys, target_percent):
    widths = np.array(sorted(frequencies, reverse=True), dtype=np.int64)
    active_keys = sum(frequencies.values())
    if active_keys > total_keys:
        raise ValueError("Hit keys exceed the Trace key universe")
    if active_keys < total_keys:
        widths = np.r_[widths, 0]
    counts = np.array([frequencies[hit] if hit else total_keys - active_keys for hit in widths], dtype=np.int64)
    key_ends = np.r_[0, np.cumsum(counts)]
    hit_ends = np.r_[0, np.cumsum(widths * counts)]
    total_hits = int(hit_ends[-1])
    target_keys = None
    if total_hits:
        target = (total_hits * target_percent + 99) // 100
        tier = np.searchsorted(hit_ends, target) - 1
        target_keys = int(key_ends[tier] + (target - hit_ends[tier] + widths[tier] - 1) // widths[tier])
    ranks = key_ends
    if total_keys:
        ranks = np.r_[ranks, np.geomspace(1, total_keys, style.CURVE_POINTS).round().astype(np.int64)]
    if target_keys is not None:
        ranks = np.r_[ranks, target_keys - 1, target_keys]
    ranks = np.unique(ranks)
    tiers = np.searchsorted(key_ends, ranks[1:]) - 1
    hits = np.r_[0, hit_ends[tiers] + (ranks[1:] - key_ends[tiers]) * widths[tiers]]
    return pd.DataFrame({
        "ranked_pk": ranks,
        "total_pk": total_keys,
        "pk_percent": ranks / total_keys * 100 if total_keys else np.nan,
        "cumulative_hits": hits,
        "total_hits": total_hits,
        "hit_percent": hits / total_hits * 100 if total_hits else np.nan,
        "is_threshold": ranks == target_keys,
    })


def plot_concentration(series, output):
    global_config.apply_style()
    figure, axis = plt.subplots(figsize=style.FIGURE_SIZE)
    curves = series.groupby("trace", sort=False)
    colors = plt.get_cmap(style.COLOR_MAP, len(curves))
    axis.axhline(style.TARGET_PERCENT, color=global_config.MUTED_TEXT_COLOR,
                 linewidth=0.6, linestyle="--", zorder=1)
    for index, (_, curve) in enumerate(curves):
        color = colors(index)
        crossing = curve[curve["is_threshold"]]
        label = curve["label"].iat[0]
        if not crossing.empty:
            key_share = crossing["pk_percent"].iat[0]
            hit_share = crossing["hit_percent"].iat[0]
            axis.plot(key_share, hit_share, marker="o", markersize=3.5, color=color, zorder=4)
            label += f" ({key_share:.3g}%)"
        axis.semilogx(curve["pk_percent"].where(curve["pk_percent"] > 0), curve["hit_percent"],
                      label=label, color=color, linestyle=style.LINE_STYLES[index % len(style.LINE_STYLES)])
    axis.set(xlabel=style.X_LABEL, ylabel=style.Y_LABEL, xlim=(None, 100), ylim=(0, 102),
             yticks=sorted({0, 25, 50, 75, style.TARGET_PERCENT, 100}))
    axis.xaxis.set_major_formatter(FuncFormatter(
        lambda value, _: np.format_float_positional(value, precision=3, fractional=False, trim="-") + "%"))
    axis.yaxis.set_major_formatter(PercentFormatter(100, decimals=0))
    axis.legend(title=f"PK share at {style.TARGET_PERCENT}% of hits", **style.LEGEND_STYLE)
    save_figure(figure, output)
    series.to_csv(output.with_suffix(".csv"), index=False)
    plt.close(figure)
    print(output, flush=True)


def main():
    config = load_inputs(parse_arguments(__doc__, analysis_paths=True))
    counts_path = config["analysis_directory"] / "access_key_counts.json"
    if not counts_path.is_file():
        raise FileNotFoundError(f"Missing key counts at {counts_path}. Run count-access-keys first.")
    counts = json.loads(counts_path.read_text())
    series = []
    for trace in config["traces"]:
        directory, summary = load_analysis(config, trace)
        name = trace["name"]
        if name not in counts:
            raise ValueError(f"Missing key counts for {name}. Run count-access-keys first.")
        total = key_count(summary, counts[name])
        curve = concentration_curve(trace_frequencies(directory, summary), total, style.TARGET_PERCENT)
        series.append(curve.assign(trace=name, label=trace["label"], manifest_sha256=summary["manifest_sha256"]))
    plot_concentration(pd.concat(series, ignore_index=True), config["plot_directory"] / "access_concentration")


if __name__ == "__main__":
    main()
