#!/usr/bin/env python3
"""Plot exact-PK hits separately for each table and operation."""

import argparse

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from matplotlib.ticker import EngFormatter
import numpy as np
import pandas as pd

from _common import load_analysis, load_inputs, save_figure
from graph.config import access_locality_config as style
from graph.config.global_config import apply_style


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    config = load_inputs(style.TRACES)
    apply_style()
    for trace in config["traces"]:
        directory, summary = load_analysis(config, trace)
        for table in summary["tables"]:
            data = pd.read_csv(directory / f"table-{table['id']}.csv.gz",
                               dtype={"pk": str, "key": str}, keep_default_na=False)
            ticks = None
            if table["key_type"] == "int":
                y = data["pk"].map(int)
            else:
                keys = data[["key", "pk"]].drop_duplicates("key").sort_values("key")
                positions = dict(zip(keys["key"], range(len(keys))))
                y = data["key"].map(positions)
                ticks = keys.iloc[np.unique(np.linspace(0, len(keys) - 1,
                                                        min(style.KEY_TICKS, len(keys)), dtype=int))]
            left = data["window"] * summary["window_requests"]
            x = (left + np.minimum(left + summary["window_requests"], summary["workload_requests"])) / 2
            for operation, columns in (("get", ["get_hits"]), ("scan", ["scan_hits"]),
                                       ("write", ["put_hits", "delete_hits"])):
                hits = data[columns].sum(axis=1)
                active = hits > 0
                fig, ax = plt.subplots(figsize=style.FIGURE_SIZE)
                points = ax.scatter(x[active], y[active], c=hits[active],
                                    norm=LogNorm(vmin=1, vmax=max(2, int(hits.max()) if len(hits) else 2)),
                                    **style.POINT_STYLE)
                ax.set_xlabel(style.X_LABEL.format(window_requests=summary["window_requests"]))
                ax.set_ylabel(style.Y_LABEL)
                ax.set_xlim(0, max(1, summary["workload_requests"]))
                ax.xaxis.set_major_formatter(EngFormatter())
                if len(y):
                    low, high = y.min(), y.max()
                    pad = max(0.5, (high - low) * 0.02)
                    ax.set_ylim(low - pad, high + pad)
                if ticks is not None:
                    ax.set_yticks([positions[key] for key in ticks["key"]], ticks["pk"])
                else:
                    ax.ticklabel_format(axis="y", style="plain", useOffset=False)
                if not active.any():
                    ax.text(0.5, 0.5, "No hits", transform=ax.transAxes, **style.EMPTY_TEXT_STYLE)
                fig.colorbar(points, ax=ax, **style.COLORBAR_STYLE)
                output = config["plot_directory"] / trace["name"] / f"table-{table['id']}" / f"access_{operation}"
                save_figure(fig, output)
                plt.close(fig)
                print(output)


if __name__ == "__main__":
    main()
