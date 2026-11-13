#!/usr/bin/env python3
"""Plot Get, Scan and Write locality from Java-extracted returned-row densities."""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from matplotlib.ticker import EngFormatter
import numpy as np
import pandas as pd

from _common import DEFAULT_CONFIG, STYLE, load_analysis, load_config, save_figure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    config = load_config(args.config)
    plt.style.use(STYLE)
    for trace in config["traces"]:
        directory, summary = load_analysis(config, trace)
        table = pd.read_csv(directory / "access_density.csv.gz")
        windows = max(1, summary["windows"])
        matrices = [np.zeros((summary["pk_bins"], windows), dtype=np.int64) for _ in range(3)]
        if not table.empty:
            x, y = table["window"].to_numpy(), table["pk_bin"].to_numpy()
            matrices[0][y, x] = table["get_hits"]
            matrices[1][y, x] = table["scan_hits"]
            matrices[2][y, x] = table["put_hits"] + table["delete_hits"]
        maximum = max(2, *(int(m.max()) for m in matrices))
        fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True, sharey=True,
                                 layout="constrained")
        counts, hits = summary["operation_counts"], summary["row_hits"]
        names = {t["id"]: t["name"] for t in summary["table_names"]}
        for ax, title, matrix, requests, returned in zip(
                axes, ("Get", "Scan", "Write (Put / Delete; updates included)"), matrices,
                (counts["get"], counts["scan"], counts["put"] + counts["delete"]),
                (hits["get"], hits["scan"], hits["put"] + hits["delete"])):
            image = ax.imshow(np.ma.masked_equal(matrix, 0), origin="lower", aspect="auto",
                              interpolation="nearest", norm=LogNorm(vmin=1, vmax=maximum),
                              extent=(0, windows * summary["window_requests"], 0,
                                      summary["pk_bins"] * summary["ranks_per_bin"]))
            ax.set_title(f"{title} — {requests:,} requests; {returned:,} row hits", loc="left", fontsize=10)
            ax.set_ylabel("Ordered PK rank")
            ax.set_xlim(0, max(1, summary["workload_requests"]))
            ax.set_ylim(0, max(1, summary["primary_key_count"]))
            ax.yaxis.set_major_formatter(EngFormatter())
            if not returned:
                ax.text(0.5, 0.5, "No requests" if not requests else "No returned rows",
                        transform=ax.transAxes, ha="center", va="center", color="0.4")
            for region in summary["tables"]:
                if region["start"]:
                    ax.axhline(region["start"], color="0.5", linewidth=0.4, alpha=0.5)
        if len(summary["tables"]) > 1:
            regions = [f"{names[r['table']]}: {r['start']:,}–{r['start'] + r['keys'] - 1:,}"
                       for r in summary["tables"]]
            axes[0].text(1.01, 1, "PK-rank table ranges\n" + "\n".join(regions),
                         transform=axes[0].transAxes, va="top", fontsize=7, linespacing=1.5)
        axes[-1].set_xlabel(f"Workload request sequence ({summary['window_requests']:,} requests/window)")
        axes[-1].xaxis.set_major_formatter(EngFormatter())
        scope = "PK only" if summary["secondary_index_coverage"] == "primary_only" else "PK + secondary indexes"
        fig.suptitle(f"{trace['label']} — access locality\n"
                     f"{summary['baseline_rows']:,} baseline rows; {scope}", fontsize=12)
        fig.colorbar(image, ax=axes, fraction=0.025, pad=0.12,
                     label="Returned rows / write entries per cell (log)")
        save_figure(fig, config["plot_directory"] / trace["name"] / "access_locality")
        plt.close(fig)
        print(config["plot_directory"] / trace["name"] / "access_locality")


if __name__ == "__main__":
    main()
