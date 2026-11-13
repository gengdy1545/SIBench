# Plotting

Run plotting scripts from the repository root with Python 3.9+.
Install their dependencies in your Python environment:

```bash
python3 -m pip install -r graph/requirements.txt
```

## Layout

| Path | Responsibility |
| --- | --- |
| `scripts/trace/` | Trace distributions, access hotspots and update locality. |
| `scripts/index/` | Index Service performance, backend comparisons and resources. |
| `scripts/pixels/` | Pixels query performance and sink freshness. |
| `config/style.mplstyle` | Shared Matplotlib fonts, colors and export defaults. |
| `requirements.txt` | Plotting dependencies. |

Plotting consumes analysis tables and experiment reports. Trace decoding, key
interpretation and transaction semantics belong to the Java analysis layer.
The existing summary entry points remain in the repository's root `scripts/`
directory, alongside SIBench build and orchestration scripts.

## Update locality

Generate locality tables with `analyze-locality` as described in the root
[README](../README.md#analysis), then plot them:

```bash
python3 graph/scripts/trace/plot_update_locality.py \
  results/analysis/trace/ycsb-a \
  --out results/plots/trace/ycsb-a
```

The input directory must contain `summary.json`, `windows.csv` and
`heatmap.csv.gz`. If `l0-batches.csv` is present, the script also plots the
assumed static L0/L1 overlap. These are layout estimates, not measurements of
actual SST or compaction behavior.

Outputs include per-index heatmaps and block coverage, a source-wide business
coverage comparison, and optional per-index L0 overlap figures. Without `--out`,
the script writes to `results/plots/trace/locality/`.

## Style and output

Load `config/style.mplstyle` with `plt.style.use()` before creating figures.
Resolve the style path relative to the script's own location, not the working
directory. Select Matplotlib's `Agg` backend in the script before importing
`pyplot` so plotting works on hosts without a display. Create output directories
before exporting, save each figure as both PDF and PNG, and close figures after
saving. Execute scripts directly with `python3 graph/scripts/<domain>/<script>.py`;
no Python path modification or package installation is needed beyond dependencies.

The shared style uses DejaVu Sans, a colorblind-friendly categorical palette,
unframed legends and white backgrounds. Use line styles or markers as well as
color when distinguishing series. Heatmaps use the sequential `magma` palette.
Export produces vector PDF with embedded TrueType fonts and 300-DPI PNG.

Keep derived tables under `results/analysis/{trace,index,pixels}/` and figures
under `results/plots/{trace,index,pixels}/`, with descriptive source or workload
subdirectories when needed. These ignored outputs use fixed paths: rerunning
overwrites matching filenames without run IDs or backups. Unrelated files are
not removed automatically. Keep raw inputs outside the plotting source tree.
