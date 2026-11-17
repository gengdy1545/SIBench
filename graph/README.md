# Plotting

Figures are rendered from analysis tables and Trace manifests. Trace decoding, key
interpretation and transaction semantics stay in the Java analysis layer.

## Setup

Python 3.9+ and the Linux Libertine O font:

```bash
python3 -m pip install -r graph/requirements.txt
sudo apt-get install fonts-linuxlibertine   # Ubuntu/Debian
```

Build `build/sibench/sibench.jar` first (`mvn package`); every entry point uses it to
resolve Trace paths.

## Layout

| Path | Contents |
| --- | --- |
| `scripts/trace/` | Operation mix, access locality and update locality. |
| `config/global_config.py` | Paths, widths, typography, colors, export. |
| `config/*_config.py` | One file per figure: `TRACES` selection, size, labels and local overrides. |

Run scripts from any directory; paths resolve from the repository root. Outputs use
fixed names under `results/analysis/trace/` and `results/plots/trace/`, so a rerun
overwrites them.

## Input selection

`RUNTIME_CONFIG` (default `config/sibench.yaml`) names the runtime YAML whose
benchmark entries locate the local Traces. Select entries and display labels with
`TRACES` in each figure's configuration; list order is the vertical order. To use
another runtime or JAR for one run, pass `--runtime PATH` and `--jar PATH`.

Plotting never generates or downloads a Trace. Fetch one with the
[transfer workflow](../docs/trace.md#selection-and-storage).

## Operation mix

```bash
python3 graph/scripts/trace/plot_operation_mix.py
```

One 100% stacked bar per Trace: Get, Scan, Update and Insert (`OPERATION_ORDER`).
Shares come from manifest request counts through `scripts/summarize_trace.py`. Put is split
into Insert and Update by business write origin; Scans stay together. Each ReadGroup
member counts separately. The input must have only primary-index requests and no
Deletes, and Insert + Update must equal Put; otherwise the script fails rather than
drop requests. Operations absent from every selected Trace are omitted.

The shares describe exported requests. Transaction-local intermediate writes are
[coalesced](../docs/trace.md#operations-and-index-expansion), so they are not SQL
statement proportions. Baseline loading, transaction markers and replay WriteBatch
RPCs are excluded. Output: `operation_mix.{pdf,png,csv}`; the CSV keeps zero counts and
manifest identities.

## Access locality

```bash
python3 graph/scripts/trace/prepare_analysis.py    # Java extraction, once per Trace
python3 graph/scripts/trace/plot_access_locality.py
```

Preparation runs `analyze-accesses` (below) for each selected Trace; plotting reads its
output. Rerun plotting after appearance changes, and preparation after a change of
Trace, analyzer or runtime configuration. Plotting rejects stale or mismatched analyses.

`analyze-accesses` validates the full Trace, loads the baseline and committed history
into the local versioned RocksDB backend, and executes each read at its snapshot with
its private writes. It rejects result-count mismatches.

```bash
java -jar build/sibench/sibench.jar analyze-accesses \
  config/sibench.yaml ycsb-a results/analysis/trace/ycsb-a/accesses
```

The runtime configuration's `analysis.accesses` sets requests per window
(`window_requests`) and the byte budget of each external sort (`sort_memory_bytes`).

**Counting.** Hits are kept per exact `(table, primary key)` per window; there are no
bins or ranks. A window holds `window_requests` exported workload requests (10,000 in
the public configuration; shorter inputs give one window). A Scan returning N rows adds N hits to its
window; every exported Put or Delete adds one write hit, including secondary
maintenance. Counts are returned rows and write entries, not entries examined,
physical I/O or compaction cost.

**Axes.** Integer keys use their values on the vertical axis; other key types use
ordered categorical positions whose spacing has no meaning. Only tick labels are
thinned, never hits. Color is the hit count on a log scale.

**Outputs.** Under `results/analysis/trace/<name>/accesses/`: `summary.json`,
`table-<id>.csv.gz` (`window,pk,key,get_hits,scan_hits,put_hits,delete_hits`, where
`key` is the canonical hex encoding), `analysis.log` and `preparation.json`, which
binds configuration, command and JAR SHA-256. Plots are
`results/plots/trace/<name>/table-<id>/access_{get,scan,write}.{pdf,png}`. Temporary
state under `build/sibench/analysis/` is removed on success and kept after a failure.

**Resources.** The preparation script caps the Java heap at 2 GiB (not native memory).
Sorts are disk-backed with a 65,536-entry lookup cache. Python loads one table at a
time and draws every nonzero point, so large tables need substantial RAM and time.

## Update locality

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  config/sibench.yaml ycsb-a results/analysis/trace/ycsb-a
python3 graph/scripts/trace/plot_update_locality.py \
  results/analysis/trace/ycsb-a --out results/plots/trace/ycsb-a
```

The input directory needs `summary.json`, `windows.csv` and `heatmap.csv.gz`; with
`l0-batches.csv` it also plots the assumed static L0/L1 overlap. Outputs are per-index
heatmaps and block coverage, a business coverage comparison and optional L0 figures
(default `--out`: `results/plots/trace/locality/`). These are layout estimates, not
measurements of SST or compaction behavior.

## Style

Following [retina-graph](https://github.com/gengdy1545/retina-graph/tree/61fdb5a041a6e477b6613616793a7a161c6ec586/config),
shared appearance lives in `global_config.py` and each script imports its own
`*_config.py`:

```python
from graph.config.global_config import SINGLE_COLUMN_WIDTH, apply_style
from graph.config import operation_mix_config as style

apply_style()
fig, ax = plt.subplots(figsize=style.FIGURE_SIZE)   # e.g. (SINGLE_COLUMN_WIDTH, 2.6)
```

- Widths are `SINGLE_COLUMN_WIDTH` (inches) or `DOUBLE_COLUMN_WIDTH`; each figure sets its
  own height.
- Change fonts, colors, margins and export settings globally; keep figure-specific
  overrides in the figure's configuration. A local constant only takes effect where its
  consumer uses it, e.g. `LEGEND_STYLE` must include `"fontsize": LEGEND_FONT_SIZE`.
- Text is Linux Libertine O: 10 pt body and labels, 9 pt legend and annotations (operation mix
  overrides them to 8 pt and 7 pt).
  Colors are a Morandi-inspired categorical palette on white; heatmaps use `magma`.
  Distinguish series by line style or marker as well as color.
- `FIGURE_MARGIN_MM` (default 1 mm) pads all four sides under constrained layout. Exports
  keep the configured canvas size, so do not use `bbox_inches="tight"`.
- Scripts select the `Agg` backend before importing `pyplot`, create output directories
  before saving and close figures afterwards. `OUTPUT_FORMATS` and `SAVE_PARAMS` default
  to PDF (fonts embedded) and 300-DPI PNG.
