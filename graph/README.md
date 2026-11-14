# Plotting

Run plotting scripts from the repository root with Python 3.9+.
Install their dependencies in your Python environment:

```bash
python3 -m pip install -r graph/requirements.txt
```

Install the Linux Libertine O font before plotting. On Ubuntu/Debian:

```bash
sudo apt-get install fonts-linuxlibertine
fc-match 'Linux Libertine O'
```

## Layout

| Path | Responsibility |
| --- | --- |
| `scripts/trace/` | Trace distributions, access hotspots and update locality. |
| `scripts/index/` | Index Service performance, backend comparisons and resources. |
| `scripts/pixels/` | Pixels query performance and sink freshness. |
| `config/global_config.py` | Shared data locations, analysis settings, column widths, typography, colors and export settings. |
| `config/operation_mix_config.py` | Operation-mix benchmark selection, size, bars, axes and legend. |
| `config/access_locality_config.py` | Access-analysis benchmark selection and per-table/per-operation plot appearance. |
| `config/update_locality_config.py` | Separate sizes and appearance for heatmap, coverage, business coverage and L0 figures. |
| `requirements.txt` | Plotting dependencies. |

Plotting consumes analysis tables and experiment reports. Trace decoding, key
interpretation and transaction semantics belong to the Java analysis layer.
The existing summary entry points remain in the repository's root `scripts/`
directory, alongside SIBench build and orchestration scripts.

## Trace storage

Set `RUNTIME_CONFIG` in `config/global_config.py` to the SIBench runtime YAML
selecting the existing inputs, such as `config/sibench.yaml`. Trace directories
come from its named entries. The Java `trace-paths` command resolves them, so Python
does not duplicate configuration semantics. Use the persistent local Trace directly;
no additional cache is needed. For another host, use the verified transfer workflow
in [Trace selection and storage](../docs/trace.md#selection-and-storage).
Plotting never generates or downloads missing input. Keep each complete Trace bundle
together; dataset scale and index scope come from its manifest. Analysis results bind
the selected input and analyzer identities.

All three entry points require the configured SIBench JAR to resolve benchmark paths,
including the manifest-only operation-mix plot.

## Operation mix and access locality

Edit `TRACES` in each figure's configuration to select benchmark names and display
labels. Common paths and extraction settings live in `config/global_config.py`;
paths resolve from the repository root even when scripts run from another directory.
Trace list order controls the benchmarks from top to bottom on the vertical axis.
Each benchmark has one horizontal 100% stacked bar: Get, Scan, Put and
Delete from left to right, with consistent solid colors. The single-column figure
uses horizontal benchmark names and percentage labels with up to two decimal
places and no trailing zeros. Labels sit inside the segments; shares below 6%
use percentages to the right of their segment beside a 2.5 pt square in the
operation's color, without enlarging or removing the segment, with
values below 0.01% marked `<0.01%`. Empty workloads have no filled bar.
Operations absent from every selected workload are omitted from the bars and
legend; the remaining operations keep their assigned colors.
The horizontal label, `Share of replay operations`, means the fraction of exported workload
requests belonging to each operation. Shared layout padding accommodates labels
and the legend while preserving the configured single-column width.
Each entry point imports its corresponding figure configuration and the global
configuration directly; there is no `--config` argument or additional plotting
input configuration. `prepare_analysis.py` uses `access_locality_config.py`, matching
the figure it prepares. Input names, manifests, parameters and output paths remain
validated. Analysis reports and effective snapshots remain JSON.
The default window contains 10,000 requests, so a 10M-request Trace has 1,000
windows. Inputs shorter than the configured window produce one window; reduce
`WINDOW_REQUESTS` in the global configuration and rerun preparation to inspect their temporal detail without
generating data.

```bash
python3 graph/scripts/trace/plot_operation_mix.py
python3 graph/scripts/trace/prepare_analysis.py
python3 graph/scripts/trace/plot_access_locality.py
```

The operation mix chart reuses `scripts/summarize_trace.py` to read recorded
Get/Scan/Put/Delete counts. Each Trace's shares sum to 100% when its workload is
nonempty. These are exported Trace requests: [transaction-local intermediate
writes are coalesced](../docs/trace.md#operations-and-index-expansion), so the shares
do not represent original SQL statement proportions. Baseline loading,
transaction markers and replay-assembled WriteBatch
RPCs are excluded; ReadGroup members count individually. All exported indexes
count, so inputs with different index scopes are not a matched-scope comparison.
Manifest counts alone do not independently validate Trace payloads. Output is
`results/plots/trace/operation_mix.{pdf,png,csv}`; CSV retains all four operation
types, including zero counts, fractions and input manifest identities.

`prepare_analysis.py` resolves paths with the configured SIBench JAR, then invokes its analyzer once per input. Build
it with the root Maven project (`mvn package`, with the native dependency paths
required by your build). The Java CLI can also be called directly:

```bash
java -jar build/sibench/sibench.jar analyze-accesses \
  config/sibench.yaml ycsb-a results/analysis/trace/ycsb-a/accesses \
  10000 134217728
```

The final arguments are requests per window and the byte budget for each external sort. The analyzer validates the complete input, loads its
baseline and committed version history into the existing local RocksDB versioned
backend, and executes each recorded read at its snapshot with its private writes.
It preserves range bounds, direction and limits, maps secondary results back to
their primary keys, and rejects result-count mismatches. Group result counts are
checked after union/intersection; density counts every member's returned rows.

Access counts retain each exact `(table ID, primary key)` separately within each
window. There are no PK bins, ranges or global ranks. Integer PKs use their actual
numeric values on the vertical axis. Other supported key types use distinct
ordered categorical positions, with original key labels; their spacing has no
numeric or physical meaning. Java exports display values and canonical key bytes,
so Python does not decode Trace keys. Only tick labels are thinned for readability;
no keys or hits are sampled away.

Later inserts contribute at their actual keys only in the windows that contain
their writes or returned rows. They do not change earlier complete-window counts.
Deletes do not shift keys, and reinsertion uses the same key. No baseline-derived
or final-key-universe coordinate system is constructed. Axis bounds describe the
observed hits, not a table's live row count or SST layout. Physical pixels can
still overlap when many keys are close together; the exported counts remain exact.

The horizontal coordinate counts all exported workload requests in stored Trace
order, excluding baseline and transaction markers. Each ReadGroup member gets a
position. A Scan returning N rows contributes N hits at that one request's window,
not N request positions. Empty queries have no row hits. Every exported Put/Delete
contributes one write hit, including secondary maintenance; updates are represented
by their recorded Put/Delete entries, not an invented Update RPC.

Java emits `summary.json`, one `table-<id>.csv.gz` per table, `analysis.log` and
`preparation.json` under `results/analysis/trace/<name>/accesses/`. Each CSV has
`window,pk,key,get_hits,scan_hits,put_hits,delete_hits`: `pk` is the original key's
display value and `key` is its canonical order-preserving encoding in hexadecimal.
The summary maps table IDs to source names and key types. The preparation record
binds the effective configuration, command and JAR SHA-256; the summary binds input
identities and statistics. A failed preparation invalidates the old summary.
Temporary state stays under `build/sibench/analysis/`, is deleted after success,
and remains for diagnosis after failure. Outputs must not overlap inputs.

Plotting produces separate `access_get`, `access_scan` and `access_write` PNG/PDF
files under `results/plots/trace/<name>/table-<id>/`. Every point is one exact PK's
hit count in one window, plotted at that window's midpoint. The last midpoint uses
the actual remaining requests. Color shows the count on a logarithmic scale;
zero hits have no point and empty plots say `No hits`. Plots have axis labels and
a colorbar, with no top titles or PK-rank table-range annotation. File paths and
summary metadata identify the benchmark, table and operation. Counts measure
returned rows or write entries, not engine entries examined, physical I/O or
compaction costs.

Appearance changes require only plotting again. Input, analyzer or window changes
require preparation again. The plotter rejects stale input/analyzer identities and
the replaced PK-bin summaries. Old aggregated CSVs cannot recover exact keys;
re-extract from the preserved Trace using the current CLI and Python configuration,
which no longer accept `pk_bins`. Historical reports can remain recorded evidence
but are not inputs to the current plotter.

Extraction still validates the full Trace and executes snapshot-correct reads.
It externally sorts RowID-to-PK mappings and exact window/PK hits; each sort uses
the configured byte budget. Disk-backed maps and the 65,536-entry lookup cache
avoid retaining the entire key universe in memory. The preparation script caps the
Java heap at 2 GiB; this does not cap native memory. Exact hit sorting and CSV output
can be substantially larger than the former binned tables, especially for Scans.
Python loads one table's counts at a time and renders every nonzero point, with
rasterized points in the PDF. Large tables can require substantial RAM and drawing
time. Small tests do not establish full-input extraction or plotting capacity;
large runs need a separate resource budget.

## Update locality

Generate locality tables, then plot them:

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  config/sibench.yaml ycsb-a results/analysis/trace/ycsb-a
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

Configuration follows [retina-graph's Python configuration structure](https://github.com/gengdy1545/retina-graph/tree/61fdb5a041a6e477b6613616793a7a161c6ec586/config):
shared appearance lives in `global_config.py`; each plotting script imports its
own `*_config.py`. For example:

```python
from graph.config.global_config import apply_style
from graph.config import operation_mix_config as style

apply_style()
fig, ax = plt.subplots(figsize=style.FIGURE_SIZE)
```

`global_config.py` owns `SINGLE_COLUMN_WIDTH` in inches, following retina-graph;
`DOUBLE_COLUMN_WIDTH` is derived as twice that width. Each figure configuration
selects one of these constants and sets its own height, for example:

```python
from graph.config.global_config import SINGLE_COLUMN_WIDTH

FIGURE_SIZE = (SINGLE_COLUMN_WIDTH, 2.6)
```

Use `DOUBLE_COLUMN_WIDTH` for a double-column figure. Change shared widths,
colors, fonts, margins and export settings in the global configuration rather than copying
their values into each figure. Heights do not change automatically with input size.
`update_locality_config.py` names the dimensions of each of its four outputs
separately. Titles, labels and legends belong alongside those
dimensions; plotting scripts consume them directly.

`FIGURE_MARGIN_MM` sets equal outer layout padding on all four sides (default:
1 mm). `apply_style()` enables constrained layout for all figures and converts
this value to Matplotlib's inch-based horizontal and vertical padding. Labels,
legends and colorbars participate in layout; individual figures do not set outer
margins. Padding applies to content bounding boxes, so visible ink may leave
slightly more whitespace. Exports preserve the configured canvas dimensions
without tight cropping or scaling the text.

Select Matplotlib's `Agg` backend in the script before importing
`pyplot` so plotting works on hosts without a display. Create output directories
before exporting and close figures after saving. `OUTPUT_FORMATS` and `SAVE_PARAMS`
in the global configuration control export; defaults are PDF and 300-DPI PNG.
Execute scripts directly with `python3 graph/scripts/<domain>/<script>.py`;
the shared entry-point helper resolves imports and paths from the repository location.

The global configuration uses Linux Libertine O and separately defines the base text,
figure titles/labels, axes titles/labels, x/y tick labels and legend text/titles
at 10 pt. `ANNOTATION_FONT_SIZE` separately controls operation-mix percentage
labels; colorbars inherit the axes and tick settings. Keep typography in this
file rather than overriding it in scripts.
The shared categorical palette uses Morandi-inspired blue, sand, sage, rose,
mauve, taupe and warm gray, with unframed legends and white backgrounds.
Use line styles or markers as well as color when distinguishing series. Heatmaps
use the sequential `magma` palette. PDF exports embed fonts. Saving preserves the
configured canvas size; keep titles and annotations inside it rather than enabling
`bbox_inches="tight"`, which changes the exported dimensions.

With the plotting dependencies and font installed, run the consumer and export checks:

```bash
python3 -m unittest discover -s src/test/python -p 'test_trace_plots.py'
```

Keep derived tables under `results/analysis/{trace,index,pixels}/` and figures
under `results/plots/{trace,index,pixels}/`, with descriptive source or workload
subdirectories when needed. These ignored outputs use fixed paths: rerunning
overwrites matching filenames without run IDs or backups. Unrelated files are
not removed automatically. Keep raw inputs outside the plotting source tree.
