# Plotting

Figures are rendered from analysis tables, Trace manifests and replay reports.
Trace decoding, key interpretation and transaction semantics stay in the Java
analysis layer.

## Setup

Python 3.9+ and the Linux Libertine O font are required. Access locality,
cumulative access coverage and access concentration also use GNU `sort`.

```bash
python3 -m pip install -r graph/requirements.txt
sudo apt-get install fonts-linuxlibertine   # Ubuntu/Debian
```

For Trace figures, build `build/sibench/sibench.jar` first (`mvn package`); their
entry points use it to resolve Trace paths. Replay figures read saved reports and
need only the plotting dependencies.

## Layout

| Path | Contents |
| --- | --- |
| `scripts/trace/` | Operation mix, access locality, access coverage, access concentration and update locality. |
| `scripts/replay/` | Index Service replay performance. |
| `config/global_config.py` | Paths, widths, typography, colors, export. |
| `config/*_config.py` | One file per figure: `TRACES` selection, size, labels and local overrides. |

Run scripts from any directory; configured paths resolve from the repository root
and command-line paths from the working directory. Outputs use fixed names under
`results/analysis/trace/`, `results/plots/trace/` and `results/plots/replay/`, so a
rerun overwrites them.

## Trace input selection

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
Use `--analysis-directory PATH` to reuse a completed extraction and `--out PATH`
to write a separate set of figures. These paths resolve from the working directory.

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

**Key-range heatmap.** Each column combines ten extraction windows by default
(100,000 requests with the public configuration). Color shows the fraction
of that column's hits in each key interval, on a shared logarithmic scale.
Integer keys use up to 256 equal-width intervals across the observed PK span.
Other keys are ordered by their canonical encoding and use up to 256 intervals
with equal numbers of distinct observed keys. Selections with the same
baseline, table name and key type share the interval boundaries. The vertical
axis shows position in the resulting span or rank order. Zero-hit cells are blank.

**Window reuse.** At each extraction window, the curves show the share of hits
after each key's first hit in that window, and the share of hits on keys also
hit in the immediately preceding window by the same operation. The first
window has no previous-window value. Windows with no hits for the operation
leave gaps. Get and Scan count returned rows and Write combines Put and Delete.
Both panels use the exported request sequence and include the final partial
window. These figures describe logical PK locality. Request-level reuse
intervals require the ordered access stream, and physical locality requires
storage layout data.

**Outputs.** Under `results/analysis/trace/<name>/accesses/`: `summary.json`,
`table-<id>.csv.gz` (`window,pk,key,get_hits,scan_hits,put_hits,delete_hits`, where
`key` is the canonical hex encoding), `analysis.log` and `preparation.json`, which
binds configuration, command and JAR SHA-256. Plots are
`results/plots/trace/<name>/table-<id>/access_{get,scan,write}.{pdf,png}`. Temporary
state under `build/sibench/analysis/` is removed on success and kept after a failure.

**Resources.** The preparation script caps the Java heap at 2 GiB (native
memory is separate). Java uses disk-backed sorts and a 65,536-entry lookup cache.
Plotting streams CSV batches into a fixed-width heatmap and keeps the current
and preceding windows for exact key matching. It scans the key column first
to set shared intervals. Non-integer keys also need temporary disk space for
the distinct-key sort.

The plotting regression check uses the dependencies above.

```bash
python3 -m unittest discover -s src/test/python -p test_access_locality.py
```

## Cumulative access coverage

```bash
java -jar build/sibench/sibench.jar count-access-keys config/sibench.yaml \
  > results/analysis/trace/access_key_counts.json
python3 graph/scripts/trace/plot_access_coverage.py
```

This figure shares the [access-locality extraction](#access-locality) and its
path overrides. It plots one curve per selected Trace on a single double-column
figure. Selections default to those in `access_locality_config.py`, and
`access_coverage_config.py` owns the appearance.

Each point counts the distinct (table, PK) pairs returned by a workload Get
or Scan up to the end of that window. Repeated reads add one key only at its
first window. The percentage divides that count by the fixed union of keys
in the baseline and workload Puts, including Puts carried in private read
contexts. Repeated Puts, deletes and reinsertion of the same PK do not change
that union. The horizontal axis counts all exported workload requests.
Both axes are logarithmic. Axis title typography follows the global style.
The CSV includes zero-read prefixes, the final partial window, the distinct
read count, the total key count, the percentage and the Trace manifest hash.
An empty key universe has an undefined percentage.

`count-access-keys` streams the validated Trace files without replaying reads.
It shares a baseline sort across byte-identical baseline inputs and uses
`analysis.accesses.sort_memory_bytes`. Add benchmark entry names after the
runtime path to count a subset. Place its JSON output in the analysis directory
selected for plotting. The plotter checks the key-count and access-analysis
manifest identities, streams one table at a time to find first reads, and
reconciles Get and Scan hit totals to the extraction summary. Outputs are
`access_coverage.{pdf,png,csv}` in the plot directory.

```bash
python3 -m unittest discover -s src/test/python -p test_access_coverage.py
```

## Access concentration

```bash
python3 graph/scripts/trace/plot_access_concentration.py
```

Use the same access extraction, key-count receipt and path overrides as
[cumulative access coverage](#cumulative-access-coverage). The script
sums Get, Scan, Put and Delete hits for each exact (table, PK) pair over the
Trace and reconciles the totals to the analysis summary. A Scan contributes
one hit per returned row. The plot ranks keys by total hits, including
zero-hit keys in the fixed denominator. The per-table sort uses temporary
disk space for intermediate key counts.

The horizontal axis shows the share of the key universe taken from the top
of the ranking, on a logarithmic scale. The vertical axis shows their
cumulative share of key hits on a linear scale. The 90% guide and legend
report the first whole-key rank meeting that hit threshold. The CSV retains
the integer counts and exact threshold row. The renderer samples additional
display points between equal-frequency ranks for the logarithmic horizontal
axis. Outputs are `access_concentration.{pdf,png,csv}`.

```bash
python3 -m unittest discover -s src/test/python -p test_access_concentration.py
```

## Replay performance

```bash
python3 graph/scripts/replay/plot_replay_performance.py --results path/to/sweep
```

Six vertically stacked bar panels share the Trace axis: baseline load and workload
throughput, workload read P50/P99 and write P50/P99. Each Trace has one bar per
backend. `replay_performance_config.py` selects the sweep directory
(`RESULTS_DIRECTORY`, default `results/replay/`, holding
`<trace>/<backend>/report.json` from `replay-service`),
`TRACES`, `BACKENDS` and the report field of each panel (`METRICS`). Latency panels use a
logarithmic axis. A missing report or a status other than `passed` stops plotting; a
latency without samples (no workload writes in YCSB-C) is shown as n/a. Invalid
numeric measurements, including nonpositive sampled latency on a log axis, are
rejected. Read latency
combines Get and Scan, as in the [replay report](../docs/index-service.md#replay-and-measurement).
`--results PATH` and `--out PATH` override the input and output directories. Outputs are
`replay_performance.{pdf,png,csv}` under `results/plots/replay/`; the CSV keeps every
value in source units (operations/s or ms) with its report SHA-256; unsampled
latencies have an empty value. The plots summarize the selected reports and do not
establish equivalent experiment settings or independent value correctness.

```bash
python3 -m unittest discover -s src/test/python -p test_replay_performance.py
```

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
