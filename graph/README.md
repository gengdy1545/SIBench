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
| `config/traces.json` | Ordered Trace selection, paths and analysis parameters. |
| `requirements.txt` | Plotting dependencies. |

Plotting consumes analysis tables and experiment reports. Trace decoding, key
interpretation and transaction semantics belong to the Java analysis layer.
The existing summary entry points remain in the repository's root `scripts/`
directory, alongside SIBench build and orchestration scripts.

## Trace storage

COS holds the current published collection, with one selected Trace per
benchmark/workload. Local Trace directories are disposable caches, conventionally
under `results/cache/traces/<name>/`. Keep the complete manifest, baseline,
workload and work-unit files together; verify downloaded bytes against the
published inventory before analysis. Retain input identities in analysis results.
Plotting consumes explicitly selected inputs and does not start benchmark
generation when a cache is missing. Dataset scale and index scope must be taken
from each input manifest; a current-format development input is not a full-scale
benchmark result.

## Operation mix and access locality

Edit `config/traces.json` to select Trace directories and display labels. Paths
inside this file resolve from the repository root, including when the command
runs from another directory. Trace list order controls the bars within each
operation group. All three entry points accept `--config PATH`.
The default window contains 10,000 requests, so a 10M-request Trace has 1,000
windows. Current small YCSB inputs fit in one window; reduce `window_requests`
and rerun preparation to inspect their temporal detail without generating data.

```bash
python3 graph/scripts/trace/plot_operation_mix.py
python3 graph/scripts/trace/prepare_analysis.py
python3 graph/scripts/trace/plot_access_locality.py
```

The operation mix chart reuses `scripts/summarize_trace.py` to read recorded
Get/Scan/Put/Delete counts. Each Trace's shares sum to 100% when its workload is
nonempty. Baseline loading, transaction markers and replay-assembled WriteBatch
RPCs are excluded; ReadGroup members count individually. All exported indexes
count, so inputs with different index scopes are not a matched-scope comparison.
Manifest counts alone do not independently validate Trace payloads. Output is
`results/plots/trace/operation_mix.{pdf,png,csv}`; CSV retains exact counts,
fractions and input manifest identities.

`prepare_analysis.py` invokes the configured SIBench JAR once per input. Build
it with the root Maven project (`mvn package`, with the native dependency paths
required by your build). The Java CLI can also be called directly:

```bash
java -jar build/sibench/sibench.jar analyze-accesses \
  results/cache/traces/ycsb-a results/analysis/trace/ycsb-a/accesses \
  10000 2000 134217728
```

The final arguments are requests per window, maximum PK bins and the byte budget
for each external sort. The analyzer validates the complete input, loads its
baseline and committed version history into the existing local RocksDB versioned
backend, and executes each recorded read at its snapshot with its private writes.
It preserves range bounds, direction and limits, maps secondary results back to
their primary keys, and rejects result-count mismatches. Group result counts are
checked after union/intersection; density counts every member's returned rows.

The vertical coordinate is the rank of distinct `(table ID, primary key)` pairs,
sorted by source key order and including baseline keys, workload writes and
private write contexts. Tables occupy adjacent labeled regions. This is not the
original numeric PK, RowID, physical address or live row count. Updates keep their
PK position; aborted private inserts can occupy a rank without a shared write.
The actual upper range comes from the data, rather than a fixed 97M bound.
Bins can span table boundaries; increase `pk_bins` for finer spatial resolution.

The horizontal coordinate counts all exported workload requests in stored Trace
order, excluding baseline and transaction markers. Each ReadGroup member gets a
position. A Scan returning N rows contributes N hits at that one request's window,
not N request positions. Empty queries have no row hits. Every exported Put/Delete
contributes one write hit, including secondary maintenance; updates are represented
by their recorded Put/Delete entries, not an invented Update RPC.

Java emits `summary.json`, `access_density.csv.gz`, `analysis.log` and
`preparation.json` under `results/analysis/trace/<name>/accesses/`. The preparation
record binds the effective configuration, command and JAR SHA-256; the summary
binds input identities and statistics. A failed preparation invalidates the old
summary. Temporary state stays under `build/sibench/analysis/`, is deleted after
success, and remains for diagnosis after failure. Outputs must not overlap inputs.

Plotting produces `results/plots/trace/<name>/access_locality.{pdf,png}` with three
vertically aligned Get, Scan and Write panels. They share axes and a logarithmic
hit-count color scale. Zero cells are blank and empty panels remain visible.
Counts measure returned rows or write entries, not source SQL result rows, engine
entries examined, physical I/O, elapsed time or cost.

Changing only appearance requires rerunning the plotting commands. Changing
inputs, analyzer JAR, window size or PK binning requires preparation again. The
locality plotter checks the preparation JAR digest and the summary input identity
and dimensions before accepting cached extraction. Extraction costs include full baseline loading, retained
version storage, sorting and enumeration of all Scan results, which can greatly
exceed the request count. Two external sorts can each use the configured budget;
disk-backed rank maps and group sets use bounded entry buffers, and native index
memory is additional. RowID-to-rank lookups keep at most 65,536 hot entries in an
LRU cache. The preparation script caps the Java heap at 2 GiB; this does not cap
native memory. Python materializes three density arrays for one Trace at
a time. No 87M-row time, memory or disk-capacity claim follows from small inputs.

## Update locality

Generate locality tables, then plot them:

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/cache/traces/ycsb-a config/sibench.yaml results/analysis/trace/ycsb-a
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
