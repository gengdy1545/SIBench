# Pixels experiments over reusable baselines

`pixels-experiment CONFIG [CDC_INPUT_DIRECTORY] PIXELS_BASELINE_DIRECTORY FRESH_OUTPUT_DIRECTORY` restores a sealed COS columnar baseline. With `updates: true`, the required [published CDC input](cdc-input.md) must bind the same CSV identity. With `updates: false`, omit the CDC argument; only the baseline manifest, its local schema/DDL and its canonical COS files are needed. Each run copies and imports existing Pixels files into fresh service state. Build both Pixels phases before running an experiment. PostgreSQL, Debezium and HyBench are not started during file replay. Index Service replay remains independent: export an Index Trace once, then use `replay-service` without Pixels data.

## Prepare once, restore for each run

Configure `data_storage` in the Pixels engine YAML:

```yaml
data_storage:
  config: ../../index/storage.yaml
  profile: cos
  prefix: sibench/pixels
```

The config path is relative to the engine YAML. The referenced profile owns the COS endpoint, region, bucket and credential environment variable names. Use virtual-hosted addressing. Supply those variables on the Pixels host and, for an external query cluster, its workers. Secrets are resolved in the host processes and are not returned in the effective Pixels properties. Without `data_storage`, the live `generate-pixels` workflow can still use local files; reusable columnar preparation/import requires COS.

```bash
java -jar build/sibench/sibench.jar prepare-csv config/sibench.yaml results/capture/hybench-csv
# Run the independent PG and Pixels branches concurrently:
java -jar build/sibench/sibench.jar prepare-pixels-inputs config/sibench.yaml \
  results/capture/hybench-csv results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar check-pixels-baseline results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/capture/hybench results/inputs/hybench-pixels results/pixels/run
```

For static queries, set `pixels_experiment.updates: false`, `queries.enabled: true` with a rendered SQL directory and a [Trino deployment](#trino-deployment), then omit CDC:

```bash
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/inputs/hybench-pixels results/pixels/static-run
```

Static runs derive schema and row counts from the baseline. They do not open original CSV or CDC files, create a sink, or report a CDC manifest identity. Supplying CDC with updates disabled, or omitting it with updates enabled, is an error. Baseline manifest, schema and remote file integrity checks still apply.

CDC output uses the benchmark's `artifacts.directory`. The branches can also be invoked separately with `prepare-cdc CONFIG CSV_INPUT_DIRECTORY` and `prepare-pixels-baseline CONFIG CSV_INPUT_DIRECTORY FRESH_OUTPUT_DIRECTORY`. A failed branch leaves the other completed artifact usable; neither branch waits for the other's baseline. Preparation disables Trace, queries and freshness. Both branches are bounded by the HyBench profile's `native.timeout_minutes`, not by experiment settings.

Columnar preparation converts CSV only once, runs normal Pixels LOAD and inspects the published files. Each table's LOAD write transaction renews its coordinator lease until commit, so a LOAD may outlast one lease period; a renewal failure or rejected commit fails preparation without publishing. Its manifest binds the CSV identity, schema/DDL, expected rows, COS file paths, sizes, SHA-256 digests, row-group counts, hidden timestamp upper bound, index bucket count and source/build identities. It is published after normal service shutdown. Its COS session prefix is retained as the canonical baseline. `check-pixels-baseline` verifies the local manifest/descriptors; remote file contents are verified during import.

Use the [input publication commands](cdc-input.md#cos-publication-and-local-materialization) to persist all three completed artifacts under one content-store prefix:

```bash
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/capture/hybench-csv
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/capture/hybench
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/inputs/hybench-pixels
```

Retain each returned publication digest. Fetch missing local inputs by that digest; reuse existing verified local directories. `publish-input` / `fetch-input` transfer the baseline manifest and its local schema/DDL. The columnar objects stay at the COS locations bound by that manifest; these commands do not duplicate or relocate them. Preserve that canonical COS prefix alongside the published manifest.

Each experiment starts empty metadata, etcd, indexes and Retina. Transaction allocation and visibility watermarks start beyond the imported files' hidden LOAD timestamps. Tables/layouts are recreated, files are copied server-side into a unique run prefix and checked against their bound digests/footers. Import assigns fresh physical identities and rebuilds primary/main indexes and Retina visibility from the bound files. Its copy, verification and reconstruction costs are recorded separately from application and query measurement.

The canonical baseline is immutable. Successful experiments stop services and delete only their own local state and COS prefix. Failed runs retain their owned objects and logs for diagnosis. Restore by starting another fresh experiment; do not loop CDC over an updated instance or edit the canonical objects. No CSV parsing/encoding is performed by the restore path. For update experiments, the CDC input's retained CSV identity checks remain part of input verification.

Current integration uses one Retina/index server and ordered baseline files. The target COS endpoint/region/bucket and Pixels source/patch identity must match the baseline; bucket settings and file digests must also match. Different physical table/index/file/RowIDs across runs are allowed because all mappings are reconstructed together. This is fresh baseline reconstruction, not crash recovery of an updated service or an index checkpoint cache.

## Modes and configuration

The required `pixels_experiment` block in [the runtime configuration](../config/sibench.yaml) owns experiment switches. Engine resources and the Pixels host remain in [the Pixels engine configuration](../config/engine/pixels/pixels.yaml).

| Workflow | updates | trace | queries.enabled | freshness.enabled |
| --- | --- | --- | --- | --- |
| Prepare CSV / Pixels baseline | Off | Off | Off | Off |
| Prepare CDC (`prepare-cdc`) | Source TP/AT capture | Off | Off | Off |
| Prepare Index Trace | On | On | Off | Off |
| Pixels application only (default) | On | Off | Off | Off |
| Static query experiment | Off | Off | On | Off |
| Update, then query | On | Off | On; `timing: after` | Optional |
| Query during application | On | Off | On; `timing: during` | Optional |
| Sink freshness experiment | On | Off | Independently optional | On |

Trace preparation rejects business queries and freshness. Static experiments reject an update prefix or freshness. `max_transactions: 0` consumes the complete input once; a positive value selects that many complete source transactions, including any marker transactions within the prefix. It must fit the published input count. File replay does not loop and does not use the benchmark profile's Index Trace request budget.

The baseline manifest and local schema/DDL are checked before target startup. Update experiments also check the CDC manifest, common CSV/schema bindings, complete CDC transactions and the hashes of every CDC file. They do not read the CDC input's retained CSV copies, so those copies are checked by descriptor and size only; `check-cdc-input`, `fetch-input` and preparation still hash them in full. Pixels creates new physical table/index IDs; the sink converter resolves source database/table names against that metadata and never routes by capture-local frame IDs. Imported footers and the primary-key scan independently check row counts. Trace-enabled runs capture index reconstruction as their baseline, then the actual CDC index calls; they never splice a baseline from another physical run.

With Trace disabled, Pixels uses its RocksDB index provider and creates no tracer channel, projector or Trace directory. The file sink still reports every native transaction begin and successful commit, propagates failures, verifies applied transaction/change totals, drains and closes normally. With Trace enabled, the existing tracer barriers bind index calls to the actual Pixels versions, RowIDs and physical locations. Two executions need not allocate identical physical IDs.

`timeout_seconds` bounds each experiment's target operations and the file application/drain task; it does not bound input preparation. Trino has a separate per-query timeout; managed node readiness is bounded to one minute. Sink close and process cleanup have their own bounds. These are phase limits, not a total CPU/memory or whole-suite time budget. Query rounds and selected input determine the experiment's cost.

## Query workload and results

Queries default off. `queries.directory` names ready-to-run UTF-8 `.sql` files, sorted by filename; `rounds` is a positive finite count. Each file contains one statement with parameters already resolved. SIBench issues `EXPLAIN`, then executes the statement, consumes every result page and rejects SQL/HTTP errors and timeouts. It does not retry business statements automatically. Outstanding query continuations are cancelled on client failure where possible.

Ordinary SQL directories require no reference answers and do not export or compare result values; their result records contain completion and performance metrics. Reference-answer comparison is limited to the explicitly prepared query sets below. For separate measurements, leave `queries.enabled: false` in application throughput/freshness experiments and `freshness.enabled: false` in SQL performance experiments. Both workflows can use the same dedicated Trino cluster in successive runs.

Each result records the exact SQL, its input digest, source filename/round, Query ID, fully consumed output-row count, client elapsed time, StatementStats and the server's full QueryStats. Plans have their own Query IDs. Elapsed query time ends after the last result page and excludes the subsequent metrics request. `processedBytes` must not be reported as physical Scan I/O; use the appropriate server/connector counters and retain their names and scope. A query during updates may observe a different read snapshot from its separate EXPLAIN request.

### Rendered HyBench queries with recorded answers

`render-queries CONFIG CSV_INPUT_DIRECTORY FRESH_OUTPUT_DIRECTORY SEED INSTANCES` renders HyBench IQ-1–IQ-5, AP-1–AP-7 and AP-13 from the verified client build's `conf/stmt_postgres.toml`. Each template gets `INSTANCES` parameter draws from HyBench's own domains (customers 1…`customer_number`, companies after them, loan applicants present in the CSV), seeded by `SEED`. HyBench's blocked-account sampling and random-row helper queries are not reproduced. Each statement is written with literal parameters, so PostgreSQL and Trino execute identical text.

The command verifies the sealed CSV and loads it into a fresh, uninstrumented PostgreSQL from the source-only Pixels build. It records each statement's answer there, publishes `manifest.json` last, deletes the database and only then removes its `INCOMPLETE` marker; a directory that still has the marker is rejected rather than run as plain SQL. The manifest binds the CSV identity, template digest, seed, client and PostgreSQL build, and every SQL file by size and digest. Set `queries.directory` to the output. A rendered set requires `updates: false` and a baseline from the same CSV; unlisted, missing or changed SQL files are rejected before any service starts.

After each Trino execution the experiment compares the consumed rows with the recorded answer and fails on the first difference; the result file records `oracle: matched`. Rows are compared as a multiset. For `ORDER BY … LIMIT` statements only the ordered sort-key values are compared, because tied rows may legitimately differ. Integer-typed values compare exactly; other numbers compare to five significant digits (the engines sum and average `real` columns in different orders and precisions). Timestamps compare without trailing fractional zeros and `CHAR` without padding.

Excluded templates: IQ-6, AP-8 and AP-10 (a `LIMIT` inside a subquery chooses among ties), AP-9 and AP-10 (PostgreSQL casts and date arithmetic would need translation), AP-11 (its answer can list every investing customer and company) and AP-12 (`100.0` makes the rate a PostgreSQL `numeric` but a one-decimal Trino `DECIMAL`).

Recorded answers describe the sealed baseline only; queries during or after updates have no recorded answer. Retain plans and matching results when comparing access paths. Query execution alone does not establish primary-index use, index speedup or complete physical I/O accounting.

Successful runs retain effective configuration, input/build identities, actual host and Trino settings, baseline counts, application counts, timings, query plans/results and optional sink metrics. They stop the owned services before publishing success or Trace, then delete their working Pixels state. Failed runs retain diagnostics and do not publish a successful experiment result. Remote working directories are unique per session. A published Trace remains available if later working-state cleanup fails.

## Sink freshness

Business HTTP requests and the file sink's freshness JDBC client use UTC. The rebased timestamp column and sampling literals therefore share the same interpretation across host time zones.

Freshness defaults off and is independent of business queries. It requires updates and a dedicated `fresh_ts` or `freshness_ts` column of type `timestamp(6)`, selected explicitly in the configuration. SIBench checks the bound source schema and the sink checks target metadata. Only that column's after-image is rebased to the sink's current input time; business timestamps and other columns are preserved. Ordinary file replay preserves all row bytes.

The sink performs JDBC sampling, with an explicit table/column, a query timeout, error propagation and a required non-null sample. Its CSV records sample time, age of the maximum visible measurement timestamp, and query duration. `sample_seconds` retains the sink for a bounded sampling window after committed drain. This window is included in application/drain timing; it is measurement overhead, not steady-state throughput.

This is sampled visible-data age for this run. A visible maximum does not prove visibility of every earlier update or measure each update's first-visible latency. File storage time and the historical PG/Debezium capture are excluded. Account for sampler interval, query duration and clock differences. The current binding uses HTTP Trino JDBC without password authentication; HTTPS/authenticated freshness requires a separate implementation. Business query HTTP(S) support does not imply that capability in the sink sampler.

## Trino deployment

SIBench uses JDK 17. The pinned connector POM selects the Trino release and JDK 23 builds/runs that release. Build into a fresh directory:

```bash
TRINO_JAVA_HOME=/path/to/jdk23 bash scripts/build-trino.sh build/services/pixels-trino
```

The build uses pinned sources and project patches without changing upstream checkouts. Pixels stores `CHAR` values with their padding, matching PostgreSQL and the index keys; the connector patch trims that padding from loaded values and from row-group statistics, as Trino's `CHAR` type requires. Without it a projected `CHAR` fails, and padded statistics could prune row groups that match an unpadded literal. Its `java-build.json` receipt binds the source and artifact identities. Only the connector is built and installed; experiments obtain metrics from Trino's query API.

Obtain the matching `trino-server-RELEASE.tar.gz` and its `.sha1` checksum from the official Maven Central coordinate `io.trino:trino-server:RELEASE:tar.gz`. If its redirect is unavailable, a mirror payload must still match the checksum obtained from Central. On the execution host install the source-built connector, checked server archive and an isolated JDK:

```bash
python3 scripts/install_trino.py CONNECTOR_BUILD SERVER_TAR_GZ CENTRAL_SHA1_FILE JDK23 FRESH_DESTINATION
```

The installer verifies inputs, installs only the required connector, copies the JDK and publishes file hashes in `deployment.json`. Keep the archive, checksum and matching build receipt for regeneration. The Java runtime requires a valid trust store.

For a managed node, set `trino.deployment_directory` to that absolute destination **on the Pixels host** and leave `trino.server` empty. SIBench starts one coordinator that also acts as a worker for each experiment, using that run's Pixels properties, then stops its Java process with the session.

For an existing dedicated cluster, set `trino.server` to its fixed HTTP(S) coordinator origin and leave `trino.deployment_directory` empty. SIBench sends SQL and freshness requests to that cluster without starting, restarting or stopping its processes. Queries or freshness require exactly one of these two settings. Freshness currently requires HTTP.

An external cluster requires fixed Pixels service ports in the engine's existing `properties` file, for example:

```properties
etcd.port=18801
metadata.server.port=18802
trans.server.port=18803
retina.server.port=18804
node.server.port=18805
query.schedule.server.port=18806
index.server.port=18807
scaling.metrics.server.port=18808
worker.coordinate.server.port=18809
```

Ports must be distinct, within 1–65535, and available on the Pixels host; occupied fixed ports fail before services start. Unspecified service ports remain automatically allocated for managed or query-disabled runs; the etcd peer port is always automatically allocated. Fixed ports change listening addresses only: each run still creates fresh metadata, index and working data. Run experiments using the same ports serially. Configure every Trino coordinator/worker's Pixels connector with the same service hosts/ports and COS settings/credentials, using the matching source-built connector. Workers on other hosts require a reachable `pixels_host: HOST:PORT` agent; `pixels_host: local` binds Pixels to loopback. Each run records its effective `pixels.properties`; external cluster build identities and resource settings must be recorded by the experiment operator. External configuration is not automatically rewritten or certified by SIBench.

Restart all relevant Trino nodes between runs when the experiment requires clearing process caches or reloading connector configuration. Restarting Trino does not clear operating-system or remote-storage caches; record cache conditions separately when comparing performance.

Managed-node resource settings are explicit: JVM heap, per-query user memory, total query memory, heap headroom and CPU/worker-thread count. Query total memory plus heap headroom must fit the heap. Set these values for the execution host; leave enough memory for Pixels/native allocations, other processes and the OS. External clusters use their own resource settings. Cloud-function execution is disabled in managed setup, which verifies an active node and a successful Pixels marker-table scan before business queries.
