# Pixels experiments over CDC files

`pixels-experiment CONFIG CDC_INPUT_DIRECTORY PIXELS_BASELINE_DIRECTORY FRESH_OUTPUT_DIRECTORY` joins a [published CDC input](cdc-input.md) with a sealed COS columnar baseline from the same CSV identity. Each run copies and imports existing Pixels files into fresh service state. Build both Pixels phases before running an experiment. PostgreSQL, Debezium and HyBench are not started during file replay. Index Service replay remains independent: export an Index Trace once, then use `replay-service` without Pixels data.

## Prepare once, restore for each run

Configure `data_storage` in the Pixels engine YAML:

```yaml
data_storage:
  config: ../../index/storage.yaml
  profile: cos
  prefix: sibench/pixels
```

The config path is relative to the engine YAML. The referenced profile owns the COS endpoint, region, bucket and credential environment variable names. Use virtual-hosted addressing. Supply those variables on the Pixels host and, for an external query cluster, its workers. Secrets are resolved in the host processes and are not returned in the effective Pixels properties. Without `data_storage`, the live `generate` workflow can still use local files; reusable columnar preparation/import requires COS.

```bash
java -jar build/sibench/sibench.jar prepare-csv config/sibench.yaml results/capture/hybench-csv
# Run the independent PG and Pixels branches concurrently:
java -jar build/sibench/sibench.jar prepare-pixels-inputs config/sibench.yaml \
  results/capture/hybench-csv results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar check-pixels-baseline results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/capture/hybench results/inputs/hybench-pixels results/pixels/run
```

CDC output uses the benchmark's `artifacts.directory`. The branches can also be invoked separately with `prepare-cdc CONFIG CSV_INPUT_DIRECTORY` and `prepare-pixels-baseline CONFIG CSV_INPUT_DIRECTORY FRESH_OUTPUT_DIRECTORY`. A failed branch leaves the other completed artifact usable; neither branch waits for the other's baseline. Preparation disables Trace, queries and freshness.

Columnar preparation converts CSV only once, runs normal Pixels LOAD and inspects the published files. Its manifest binds the CSV identity, schema/DDL, expected rows, COS file paths, sizes, SHA-256 digests, row-group counts, hidden timestamp upper bound, index bucket count and source/build identities. It is published after normal service shutdown. Its COS session prefix is retained as the canonical baseline. `check-pixels-baseline` verifies the local manifest/descriptors; remote file contents are verified during import.

Use the [input publication commands](cdc-input.md#cos-publication-and-local-materialization) to persist all three completed artifacts under one content-store prefix:

```bash
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/capture/hybench-csv
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/capture/hybench
java -jar build/sibench/sibench.jar publish-input config/index/storage.yaml cos sibench/inputs/hybench results/inputs/hybench-pixels
```

Retain each returned publication digest. Fetch missing local inputs by that digest; reuse existing verified local directories. `publish-input` / `fetch-input` transfer the baseline manifest and its local schema/DDL. The columnar objects stay at the COS locations bound by that manifest; these commands do not duplicate or relocate them. Preserve that canonical COS prefix alongside the published manifest.

Each experiment starts empty metadata, etcd, indexes and Retina. Transaction allocation and visibility watermarks start beyond the imported files' hidden LOAD timestamps. Tables/layouts are recreated, files are copied server-side into a unique run prefix and checked against their bound digests/footers. Import assigns fresh physical identities and rebuilds primary/main indexes and Retina visibility from the bound files. Its copy, verification and reconstruction costs are recorded separately from application and query measurement.

The canonical baseline is immutable. Successful experiments stop services and delete only their own local state and COS prefix. Failed runs retain their owned objects and logs for diagnosis. Restore by starting another fresh experiment; do not loop CDC over an updated instance or edit the canonical objects. No CSV parsing/encoding is performed by the restore path. The CDC input's retained CSV identity checks remain part of input verification.

Current integration uses one Retina/index server and ordered baseline files. The target COS endpoint/region/bucket and Pixels source/patch identity must match the baseline; bucket settings and file digests must also match. Different physical table/index/file/RowIDs across runs are allowed because all mappings are reconstructed together. This is fresh baseline reconstruction, not crash recovery of an updated service or an index checkpoint cache.

## Modes and configuration

The `pixels_experiment` block in [the runtime configuration](../config/sibench.yaml) owns experiment switches. Engine resources and the Pixels host remain in [the Pixels engine configuration](../config/engine/pixels/pixels.yaml).

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

The input manifests, common CSV/schema bindings, complete CDC transactions and hashes are checked before target startup. Pixels creates new physical table/index IDs; the sink converter resolves source database/table names against that metadata and never routes by capture-local frame IDs. Imported footers and the primary-key scan independently check row counts. Trace-enabled runs capture index reconstruction as their baseline, then the actual CDC index calls; they never splice a baseline from another physical run.

With Trace disabled, Pixels uses its RocksDB index provider and creates no tracer channel, projector or Trace directory. The file sink still reports every native transaction begin and successful commit, propagates failures, verifies applied transaction/change totals, drains and closes normally. With Trace enabled, the existing tracer barriers bind index calls to the actual Pixels versions, RowIDs and physical locations. Two executions need not allocate identical physical IDs.

`timeout_seconds` bounds target operations and the file application/drain task. Trino has a separate per-query timeout; managed node readiness is bounded to one minute. Sink close and process cleanup have their own bounds. These are phase limits, not a total CPU/memory or whole-suite time budget. Query rounds and selected input determine the experiment's cost.

## Query workload and results

Queries default off. `queries.directory` names ready-to-run UTF-8 `.sql` files, sorted by filename; `rounds` is a positive finite count. Each file contains one statement with parameters already resolved. SIBench issues `EXPLAIN`, then executes the statement, consumes every result page and rejects SQL/HTTP errors and timeouts. It does not retry business statements automatically. Outstanding query continuations are cancelled on client failure where possible.

Each result records the exact SQL, its input digest, source filename/round, Query ID, fully consumed output-row count, client elapsed time, StatementStats and the server's full QueryStats. Plans have their own Query IDs. Elapsed query time ends after the last result page and excludes the subsequent metrics request. `processedBytes` must not be reported as physical Scan I/O; use the appropriate server/connector counters and retain their names and scope. A query during updates may observe a different read snapshot from its separate EXPLAIN request.

Supply fully rendered SQL; SIBench does not provide benchmark parameter rendering. Retain plans and matching results when comparing access paths. Query execution alone does not establish primary-index use, index speedup or complete physical I/O accounting.

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

The build uses pinned sources and project patches without changing upstream checkouts. Its `java-build.json` receipt binds the source and artifact identities. Only the connector is built and installed; experiments obtain metrics from Trino's query API.

Obtain the matching `trino-server-RELEASE.tar.gz` and its `.sha1` checksum from the official Maven Central coordinate `io.trino:trino-server:RELEASE:tar.gz`. If its redirect is unavailable, a mirror payload must still match the checksum obtained from Central. On the execution host install the source-built connector, checked server archive and an isolated JDK:

```bash
python3 scripts/install_trino.py CONNECTOR_BUILD SERVER_TAR_GZ CENTRAL_SHA1_FILE JDK23 FRESH_DESTINATION
```

The installer verifies inputs, installs only the required connector, copies the JDK and publishes file hashes in `deployment.json`. Keep the archive, checksum and matching build receipt for regeneration. The Java runtime requires a valid trust store.

Set `trino.deployment_directory` to that absolute destination **on the Pixels host**. SIBench starts one coordinator that also acts as a worker for each experiment, using that run's Pixels properties, then stops its Java process with the session. COS files can be shared with a separate worker, which still needs the exact metadata/Retina endpoints, credentials and a separately validated deployment. Empty `deployment_directory` uses `trino.server`; that external cluster must already be configured for the exact target metadata and storage of the run.

Resource settings are explicit: JVM heap, per-query user memory, total query memory, heap headroom and CPU/worker-thread count. Query total memory plus heap headroom must fit the heap. Set these values for the execution host; leave enough memory for Pixels/native allocations, other processes and the OS. Cloud-function execution is disabled. Managed setup verifies an active node and a successful Pixels marker-table scan before business queries.
