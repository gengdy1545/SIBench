# Pixels experiments

`pixels-experiment CONFIG [CDC_INPUT_DIRECTORY] PIXELS_BASELINE_DIRECTORY FRESH_OUTPUT_DIRECTORY` restores a sealed COS baseline into fresh Pixels state. Update runs need CDC files bound to the same CSV identity; static runs omit CDC. File replay starts no source PostgreSQL, Debezium or HyBench process. Build both Pixels phases first.

## Prepare once, restore for each run

Set `data_storage` in [the Pixels engine YAML](../config/engine/pixels/pixels.yaml):

```yaml
data_storage: cos://YOUR-BUCKET/sibench/baselines/pixels
```

The connection comes from `object_storage.cos` in the runtime CONFIG. Use virtual-hosted addressing and provide its credential variables on the Pixels host and external Trino workers. Pixels data paths require `cos://` (`s3://` is rejected); reusable baselines need COS.

```bash
java -jar build/sibench/sibench.jar prepare-csv config/sibench.yaml results/capture/hybench-csv
java -jar build/sibench/sibench.jar check-csv-input results/capture/hybench-csv
java -jar build/sibench/sibench.jar prepare-pixels-inputs config/sibench.yaml \
  results/capture/hybench-csv results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar check-cdc-input results/capture/hybench
java -jar build/sibench/sibench.jar check-pixels-baseline results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/capture/hybench results/inputs/hybench-pixels results/pixels/run
```

`prepare-pixels-inputs` runs the CDC and baseline branches concurrently: CDC goes to the HyBench profile's `artifacts.directory`, the baseline to the explicit output. Each also has its own entry point, `prepare-cdc CONFIG CSV_INPUT_DIRECTORY` and `prepare-pixels-baseline CONFIG CSV_INPUT_DIRECTORY FRESH_OUTPUT_DIRECTORY`. Preparation disables Trace, queries and freshness, uses `native.timeout_minutes`, and a failed branch leaves the other's finished artifact usable.

CSV preparation needs the built HyBench client; CDC also needs the Pixels build's source PostgreSQL and sink binding on the generator, and never contacts `pixels_host`. Select HyBench in CONFIG independently of its native capture engine. CSV output must be fresh and below `results/capture`; every other output must be fresh too. Source, schema, scale, data parameters and build identities must match, while client counts and phase durations may differ.

CDC captures committed row changes from TP and XP once, with AP/IQ disabled, using the configured durations and transaction-client counts; `trace.workload_requests` does not bound it. Primary-key changes are rejected and FULL replica identity is required. Each phase waits up to two minutes for a committed marker at the file writer, then 60 seconds for normal shutdown. Missing or incomplete source data, or a failed shutdown, prevents publication and keeps the diagnostics and database; success deletes the working database. A marker proves capture progress, not target visibility or durability.

Inputs are immutable, including CDC's hard-linked baseline copies. Preparation binds the sealed CSV by descriptors and sizes; CDC hashes its copies once at publication, after the PostgreSQL load, and the Pixels branch hashes them during conversion. Manifests bind relative paths, sizes, SHA-256, schemas, counts and source/build identities. Validation rejects symbolic links, traversal and aliases, missing, unlisted or reordered files, malformed counts, hash or schema mismatches, and incomplete or unsupported transactions. Manifests and single CDC frames are limited to 16 MiB; files hold whole transactions, with the positive int32 `sink.proto.maxRecords` limiting transactions per file. CDC totals include markers; `businessChanges` excludes marker rows and `markers` counts marker transactions. None of these are distinct-row or index-operation counts.

The baseline manifest also binds COS paths, row groups, hidden timestamps and the index bucket count. Empty tables keep their schema and zero counts without files. `check-pixels-baseline` checks the local descriptors and import verifies remote content. Finished CSV/CDC inputs can move without the preparation database; keep local capture diagnostics separately if source reproduction matters.

Native LOAD renews its write-transaction lease while encoding files and publishing visibility, and waits for any in-flight renewal before commit. A renewal or visibility failure fails the LOAD and prevents baseline publication; a longer experiment timeout does not replace renewal.

Each run copies the bound files into its own COS prefix, validates digests, footers and row counts, and rebuilds indexes and visibility with fresh physical identities. The target COS endpoint, region, bucket and settings and the Pixels source/patch identity must match the baseline. Restore cost is reported apart from application and query cost. The canonical baseline is immutable: repeat an experiment by restoring fresh state, not by replaying CDC over an updated instance.

Success needs a normal service shutdown before the result or Trace is published, then removes the owned working state and the run's COS prefix. A failure keeps diagnostics and owned objects, and a cleanup failure leaves a published Trace valid. This rebuilds a baseline; it is not crash recovery.

## Publish and fetch inputs

Configure connections in [config/sibench.yaml](../config/sibench.yaml); the URI supplies bucket and prefix. CSV, CDC and Pixels baseline inputs share these commands:

```bash
java -jar build/sibench/sibench.jar publish-input config/sibench.yaml \
  cos://YOUR-BUCKET/sibench/inputs/hybench results/capture/hybench-csv
java -jar build/sibench/sibench.jar fetch-input config/sibench.yaml \
  cos://YOUR-BUCKET/sibench/inputs/hybench MANIFEST_SHA256 results/inputs/hybench-csv
```

Use the returned `manifest_sha256`, not the inner input-manifest digest. Objects live under `PREFIX/objects/` and inventories at `PREFIX/manifests/<sha256>.json`. Only manifest-bound local files transfer; columnar objects stay at their bound COS paths, so keep that canonical prefix with the baseline manifest.

Publication reuses content objects and needs neither a conditional PUT nor an exclusive writer, but keys must stay immutable under trusted producers. A PUT acknowledgment does not certify remote durability or visibility. Fetch verifies exact inventories, hashes and semantic checks, needs a fresh destination and writes the canonical manifest last; a failure keeps partial files without it. Check an existing cache with its `check-*` command rather than fetching over it.

Files over 64 MiB use multipart upload, aborted on failure. API calls are bounded to ten minutes in total and three minutes per attempt; whole-input time depends on size and count. Failed uploads may leave unreferenced objects, and transfer never deletes or garbage-collects remote content.

## Modes and configuration

[config/sibench.yaml](../config/sibench.yaml) owns `pixels_experiment`; engine resources and host settings remain in the engine YAML.

| Setting | Behavior |
| --- | --- |
| `updates` | Defaults true; requires CDC. False rejects a CDC argument, update prefix and freshness. |
| `trace` | Defaults false. True exports index reconstruction and actual calls from this run; rejects queries/freshness and a HyBench profile with `trace.secondary_indexes: true` (Pixels exports primary indexes only). |
| `max_transactions` | 0 applies all input once; positive values select a complete transaction prefix, including markers, within the input count. |
| `queries.enabled` | Defaults false; `timing` is `after` updates or `during` application. |
| `freshness.enabled` | Defaults false; independent of business queries, requires updates. |
| `timeout_seconds` | Bounds target operations and application/drain; preparation, per-query timeout and cleanup have separate limits. |

Static queries use `updates: false`, `queries.enabled: true`, a SQL directory and [Trino](#trino-deployment):

```bash
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/inputs/hybench-pixels results/pixels/static-run
```

Static runs read no original CSV or CDC. Update runs verify CDC hashes, complete transactions and the common CSV/schema bindings; retained CSV copies are checked by descriptor and size, and only preparation, fetch and `check-cdc-input` hash them fully. Even without Trace, application verifies transaction and change totals and a normal drain. File replay never loops and ignores the Index Trace request budget. Phase timeouts are not whole-run resource quotas.

## Query workload and results

`queries.directory` contains ready-to-run UTF-8 `.sql` files, one resolved statement each, sorted by filename. `rounds` is positive and finite. Each statement gets EXPLAIN, execution and full page consumption. SQL/HTTP errors and timeouts fail; business statements are not automatically retried.

Ordinary SQL needs no reference answer and records performance/completion only. Results retain exact SQL/digest, file/round, Query ID, row count, client elapsed time, StatementStats and full QueryStats; plans have separate IDs. Elapsed time excludes the later metrics request. `processedBytes` is not physical scan I/O. During updates, EXPLAIN and execution may see different snapshots. Query completion alone does not establish index use or speedup.

For an independent static correctness check:

```text
render-queries CONFIG CSV_INPUT_DIRECTORY FRESH_OUTPUT_DIRECTORY SEED INSTANCES
```

This renders IQ-1–IQ-5, AP-1–AP-7 and AP-13 from the verified HyBench templates, with seeded parameter draws from its data domains, and records answers in fresh source PostgreSQL. It does not reproduce blocked-account/random-row helper sampling. SQL, templates, inputs and builds are identity-bound. Set `queries.directory` to the output and use `updates: false` with the same CSV-derived baseline. Changed/unlisted SQL or `INCOMPLETE` outputs fail before startup.

Results compare as multisets; ORDER BY/LIMIT compares ordered sort keys because tied rows may differ. Integers compare exactly, other numbers to five significant digits; timestamps ignore trailing fractional zeros and CHAR ignores padding. A mismatch fails; success records `oracle: matched`. Other templates are unsupported, and recorded answers do not cover updated data.

## Sink freshness

Freshness requires an explicit dedicated `fresh_ts` or `freshness_ts` column of type `timestamp(6)`. Only that column's after-image is rebased to current sink input time; ordinary replay preserves all row bytes. Queries/sampling use UTC.

The sampler requires non-null results and propagates query errors. Its CSV records sample time, age of the maximum visible measurement timestamp and query duration. `sample_seconds` adds a bounded post-drain window included in application timing. This is sampled visible-data age, not each update's first-visible latency or proof that every earlier update is visible. Historical capture/file-storage time is excluded; account for sampling/query/clock effects.

Freshness currently uses HTTP Trino JDBC without password authentication. Business query HTTP(S) support does not extend that capability to the sampler. Separate SQL-performance and freshness runs when their overheads must be measured independently.

## Trino deployment

SIBench/Pixels use JDK 17; the connector's pinned POM selects the Trino release, built/run with JDK 23:

```bash
TRINO_JAVA_HOME=/path/to/jdk23 bash scripts/build-trino.sh build/services/pixels-trino
python3 scripts/install_trino.py CONNECTOR_BUILD SERVER_TAR_GZ CENTRAL_SHA1_FILE JDK23 FRESH_DESTINATION
```

Obtain the matching `trino-server-RELEASE.tar.gz` and `.sha1` from Maven Central (`io.trino:trino-server:RELEASE:tar.gz`); mirrored archives must match Central's checksum. Install on the execution host into a fresh directory. The installer verifies inputs, copies the JDK/connector and publishes `deployment.json`. All deployed files must resolve inside that directory; the runtime needs a valid trust store.

Queries or freshness require exactly one setting:

| Setting | Lifecycle |
| --- | --- |
| `trino.deployment_directory` | Absolute path on the Pixels host. SIBench starts/stops a coordinator-worker per run. |
| `trino.server` | Fixed HTTP(S) origin of an existing dedicated cluster. The operator manages its processes/configuration. Freshness requires HTTP. |

External clusters need fixed, distinct, available Pixels ports (1–65535) in the engine `properties` file: `etcd.port`, `metadata.server.port`, `trans.server.port`, `retina.server.port`, `node.server.port`, `query.schedule.server.port`, `index.server.port`, `scaling.metrics.server.port` and `worker.coordinate.server.port`. Managed/query-disabled runs can allocate unspecified ports automatically; the etcd peer port is always automatic. Serialize runs sharing fixed ports.

Configure every Trino node with the matching source-built connector, Pixels hosts/ports and COS settings/credentials. Remote workers need a reachable `pixels_host: HOST:PORT`; `local` binds Pixels to loopback. SIBench records effective Pixels settings; the operator records external build/resource settings and handles restarts. Restarting Trino clears process caches, not OS/storage caches.

Managed heap, query memory, total query memory, heap headroom and thread count are explicit. Total query memory plus headroom must fit the heap, with room left for Pixels/native/OS memory. Managed startup checks readiness within one minute and a successful Pixels marker scan; cloud-function execution is disabled.

## Bounded integration checks

After both Pixels build phases and the root Maven prerequisites, run the local LOAD checks explicitly:

```bash
mkdir -p results/capture
mvn -Dsibench.pixels.build=build/services/pixels -Dtest=PixelsLoadLeaseCheck test
```

The lease check holds a four-row FIFO input open for 340 seconds, beyond the coordinator's 300-second lease, and requires a successful commit; a second case rejects failed visibility publication. Neither requires COS or Trino. Delete/reinsert RowID and Locator results of applied CDC are checked by `PixelsExperimentCheck`, which needs COS.

For a service-free check of renewal completion and rejection, compile and run the test probe against the built Pixels CLI:

```bash
mkdir -p build/sibench/pixels-probes
javac --release 17 -cp build/services/pixels/jars/pixels-cli.jar \
  -d build/sibench/pixels-probes src/test/native/pixels/LoadLeaseProbe.java
PIXELS_HOME="$PWD/build/services/pixels/home" \
  java -cp build/sibench/pixels-probes:build/services/pixels/jars/pixels-cli.jar \
  org.sibench.pixels.LoadLeaseProbe
```

File replay checks additionally require COS credentials and a unique `sibench.storage.uri`, with connections selected by `sibench.storage.config`. Run `PixelsExperimentCheck#finiteFilesApplyWithoutTraceAndExportOptionalTrace+completePrefixAndStaticModesDoNotConsumeExtraTransactions` with the same Pixels build. They prepare eight-row CSV/CDC and columnar inputs, then check full, prefix, static and Trace-export modes. They do not establish full-scale performance or SQL correctness after updates.
