# SIBench

SIBench captures index accesses from native HyBench, YCSB JDBC, and sysbench PostgreSQL workloads. It produces real Parquet row data and a reusable protobuf Trace for local RocksDB or rocksdb-cloud replay.

The pipeline is native load → baseline export → native workload and Trace generation → service replay. Trace preserves index-access and transaction semantics under the source execution plans; it does not measure end-to-end SQL performance or reproduce session arrival timing.

See the [Trace contract](docs/trace.md) for data and capture semantics, and the [Index Service contract](docs/index-service.md) for execution, response checks and durability.

## Build

JDK 17, Maven, and Git are required. Set `JAVA_HOME` to the selected JDK.

```bash
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar --help
```

The build produces `build/sibench/sibench.jar` with its dependencies. Commands use the current repository by default; use `--root PATH` to select another root. Failures return a nonzero status. Configured output directories, report files, and new index directories must not already exist, preserving failed runs for diagnosis.

### Native capture components

C/C++, make, and zstd development libraries are required. Extract clean PostgreSQL 14.24 sources into `build/benchmarks/postgresql-14.24`, then build the instrumented server:

```bash
make -C src/main/native/postgres install extension
```

The patch changes PG internal structures; extract a fresh source tree before rebuilding an older tree or reusing object files. Artifacts are written to `build/services/postgresql-capture/`. Source clients also require patch, tar, and either Maven (HyBench / YCSB) or autotools, pkg-config, libtool, and libpq development libraries (sysbench). Build the required clients from clean submodules:

```bash
scripts/build-benchmarks.sh config/benchmark/hybench.yaml
scripts/build-benchmarks.sh config/benchmark/ycsb.yaml
scripts/build-benchmarks.sh config/benchmark/sysbench.yaml
```

Builds use fresh copies under `build/benchmarks/` and reject existing destinations. Capture verifies `native-build.json`, which binds the pinned source, applied patches and executable/dependency digests. Rebuild clients when that inventory is missing. sysbench's `native.binary` must name an inventoried executable, normally `src/sysbench`; YCSB's distribution path comes from its build output.

The build applies versioned patches from `scripts/patches/` to the copies, leaving submodules unchanged. They provide cooperative stopping between native business operations; the HyBench dependency patch excludes the unused dsol XML simulator.

### rocksdb-cloud JNI

This component is required only for the cloud backend. Source revisions are pinned by the `index/rocksdb-cloud` and `third_party/aws-sdk-cpp` submodule gitlinks; Java dependencies are pinned in `pom.xml`. The build script requires both submodules to be clean and at their pinned commits.

The script targets yum-based systems and requires patch, C/C++, CMake, make and JDK 17. It installs development dependencies as root and builds the SDK and JNI under `build/index/`. Use a build host ABI-compatible with the runtime host.

```bash
scripts/build-rocksdb-cloud.sh
```

The output is `build/index/librocksdb_cloud_jni.so`; select it at runtime with `-Dsibench.rocksdb_cloud.library=PATH`. Upstream licenses remain in their submodules: [rocksdb-cloud](index/rocksdb-cloud/COPYING), [AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt).

## Native capture

```bash
scripts/native-capture.sh config/benchmark/ycsb.yaml
```

Run as a non-root user able to manage its own PG cluster. Java selection prefers `SIBENCH_JAVA_HOME`, then `JAVA_HOME`, then PATH; `SIBENCH_HEAP` defaults to `-Xmx12g`.

All three public profiles use **87,060,000 baseline rows** (HyBench SF10):

| Configuration | Responsibility |
| --- | --- |
| [hybench.yaml](config/benchmark/hybench.yaml), [ycsb.yaml](config/benchmark/ycsb.yaml), [sysbench.yaml](config/benchmark/sysbench.yaml) | Native workload, scale, concurrency, database identity and data layout |
| [sibench.yaml](config/sibench.yaml) | PG settings, generation limits, replay and locality analysis |
| [rocksdb.yaml](config/index/rocksdb.yaml), [storage.yaml](config/index/storage.yaml) | Index service and object storage |

Each run uses one fresh PG database for the following sequence:

1. Load data and create indexes with the native benchmark loader.
2. Install capture triggers and run one ordinary `VACUUM ANALYZE` before taking the initial snapshot. This prepares visibility maps and planner statistics; it is not `VACUUM FULL`.
3. Export the initial snapshot, then execute native workloads in the same database. HyBench keeps changes between phases. SIBench does not issue `VACUUM` during workloads; the default PG configuration also disables autovacuum.

Preparation is included in load time and total generation time, with its duration recorded separately as `load_vacuum_analyze_millis` in the timing report. This initial maintenance can enable index-only scans to avoid heap visibility checks, changing physical access paths and the captured index workload. Later writes naturally clear affected all-visible bits; PG falls back to checking tuple visibility under the transaction snapshot, so missing bits do not cause missing rows. Whether preparation reduces total generation time depends on the workload and must be measured including its cost.

Source tables are unlogged; the default PG settings disable `fsync` and `full_page_writes`. This disposable working database uses local trust authentication and is not a reusable initial-state cache.

PG/JDBC facts stream into the Java generator, which projects transactions directly into Trace. `capture.output` selects artifacts:

| Output | Retained artifacts |
| --- | --- |
| `proto` (default) | Baseline and incremental Parquet, protobuf requests, manifests, source identities, access ledger, completeness and timing reports |
| `full` | The same artifacts, plus compressed raw facts and detailed index operations |

Both modes retain real row data and Locators. Baseline and incremental Parquet target **512 MiB**, with the configured overshoot tolerance and one smaller EOF tail per write stream. This size policy does not apply to Trace or backend SST files; see [Data files](docs/trace.md#data-files).

Generation validates source/build/schema identity, baseline row counts, source completion, transactions and PG access counters before publication. Successful runs stop PG and remove its database unless `SIBENCH_KEEP_DATABASE=1`; proto output also removes source-control scratch. Failed runs retain diagnostics and `INCOMPLETE` and cannot be resumed. Always use a fresh output path. `native-run.log` records native stdout; `trace/source-capture-run.json` retains capture status and build identities.

Provision space for PG, Parquet, Trace and compiler scratch. Pending-fact limits and optional cooperative operation/byte targets are configured under `generation`; they are not whole-process memory or disk caps. Target stops validate only the executed prefix. See [Output strategies](docs/trace.md#output-strategies) for counting, drain and failure semantics. Non-C/POSIX YCSB export additionally needs temporary sorting space proportional to the input.

Spilled source facts use bounded parallel JSON decoding; transaction projection and event delivery remain in source order. Decoder buffers are limited by text size and record count, with oversized individual events handled alone; these limits are not a whole-process heap cap. Baseline counts use normal planner settings, independently of YCSB's ordered export-read constraints.

### Independent source acceptance

Normal generation checks each Baseline table against its configured native row count in the exported snapshot, along with file boundaries and file SHA-256 identities. Independent value and row-state audits are separate workflows:

| Check | Entry point | Required source state |
| --- | --- | --- |
| Initial Parquet values | `java -jar build/sibench/sibench.jar audit-baseline PROFILE BASELINE --jdbc URL --user ROLE` | The unchanged initial database, before workload execution. |
| Captured row changes and final values | `java -jar build/sibench/sibench.jar audit-rows PROFILE BASELINE RAW --jdbc URL --user ROLE` | The same run's final database, with business clients stopped. |

Use `scripts/native-capture.sh --audit-baseline PROFILE` to run the initial comparison immediately after export. It records the audit command, timing and result separately and stops on failure. To retain the final database for `audit-rows`, set `SIBENCH_KEEP_DATABASE=1`; restart the cleanly stopped database on the same host boot before auditing. A final-state audit cannot replace the initial comparison.

These audits read full inputs and require additional workspace. Measure normal generation in a separate run: subtracting audit time does not remove its cache effects. Trace publication and service acceptance are described below and in the [Trace](docs/trace.md#validation-responsibilities) and [Index Service](docs/index-service.md#validation-boundaries) contracts.

## Replay and service

Replay checks input integrity, service capabilities, response completion, native result counts and Flush publication. It does not prepare per-query answers or compare every returned key, version, value or order. The same immutable Trace can be reused across client counts, dispatch modes and backends.

`check-replay CONFIG TRACE` is a standalone input diagnostic; replay already performs preflight, so it is not a prerequisite step. Small independent reference models remain in `src/test/`.

Start the actual service, then replay from another terminal:

```bash
java -jar build/sibench/sibench.jar index-server config/index/rocksdb.yaml
```

```bash
T=results/capture/ycsb/trace
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  "$T" results/service.json
```

The public runtime template uses RowID values, 64 clients and a 600-second request timeout covering sending and response waiting. Both sides must agree on value mode and secondary-index filtering.

For in-process local replay, set `target.service.adapter: rocksdb` and a fresh `target.service.path`. For cloud service execution, build the JNI above, select `service.backend: rocksdb_cloud`, and set `service.storage`, `service.profile` and an independent `service.object_path`. Replace storage placeholders and supply credentials through the environment; the service does not create buckets.

To diagnose an existing index explicitly, set `service.reopen: true` and use `verify-service` for persisted snapshot queries. This is outside required acceptance; see [Publication and reopening](docs/index-service.md#publication-and-reopening).

## Analysis

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/capture/ycsb/trace config/sibench.yaml results/locality
python3 scripts/plot_update_locality.py results/locality --out results/locality/plots
java -jar build/sibench/sibench.jar inspect-scale
```

Plotting requires Python 3, pandas, and matplotlib. Static block / file overlap does not measure actual SST or compaction costs. `inspect-scale` checks default source configurations and displays row counts with status `profiles_valid` without generating data. Only native capture profiles are supported.

## Tests

Maven runs unit tests by default, excluding `*IT` and capacity-diagnostic `main` entry points. Python unit tests do not start external services.

Python test entry points require Python 3.9 or later on a Unix host.

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
bash -n scripts/native-capture.sh
bash -n scripts/build-benchmarks.sh
bash -n scripts/build-rocksdb-cloud.sh
```

### Native generation integration

Build the current instrumented PG and packaged SIBench JAR, resolve Maven
dependencies, and run as a non-root PG service user. Each check creates a fresh
32 MiB-buffer PG cluster, uses a new output directory, records effective
configuration/build identities, removes successful working data, and retains
failed diagnostics. Native modes also require the pinned Sysbench and YCSB builds.

```bash
python3 src/test/python/direct_capture_it.py \
  --postgres-bin build/services/postgresql-capture/runtime/bin \
  --work-directory "results/capture/direct-source-$(date -u +%Y%m%dT%H%M%SZ)"
```

| Mode flag | Input and execution deadline | Checks |
| --- | --- | --- |
| Omitted | Two three-row schemas, proto/full, two sessions; 120 s | Snapshots, rollback, delete/reinsert identity, final handoff, disconnected receivers, PG counters and local service. |
| `--native-clients` | Sys: two tables × 512 rows; Y: 64 rows; one client, four native operations per workload; 120 s | Native load/export/run, complete publication without raw files, counters and local service, and rejection of incomplete sysbench and YCSB loads. |
| `--generation-budgets` | Sys: two tables × 64 rows; Y: 64 rows; 50,000 requested native operations; 180 s | Two clients at 40 Trace records or 4 KiB of proto; 32 clients at one record. Checks early stop, exact final accounting and audits/replay, including complete YCSB read-modify-write operations. The 32-client cases use full output and independent retained-row snapshot validation. |

The mode flags are mutually exclusive. These are bounded correctness checks,
without full-scale captures, cloud experiments or performance claims.

### Full-scale acceptance

Full-scale acceptance uses all three public profiles: HyBench SF10 and 87,060,000 baseline rows each for sysbench and YCSB. Run every configured native workload, publish the complete Trace, then complete one local and one real-cloud replay with response/count checks and Flush. Early budget stops and shortened workloads are partial acceptance. Normal reopen and fault recovery are outside this scope.

Bind results to effective configurations, build/input identities and same-run stage times, throughput, artifact sizes and memory/disk measurements. Small tests do not certify full-input capacity. The `full` output setting is independent of full-scale acceptance; the normal proto output supports it.

The test-classpath `org.sibench.adapters.IndexServerAcceptance SERVICE_CONFIG` starts the real gRPC service. Sending `close` on stdin closes it and the backend and returns `CLOSED`, without another query pass.

### Explicit diagnostics and source acceptance

Run standalone native measurement tests for the cloud JNI with `make -C src/main/native/index/rocksdb_cloud test`. They require the cloud backend build above and the same build dependencies, and do not connect to cloud services.

Select integration tests explicitly with `mvn -Dtest=ClassName test`, connect to an existing isolated PG14.24 instance, and use a fresh evidence directory:

| Integration test | Required configuration |
| --- | --- |
| `PgSchemaIntegrationIT` | `SIBENCH_SCHEMA_TEST_JDBC`; `-Dsibench.schema.evidence=DIR` |
| `PgBaselineExportIntegrationIT` | The same JDBC variable; the current fixture uses postgres / an empty password; `-Dsibench.layout.evidence=DIR` |
| `RowStateAuditIntegrationIT` | `SIBENCH_ROW_TEST_JDBC`, `SIBENCH_ROW_CAPTURE_DIR`; `-Dsibench.row.evidence=DIR` |
| `IndexKeyOrderIT` | `SIBENCH_KEY_TEST_JDBC`; creates temporary tables only |
| `NativeLoadIntegrationIT` | `SIBENCH_SCHEMA_TEST_JDBC`, `SIBENCH_SCHEMA_TEST_USER`, and `PGPASSWORD`; checks unlogged loading against native DDL. |

Schema, row-audit, and key-order tests select users through `SIBENCH_SCHEMA_TEST_USER`, `SIBENCH_ROW_TEST_USER`, and `SIBENCH_KEY_TEST_USER` respectively, defaulting to postgres; the password comes from `PGPASSWORD`. The first three test classes create and remove their own randomly named databases; the evidence directory's parent must exist. The row-audit trigger library defaults to `build/services/postgresql-capture/row_capture.so`; select a separately built library with `-Dsibench.row.library=FILE`. Row-audit tests also require the instrumented PG server to start with `SIBENCH_CAPTURE_DIR` set to that same capture directory, `SIBENCH_CAPTURE_COMPRESS=zstd`, and `SIBENCH_CAPTURE_DETAIL=summary`, with `compute_query_id=on` enabled.

Check the capture script's load WAL limit, clean in-place restart, and backend-crash shutdown explicitly as a non-root service user. This creates isolated PG clusters, needs an unused port and at least 20 GiB free, and has a three-minute execution limit:

```bash
python3 src/test/python/native_capture_it.py \
  --postgres-bin build/services/postgresql-capture/runtime/bin \
  --capture-script scripts/native-capture.sh --work-directory /path/to/new-evidence-directory
```

## Supported scope

Supported layouts and access restrictions are defined in [Source scope](docs/trace.md#source-scope) and [Access projection](docs/trace.md#access-projection). Dynamic DDL, additional index types and primary-key value UPDATEs are rejected.

Data-file Rewrite, physical version GC, resumable segmented compilation, and automatic recovery of writes with unknown outcomes are not implemented. Default tests and short inputs do not establish sustained saturation capacity, support for huge query groups, crash recovery at the target scale, power-loss durability, or backend performance rankings.
