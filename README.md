# SIBench

SIBench captures index accesses from native HyBench, YCSB JDBC, and sysbench PostgreSQL executions. It generates Parquet data and deterministic Trace artifacts for update-locality analysis and KV Index Service evaluation. It supports local RocksDB and rocksdb-cloud, with service replay checking each query's result count and complete ordered SHA-256 digest.

The pipeline is native generation and loading → Baseline export and identity checks → native execution capture → Trace compilation → analysis or replay. Trace represents index accesses under fixed PG execution plans; it does not represent end-to-end SQL performance or the original session arrival timing.

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

Builds use fresh copies under `build/benchmarks/` and reject existing build destinations. `native-build.json` pins the source commit and SHA digests of the actual executable, classes, and dependency JARs. Capture verifies this record; capture identities also include the actual client. sysbench's `native.binary` must select a file in that inventory; the standard build uses `src/sysbench`. A missing build-time inventory requires rebuilding the client from source. HyBench resolves dependencies through its POM. Its build copy applies `scripts/patches/hybench-1.0-snapshot.patch` to exclude the dsol XML simulator unused by `PowerDist`; the actual POM also contributes to the build digest. YCSB's distribution path is determined by the build output.

### rocksdb-cloud JNI

This component is required only for the cloud backend. Source revisions are pinned by the `index/rocksdb-cloud` and `third_party/aws-sdk-cpp` submodule gitlinks; Java dependencies are pinned in `pom.xml`. The build script requires both submodules to be clean and at their pinned commits.

The script targets yum-based systems and requires patch, C/C++, CMake, make, and JDK 17. It installs curl, OpenSSL, and compression development dependencies as root, and applies the SDK path patch shipped with the JNI code to the build copy so that it uses this build's SDK headers. SDK and RocksDB build copies and JNI artifacts all live under `build/index/`; the AWS SDK is installed into its `aws-sdk/` subdirectory. The SDK builds the core, s3, transfer, and kinesis components; see the script for exact options. The build environment must be ABI-compatible with the runtime host.

```bash
scripts/build-rocksdb-cloud.sh
```

The output is `build/index/librocksdb_cloud_jni.so`; select it at runtime with `-Dsibench.rocksdb_cloud.library=PATH`. Upstream licenses remain in their submodules: [rocksdb-cloud](index/rocksdb-cloud/COPYING), [AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt).

## Native capture

```bash
scripts/native-capture.sh config/benchmark/ycsb.yaml
```

Run as a non-root user able to manage its own PG cluster. Java selection prefers `SIBENCH_JAVA_HOME`, then `JAVA_HOME`, then PATH. `SIBENCH_HEAP` defaults to `-Xmx12g`.

All three sources default to **87,060,000 rows**, corresponding to HyBench SF10. Filenames remain stable; each file declares the scale, table count, and concurrency:

| Configuration | Responsibility |
| --- | --- |
| `config/benchmark/hybench.yaml`, `ycsb.yaml`, `sysbench.yaml` | Native workloads, capture database identities, and source execution constraints |
| `config/sibench.yaml` | PG server settings, reference / service replay drivers, and locality analysis |
| `config/index/rocksdb.yaml`, `storage.yaml` | Index service and object storage backends |

All three sources use the same generation entry point: native baseline loading, initial-state export, native workload capture, and Trace compilation. Source adapters retain their own native clients and loading methods. Each run creates one fresh database, exports its initial state, and executes the native workloads in that same database. HyBench does not restore data between workload phases. Dedicated clusters use local trust authentication and empty client passwords; inherited `PGPASSWORD` is never written to command lines or logs.

Generation PostgreSQL is disposable. Source tables and their indexes are unlogged, and the default settings disable `fsync` and `full_page_writes`. Native table creation is converted while tables are empty, before loading. Failed or crashed state must be rebuilt; existing database directories are never reused. This policy does not change Index Service durability. WAL and checkpoint settings remain targets, not hard disk limits.

Production checks source provenance, client builds, schema, input files, request/session completion, transaction semantics, and the access ledger against PostgreSQL index counters. Baseline file identity is recorded in `baseline-manifest.json`; this is not an independent value-audit claim. Full baseline/row-state comparisons are explicit acceptance commands. Production writes Baseline, raw capture, Trace, timing and effective configurations, stops PG before compilation, and removes the database after success unless `SIBENCH_KEEP_DATABASE=1`. Failed artifacts remain for diagnosis and cannot be resumed. Use a fresh output path for each run.

Native execution stdout is saved as `native-run.log`. Structured capture status and build identities are recorded in `raw/capture-run.json`.

Provision disk capacity for one working database, Parquet / Trace artifacts, and compiler workspaces. Generation does not retain a reusable initial database or copy it into a capture database. Explicit acceptance needs additional audit workspace.

For YCSB databases with `C` or `POSIX` collation, Baseline export reads ordered primary-index ranges from one snapshot and forbids PG temporary spills in those transactions. Two read windows share 128 MiB of encoded-row queue credit; a row must fit one reader's share of a 64 MiB window. This bounds the queues, not total JVM memory or all pipeline disk use. Other database collations use explicit `C` sorting and still require temporary space proportional to the input.

### Independent source acceptance

Normal generation checks Baseline row coverage, file boundaries and file SHA-256 identities. Independent value and row-state audits are separate workflows:

| Check | Entry point | Required source state |
| --- | --- | --- |
| Initial Parquet values | `java -jar build/sibench/sibench.jar audit-baseline PROFILE BASELINE --jdbc URL --user ROLE` | The unchanged initial database, before workload execution. |
| Captured row changes and final values | `java -jar build/sibench/sibench.jar audit-rows PROFILE BASELINE RAW --jdbc URL --user ROLE` | The same run's final database, with business clients stopped. |

Use `scripts/native-capture.sh --audit-baseline PROFILE` to run the initial comparison immediately after export. It records the audit command, timing and result separately and stops on failure. To retain the final database for `audit-rows`, set `SIBENCH_KEEP_DATABASE=1`; restart the cleanly stopped database on the same host boot before auditing. A final-state audit cannot replace the initial comparison.

These audits read full inputs and require additional workspace. Measure normal generation in a separate run: subtracting audit time does not remove its cache effects. Trace conversion and service acceptance are described below and in the [Trace](docs/trace.md#validation-responsibilities) and [Index Service](docs/index-service.md#validation-boundaries) contracts.

## Replay and service

The following commands reuse an existing Trace without connecting to PG:

```bash
T=results/poc/ycsb/trace
java -jar build/sibench/sibench.jar replay config/sibench.yaml "$T" results/reference.json
```

`replay` performs an in-process reference check and generates per-query expectations for the service. Reuse the reference report and digest file for the same Trace, value mode, and secondary-index filtering configuration. Changing the client count, dispatch mode, or backend does not require regenerating answers. Adjust `client_count`, `dispatch_mode`, and the secondary-index switch in the reference configuration instead of maintaining a template for every combination.

`replay-service` requires `--reference REPORT`. Prepare the reference explicitly; service replay rejects a missing reference before reading the configuration or Trace and does not generate answers implicitly.

Replay entry points include input preflight, load verification, and result checks. `check-replay CONFIG TRACE` is a standalone input diagnostic that fully consumes Baseline; it need not run separately before every replay. For changes to compilation or reader semantics, backend integration, and formal acceptance, explicitly run `validate-snapshots CONFIG TRACE REPORT` at the affected boundary to check results against an independent Parquet row-history oracle.

`validate-snapshots` also writes the per-query expectations during its reference-model pass. Its report can be passed directly to `replay-service --reference REPORT` and `verify-service`; a separate `replay` run is unnecessary when the oracle already prepares the required value mode.

Start the actual service, then replay from another terminal:

```bash
java -jar build/sibench/sibench.jar index-server config/index/rocksdb.yaml
```

```bash
T=results/poc/ycsb/trace
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  "$T" results/service.json --reference results/reference.json
```

Reference and service replay share `config/sibench.yaml`; the command selects the execution target. The public template uses row_id, 64 clients, complete-commit publication, and a 600-second request timeout for full source inputs. The timeout includes local sending and waiting for the service response. Open an existing index with `service.reopen: true`, then use `verify-service` to check persisted snapshot queries. See above for the JNI build and the [Index Service contract](docs/index-service.md) for storage configuration and acknowledgment levels. The COS bucket is a placeholder; configure your own storage and environment credentials before running.

## Analysis and derived generation

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/poc/ycsb/trace config/sibench.yaml results/locality
python3 scripts/plot_update_locality.py results/locality --out results/locality/plots
java -jar build/sibench/sibench.jar inspect-scale
```

Plotting requires Python 3, pandas, and matplotlib. Static block / file overlap does not measure actual SST or compaction costs. `inspect-scale` checks default source configurations and displays row counts without generating data. `prepare --dry-run PROFILE` is a separate derived-generation entry point that does not accept native capture configurations. Derived examples are development / test inputs; their scale does not indicate compressed file size.

## Tests

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
bash -n scripts/native-capture.sh
bash -n scripts/build-benchmarks.sh
bash -n scripts/build-rocksdb-cloud.sh
```

Run standalone native measurement tests for the cloud JNI with `make -C src/main/native/index/rocksdb_cloud test`. They require the cloud backend build above and the same build dependencies, and do not connect to cloud services.

Maven runs Java unit tests by default, excluding `*IT` and capacity-diagnostic `main` entry points. Python tests check script behavior and report analysis without starting external services. Select integration tests explicitly with `mvn -Dtest=ClassName test`, connect to an existing isolated PG14.24 instance, and use a fresh evidence directory:

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

The current [Trace](docs/trace.md) and [Index Service](docs/index-service.md) support fixed source layouts and reject additional index types, dynamic DDL, and primary-key value UPDATEs. YCSB has no secondary indexes; standard sysbench scripts do not query by k; HyBench does not run freshness DELETE by default. The default data-file target is 512 MiB. Layout changes require rebuilding the corresponding physical inputs.

Data-file Rewrite, physical version GC, resumable segmented compilation, and automatic recovery of writes with unknown outcomes are not implemented. Default tests and short inputs do not establish sustained saturation capacity, support for huge query groups, crash recovery at the target scale, power-loss durability, or backend performance rankings.
