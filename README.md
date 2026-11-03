# SIBench

SIBench captures index accesses from native HyBench, YCSB JDBC, and sysbench PostgreSQL executions. It generates compressed protobuf Trace requests by default; full output also retains Parquet data and detailed index operations for analysis and independent row validation. It supports local RocksDB and rocksdb-cloud, with service replay checking each query's result count and complete ordered SHA-256 digest.

The pipeline is native generation and loading → Baseline export and identity checks → native workload execution with direct Trace generation → publication → analysis or replay. Trace represents index accesses under fixed PG execution plans; it does not represent end-to-end SQL performance or the original session arrival timing.

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

Each build also applies its versioned `scripts/patches/*-stop.patch`. These add a
cooperative stop check between native business operations, preserving their SQL,
transaction bodies and configured concurrency. Capture derives the expected patched
source from the pin, checks every source file, and binds the patch digest in the
build record. The submodules remain unchanged.

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

`capture.output` selects `proto` (the default) or `full` in the benchmark profile.
Proto export reads only primary and secondary index columns from one initial
snapshot and produces final Index Service requests. Generation retains those
encoded shards under `trace/baseline/` and writes `trace/workload.pb.zst`, with
source positions, native expected counts, work units, checksums and input identities.
Same-filesystem baseline retention uses hard links instead of rebuilding requests.
Export checks the actual source schema and uses its snapshot planning count to
enforce YCSB's declared row count before writing data. Row counts are recorded in
the export manifest; loading does not scan the tables again just to count them.
Proto file digests reuse the successfully closed writer's byte-stream digest;
consumers check the stored bytes while reading them.

During the native workload, PG and JDBC facts stream to the Java generator over
a local Unix socket. The existing transaction/snapshot projection and validation
produce final requests in that process; production does not save a whole-run raw
capture or run a separate file conversion. Index Service replay is a separate phase.
Proto output writes no complete-row Parquet, detailed `workload.ops` or retained
raw event files. The published Trace has no Baseline data-file dependency.

Unresolved transactions and completed transactions waiting for safe source order
may use temporary segments under `artifacts/transactions/`.
`generation.pending_transaction_bytes` and `pending_transactions`
in `config/sibench.yaml` limit their combined encoded fact bytes and count
(defaults: 128 GiB and 67,108,864). Pending facts share a 64 MiB encoded-memory
budget; a transaction spills when its facts exceed 256 KiB or the shared budget
is exhausted. Temporary transactions use independent zstd frames with checksums; the byte
budget counts uncompressed encoded facts, so compression does not bypass it.
Crossing either pending limit fails generation and prevents
Trace publication. Size the pending limits for the finite native workload and
available workspace. Frames from one source backend share segments with an
8 MiB compressed target; a complete transaction frame may exceed that target.
Processing releases each transaction's reference. Closed segments disappear when
their last transaction is released; an active tail rotates at the target or closes
when its source backend ends. This avoids a disk block and a compression buffer
per small transaction. They admit facts without reserving disk space; file-system
allocation and metadata add overhead. These are pending-fact limits, not whole-process memory or
total disk caps: PG, source auditing and RowID state still need resources.
JDBC auditing releases ended requests once server execution, consumed results
and transaction association agree; late consumption of an older open ResultSet
keeps that request live. Each session admits at most 65,536 pending requests and
1,048,576 buffered row-hash/detail entries, failing generation on overflow. These
are entry counts, not byte or RSS caps. PG and the generator run concurrently. Failure
diagnostics stream at most 65,536 transaction details, declare truncation, and retain
the exact aggregate pending metrics. Failures retain pending spool diagnostics and
`INCOMPLETE`; they do not produce a successful partial Trace.

Optional `generation.operation_target` and `generation.output_byte_target` in
`config/sibench.yaml` request a normal stop; omitted or zero means no extra target.
The operation target counts emitted workload Trace records, including transaction
control and index expansion. The byte target counts the complete compressed
canonical proto files, including Baseline; it excludes metadata, raw, Parquet,
PG and temporary storage. It must exceed the complete Baseline proto size;
initial-state-only captures are unsupported.

After a complete source transaction reaches either target, clients stop admitting
new business operations, finish their active operations, and close normally.
Already produced facts are drained and audited, so projected-source lag, active
transactions and buffered compressed blocks can cause overshoot. The
`generation_budget` manifest/timing entry records targets, request-time progress,
actual counts/bytes, overshoot and whether a target or the native workload ended
the run. Later native workloads/phases are skipped after a target stop. All
source-completeness and native-result checks apply to the actual executed prefix.
These targets do not impose a memory or disk hard cap. With targets enabled,
HyBench's native timer also lets active operations finish instead of interrupting them.

Full output uses the same generation path and additionally saves compressed raw
facts, complete-row Baseline, incremental Parquet and detailed operations as
side outputs. Select it for Locator values, `--audit-baseline`, `audit-rows`,
`validate-snapshots` or data-file relocation. Proto-only output supports RowIDs.
The usual `check-replay`, `replay`, `replay-service`, `verify-service` and locality
commands read published proto inputs. Replay forwards stored write/search/group
payloads while assigning replay IDs and transaction positions. Both output modes
use this protobuf path; full-mode writes select RowID or Locator values from
the stored metadata. Detailed operations are diagnostic side output. Operation views
support validation and the reference model. Run `replay` before service replay
to check native counts and supply ordered result digests; this does not claim
an independent Parquet row oracle.

Run as a non-root user able to manage its own PG cluster. Java selection prefers `SIBENCH_JAVA_HOME`, then `JAVA_HOME`, then PATH. `SIBENCH_HEAP` defaults to `-Xmx12g`.

All three sources default to **87,060,000 rows**, corresponding to HyBench SF10. Filenames remain stable; each file declares the scale, table count, and concurrency:

| Configuration | Responsibility |
| --- | --- |
| `config/benchmark/hybench.yaml`, `ycsb.yaml`, `sysbench.yaml` | Native workloads, capture database identities, and source execution constraints |
| `config/sibench.yaml` | PG server settings, pending generation budgets, replay drivers, and locality analysis |
| `config/index/rocksdb.yaml`, `storage.yaml` | Index service and object storage backends |

All three sources use the same generation entry point: native baseline loading, initial-state export, and native workload execution with direct Trace generation. Source adapters retain their own native clients and loading methods. Each run creates one fresh database, exports its initial state, and executes the native workloads in that same database. HyBench does not restore data between workload phases. Dedicated clusters use local trust authentication and empty client passwords; inherited `PGPASSWORD` is never written to command lines or logs.

Generation PostgreSQL is disposable. Source tables and their indexes are unlogged, and the default settings disable `fsync` and `full_page_writes`. Native table creation is converted while tables are empty, before loading. Failed or crashed state must be rebuilt; existing database directories are never reused. This policy does not change Index Service durability. WAL and checkpoint settings remain targets, not hard disk limits.

Production checks source provenance, client builds, schema, input files, request/session completion, transaction semantics, and the access ledger against PostgreSQL index counters. Baseline file identity is recorded in `baseline-manifest.json`; this is not an independent value-audit claim. Full baseline/row-state comparisons are explicit acceptance commands for full output. Production writes the selected Trace, timing and effective configurations, publishes only after source audits and PG counter checks, stops PG, and removes the database after success unless `SIBENCH_KEEP_DATABASE=1`. Full output retains the Baseline and raw capture; proto output releases the consumed initial export and source control directory after publication. Failed artifacts remain for diagnosis and cannot be resumed. Use a fresh output path for each run.

Native execution stdout is saved as `native-run.log`. Structured capture status and build identities are recorded in `raw/capture-run.json` during generation and copied to `trace/source-capture-run.json` before publication. Proto mode uses the configured raw directory for control metadata only.

Provision disk capacity for one working database, Parquet / Trace artifacts, and compiler workspaces. Generation does not retain a reusable initial database or copy it into a capture database. Explicit acceptance needs additional audit workspace.

For YCSB databases with `C` or `POSIX` collation, Baseline export reads ordered primary-index ranges from one snapshot and forbids PG temporary spills in those transactions. Two read windows share 128 MiB of encoded-row queue credit; a row must fit one reader's share of a 64 MiB window. This bounds the queues, not total JVM memory or all pipeline disk use. Other database collations use explicit `C` sorting and still require temporary space proportional to the input.

### Independent source acceptance

Normal generation checks Baseline row coverage, file boundaries and file SHA-256 identities. Independent value and row-state audits are separate workflows:

| Check | Entry point | Required source state |
| --- | --- | --- |
| Initial Parquet values | `java -jar build/sibench/sibench.jar audit-baseline PROFILE BASELINE --jdbc URL --user ROLE` | The unchanged initial database, before workload execution. |
| Captured row changes and final values | `java -jar build/sibench/sibench.jar audit-rows PROFILE BASELINE RAW --jdbc URL --user ROLE` | The same run's final database, with business clients stopped. |

Use `scripts/native-capture.sh --audit-baseline PROFILE` to run the initial comparison immediately after export. It records the audit command, timing and result separately and stops on failure. To retain the final database for `audit-rows`, set `SIBENCH_KEEP_DATABASE=1`; restart the cleanly stopped database on the same host boot before auditing. A final-state audit cannot replace the initial comparison.

These audits read full inputs and require additional workspace. Measure normal generation in a separate run: subtracting audit time does not remove its cache effects. Trace publication and service acceptance are described below and in the [Trace](docs/trace.md#validation-responsibilities) and [Index Service](docs/index-service.md#validation-boundaries) contracts.

## Replay and service

The following commands reuse an existing Trace without connecting to PG:

```bash
T=results/capture/ycsb/trace
java -jar build/sibench/sibench.jar replay config/sibench.yaml "$T" results/reference.json
```

`replay` performs an in-process reference check and generates per-query expectations for the service. Reuse the reference report and digest file for the same Trace, value mode, and secondary-index filtering configuration. Changing the client count, dispatch mode, or backend does not require regenerating answers. Adjust `client_count`, `dispatch_mode`, and the secondary-index switch in the reference configuration instead of maintaining a template for every combination.

`replay-service` requires `--reference REPORT`. Prepare the reference explicitly; service replay rejects a missing reference before reading the configuration or Trace and does not generate answers implicitly.

Replay entry points include input preflight, load verification, and result checks. `check-replay CONFIG TRACE` is a standalone input diagnostic that fully consumes Baseline; it need not run separately before every replay. Full-output acceptance uses `validate-snapshots CONFIG TRACE REPORT` at the affected boundary to check results against an independent Parquet row-history oracle. Proto inputs use `replay` for native-count checks and reference digests; source-semantic changes additionally need controlled source observations, and real-service acceptance remains separate.

`validate-snapshots` also writes the per-query expectations during its reference-model pass. Its report can be passed directly to `replay-service --reference REPORT` and `verify-service`; a separate `replay` run is unnecessary when the oracle already prepares the required value mode.

Ordinary `replay` checks native counts and writes complete ordered query digests.
The additional whole-primary-state checksum scan belongs to the explicit
`validate-replay-matrix` diagnostic, which compares final states across variants.

Start the actual service, then replay from another terminal:

```bash
java -jar build/sibench/sibench.jar index-server config/index/rocksdb.yaml
```

```bash
T=results/capture/ycsb/trace
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  "$T" results/service.json --reference results/reference.json
```

Reference and service replay share `config/sibench.yaml`; the command selects the execution target. The public template uses row_id, 64 clients, complete-commit publication, and a 600-second request timeout for full source inputs. The timeout includes local sending and waiting for the service response. Open an existing index with `service.reopen: true`, then use `verify-service` to check persisted snapshot queries. See above for the JNI build and the [Index Service contract](docs/index-service.md) for storage configuration and acknowledgment levels. The COS bucket is a placeholder; configure your own storage and environment credentials before running.

## Analysis and derived generation

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/capture/ycsb/trace config/sibench.yaml results/locality
python3 scripts/plot_update_locality.py results/locality --out results/locality/plots
java -jar build/sibench/sibench.jar inspect-scale
```

Plotting requires Python 3, pandas, and matplotlib. Static block / file overlap does not measure actual SST or compaction costs. `inspect-scale` checks default source configurations and displays row counts with status `profiles_valid` without generating data. `prepare --dry-run PROFILE` is a separate derived-generation entry point that does not accept native capture configurations. Derived examples are development / test inputs; their scale does not indicate compressed file size.

## Tests

Service acceptance covers actual local/cloud replay correctness, Flush, and
snapshot checks after a clean close and reopen. Fault-recovery experiments,
including forced termination and cloud cold recovery after local-directory loss,
are outside required acceptance; see the [validation boundaries](docs/index-service.md#validation-boundaries).

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
| Omitted | Two three-row schemas, two sessions; 120 s | Snapshots, rollback, delete/reinsert identity, final handoff, disconnected receivers, PG counters, reference and local service. |
| `--native-clients` | Sys/Y: 64 rows, one client, four native operations per workload; 120 s | Native load/export/run, complete publication without raw files, counters, reference/local service, and rejection of an incomplete YCSB load. |
| `--generation-budgets` | Sys/Y: 64 rows, 50,000 requested native operations; 180 s | Two clients at 40 Trace records or baseline proto bytes + 64; 32 clients at one record. Checks early stop, exact final accounting and audits/replay, including complete YCSB read-modify-write operations. The 32-client cases use full output and independent retained-row snapshot validation. |

The mode flags are mutually exclusive. These are bounded correctness checks,
without full-scale captures, cloud experiments or performance claims.

### Explicit diagnostics and source acceptance

For a bounded protobuf decoder diagnostic, compile test classes with Maven and run:

```bash
CP=build/sibench/test-classes:build/sibench/sibench.jar
java -Xmx1g -cp "$CP" org.sibench.capture.ProtoTraceBenchmark create /tmp/sibench-codec 1000000 250000
java -Xmx1g -cp "$CP" org.sibench.capture.ProtoTraceBenchmark read /tmp/sibench-codec 5
```

Use a fresh fixture directory. This checks physical/logical digests and operation
coverage, and reports warmed read time, thread CPU and allocation. Its fixed
primary-write/read-your-writes input is a decoder diagnostic, not a native workload
or Index Service throughput result.

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
