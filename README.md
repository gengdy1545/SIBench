# SIBench

SIBench generates index workloads with native storage, then replays their unified Trace against an Index Service. Each benchmark explicitly selects its native engine. YCSB API uses embedded RocksDB; sysbench capture supports PostgreSQL and DuckDB; HyBench capture uses DuckDB. RocksDB and PostgreSQL generation retain native data and export index operations online.

Explicit native checks cover small end-to-end generation, relocated native data reads, service replay and reference replay for all three sources. YCSB and sysbench checks exercise their six configured workloads; HyBench checks exercise AP → TP → concurrent XP with index-operator output as the Search boundary. These checks do not certify full-scale generation.

See the [Trace contract](docs/trace.md) and [Index Service contract](docs/index-service.md). Trace models actual index accesses and transaction visibility; it does not measure end-to-end SQL performance or reproduce client arrival timing.

## Build

JDK 17, Maven and Git are required. Set `JAVA_HOME` to the selected JDK.

```bash
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar --help
```

The package is `build/sibench/sibench.jar`, including Java dependencies. Commands use the working directory as repository root unless `--root PATH` is supplied. Outputs must be fresh; failures retain diagnostics and cannot be resumed.

### DuckDB and benchmark clients

DuckDB requires C++17, CMake, make and patch. The source pin belongs to the `third_party/duckdb` gitlink; its license stays in that submodule. Java dependencies belong to `pom.xml`.

```bash
scripts/build-duckdb.sh config/sibench.yaml
scripts/build-benchmarks.sh config/benchmark/ycsb.yaml
scripts/build-benchmarks.sh config/benchmark/sysbench.yaml
scripts/build-benchmarks.sh config/benchmark/hybench.yaml
```

The engine build directory comes from `duckdb.build_directory` in the runtime configuration. `JOBS` controls build parallelism (default 4). The build copies pinned sources, applies the project instrumentation and creates `duckdb_host`, `duckdb`, and `src/libduckdb.so` there. Existing destinations are rejected. The build snapshots the gitlink, instrumentation, host source and recipe before compilation, checks prepared sources again afterward, and writes `engine-build.json` only after success. The receipt binds host/library/CLI hashes, CMake settings, compile commands, compiler version and the prepared-source inventory. Generation verifies that receipt and current inputs before launching the host; the host uses its bundled library and reports the matching source identity. This is a local build receipt, not a hermetic-build or cross-machine reproducibility guarantee.

Client builds use fresh copies under `build/benchmarks/`, leaving submodules unchanged. HyBench and YCSB require Maven; sysbench additionally needs autotools, pkg-config, libtool, libaio and libpq development libraries. `native-build.json` binds the pinned sources, applied patches, adapters and executable/dependency digests. Generation checks that inventory. sysbench builds both its PostgreSQL and DuckDB drivers; YCSB builds upstream core with the API binding. The HyBench patch supplies DuckDB JDBC/dialect support, propagates worker failures and samples only available risk-account IDs. Empty risk-account populations fail loading explicitly.

### PostgreSQL native components

The `third_party/postgresql` gitlink pins the instrumented upstream source; its license remains in [COPYRIGHT](third_party/postgresql/COPYRIGHT). Building from Git requires a C compiler, make, patch, Perl, Bison, Flex and libzstd development headers. PostgreSQL runs as an unprivileged user.

```bash
bash scripts/build-postgres.sh config/sibench.yaml
```

`postgres.build_directory` owns the build location; choose a fresh directory in a local runtime configuration if the default already exists. The build records the source pin, prepared source hashes, compiler identity, configure settings and installed artifacts in `engine-build.json`. The runtime is under `runtime/` inside that directory. Native page, relation-segment and WAL segment sizes keep upstream defaults. The dedicated standalone cluster uses minimal WAL with no replication sender; fsync, synchronous commit and full-page writes are disabled during generation. Business, history and metadata tables use UNLOGGED persistence. Native transactions still apply; interrupted generation fails without publication and retains diagnostics. Successful publication stops the cluster and runs `initdb --sync-only` once to persist its native files. Query and maintenance parallel workers are disabled to keep the dedicated cluster within bounded memory, including containers with small shared-memory mounts. Generation streams sequenced facts over a local socket, orders transactions using native completion watermarks, and reclaims bounded transaction spills after projection. Capture backends wait at transaction boundaries when 128 outcomes remain unconsumed. Row-change events contain indexed attributes, while native history retains full row contents. Component checks can capture files explicitly. Both capture transports require a clean closing record and exclude indexes used by `sibench_history` itself. Business rows, before-images and RowID allocations share the same PostgreSQL transaction; capture retains completed actions and allocated identities even when that transaction aborts. No exported mutable row copy is maintained.

### RocksDB source storage

YCSB uses the Maven-owned `rocksdbjni` runtime. A thin C++17/JNI bridge exposes native timestamps and computes SST/Blob SHA-256 through the native file-checksum interface; Java owns API interpretation and Trace output. Build on Linux/glibc x86-64 with Python 3.9 or newer, C++17, JDK 17, pkg-config and OpenSSL development headers/static libcrypto:

```bash
bash scripts/build-rocksdb.sh build/services/rocksdb
```

Use the fresh directory selected by `rocksdb.build_directory`. The bridge links the exact Maven runtime and uses matching headers from the pinned `index/rocksdb-cloud` source; it does not build or load the cloud backend. Header/runtime versions and binary identities must match. `engine-build.json` records the headers, recipe, compile command, statically linked OpenSSL identity and artifacts. Existing build directories are rejected. Native dependencies remain under their upstream licenses.

### rocksdb-cloud JNI

This component is required only for the cloud backend. Source revisions are pinned by the `index/rocksdb-cloud` and `third_party/aws-sdk-cpp` submodule gitlinks; Java dependencies are pinned in `pom.xml`. The build script requires both submodules to be clean and at their pinned commits.

The script targets yum-based systems and requires patch, C/C++, CMake, make and JDK 17. It installs development dependencies as root and builds the SDK and JNI under `build/index/`. Use a build host ABI-compatible with the runtime host.

```bash
scripts/build-rocksdb-cloud.sh
```

The output is `build/index/librocksdb_cloud_jni.so`; select it at runtime with `-Dsibench.rocksdb_cloud.library=PATH`. Upstream licenses remain in their submodules: [rocksdb-cloud](index/rocksdb-cloud/COPYING), [AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt).

## Native generation

The runtime configuration selects exactly which profiles to run, their modes and engines:

```yaml
benchmarks:
  - config: config/benchmark/ycsb.yaml
    mode: api
    engine: rocksdb
  - config: config/benchmark/sysbench.yaml
    mode: capture
    engine: postgres
  - config: config/benchmark/hybench.yaml
    mode: capture
    engine: duckdb
duckdb:
  build_directory: build/services/duckdb
  memory_limit: 16GiB
  threads: 8
postgres:
  build_directory: build/services/postgres
rocksdb:
  build_directory: build/services/rocksdb
  cache_bytes: 1073741824
  write_buffer_bytes: 4294967296
  background_jobs: 16
```

All three public profiles retain **87,060,000 baseline rows** (HyBench SF10). This is a substantial run, not a quickstart fixture. Set the required scale/workloads and fresh artifact paths in experiment configurations before using a smaller run. HyBench's `sf` selects the upstream parameter table. Optional `native.parameters` overrides only `customer_number`, `company_number`, `transfer_number`, `checking_number`, `loanapps_number` and `loantrans_number`, as positive integers. For an 8,706-row run, use `sf: 1x` and respectively 300, 2, 6000, 600, 600, 600; each account table then has 302 rows. The effective `parameters.toml` is shared by loading and workload execution, preserved with its digest, and used by `inspect-scale` for expected counts. Keep these overrides in experiment configurations.

```bash
java -jar build/sibench/sibench.jar generate config/sibench.yaml
```

| Configuration | Responsibility |
| --- | --- |
| [ycsb.yaml](config/benchmark/ycsb.yaml), [sysbench.yaml](config/benchmark/sysbench.yaml), [hybench.yaml](config/benchmark/hybench.yaml) | Source assertion, native scale, workload, client concurrency, timeout, build and output paths |
| [sibench.yaml](config/sibench.yaml) | Selected benchmarks/modes/engines, native runtimes, replay and locality analysis |
| [rocksdb.yaml](config/index/rocksdb.yaml), [storage.yaml](config/index/storage.yaml) | Index service and object storage |

`generate` validates all selected inputs before starting any host. Empty/duplicate selections, unsupported mode pairs, changed source/build inputs, unknown options and existing/overlapping outputs fail preflight. Unselected profiles are not read. The first failed benchmark stops the sequence.

Public profiles retain 87,060,000 baseline rows and request 60,000,000 effective workload index requests through `trace.workload_requests`. The common Trace writer counts after secondary-index filtering: each write, point/range search or SearchGroup counts once; baseline entries and transaction markers do not consume this budget. Omitting the target runs each configured native batch once. Native event counts and phase durations define finite source batches; generation repeats batches on the same database until the exported-request quota is reached. Remaining requests are divided equally among remaining workloads, carrying earlier overshoot forward. HyBench runs AP once, then divides the remaining target between TP and XP; all phases are retained. A source batch with no exported progress fails instead of looping indefinitely.

YCSB and PostgreSQL stop admitting business events when the writer reaches a workload quota, then drain admitted events. YCSB F keeps its separate Read/Update API boundaries and finishes the admitted pair. DuckDB checks the same counter after each drained capture window, so its overshoot can include a complete native batch. No Trace is truncated or replayed to fill the target. `request-budget.json` and manifest source evidence report target, actual and overshoot counts, including per-workload totals. The workload supply loop also checks `timeout_minutes` between batches; individual source execution and shutdown retain their own bounds.

Each benchmark uses one fresh database of its selected engine. PostgreSQL loads native rows, vacuums/analyzes the loaded tables, installs native history/identities and emits baseline indexes during the necessary baseline read. Loaded-page visibility allows key-only identity creation to use the primary index without rereading wide rows; metadata statistics are refreshed before workloads. It then runs the pinned workloads while exporting final index operations. Source stream sequences and client/server counters establish completeness during these reads. Normal shutdown moves the native cluster into `trace/data/`; publication hashes its files once. Generation does not replay its output or scan all historical rows for a second correctness audit. `result.json` records load, baseline/maintenance, workload/export, shutdown and publication durations. The manifest retains peak pending-transaction/encoded-byte counters and the drained queue counts.

YCSB runs the pinned upstream loader and A/B/C/F/D/E clients in the same JVM as one fresh RocksDB instance. Each stage uses a fresh client class loader so upstream counters and measurements do not leak between workloads. The adapter batches baseline inserts only, emits baseline indexes while loading, and exports workload operations directly as each native operation completes. Native row locks protect partial updates; a short publication section orders commit timestamps and Trace output. Timestamped native records retain historical row values, including versions preceding deletion/reinsertion. Read-modify-write remains two upstream API calls. WAL is disabled for generation writes; successful completion flushes all column families and closes the database before publication. SST and Blob identities are computed while native files are written and collected after successful close; publication hashes only the remaining metadata. Consumption independently checks all retained file bytes. There is no recovery, reusable initial state, raw operation file or final replay audit.

`rocksdb.cache_bytes` bounds the shared block cache; `write_buffer_bytes` configures each row memtable during baseline loading (up to three). Native flush completes baseline loading, then row memtables use at most 2 GiB each (up to three) during workloads. `background_jobs` configures background jobs, and each compaction may use up to `background_jobs - 1` parallel partitions. Metadata uses smaller memtables. These are engine budgets, not a total process-memory cap. Between upstream clients, the Linux bridge returns free allocator pages to the operating system; it does not change the allocation policy or discard native data. The transaction conflict-check memtable cache uses a minimal retention budget because these row-locked transactions do not use native transaction snapshots; timestamped history remains in native files. Row SSTs store values inline with Zstd level 1 compression. The row column family disables background compaction and L0 file-count stalls to avoid rewriting its finite retained history; memtable flushing remains bounded. More row files increase read amplification, so larger or update-heavy captures require separate capacity evaluation. Metadata uses LZ4 and normal leveled compaction. Source distributions, concurrency and operation boundaries are unchanged. These defaults are a starting configuration, not a full-scale performance certification.

DuckDB incrementally binds drained capture windows:

1. Run the upstream loader and check table counts. HyBench CSV input identities are recorded before COPY; successful loading removes the CSVs and retains the two runtime ID lists, whose hashes are checked before workload execution.
2. Install native RowID/history tracking, export baseline indexes from the loaded initial rows, then execute configured workloads in that same host. DuckDB owns data changes, snapshots, Commit and Abort.
3. Drain a native batch, close its logging sink, bind its facts to client receipts and append successful mutations, supported accesses and transaction outcomes to the shared Trace. Each window is read once. Persistent native resolution/read-point and identity mappings preserve cross-window visibility; temporary bindings are released before the next batch. Capture mode requires an adopted ART access rather than inferring one from SQL.
4. Seal history, close the host normally and publish `trace/`. The database file is moved into the artifact without exporting/reimporting the rows.

The complete artifact contains `baseline.pb.zst`, `workload.pb.zst`, `work-units.jsonl.zst`, native files under `data/` and `manifest.json`. It supports RowID index values and native historical rows after relocation; Locator replay is rejected before backend creation. DuckDB uses `data/data.duckdb`; PostgreSQL uses its native cluster layout; RocksDB retains its SST/Blob/MANIFEST layout. Effective configurations, build/input hashes and client logs remain beside the Trace. PostgreSQL retains source count/digest reports, while the current DuckDB path retains facts and receipts. `INCOMPLETE` prevents consumption after a generation/publication failure.

`TraceManifest.openData(trace)` opens sealed DuckDB data read-only. `TraceManifest.openData(trace, rocksdbBuildDirectory)` reads RocksDB data in place with the verified timestamp bridge, without a writable copy or data recovery. PostgreSQL row consumption uses `TraceManifest.openData(trace, runtime, freshWorkingDirectory)`: it checks the data/runtime inventory and starts an isolated native copy with read-only transactions. Copy-on-write is used where supported; other filesystems require a full copy. Closing the reader stops its server and removes the copy. This cost belongs to explicit row consumption, not generation or index-only replay.

`source-diagnostics.json` contains per-window summaries of receipt-bound storage statements, adopted/abandoned ART attempts and mutation scopes before projection. Failed-statement scopes remain diagnostic even when an internal batch completed; unknown physical partial writes are not reported as zero. Probe timing covers ART probes only. Statements without adopted ART are counted separately, without inventing a Search or assuming they performed no index maintenance. See the [Trace contract](docs/trace.md#access-projection) for projection limits.

`memory_limit` controls DuckDB's memory budget, not total JVM/native RSS or disk use. Each client stage has its profile's `timeout_minutes`; host setup and normal shutdown have separate bounds. These are not a whole-run resource quota. Rewrite and GC are not implemented.

Each benchmark profile sets `trace.secondary_indexes: false` by default. This exports primary-index requests while retaining the source database's native secondary indexes and workload plans. Set it to `true` to export all source indexes. Mixed primary/secondary query groups are omitted as a whole in primary-only output. Transaction boundaries and outcomes remain, including empty transactions. Consumption uses the separate `target.secondary_indexes.enabled` setting, also false in the public template; enabling it requires a Trace exported with secondary indexes.

Baseline and workload files are separate. Their `proto_files` manifest entries report compressed file bytes, uncompressed message bytes, effective requests, index entries, primary/secondary entries, request types and transaction markers. Groups count once as requests and once per member as entries; these counts differ from RPC batches and returned rows. Primary-only baseline Insert entries equal baseline rows. See the [Trace contract](docs/trace.md#protobuf-format) for the exact statistics.

## Replay and service

Replay checks input integrity, service capabilities, response completion, native result counts and Flush publication. It does not prepare per-query answers or compare every returned key, version, value or order. The same immutable Trace can be reused across client counts, dispatch modes and backends.

`check-replay CONFIG TRACE` is a standalone input diagnostic; replay already performs preflight, so it is not a prerequisite step. Small independent reference models remain in `src/test/`.

`plan-operations PROFILE TRACE REPLAY_CONFIG` estimates native workload parameters from a calibration Trace using the replay configuration's effective request budget. Each query group counts once; baseline entries and transaction markers are excluded, while requests in aborted transactions still count. All configured workload knobs scale by one factor. The estimate does not guarantee supply: generation currently runs the configured native events/durations, and `check-replay` rejects insufficient output. It does not stop generation automatically at the target.

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

Default Maven tests do not launch the upstream benchmark suites. Native `*Check` classes and `*IT` diagnostics must be selected explicitly. Python tests exercise orchestration with fixtures, without external services; Python 3.9+ and a Unix host are required.

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
bash -n scripts/build-duckdb.sh scripts/build-postgres.sh scripts/build-rocksdb.sh scripts/build-benchmarks.sh scripts/build-rocksdb-cloud.sh
```

After building the instrumented engine, select native checks explicitly:

```bash
mvn -Dtest=PostgresHistoryCheck,PostgresAccessesCheck \
  -Dsibench.postgres.runtime=build/services/postgres/runtime test
mvn -Dtest=PostgresCaptureCheck,PostgresVersionsCheck,PostgresProjectionCheck,PostgresGenerationCheck \
  -Dsibench.postgres.runtime=build/services/postgres/runtime \
  -Dsibench.sysbench.build=build/benchmarks/sysbench test
mvn -Dtest=RocksDbHistoryCheck,RocksYcsbCheck,RocksGenerationCheck \
  -Dsibench.rocksdb.library=$PWD/build/services/rocksdb/libsibench_rocksdb.so \
  -Dsibench.rocksdb.build=build/services/rocksdb \
  -Dsibench.ycsb.build=build/benchmarks/YCSB test
mvn -Dtest=DuckDbTraceCheck,DuckDbDataCheck \
  -Dsibench.duckdb.host=build/services/duckdb/duckdb_host test
mvn -Dtest=DuckDbGenerationCheck \
  -Dsibench.duckdb.host=build/services/duckdb/duckdb_host \
  -Dsibench.sysbench.build=build/benchmarks/sysbench \
  -Dsibench.hybench.build=build/benchmarks/HyBench-2024 test
```

The PostgreSQL checks use fresh local clusters and loopback JDBC connections. Component checks cover rollback and transaction-local visibility, committed before-images, delete/reinsert RowIDs, exclusion of internal history indexes, native scan cardinality before SQL filtering/aggregation, exact consumed prefixes and source/Trace completion matching. `PostgresCaptureCheck` pauses consumption, verifies bounded source waiting and drains all 512 native transactions. `PostgresVersionsCheck` grows an initially empty commit mapping to 100,000 rows and requires indexed prepared lookup while source planner settings remain unchanged. `PostgresProjectionCheck` independently replays late statement failure, local Abort reads and controlled RC/RR interleavings, then checks historical rows after moving the native data. `PostgresGenerationCheck` also runs the production request controller with a 600-request target and verifies its Trace independently. It runs all six sysbench workloads at 2 × 512 rows and 256 events each with primary-only and full-index exports, and directly publishes their Trace, independently replays it and reads relocated native data. These checks do not certify full-scale performance. Default tests reject truncated, reordered and unknown capture events and changed build identities.

`DuckDbTraceCheck` checks native capture publication, Abort, historical visibility, manifest rejection and portable official replay. A two-window case verifies committed updates, Abort, delete/reinsert and cross-window snapshot versions. Its independent small-data oracle reconstructs committed snapshots from native current rows, before-images and RowID histories, and rejects a deliberately incorrect Trace even when its file digests are valid. `DuckDbDataCheck` checks sealed native data after closing and moving it. `DuckDbGenerationCheck` runs all six sysbench workloads at two tables × 512 rows, with two workers. HyBench uses 8,706 rows, one worker per client group and one minute per AP/TP/XP phase. Hosts use 1 GiB/two threads; the test deadline is 600 seconds. Both use a 1,000 effective-request target. Both capture artifacts are relocated before native reads and official/reference replay. Set `sibench.generation.output` to a fresh directory under `results/capture/` to retain its evidence.

`RocksDbHistoryCheck` checks timestamp history through SST/Blob compaction and reopen, independent native file digests, rollback, delete/reinsert identity, native row locks, and immutable reads after relocation. `RocksYcsbCheck` preserves API byte/field semantics, lexical scans, separate read/modify/write calls, failed batches and Trace replay. `RocksGenerationCheck` uses the real YCSB A/B/C/F/D/E workloads at 1,000 baseline rows, 320-event batches and a 2,400 effective-request target, retaining 16 load and 32 workload workers. Its independent reference and official two-client replay run outside generation. It also verifies baseline fields through the relocated native reader. These checks do not certify full-scale time or memory.

`HybenchNativeCheck` requires `sibench.hybench.build` and `sibench.duckdb.host`. Its 8,706-row inputs exercise loading, dialect cases, individual business methods, failure propagation and actual AP/TP/XP phases. Run `mvn -Dtest=HybenchSamplingCheck -Dsibench.hybench.build=build/benchmarks/HyBench-2024 test` to check the patched upstream risk sampler with fewer candidates than its capacity and an empty population. `DuckDbAccessesCheck` distinguishes complete index output followed by downstream stopping from unread base batches or unread transaction-local writes. Its 32-client regression checks that parallel ART workers share a statement-local scan snapshot and cannot revisit replacement rows created by that same UPDATE.

For an existing small DuckDB Trace, the independent matrix check can reuse the artifact without recapturing:

```bash
java -cp build/sibench/test-classes:build/sibench/sibench.jar \
  org.sibench.replay.ReplayMatrixValidation TRACE FRESH_OUTPUT
```

It compares both dispatch modes with one/two clients and each supported secondary-index setting, then checks committed snapshots against native rows. It validates offline equivalence, not transport concurrency or performance. The row oracle keeps its input in memory and is intended for small acceptance; it does not require aborted row contents or certify full-scale capacity. Default Maven tests also compare encoded key ordering with DuckDB SQL ordering, including significant CHAR trailing spaces.

Independent small projection tests compare keys, RowIDs, versions and native observations. Normal service replay checks source result counts and completion; matching counts alone do not prove identical keys or ordering. Changing a backend can reuse immutable Trace; changing capture requires affected native source runs.

### Native component and capacity diagnostics

The native component checks require a C++17 compiler and, for DuckDB, the instrumented build from `scripts/build-duckdb.sh`. They exercise controlled inputs without benchmark generation:

```bash
python3 src/test/native/duckdb_index_attempts.py build/services/duckdb/duckdb
python3 src/test/native/duckdb_mutations.py build/services/duckdb/duckdb
c++ -std=c++17 -I src/main/native/sysbench -I third_party/duckdb/src/include \
  src/test/native/sysbench_errors.cc -o /tmp/sibench_sysbench_errors
/tmp/sibench_sysbench_errors
c++ -std=c++17 -pthread -I build/services/duckdb/source/src/include \
  src/test/native/duckdb_snapshot_attempts.cc -L build/services/duckdb/src -lduckdb \
  -Wl,-rpath,"$PWD/build/services/duckdb/src" -o /tmp/sibench_duckdb_snapshots
/tmp/sibench_duckdb_snapshots
```

After `mvn package`, the following explicit diagnostics use `java -cp build/sibench/test-classes:build/sibench/sibench.jar CLASS ARGS`. Choose fresh output paths and bound rows, records, repetitions and elapsed time before running them. They are excluded from the default unit suite and do not measure backend capacity.

| Class under `org.sibench.` | Arguments and scope |
| --- | --- |
| `capture.ProtoTraceBenchmark` | `create DIR ROWS TRANSACTIONS`, then `read DIR REPEATS`: synthetic codec fixture, one warm-up and the requested measured passes. Reports CPU, allocation and elapsed time for baseline and workload together; this is not a source-generated Trace or a per-phase maximum-throughput measurement. |
| `replay.DispatcherCapacityCheck` | `OPERATIONS REPORT`: generated unique baseline operations through the dispatcher, without RPC or storage. |
| `adapters.DriverCapacityCheck` | `plan TRACE RECORD_LIMIT REPORT`; `check-baseline TRACE REPORT`; `check-work-units TRACE REPORT`. Planning stops at a transaction outcome; the two checks read the entire selected input component. |
| `adapters.DriverCapacityCheck` | `record TRACE FRESH_FIXTURE RECORD_LIMIT`; `serve FIXTURE PORT`; `run FIXTURE ENDPOINT CLIENTS REPORT`. Records a complete transaction prefix using local RocksDB, then checks its recorded responses over gRPC. Setup loads the entire baseline and caps serialized fixture data at 1 GiB; also bound JVM heap and process duration. `serve-corrupt` deliberately emits incorrect results for rejection checks. |
| `adapters.BaselineCapacityCheck` | `serve PORT RECEIVER_REPORT`; `run TRACE ENDPOINT CLIENTS RECEIVER_REPORT REPORT SECONDS`. Sends baseline batches to a storage-free receiver and verifies independent field fingerprints; `serve-drop` deliberately drops a write. `series TRACE ENDPOINT RECEIVER_REPORT FRESH_OUTPUT SECONDS` repeats balanced client counts in one driver process. A zero time limit reads the entire baseline; `-Dsibench.capacity.members=N` can impose a member limit. |

Trace-based capacity checks consume exactly the declared exported index scope. Stop diagnostic server processes after the corresponding client run. Fixture recording and baseline setup can be expensive even when the measured request window is short; partial reads do not establish complete input integrity. Reuse of prior integrity evidence requires unchanged input identities.

### Full-scale acceptance

Full-scale generation, complete backend evaluation and fault diagnostics are separately triggered workflows. Public profiles require all their configured workloads and a complete Trace; small inputs or shortened workloads do not establish capacity. A full backend acceptance additionally requires complete local and real-cloud replay of the same Trace, response/count checks and Flush. No cloud credentials or remote execution are needed for normal generation.

For an explicit full-data audit of a DuckDB Trace, run `org.sibench.replay.FullSnapshotValidation TRACE FRESH_OUTPUT` on the test classpath. It streams native histories one primary key at a time, uses external reference state and sorting, compares every exported baseline/final primary and secondary entry, and checks changed keys before/at each commit. It also replays all Trace queries in the reference. This is separate from generation and reads the full input; budget its memory, temporary disk and duration explicitly. `sibench.reference.baseline_entries`, `sibench.reference.overlay_entries` and `sibench.reference.sort_bytes` bound reference buffers; `sibench.oracle.duckdb_memory` sets the native audit budget. One hot key's history still needs memory. Aborted full row contents and arbitrary additional range-order probes remain outside this audit; small range-oracle checks remain independent.

Bind results to effective configurations, build/input identities and same-run correctness, time, memory and disk measurements. The test-classpath `org.sibench.adapters.IndexServerAcceptance SERVICE_CONFIG` starts the actual gRPC service; sending `close` on stdin closes it and returns `CLOSED`.

`IndexVersionCheck SERVICE_CONFIG REPORT` (class `org.sibench.adapters.IndexVersionCheck` in test classes) explicitly validates a fresh configured backend using hand-derived historical key/version/RowID results, deletion, reinsertion and Abort, before and after Flush. Its validator rejects same-count wrong-version results. Use a COS server configuration to exercise the actual cloud backend; this bounded check does not certify every result identity in a full Trace replay.

`org.sibench.capture.ProtoTraceBenchmark read-trace TRACE REPEATS REPORT` measures full baseline and workload parsing separately, including digest verification. Each phase gets one unmeasured warm pass followed by 1–10 measured passes, with effective requests and expanded records reported separately. This is observed single-parser throughput, not a global maximum or an Index Service capacity claim.

Cloud JNI unit measurements use `make -C src/main/native/index/rocksdb_cloud test` after that backend's build. Reopening, fault injection, cold recovery and power-loss durability are separate diagnostics, not claims established by the small generation checks.

## Supported scope

See [Source scope](docs/trace.md#source-scope) and [Access projection](docs/trace.md#access-projection). Primary-key updates, unsupported access shapes and ambiguous intra-statement ordering fail explicitly. Complete index output is preserved even when downstream SQL stops; incomplete source output is rejected rather than approximated from SQL LIMIT. Large-input acceptance remains unverified.
