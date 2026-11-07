# SIBench

SIBench runs upstream benchmarks on native databases, exports a unified index Trace, and replays it against an Index Service. It measures index workloads, not end-to-end SQL performance or original client arrival timing. See the [Trace contract](docs/trace.md) and [Index Service contract](docs/index-service.md).

| Source | Mode / engine | Workload |
| --- | --- | --- |
| YCSB | `api` / RocksDB | A/B/C/F/D/E; Read-modify-write remains two API calls. |
| sysbench | `capture` / PostgreSQL or DuckDB | Six configured OLTP workloads. |
| HyBench | `capture` / DuckDB | AP → TP → concurrent XP. |

## Build

Use JDK 17, Maven and Git; set `JAVA_HOME`. Run from the repository root. Upstream sources are pinned by submodule gitlinks; Java dependencies belong to `pom.xml`.

| Component | Additional prerequisites |
| --- | --- |
| RocksDB | Linux/glibc x86-64, Python 3.9+, CMake 3.11.4+, make, patch, C++17, pkg-config; OpenSSL headers/static libcrypto; LZ4, Zstd, zlib, Snappy and bzip2 development libraries. Execution hosts need compatible C++ and compression libraries. |
| DuckDB | Linux/glibc x86-64, Python 3.9+, C++17, CMake 3.16+, make and patch. |
| PostgreSQL | C compiler, make, patch, Perl, Bison, Flex and libzstd development headers; run PostgreSQL as an unprivileged user. |
| Benchmark clients | HyBench/YCSB use Maven; sysbench also needs autotools, pkg-config, libtool, libaio and libpq development libraries. |

Build the embedded native runtimes before the Java application:

```bash
git submodule update --init --recursive
bash scripts/build-rocksdb.sh build/services/rocksdb
bash scripts/build-duckdb.sh build/services/duckdb
mvn package
java -jar build/sibench/sibench.jar --help
```

The application JAR includes source-built RocksJava and DuckDB JDBC runtimes. Their loaders require the packaged native resources. DuckDB JDBC uses the same patched core sources and static archives as the capture host. For different fresh build directories, pass absolute paths with `-Drocksdb.build.directory=PATH` and `-Dduckdb.build.directory=PATH` to Maven, and select the corresponding directories in `config/sibench.yaml`.

Build the engines and clients needed by the selected sources:

```bash
scripts/build-postgres.sh config/sibench.yaml
scripts/build-benchmarks.sh config/benchmark/ycsb.yaml
scripts/build-benchmarks.sh config/benchmark/sysbench.yaml
scripts/build-benchmarks.sh config/benchmark/hybench.yaml
```

Runtime engine paths come from `ENGINE.build_directory`; clients build under `build/benchmarks/`. Builds require clean pinned sources and fresh destinations, retain diagnostics on failure, and record input/artifact identities checked by generation. `JOBS` controls parallel compilation. Upstream licenses remain in [RocksDB](third_party/rocksdb/COPYING), [DuckDB](third_party/duckdb/LICENSE), [DuckDB Java](third_party/duckdb-java/LICENSE) and [PostgreSQL](third_party/postgresql/COPYRIGHT); both embedded runtime JARs include their license texts.

### rocksdb-cloud JNI

Only the cloud backend needs this build. It requires clean pinned `index/rocksdb-cloud` and `third_party/aws-sdk-cpp` submodules, patch, C/C++, CMake, make and JDK 17. The script targets yum-based systems and installs development dependencies as root; use a build host ABI-compatible with execution hosts.

```bash
scripts/build-rocksdb-cloud.sh
```

Output: `build/index/librocksdb_cloud_jni.so`; override with `-Dsibench.rocksdb_cloud.library=PATH`. Licenses: [rocksdb-cloud](index/rocksdb-cloud/COPYING), [AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt).

### SlateDB JNI

Requires Linux x86-64, Python 3, a C/C++ linker, `rustup` with the toolchain pinned by `index/slatedb/rust-toolchain.toml`, and the clean SlateDB submodule at its gitlink. Cargo uses the adapter's locked dependencies. Build on an ABI-compatible host:

```bash
JOBS=4 scripts/build-slatedb.sh build/index/slatedb-build
java -Dsibench.slatedb.library=build/index/slatedb-build/libslatedb_jni.so \
  -jar build/sibench/sibench.jar index-server config/index/slatedb.yaml
```

The destination must be fresh. It retains source/build identities, native test results through the build exit status, and the JNI library. License: [SlateDB](index/slatedb/LICENSE).

## Native generation

| Configuration | Responsibility |
| --- | --- |
| [sibench.yaml](config/sibench.yaml) | Explicit benchmark/mode/engine selection, native engine resources, replay and analysis. |
| [ycsb.yaml](config/benchmark/ycsb.yaml), [sysbench.yaml](config/benchmark/sysbench.yaml), [hybench.yaml](config/benchmark/hybench.yaml) | Source, schema, scale, workloads, concurrency, timeout, build/output paths and Trace export. |
| [rocksdb_cloud.yaml](config/index/rocksdb_cloud.yaml), [slatedb.yaml](config/index/slatedb.yaml), [storage.yaml](config/index/storage.yaml) | Index Service and object storage. |

Public profiles use **87,060,000 baseline rows** and **10,000,000 effective workload requests** each: these are substantial runs. For shorter runs, use separate configurations with adjusted source scale/workloads and fresh output paths. HyBench `sf` selects the upstream parameter table; `native.parameters` can override positive `customer_number`, `company_number`, `transfer_number`, `checking_number`, `loanapps_number` and `loantrans_number`.

```bash
java -jar build/sibench/sibench.jar inspect-scale
java -jar build/sibench/sibench.jar generate config/sibench.yaml
```

`generate` validates every selected input before starting a host, ignores unselected profiles, runs selections in order and stops at the first failure. Each source uses one fresh database for loading, history and workload execution. Existing/overlapping outputs, changed build/source inputs and unsupported configurations are rejected. Successful publication requires normal shutdown; failures retain diagnostics and cannot resume.

`trace.workload_requests` counts each exported write and SearchGroup member (or standalone Search) after committed-state coalescing and index filtering; baseline and source transaction markers are excluded. Generation repeats finite source batches on the same database, distributes remaining requests across remaining workloads, and drains admitted events/transactions. HyBench runs AP once before allocating the remainder to TP/XP. Actual output may exceed the target, especially by a whole DuckDB window; no-progress batches fail. Omitting the target runs each configured batch once. `request-budget.json` records actual counts and overshoot.

`trace.secondary_indexes` defaults to false: only primary-index requests are exported, while source secondary indexes remain in use. Set true for all source indexes. Consumption has a separate `target.secondary_indexes.enabled` switch and cannot request indexes absent from the Trace. [Filtering and expansion rules](docs/trace.md#operations-and-index-expansion) preserve whole query groups and transaction outcomes.

Published artifacts contain separate baseline/workload streams, work units, sealed native data and a manifest with identities and statistics; an `INCOMPLETE` marker prevents consumption. Native data remain available for historical row reads after relocation. See [publication](docs/trace.md#native-publication) and [row consumption](docs/trace.md#native-row-consumption-component). Current native output supports RowID values only.

Engine memory settings are not total process-memory caps; client `timeout_minutes` and separate setup/shutdown bounds are not whole-run resource quotas. Generation provides no crash recovery or reusable initial-state cache. Large-input capacity and performance require separate validation.

## Replay and service

Start a service, then replay a published Trace from another terminal:

```bash
java -jar build/sibench/sibench.jar index-server config/index/rocksdb_cloud.yaml
```

```bash
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  results/capture/ycsb/trace results/service.json
```

The runtime template selects gRPC, RowID values and explicit client/in-flight limits. Both ends must agree on value mode and secondary-index filtering. The comparison backends are RocksDB-Cloud and SlateDB; select their configuration under `config/index/`. Replace storage placeholders and supply credentials through the environment. Each run needs a fresh local path and object prefix; buckets must already exist. Local RocksDB remains an in-process test fixture and the YCSB generation engine, outside the Index Service comparison.

Replay checks input integrity, service capabilities, response completion, native result counts and Flush completion and binding. Equal counts do not certify matching keys, versions, values or order. The same immutable Trace can be reused across backends and client settings.

`check-replay CONFIG TRACE` is an optional preflight-only diagnostic; replay already performs these checks. `plan-operations PROFILE TRACE REPLAY_CONFIG` estimates source batch parameters from a calibration Trace, not achievable throughput. Request supply, dispatch warmup and operation rates count the same basic operations; see [replay preflight](docs/trace.md#replay-preflight). To check persisted snapshots explicitly, use `service.reopen: true` and `verify-service`; [only committed-snapshot queries are exported](docs/index-service.md#persistence-and-reopening).

## Analysis

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/capture/ycsb/trace config/sibench.yaml results/locality
python3 scripts/plot_update_locality.py results/locality --out results/locality/plots
```

Locality windows count final primary-entry changes per committed version, classified by the surviving source operation; intermediate row updates are not counted. Plotting requires Python 3, pandas and matplotlib. Static layout overlap does not measure actual SST/compaction cost. `inspect-scale` reports configured native source counts without generating data.

## Tests

After building both embedded native runtimes, run the default checks; Python requires 3.9+, and socket tests require local port access:

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
```

Native `*Check` and `*IT` classes are explicit opt-ins, not part of the default suite. Use `mvn -Dtest=CLASS_LIST -DPROPERTY=PATH ... test`:

| Checks | Required builds / properties |
| --- | --- |
| `PostgresHistoryCheck,PostgresAccessesCheck,PostgresCaptureCheck,PostgresVersionsCheck,PostgresProjectionCheck,PostgresGenerationCheck` | `sibench.postgres.runtime=build/services/postgres/runtime`; generation also needs `sibench.sysbench.build=build/benchmarks/sysbench`. |
| `RocksDbHistoryCheck,RocksYcsbCheck,RocksGenerationCheck` | `sibench.rocksdb.library=$PWD/build/services/rocksdb/libsibench_rocksdb.so`; generation also needs `sibench.rocksdb.build=build/services/rocksdb` and `sibench.ycsb.build=build/benchmarks/YCSB`. |
| `DuckDbTraceCheck,DuckDbDataCheck,DuckDbAccessesCheck,DuckDbGenerationCheck,HybenchNativeCheck` | `sibench.duckdb.host=build/services/duckdb/duckdb_host`; source integration also needs `sibench.sysbench.build` and/or `sibench.hybench.build`. `HybenchSamplingCheck` needs only the HyBench build. |
| `SlateDbStoreCheck` | Source-built `sibench.slatedb.library`; `sibench.slatedb.storage=PATH`, `sibench.slatedb.profile=NAME`, `sibench.slatedb.object_path=FRESH_PREFIX`, and that profile's environment credentials. Uses one Db against the configured S3 store; no independent reader. |

Native component checks require Python 3 or C++17 and the built DuckDB engine:

| Entry point in [src/test/native](src/test/native) | Invocation / compile inputs |
| --- | --- |
| `duckdb_index_attempts.py`, `duckdb_mutations.py` | `python3 SCRIPT build/services/duckdb/duckdb`. |
| `sysbench_errors.cc` | Compile with `-I src/main/native/sysbench -I third_party/duckdb/src/include`, then run the binary. |
| `duckdb_snapshot_attempts.cc` | Compile with `-pthread -I build/services/duckdb/source/src/include -L build/services/duckdb/src -lduckdb`; make that library directory available at runtime. |

Cloud JNI checks use `make -C src/main/native/index/rocksdb_cloud test` after that backend's build.

After `mvn package`, explicit validation tools run with `java -cp build/sibench/test-classes:build/sibench/sibench.jar CLASS ARGS`:

| Tool under `org.sibench.` | Entry point / cost boundary |
| --- | --- |
| `replay.ReplayMatrixValidation` | `TRACE FRESH_OUTPUT`: small DuckDB Trace, dispatch/index-selection matrix and an in-memory row oracle. |
| `replay.FullSnapshotValidation` | `TRACE FRESH_OUTPUT`: independent DuckDB full-data/history audit; reads the full input and uses temporary disk. |
| `adapters.IndexVersionCheck` | `SERVICE_CONFIG REPORT`: bounded hand-derived version/delete/reinsert checks on a fresh local or cloud backend. |
| `adapters.IndexServerAcceptance` | `SERVICE_CONFIG`: starts a service; stdin `close` shuts it down. |
| `adapters.StreamingLatencyCheck` | `ENDPOINT REPORT`: 64 baseline entries and 600 ordinary workload operations on a fresh service; verifies streaming completion, count/latency accounting and phase Flush. This is a small integration check, not a full-input or performance certification. |
| [ProtoTraceBenchmark](src/test/java/org/sibench/capture/ProtoTraceBenchmark.java), [DispatcherCapacityCheck](src/test/java/org/sibench/replay/DispatcherCapacityCheck.java), [DriverCapacityCheck](src/test/java/org/sibench/adapters/DriverCapacityCheck.java), [BaselineCapacityCheck](src/test/java/org/sibench/adapters/BaselineCapacityCheck.java) | Explicit parser/driver diagnostics; invocation forms are in each class's usage checks. Bound input, duration, heap and output before running. |

Production generation, backend evaluation and independent acceptance are separate workflows. Native checks cover small inputs; they do not certify full-scale capacity. Full backend acceptance requires complete local and real-cloud replay of the same Trace with response/count checks and Flush. Fault recovery and performance studies require separate runs and budgets.

## Supported scope

Only the pinned sources and supported schemas/access shapes are accepted; see [source scope](docs/trace.md#source-scope) and [access projection](docs/trace.md#access-projection). Primary-key updates, ambiguous ordering and incomplete index output fail explicitly. Rewrite, physical version GC and automatic retries of writes with unknown outcomes are unsupported.
