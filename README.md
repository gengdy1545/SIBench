# SIBench

SIBench runs upstream benchmarks on native databases, exports a unified index Trace, and replays it against an Index Service. It measures index workloads, not end-to-end SQL performance or original client arrival timing. See the [Trace contract](docs/trace.md) and [Index Service contract](docs/index-service.md).

| Source | Mode / engine | Workload |
| --- | --- | --- |
| YCSB | `api` / RocksDB | A/B/C/F/D/E; Read-modify-write remains two API calls. |
| sysbench | Default `capture` / PostgreSQL; optional DuckDB | Six configured OLTP workloads. |
| HyBench | Default `cdc` / Pixels; optional native `capture` / PostgreSQL or DuckDB | CDC uses the HyBench TP/AT subset with AP/IQ disabled. Native capture retains AP → TP → concurrent XP. CDC exports Pixels' primary-index calls while applying changes from an uninstrumented PostgreSQL. |

## Build

Use JDK 17, Maven and Git; set `JAVA_HOME`. Run from the repository root. Upstream sources are pinned by submodule gitlinks; Java dependencies belong to `pom.xml`.

| Component | Additional prerequisites |
| --- | --- |
| RocksDB | Linux/glibc x86-64, Python 3.9+, CMake 3.11.4+, make, patch, C++17; LZ4, Zstd, zlib, Snappy and bzip2 development libraries. Execution hosts need compatible C++ and compression libraries. |
| DuckDB | Linux/glibc x86-64, Python 3.9+, C++17, CMake 3.16+, make and patch. |
| PostgreSQL | C compiler, make, patch, Perl, Bison, Flex and libzstd development headers; run PostgreSQL as an unprivileged user. |
| Benchmark clients | HyBench/YCSB use Maven; sysbench also needs autotools, pkg-config, libtool, libaio and libpq development libraries. |
| Pixels | Java phase: Maven with network access to Maven Central, and CMake/C++ for the Retina library built by Pixels' Maven build. Native phase, on the operating system that runs Pixels: CMake, C/C++, make, the PostgreSQL build tools above, curl and network access to GitHub (jemalloc, googletest and the pinned etcd release). |

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

Build the Pixels CDC engine in two phases; the native phase may run on another host after copying the directory:

```bash
scripts/build-pixels.sh java build/services/pixels
scripts/build-pixels.sh native build/services/pixels
```

Runtime engine paths come from each engine configuration's `build_directory`; clients build under `build/benchmarks/`. Builds use pinned inputs and fresh destinations, leave upstream checkouts untouched, retain diagnostics on failure, and record input/artifact identities checked by generation. `JOBS` controls parallel compilation. Upstream licenses remain in [RocksDB](third_party/rocksdb/COPYING), [DuckDB](third_party/duckdb/LICENSE), [DuckDB Java](third_party/duckdb-java/LICENSE), [PostgreSQL](third_party/postgresql/COPYRIGHT), [Pixels](third_party/pixels/LICENSE) and [pixels-sink](third_party/pixels-sink/LICENSE); both embedded runtime JARs include their license texts.

### rocksdb-cloud JNI

Only the cloud backend needs this build. It requires clean pinned `index/rocksdb-cloud` and `third_party/aws-sdk-cpp` submodules, patch, C/C++, CMake, make and JDK 17. The script targets yum-based systems and installs development dependencies as root; use a build host ABI-compatible with execution hosts.

```bash
scripts/build-rocksdb-cloud.sh
```

Outputs include `build/index/librocksdb_cloud_jni.so` and `build/index/rocksdb-cloud-build.json`, binding pinned sources, project inputs, compiler and artifacts. Existing build outputs are rejected. Override with `-Dsibench.rocksdb_cloud.library=PATH`. Licenses: [rocksdb-cloud](index/rocksdb-cloud/COPYING), [AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt).

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
| [sibench.yaml](config/sibench.yaml) | Explicit benchmark/mode/engine selection, the engine configuration files, replay and analysis. |
| [postgres.yaml](config/engine/postgres.yaml), [duckdb.yaml](config/engine/duckdb.yaml), [rocksdb.yaml](config/engine/rocksdb.yaml), [pixels/](config/engine/pixels/pixels.yaml) | Engine build directory and resources. PostgreSQL server settings and Pixels/pixels-sink resource overrides of the pinned upstream defaults live here; sink row filters, rewrites and unknown overrides are rejected; the generator assigns endpoints, run directories and settings that define Trace semantics, and rejects them in these files. |
| [ycsb.yaml](config/benchmark/ycsb.yaml), [sysbench.yaml](config/benchmark/sysbench.yaml), [hybench.yaml](config/benchmark/hybench.yaml) | Source, schema, scale, workloads, concurrency, timeout, build/output paths and Trace export. |
| [rocksdb_cloud.yaml](config/index/rocksdb_cloud.yaml), [slatedb.yaml](config/index/slatedb.yaml), [storage.yaml](config/index/storage.yaml) | Index Service and object storage. |

Public profiles use **87,060,000 baseline rows** and **10,000,000 effective workload requests** each: these are substantial runs. For shorter runs, use separate configurations with adjusted source scale/workloads and fresh output paths. HyBench `sf` selects the upstream parameter table; `native.parameters` can override positive `customer_number`, `company_number`, `transfer_number`, `checking_number`, `loanapps_number` and `loantrans_number`.

```bash
java -jar build/sibench/sibench.jar inspect-scale
java -jar build/sibench/sibench.jar generate config/sibench.yaml
```

`generate` validates every selected input before starting a host, ignores unselected profiles, runs selections in order and stops at the first failure. Native API/capture uses one fresh database per source. Pixels CDC uses a fresh source PostgreSQL and a fresh Pixels database loaded independently from the same inputs. Working databases are deleted after publication. Existing/overlapping outputs, changed build/source inputs and unsupported configurations are rejected. Successful publication requires normal shutdown; failures retain diagnostics and cannot resume.

`trace.workload_requests` counts each exported write and SearchGroup member (or standalone Search) after committed-state coalescing and index filtering; baseline and source transaction markers are excluded. Generation repeats finite source batches on the same database, distributes remaining requests across remaining workloads, and drains admitted events/transactions. HyBench native capture runs AP once before allocating the remainder to TP/XP; Pixels CDC shares the budget between TP and AT, with AP/IQ disabled. Actual output may exceed the target by source activity already captured but not yet exported when the target is reached, especially by a whole DuckDB window or a PostgreSQL capture backlog; no-progress batches fail. Omitting the target runs each configured batch once. `request-budget.json` records actual counts and overshoot.

`trace.secondary_indexes` defaults to false: only primary-index requests are exported, while source secondary indexes remain in use. Set true for all source indexes in native capture; Pixels CDC rejects secondary-index export. Consumption has a separate `target.secondary_indexes.enabled` switch and cannot request indexes absent from the Trace. [Filtering and expansion rules](docs/trace.md#operations-and-index-expansion) preserve whole query groups and transaction outcomes.

Published artifacts contain separate baseline/workload streams, work units and a manifest with identities and statistics; they contain no native database or row contents. An `INCOMPLETE` marker prevents consumption. See [publication](docs/trace.md#native-publication). SQL and RocksDB generation export RowID values; Pixels CDC exports both RowID and Locator values.

The public runtime generates HyBench with `cdc` / Pixels, using a [TP/AT transaction subset](docs/trace.md#native-publication) rather than full HyBench HTAP; select `capture` with `postgres` or `duckdb` in a runtime copy for the native-capture Trace. Pixels CDC runs Pixels next to the generator when `pixels_host` is `local`. To run Pixels on another host, start the agent there with the same build and a token file, set `pixels_host: HOST:PORT` in an experiment copy of the engine configuration, and export `SIBENCH_PIXELS_HOST_TOKEN` for `generate`; the source PostgreSQL, HyBench and pixels-sink stay with the generator:

```bash
java -jar build/sibench/sibench.jar pixels-host ADDRESS:PORT build/services/pixels WORK_ROOT TOKEN_FILE
```

The generator checks the agent's Java build and native source/capacity identity before loading, and records the agent's actual build in the Trace. Host requests have deadlines covering uploads and responses. Each uploaded shard is limited to 4 GiB; failed uploads remove only their own partial file and terminate the session. Commands redact tracer tokens. Deletion requires a run directory created and stopped by that session. PostgreSQL and Pixels load independently from the same generated CSVs. The generator converts Pixels inputs into bounded text shards, using `load.rows_per_file` for the shard size and `load.threads` for parallel file consumers. Workloads start after both baselines and per-table counts pass. `source-load.json`, `baseline-load.json`, `pixels-schema.tsv` and `progress.jsonl` record the effective schema, overlapping phases and capture progress.

Optional resource sampling: `scripts/sample-pixels-resources.sh OUT STOP_FILE [INTERVAL_SECONDS]`, with `WAL_DIR` set to the source WAL directory. Create `STOP_FILE` to stop. `pod_anon_metric` distinguishes cgroup v2 `anon` from v1 `total_rss`; unavailable readings are `null`. CLI `--timing` also records unavailable process/directory measurements as `null`; sampled disk peaks include only successful observations. Per-process RSS sums count shared pages repeatedly and are not exclusive memory usage.

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

The data API is Get/Put/Delete/Scan/BatchWrite. Replay batches baseline loading and sends each incremental operation as a separate concurrent RPC. Services own asynchronous persistence and expose no Flush RPC. Replay checks input integrity, deployment/value mode and binding, response completion and native result counts. Equal counts do not certify matching keys, versions, values or order. The same immutable Trace can be reused across backends and client settings.

`check-replay CONFIG TRACE` is an optional preflight-only diagnostic; replay already performs these checks. `plan-operations PROFILE TRACE REPLAY_CONFIG` estimates source batch parameters from a calibration Trace, not achievable throughput. Request supply, dispatch warmup and operation rates count the same basic operations; see [replay preflight](docs/trace.md#replay-preflight). To check persisted snapshots explicitly, use `service.reopen: true` and `verify-service`; [only committed-snapshot queries are exported](docs/index-service.md#internal-persistence-and-reopening).

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

Use `-Dsibench.build.directory=ABSOLUTE_PATH` for an isolated Maven build/test/package output; each concurrent invocation needs its own directory. Child-process checks use the invoking test's classpath. Native `*Check` and `*IT` classes are explicit opt-ins, not part of the default suite. Use `mvn -Dtest=CLASS_LIST -DPROPERTY=PATH ... test`:

| Checks | Required builds / properties |
| --- | --- |
| `PostgresHistoryCheck,PostgresAccessesCheck,PostgresCaptureCheck,PostgresVersionsCheck,PostgresProjectionCheck,PostgresGenerationCheck` | `sibench.postgres.runtime=build/services/postgres/runtime`; generation also needs `sibench.sysbench.build=build/benchmarks/sysbench`. |
| `RocksDbHistoryCheck,RocksYcsbCheck,RocksGenerationCheck` | `sibench.rocksdb.library=$PWD/build/services/rocksdb/libsibench_rocksdb.so`; generation also needs `sibench.rocksdb.build=build/services/rocksdb` and `sibench.ycsb.build=build/benchmarks/YCSB`. |
| `DuckDbTraceCheck,DuckDbAccessesCheck,DuckDbGenerationCheck,HybenchNativeCheck` | `sibench.duckdb.host=build/services/duckdb/duckdb_host`; source integration also needs `sibench.sysbench.build` and/or `sibench.hybench.build`. `HybenchSamplingCheck` needs only the HyBench build. |
| `PixelsGenerationCheck` | `sibench.pixels.build=build/services/pixels` (both build phases) and `sibench.hybench.build=build/benchmarks/HyBench-2024`; runs Pixels locally with small HyBench parameters and replays both value kinds. |
| `PixelsCdcCheck` | `sibench.pixels.build=build/services/pixels`; eight hand-written baseline rows, real PostgreSQL → sink → Pixels update/delete/reinsert/rollback, historical RowID/Locator checks and an injected LOAD visibility-publication failure. |
| `PixelsSourceCheck` | `sibench.postgres.runtime=build/services/pixels/postgres/runtime`; source-only PostgreSQL checks for key-change rejection, unchanged-key updates without guard function calls, rollback, preceding triggers and preservation of the original indexes. |
| `SlateDbStoreCheck` | Source-built `sibench.slatedb.library`; `sibench.slatedb.storage=PATH`, `sibench.slatedb.profile=NAME`, `sibench.slatedb.object_path=FRESH_PREFIX`, and that profile's environment credentials. Uses one Db against the configured S3 store; no independent reader. |

Native component checks require the indicated built engine and Python 3, C++17 or JDK 17:

| Entry point in [src/test/native](src/test/native) | Invocation / compile inputs |
| --- | --- |
| `duckdb_index_attempts.py`, `duckdb_mutations.py` | `python3 SCRIPT build/services/duckdb/duckdb`. |
| `sysbench_errors.cc` | Compile with `-I src/main/native/sysbench -I third_party/duckdb/src/include`, then run the binary. |
| `duckdb_snapshot_attempts.cc` | Compile with `-pthread -I build/services/duckdb/source/src/include -L build/services/duckdb/src -lduckdb`; make that library directory available at runtime. |
| `pixels/DecoderChainProbe.java` | Compile with `javac --release 17 -cp BUILD/jars/pixels-sink.jar -d TEST_CLASSES src/test/native/pixels/DecoderChainProbe.java`, then run `java -cp TEST_CLASSES:BUILD/jars/pixels-sink.jar org.sibench.pixels.DecoderChainProbe`. Checks FIFO completion of 8,192 same-stream events without starting services. |
| `pixels/SourceTransactionsProbe.java` | Compile and run with the same sink classpath as DecoderChainProbe. Hand-built BEGIN/ROW/END records check transaction identity, global/table order, exact totals and malformed-control rejection without starting services. |
| `pixels/SourceFailureProbe.java` | Run in a separate JVM with the sink classpath, a sink properties file and `sibench.report.endpoint`/`sibench.report.token`. A malformed update must report failure and exit 70 before offset acknowledgement; Optional `row-failure`, `row-null`, `transaction-failure` and `transaction-null` arguments inject invalid decoded results and require a failure report/exit 70; no database is started. |

Cloud JNI checks use `make -C src/main/native/index/rocksdb_cloud test` after that backend's build.

After `mvn package`, explicit validation tools run with `java -cp build/sibench/test-classes:build/sibench/sibench.jar CLASS ARGS`:

| Tool under `org.sibench.` | Entry point / cost boundary |
| --- | --- |
| `replay.ReplayMatrixValidation` | `TRACE FRESH_OUTPUT`: dispatch/index-selection matrix against the in-memory reference index. |
| `adapters.IndexVersionCheck` | `SERVICE_CONFIG REPORT`: bounded hand-derived version/delete/reinsert checks on a fresh local or cloud backend. |
| `adapters.IndexServerAcceptance` | `SERVICE_CONFIG`: starts a service; stdin `close` shuts it down. |
| `adapters.StreamingLatencyCheck` | `ENDPOINT REPORT`: 64 baseline entries and 600 ordinary workload operations on a fresh service; verifies individual RPC completion and count/latency accounting with batched baseline loading. This is a small integration check, not a full-input or performance certification. |
| [ProtoTraceBenchmark](src/test/java/org/sibench/capture/ProtoTraceBenchmark.java), [DispatcherCapacityCheck](src/test/java/org/sibench/replay/DispatcherCapacityCheck.java), [DriverCapacityCheck](src/test/java/org/sibench/adapters/DriverCapacityCheck.java), [BaselineCapacityCheck](src/test/java/org/sibench/adapters/BaselineCapacityCheck.java) | Explicit parser/driver diagnostics; invocation forms are in each class's usage checks. Bound input, duration, heap and output before running. |

Production generation, backend evaluation and independent acceptance are separate workflows. Native checks cover small inputs; they do not certify full-scale capacity. Full backend acceptance requires complete local and real-cloud replay of the same Trace with response/count checks and an independent check of internal persistence/reopening. Fault recovery and performance studies require separate runs and budgets.

## Supported scope

Only the pinned sources and supported schemas/access shapes are accepted; see [source scope](docs/trace.md#source-scope) and [access projection](docs/trace.md#access-projection). Primary-key updates, ambiguous ordering and incomplete index output fail explicitly. Rewrite, physical version GC and automatic retries of writes with unknown outcomes are unsupported.
