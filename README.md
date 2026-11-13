# SIBench

SIBench captures native benchmark workloads for isolated Index Service replay and runs Pixels experiments from reusable columnar baselines and optional CDC files. Pixels experiments independently select Index Trace export, Trino queries and sink freshness. See the [Trace](docs/trace.md), [Index Service](docs/index-service.md) and [Pixels experiment](docs/pixels-experiment.md) contracts.

| Source | Mode / engine | Workload |
| --- | --- | --- |
| YCSB | `api` / RocksDB | A/B/C/F/D/E; Read-modify-write remains two API calls. |
| sysbench | Default `capture` / PostgreSQL; optional DuckDB | Six configured OLTP workloads. |
| HyBench | Default `capture` / PostgreSQL; optional DuckDB | Native capture retains AP → TP → concurrent XP. The separate `generate-pixels` command applies PostgreSQL TP/AT changes to Pixels and captures its primary-index calls, with AP/IQ disabled. |

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

For optional queries, build the pinned [Pixels Trino connector](third_party/pixels-trino/README.md) with `TRINO_JAVA_HOME=/path/to/jdk23 bash scripts/build-trino.sh build/services/pixels-trino`. Pixels dependencies build with JDK 17; the connector uses JDK 23 and its POM selects the Trino release. [Install a checked distribution and configure its resources](docs/pixels-experiment.md#trino-deployment) separately. License: [pixels-trino](third_party/pixels-trino/LICENSE).

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

### Relive prototype

The [Relive submodule](index/relive/README.md) requires JDK 17, Maven and a Linux
filesystem with file locking, atomic rename and file/directory fsync. Build and
run its local protocol tests separately:

```bash
mvn -f index/relive/pom.xml -Drelive.build.directory="$PWD/build/index/relive" package
```

Its local HTTP versioned-KV protocol is separate from SIBench's gRPC service.
Source registration does not enroll Relive in backend comparisons; this branch
has no Relive replay adapter. See its README for the bounded standalone POC.

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

`generate` runs only the native API/capture selections, using one fresh database per source. It validates every selected input before starting a host, ignores unselected engine builds and Pixels experiment settings, runs selections in order and stops at the first failure. The default HyBench selection uses PostgreSQL; engines never fall back automatically. Working databases are deleted after publication. Existing/overlapping outputs, changed build/source inputs and unsupported configurations are rejected. Successful publication requires normal shutdown; failures retain diagnostics and cannot resume.

For a live PostgreSQL → sink → Pixels Trace, use `java -jar build/sibench/sibench.jar generate-pixels config/sibench.yaml`. This command uses the selected HyBench profile and `engines.pixels`, independently of the native engine selection. It starts a separate uninstrumented source PostgreSQL and fresh Pixels state loaded from the same CSVs. It does not invoke native PG capture. Choose a fresh `artifacts.directory` for each run; native and Pixels outputs cannot share an existing directory.

`trace.export_queries` defaults to false, filtering identified independent queries while retaining write-path and unclassified reads. `trace.workload_requests` sets the minimum exported workload operations under either query setting; it excludes the baseline and may be exceeded by already admitted source work. `trace.secondary_indexes` defaults to false. See [generation and publication](docs/trace.md#native-publication) for filtering coverage, source phases, request accounting, index selection and failure behavior.

For repeated Pixels runs, configure [Pixels COS storage](docs/pixels-experiment.md#prepare-once-restore-for-each-run), prepare reusable inputs, then run a fresh experiment:

```bash
java -jar build/sibench/sibench.jar prepare-csv config/sibench.yaml results/capture/hybench-csv
java -jar build/sibench/sibench.jar check-csv-input results/capture/hybench-csv
java -jar build/sibench/sibench.jar prepare-pixels-inputs config/sibench.yaml \
  results/capture/hybench-csv results/inputs/hybench-pixels
java -jar build/sibench/sibench.jar check-cdc-input results/capture/hybench
java -jar build/sibench/sibench.jar pixels-experiment config/sibench.yaml \
  results/capture/hybench results/inputs/hybench-pixels results/pixels/run
```

To compare Trino answers on the sealed baseline with an independent PostgreSQL, run `render-queries config/sibench.yaml results/capture/hybench-csv results/capture/hybench-queries SEED INSTANCES` and use its output as `queries.directory` of a static experiment.

`prepare-csv` generates and seals the baseline without a database. Parallel preparation captures PostgreSQL TP/AT changes and creates a Pixels columnar baseline from that same CSV input. Experiments import the existing Pixels files and rebuild indexes and visibility before applying CDC. Updates default on; Trace, queries and freshness default off. With `updates: false`, omit the CDC argument; static experiments need only the Pixels baseline and do not read CSV or CDC files. All output directories must be fresh. See the [input contract](docs/cdc-input.md) for publication and the [experiment contract](docs/pixels-experiment.md) for restore and measurement semantics.

Pixels runs locally by default. For a remote Pixels host, start the agent there with the matching build and a token file, set `pixels_host: HOST:PORT` in the engine configuration, and supply `SIBENCH_PIXELS_HOST_TOKEN` to the client:

```bash
java -jar build/sibench/sibench.jar pixels-host ADDRESS:PORT build/services/pixels WORK_ROOT TOKEN_FILE
```

The source PostgreSQL and sink remain on the generator during live `generate-pixels` runs. See [host lifecycle and limits](docs/trace.md#pixels-host) for identity checks, uploads and cleanup. Engine memory and phase timeouts are separate resource controls, not whole-run quotas.

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

The data API is Get/Put/Delete/Scan/WriteBatch over opaque byte keys/values and caller timestamps. Get and Scan return values only; Scan uses half-open byte bounds and preserves key order and duplicate values. Replay batches baseline loading and sends each incremental operation as a separate concurrent RPC. Services own asynchronous persistence and expose no Flush RPC. Replay checks input integrity, deployment/value mode and binding, response completion and native result counts. Equal counts do not certify matching values or order. The same immutable Trace can be reused across backends and client settings.

`check-replay CONFIG TRACE` is an optional preflight-only diagnostic; replay already performs these checks. `plan-operations PROFILE TRACE REPLAY_CONFIG` estimates source batch parameters from a calibration Trace, not achievable throughput. Request supply, dispatch warmup and operation rates count the same basic operations; see [replay preflight](docs/trace.md#replay-preflight). To check persisted snapshots explicitly, use `service.reopen: true` and `verify-service`; [only queries from successfully committed transactions are exported](docs/index-service.md#internal-persistence-and-reopening).

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
| `PixelsCdcCheck` | `sibench.pixels.build=build/services/pixels`; real PostgreSQL → sink → Pixels application and RowID/Locator validation. |
| `PixelsLoadLeaseCheck` | `sibench.pixels.build=build/services/pixels` and `mkfifo`; one local LOAD held open beyond the 300-second write-transaction lease must still commit (about seven minutes). |
| `PixelsSourceCheck` | `sibench.postgres.runtime=build/services/pixels/postgres/runtime`; source-only PostgreSQL checks for key-change rejection, unchanged-key updates without guard function calls, rollback, preceding triggers and preservation of the original indexes. |
| `CdcPreparationCheck` | `sibench.pixels.build=build/services/pixels`; real PostgreSQL CDC preparation without target services. The HyBench command check also requires `sibench.hybench.build`. |
| `InputStoreCheck` | `sibench.storage.config`, `sibench.storage.profile`, a fresh `sibench.storage.prefix` and the profile's credentials; a small remote publication/download check. |
| `PixelsPreparationCheck` | `sibench.pixels.build`, `sibench.hybench.build`, `sibench.storage.config` with the `cos` profile and its credentials; parallel preparation, baseline publication and multi-file import. |
| `PixelsExperimentCheck` | `sibench.pixels.build`, `sibench.storage.config` with the `cos` profile and its credentials. Reuse matching fixtures with `sibench.cdc.input`, `sibench.csv.input` and `sibench.pixels.baseline`. Queries also require `sibench.trino.deployment`; remote hosts use `sibench.pixels.host` and `SIBENCH_PIXELS_HOST_TOKEN`. |
| `SlateDbStoreCheck` | Source-built `sibench.slatedb.library`; `sibench.slatedb.storage=PATH`, `sibench.slatedb.profile=NAME`, `sibench.slatedb.object_path=FRESH_PREFIX`, and that profile's environment credentials. Uses one Db against the configured S3 store; no independent reader. |

Native component checks live under [src/test/native](src/test/native). DuckDB Python checks accept the instrumented DuckDB executable; C++ checks require C++17 and the matching build's headers/libraries. Compile Pixels Java probes with JDK 17 and `BUILD/jars/pixels-sink.jar`, then run with that JAR and the probe classes on the classpath. Cloud JNI checks use `make -C src/main/native/index/rocksdb_cloud test` after that backend's build. The sink's `RetinaPayloadBuilderTest` runs through its patched build POM.

After `mvn package`, explicit Java validation tools use `java -cp build/sibench/test-classes:build/sibench/sibench.jar CLASS ARGS`; their usage checks describe arguments. [ReplayMatrixValidation](src/test/java/org/sibench/replay/ReplayMatrixValidation.java) checks a Trace against the reference index; [IndexVersionCheck](src/test/java/org/sibench/adapters/IndexVersionCheck.java) checks a fresh backend. Capacity diagnostics are opt-in and need bounded inputs, resources and duration.

Generation, backend evaluation and independent acceptance are separate workflows. The default suite and small native checks do not certify full-scale performance, remote durability or fault recovery.

## Supported scope

Only the pinned sources and supported schemas/access shapes are accepted; see [source scope](docs/trace.md#source-scope) and [access projection](docs/trace.md#access-projection). Primary-key updates, ambiguous ordering and incomplete index output fail explicitly. Rewrite, physical version GC and automatic retries of writes with unknown outcomes are unsupported.
