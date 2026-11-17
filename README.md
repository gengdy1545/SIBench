# SIBench

SIBench captures native benchmark index operations into reusable Traces and replays them against Index Services. It also runs Pixels update and query experiments from reusable columnar baselines.

| Benchmark | Native engine | Workload |
| --- | --- | --- |
| YCSB | RocksDB (`api`) | A–F, each restored from the same initial files. |
| sysbench | PostgreSQL or DuckDB (`capture`) | `oltp_read_write`. |
| HyBench | PostgreSQL or DuckDB (`capture`) | Separate TP (TP-1–TP-18) and XP (concurrent AT/IQ) Traces. TP includes reads. |
| HyBench → Pixels | PostgreSQL → pixels-sink → Pixels (`cdc`) | TP/AT changes and Pixels primary-index calls; AP/IQ disabled. |

## Build

Use Linux/glibc x86-64, JDK 17 (`JAVA_HOME`), Maven and Git. Builds need fresh destinations and the pinned submodules.

| Component | Additional prerequisites |
| --- | --- |
| RocksDB and DuckDB | Python 3.9+, C++17, CMake 3.16+, make, patch; RocksDB also needs LZ4, Zstd, zlib, Snappy and bzip2 development libraries. |
| PostgreSQL | C compiler, make, patch, Perl, Bison, Flex and libzstd headers; run as an unprivileged user. |
| Benchmark clients | sysbench needs autotools, pkg-config, libtool, libaio and libpq headers; HyBench/YCSB use Maven. |
| Pixels | C/C++, CMake, make, curl, PostgreSQL build tools and network access to Maven Central/GitHub. |
| Cloud backends | RocksDB-Cloud: yum-based build host, root for dependency installation and C/C++ tools. SlateDB: Python 3, C/C++ linker and rustup with its pinned toolchain. |

Build both embedded runtimes, then the application:

```bash
git submodule update --init --recursive
bash scripts/build-rocksdb.sh build/services/rocksdb
bash scripts/build-duckdb.sh build/services/duckdb
mvn package
java -jar build/sibench/sibench.jar --help
```

Build the engines and clients you will use:

```bash
scripts/build-postgres.sh config/sibench.yaml
scripts/build-benchmarks.sh config/benchmark/ycsb.yaml
scripts/build-benchmarks.sh config/benchmark/sysbench.yaml
scripts/build-benchmarks.sh config/benchmark/hybench.yaml
scripts/build-pixels.sh java build/services/pixels
scripts/build-pixels.sh native build/services/pixels
```

Set each engine's `build_directory` in `config/engine/`. For custom embedded-runtime locations, also pass absolute `-Drocksdb.build.directory=PATH` and `-Dduckdb.build.directory=PATH` to Maven. `JOBS` controls compilation parallelism. Run native builds on hosts ABI-compatible with execution hosts; upstream licenses remain in their submodules and embedded runtime JARs.

For Index Service backends, build the selected JNI library:

```bash
scripts/build-rocksdb-cloud.sh
JOBS=4 scripts/build-slatedb.sh build/index/slatedb-build
```

RocksDB-Cloud loads `build/index/librocksdb_cloud_jni.so` by default. Select SlateDB with `-Dsibench.slatedb.library=build/index/slatedb-build/libslatedb_jni.so`; the RocksDB-Cloud override is `-Dsibench.rocksdb_cloud.library=PATH`.

## Native generation

Select benchmarks and engines in [config/sibench.yaml](config/sibench.yaml). Source scale, workloads and working outputs live in [config/benchmark/](config/benchmark/); engine paths and resources in [config/engine/](config/engine/).

**Public profiles are substantial runs:** 87,060,000 baseline rows and at least 10,000,000 exported workload requests per Trace. Use separate configurations with smaller inputs and fresh output paths for short runs.

```bash
java -jar build/sibench/sibench.jar inspect-scale
java -jar build/sibench/sibench.jar generate config/sibench.yaml
```

`generate` runs the entries with `trace.generate: true` and stops on the first failure. Each named entry (`hybench-tp`, `hybench-xp`, `ycsb-a`, ...) owns its local `trace.directory` and optional upload URI; see [Trace selection and storage](docs/trace.md#selection-and-storage). HyBench entries each load a fresh database; YCSB entries share one initial load. `generate-pixels CONFIG` captures live PostgreSQL → Pixels into a separate fresh output. There is no engine fallback.

A successful run publishes after normal shutdown and deletes its working databases. A failed run keeps diagnostics and cannot resume. Existing or overlapping outputs and mismatched source/build inputs are rejected. [Trace](docs/trace.md) defines visibility, index coverage and supported accesses.

For repeated [Pixels experiments](docs/pixels-experiment.md), prepare the CSV/CDC inputs and columnar baseline once and restore them into fresh state per run.

## Replay and service

Configure the [object-storage connections](config/sibench.yaml) and the [backend](config/index/); credentials come from the named environment variables and the bucket must exist. RocksDB-Cloud and SlateDB need a fresh local path and object prefix; Relive needs a fresh CFS directory and prefix.

```bash
java -jar build/sibench/sibench.jar index-server config/sibench.yaml config/index/rocksdb_cloud.yaml
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml ycsb-a results/service.json
```

Run the two commands in separate terminals. Every backend receives the same requests at `target.service.endpoint`, so one immutable Trace replays against any of them; change only that endpoint. Client and service must agree on value mode and secondary-index selection.

Replay checks input integrity, response completion and expected result counts; matching counts do not certify values or order, which [independent acceptance](docs/index-service.md#independent-acceptance) covers. A write acknowledgment guarantees same-instance visibility; durability depends on the backend. See the [Index Service contract](docs/index-service.md).

RocksDB-Cloud and SlateDB are the comparison targets; local RocksDB serves generation and test fixtures. The [Relive bridge](docs/index-service.md#relive-bridge) exposes Relive's HTTP service through the same gRPC operations. It reads [CFS/COS settings](config/index/relive.json) and needs a real CFS mount path (local disk state is rejected). Integration does not certify cloud deployment or comparison readiness.

## Analysis

```bash
python3 graph/scripts/trace/plot_operation_mix.py
python3 graph/scripts/trace/prepare_analysis.py
python3 graph/scripts/trace/plot_access_locality.py
```

Trace selection, `--runtime`/`--jar` overrides, outputs and update-layout analysis are in [plotting](graph/README.md).

## Tests

After building both embedded runtimes, run the default suites with Python 3.9+ and local socket access:

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
```

Use `-Dsibench.build.directory=ABSOLUTE_PATH` to isolate concurrent Maven runs. Native `*Check`/`*IT` classes are opt-in via `mvn -Dtest=CLASS -DPROPERTY=PATH test`; component checks are under [src/test/native/](src/test/native/). These suites do not certify scale performance, remote durability or fault recovery.

## Reference

- [Trace](docs/trace.md): publication, encoding, visibility and source limits.
- [Index Service](docs/index-service.md): RPC semantics, measurement and persistence.
- [Pixels experiments](docs/pixels-experiment.md): input preparation and transfer, restore, queries, freshness and Trino setup.
