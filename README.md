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

Edit [config/sibench.yaml](config/sibench.yaml) to select benchmarks and engines. Source scale, workloads and working outputs belong to [config/benchmark/](config/benchmark/); engine paths and resources belong to [config/engine/](config/engine/).

**Public profiles are substantial runs:** 87,060,000 baseline rows and at least 10,000,000 exported workload requests per Trace. Create separate configurations with smaller inputs and fresh output paths for short runs. Inspect scale before generating:

```bash
java -jar build/sibench/sibench.jar inspect-scale
java -jar build/sibench/sibench.jar generate config/sibench.yaml
```

`generate` runs entries with `trace.generate: true` and stops on failure. Each named entry (for example `hybench-tp`, `hybench-xp` or `ycsb-a`) owns its local `trace.directory` and optional upload URI in the runtime config. HyBench entries select `tp` or `xp` and each load a fresh database; YCSB entries share one initial load and run their selected workloads independently. See [Trace selection and storage](docs/trace.md#selection-and-storage). For live PostgreSQL → Pixels capture, use `generate-pixels CONFIG` with a separate fresh output. There is no automatic engine fallback.

Successful runs publish after normal shutdown, then delete working databases. Failed runs retain diagnostics and cannot resume. Existing or overlapping outputs and mismatched source/build inputs are rejected. See [Trace](docs/trace.md) for visibility, index coverage and supported accesses.

For repeated [Pixels experiments](docs/pixels-experiment.md), prepare CSV/CDC inputs and a columnar baseline once, then restore into fresh state for each run.

## Replay and service

Configure [object-storage connections](config/sibench.yaml) and the selected [backend](config/index/). Supply credentials through the named environment variables; the bucket must exist. RocksDB-Cloud and SlateDB need a fresh local path and object prefix; Relive requires a fresh CFS directory and object prefix.

Start the service, then replay from another terminal:

```bash
java -jar build/sibench/sibench.jar index-server config/sibench.yaml config/index/rocksdb_cloud.yaml
```

```bash
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  ycsb-a results/service.json
```

Client and service must agree on value mode and secondary-index selection. The same immutable Trace can be reused across backends and client settings. Replay validates input integrity, response completion and expected result counts; matching counts do not certify values or order. Write acknowledgment guarantees same-instance visibility; durability depends on the backend. See the [Index Service contract](docs/index-service.md).

Every backend receives the same proto requests at `target.service.endpoint` in the runtime CONFIG. Change that gRPC target for each test deployment; Trace parsing and recorded write/read timestamps stay the same. [Independent acceptance](docs/index-service.md#independent-acceptance) provides common protocol and full-value checks using that configuration.

RocksDB-Cloud and SlateDB are comparison targets. Local RocksDB supports generation and test fixtures. The [Relive bridge](docs/index-service.md#relive-bridge) exposes its independent HTTP service through the same five gRPC operations; integration does not certify cloud deployment or comparison readiness. Relive uses [CFS/COS settings](config/index/relive.json), referencing the same runtime object-storage connections. Supply the actual CFS mount path before starting it; local disk state is rejected.

## Analysis

Set shared data paths in [global_config.py](graph/config/global_config.py) and select benchmark names and labels in each figure's configuration under `graph/config/`. Then run:

```bash
python3 graph/scripts/trace/plot_operation_mix.py
python3 graph/scripts/trace/prepare_analysis.py
python3 graph/scripts/trace/plot_access_locality.py
```

See [plotting](graph/README.md) for dependencies, outputs and static update-layout analysis. Returned-row locality uses offline snapshot queries; layout estimates do not measure SST or compaction costs.

## Tests

After building both embedded runtimes and installing the [plotting dependencies and font](graph/README.md), run the default suites with Python 3.9+ and local socket access:

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
```

Use `-Dsibench.build.directory=ABSOLUTE_PATH` to isolate concurrent Maven runs. Native `*Check`/`*IT` classes are opt-in via `mvn -Dtest=CLASS -DPROPERTY=PATH test`; component checks are under [src/test/native/](src/test/native/). These suites do not certify scale performance, remote durability or fault recovery.

## Reference

- [Trace](docs/trace.md): publication, encoding, visibility and source limits.
- [Index Service](docs/index-service.md): RPC semantics, measurement and persistence.
- [Pixels experiments](docs/pixels-experiment.md): input preparation and transfer, restore, queries, freshness and Trino setup.
