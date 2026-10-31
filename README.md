# SIBench

SIBench 从 HyBench、YCSB JDBC 和 sysbench PostgreSQL 的原生执行中采集索引访问，生成 Parquet 与确定性 Trace，用于分析更新局部性和评估 KV Index Service。支持本地 RocksDB 和 rocksdb-cloud，服务回放逐查询核对条数及完整有序 SHA-256。

流程为原生生成与装载 → Baseline 导出与核验 → 原生执行采集 → Trace 编译 → 分析或回放。Trace 表示固定 PG 执行计划下的索引访问，不代表 SQL 端到端性能或原始会话到达时序。

## 构建

需要 JDK 17、Maven 和 Git；`JAVA_HOME` 指向所用 JDK。

```bash
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar --help
```

生成带依赖的 `build/sibench/sibench.jar`。命令默认使用当前仓库，可用 `--root PATH` 指定；失败返回非零状态。配置中的输出目录、报告文件和新索引目录必须尚不存在，不覆盖失败现场。

### 原生采集组件

需要 C/C++、make、zstd 开发库，以及支持 reflink 的文件系统；XFS 还需 `xfsprogs`。把干净的 PostgreSQL 14.24 源码解压到 `build/benchmarks/postgresql-14.24` 后构建插桩版本：

```bash
make -C src/main/native/postgres install extension
```

补丁改变 PG 内部结构，旧源码树 / 对象文件应重新解压后构建。产物位于 `build/services/postgresql-capture/`。来源客户端另需 patch、tar，以及 Maven（HyBench / YCSB）或 autotools、pkg-config、libtool 与 libpq 开发库（sysbench）。从干净子模块构建所需客户端：

```bash
scripts/build-benchmarks.sh config/benchmark/hybench.yaml
scripts/build-benchmarks.sh config/benchmark/ycsb.yaml
scripts/build-benchmarks.sh config/benchmark/sysbench.yaml
```

构建在 `build/benchmarks/` 的新副本中进行，拒绝覆盖已有构建；`native-build.json` 固定来源提交和实际执行文件 / 类 / 依赖 JAR 的 SHA。采集前核对该记录，采集及认证身份也包含实际客户端。sysbench 的 `native.binary` 必须指向清单内的文件，标准构建为 `src/sysbench`。旧构建缺少记录时须从源码重新构建，不能用事后补写记录代替来源证明。HyBench 的依赖由其 POM 解析，构建副本应用 `scripts/patches/hybench-1.0-snapshot.patch`，排除 `PowerDist` 不使用的 dsol XML 模拟器；实际 POM 也参与构建摘要。YCSB 的 distribution 路径由构建结果确定。

### rocksdb-cloud JNI

仅使用云后端时需要。源码由 `index/rocksdb-cloud` 和 `third_party/aws-sdk-cpp` 子模块 gitlink 固定；Java 依赖版本由 `pom.xml` 固定。构建脚本要求两个子模块处于固定提交且工作区干净。

脚本面向 yum 系统，需要 patch、C/C++、CMake、make 和 JDK 17，以 root 安装 curl、OpenSSL 及压缩库开发依赖，构建副本应用随 JNI 交付的 SDK 路径补丁，确保使用本次 SDK 的头文件。所有 SDK、RocksDB 构建副本和 JNI 产物均放在 `build/index/`，AWS SDK 安装到其中的 `aws-sdk/`。SDK 构建 core、s3、transfer、kinesis 组件，具体选项以脚本为准；构建环境须与运行主机 ABI 兼容。

```bash
scripts/build-rocksdb-cloud.sh
```

产物为 `build/index/librocksdb_cloud_jni.so`，运行时以 `-Dsibench.rocksdb_cloud.library=PATH` 指定。上游许可证保留在各自子模块：[rocksdb-cloud](index/rocksdb-cloud/COPYING)、[AWS SDK](third_party/aws-sdk-cpp/LICENSE.txt)。

## 原生采集

```bash
scripts/native-capture.sh config/benchmark/ycsb.yaml
```

以能运行自身 PG 集群的非 root 用户执行。Java 优先取 `SIBENCH_JAVA_HOME`、`JAVA_HOME`，否则取 PATH；`SIBENCH_HEAP` 默认 `-Xmx12g`。

默认三来源均为 **87,060,000 行**，HyBench 为 SF10。文件名保持稳定，规模、表数和并发度以文件内容为准：

| 配置 | 职责 |
| --- | --- |
| `config/benchmark/hybench.yaml`、`ycsb.yaml`、`sysbench.yaml` | 原生负载、采集库身份与来源执行约束 |
| `config/sibench.yaml` | PG 服务器参数、参考 / 服务回放驱动及局部性分析 |
| `config/index/rocksdb.yaml`、`storage.yaml` | 索引服务与对象存储后端 |

每份原生配置声明静止基库，脚本原生装载、核验后停库，以 reflink 克隆后采集；HyBench 各阶段不恢复数据。脚本管理的专用 PG 集群使用本地 trust 认证，原生客户端使用空密码；不会把继承的 `PGPASSWORD` 写入命令行或日志。运行前检查来源、客户端构建与实际 Schema，运行后核对请求、行状态、访问账本和 PG 索引计数器，输出 Baseline、raw、Trace、认证及计时，并把 benchmark / SIBench 有效配置固定到本次目录。PG 原生 `.conf` 从该配置快照生成，不再维护另一份参数文件。成功删除克隆，`SIBENCH_KEEP_DATABASE=1` 可保留；失败产物保留供诊断，不能续写。调整规模或运行参数时保存本次有效配置，并使用新的输出路径。

原生运行的标准输出保存为 `native-run.log`；结构化采集状态和构建身份见 `raw/capture-run.json`。

采集数据目录所在文件系统须支持 reflink。YCSB 的哈希装载数据在导出时做宽行排序，PG 临时空间随输入增长；磁盘容量需覆盖基库、克隆的文件系统额度、Parquet／Trace 产物和临时排序。

## 回放与服务

以下命令复用生成的 Trace，不连接 PG：

```bash
T=results/poc/ycsb/trace
java -jar build/sibench/sibench.jar replay config/sibench.yaml "$T" results/reference.json
```

`replay` 是进程内参考检查，为服务生成逐查询期望。同一 Trace、value 模式及二级过滤配置可复用参考报告与摘要文件；改变客户端数、调度模式或后端时无需重新生成答案。参考配置可调整 `client_count`、`dispatch_mode` 和二级开关，不需要为每种组合保存一份模板。

回放入口自带输入预检、加载核验和结果检查。`check-replay CONFIG TRACE` 用于单独诊断输入，会完整消费 Baseline；无需在每次回放前另跑。修改编译或读取语义、接入后端及正式验收时，按受影响边界显式运行 `validate-snapshots CONFIG TRACE REPORT`，以独立 Parquet 行历史 oracle 核对结果。

启动真实服务，再在另一个终端回放：

```bash
java -jar build/sibench/sibench.jar index-server config/index/rocksdb.yaml
```

```bash
T=results/poc/ycsb/trace
java -jar build/sibench/sibench.jar replay-service config/sibench.yaml \
  "$T" results/service.json --reference results/reference.json
```

参考和服务回放共用 `config/sibench.yaml`，命令决定运行目标。默认模板使用 row_id、64 client 和完整提交发布。已有索引以 `service.reopen: true` 打开，再用 `verify-service` 核对持久化后的快照查询。JNI 构建见上文；存储配置和确认层级见 [Index Service 契约](docs/index-service.md)。COS bucket 是占位值，运行前配置自己的存储及环境凭据。

## 分析与派生生成

```bash
java -jar build/sibench/sibench.jar analyze-locality \
  results/poc/ycsb/trace config/sibench.yaml results/locality
python3 scripts/plot_update_locality.py results/locality --out results/locality/plots
java -jar build/sibench/sibench.jar inspect-scale
```

绘图需要 Python 3、pandas 和 matplotlib；静态 block / 文件重叠不等于真实 SST 或 compaction 成本。`inspect-scale` 只核对默认来源配置并显示行数，不生成数据。`prepare --dry-run PROFILE` 是独立的派生生成入口，不能使用原生采集配置；派生样例属于开发 / 测试输入，规模不表示压缩文件大小。

## 测试

```bash
mvn test
python3 -m unittest discover -s src/test/python -p 'test_*.py'
bash -n scripts/native-capture.sh scripts/build-benchmarks.sh scripts/build-rocksdb-cloud.sh
```

云 JNI 的独立 native 计量测试使用 `make -C src/main/native/index/rocksdb_cloud test`，需要先完成上述云后端构建和相同的编译依赖，不连接云服务。

默认 Maven 运行 Java 单元测试，不执行 `*IT` 或容量诊断的 `main`。Python 检查脚本行为与报告分析，不启动外部服务。集成测试用 `mvn -Dtest=类名 test` 显式选择，连接已有隔离 PG14.24，并使用新的证据目录：

| 集成测试 | 必需配置 |
| --- | --- |
| `PgSchemaIntegrationIT` | `SIBENCH_SCHEMA_TEST_JDBC`；`-Dsibench.schema.evidence=目录` |
| `PgBaselineExportIntegrationIT` | 同一 JDBC 变量，当前夹具使用 postgres / 空密码；`-Dsibench.layout.evidence=目录` |
| `RowStateAuditIntegrationIT` | `SIBENCH_ROW_TEST_JDBC`、`SIBENCH_ROW_CAPTURE_DIR`；`-Dsibench.row.evidence=目录` |
| `IndexKeyOrderIT` | `SIBENCH_KEY_TEST_JDBC`，仅创建临时表 |

Schema / 行审计 / 键序测试的用户分别由 `SIBENCH_SCHEMA_TEST_USER` / `SIBENCH_ROW_TEST_USER` / `SIBENCH_KEY_TEST_USER` 指定，默认 postgres，密码为 `PGPASSWORD`。前三类会创建并删除自己的随机测试库，证据目录的父目录须存在。行审计的触发器库默认在 `build/services/postgresql-capture/row_capture.so`，独立构建可用 `-Dsibench.row.library=文件` 指定。行审计还需插桩 PG 在启动前设置 `SIBENCH_CAPTURE_DIR` 为同一采集目录、`SIBENCH_CAPTURE_COMPRESS=zstd`、`SIBENCH_CAPTURE_DETAIL=summary`，并启用 `compute_query_id=on`。

## 支持边界

当前 [Trace](docs/trace.md) 和 [Index Service](docs/index-service.md) 只适配固定来源布局，拒绝额外索引类型、动态 DDL 与主键值 UPDATE。YCSB 没有二级索引；sysbench 标准脚本不按 k 查询；HyBench 默认不运行 freshness DELETE。数据文件目标默认 512 MiB，布局改变需重建相应物理输入。

数据文件 Rewrite、物理版本 GC、分段续编和未知写入的自动恢复尚未实现。默认测试与短输入不证明持续饱和容量、巨大查询组、目标规模崩溃恢复、断电耐久性或后端性能排名。
