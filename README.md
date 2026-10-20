# SIBench

通过原生 benchmark 执行、真实 Parquet 和确定性索引 Trace，评估 KV Index Service。当前完成 HyBench SF1 单会话子集的采集、编译及版本化参考验证，另有三个来源的派生 Baseline 生成器。真实 Backend 和 SF1000 正式实验尚未完成。

## 构建

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar inspect-scale
```

使用 JDK 17，`mvn package` 包含测试并生成带依赖 JAR。`--root PATH` 指定仓库，默认为当前目录。配置和运行失败返回非零状态。

## 原生采集与回放

流程：原生数据生成与 PG Load → 静止边界导出并独立验证 Baseline → 连续原生事务采集 → Java 离线编译 → Index Replay。回放不执行 SQL、不连接 PG。

[hybench_sf1.yaml](config/hybench_sf1.yaml) 是当前原生采集配置；[index_replay.yaml](config/index_replay.yaml) 控制下游发送。以下使用已验证的本地 SF1 产物：

```bash
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc check "$PWD" config/hybench_sf1.yaml
java -Xmx3g -jar build/sibench/sibench.jar check-replay config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized
java -Xmx3g -jar build/sibench/sibench.jar replay config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized results/validation/hybench-sf1-snapshot-lookup-20261020/replay-check.json
java -Xmx3g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized results/validation/hybench-sf1-snapshot-lookup-20261020/snapshots-check.json
```

报告必须是新文件。`check-replay` 检查 Trace 完整性与有效操作预算；`replay` 是两种 value 的进程内正确性检查；`validate-snapshots` 使用独立行历史 oracle，派生探针不计入原生负载或性能。

- `target.secondary_indexes.enabled` 默认 true；false 过滤全部二级加载、查询和维护，完整 Trace/Parquet 不变。
- `target.transaction_writes.coalesce_same_key` 默认 true；只合并同事务同索引键最后效果，原始修改保留。
- [long_replay.yaml](config/long_replay.yaml) 要求 single_pass、666,000,000 个有效操作；SF1 短输入会在预检失败。持续调度和分段供应尚未实现。
- 异常运行保留诊断并拒绝编译；修复后从明确初始状态换新目录重跑。

原生 PG 构建、采集入口、参数和验证结果见 [SF1 报告](docs/validation/hybench-sf1-poc.md)。

## Baseline 生成配置

此入口使用 `deterministic_derived`，与原生采集分开。规模不是压缩文件大小，三套配置均待容量校准。

| 来源 | 配置 | 初始规模 |
| --- | --- | ---: |
| HyBench | [hybench_sf1000.yaml](config/hybench_sf1000.yaml) | 1000x，8,706,000,000 行 |
| YCSB | [ycsb_large.yaml](config/ycsb_large.yaml) | 1,000,000,000 行，10 × 100 字节字段 |
| sysbench | [sysbench_large.yaml](config/sysbench_large.yaml) | 100 表 × 10,000,000 行 |

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/hybench_sf1000.yaml
java -jar build/sibench/sibench.jar prepare --dry-run config/ycsb_large.yaml
java -jar build/sibench/sibench.jar prepare --dry-run config/sysbench_large.yaml
```

dry-run 只校验源码指纹、配置和 schema，不创建数据。去掉 `--dry-run` 会真实生成，输出目录必须不存在；当前无续写，不覆盖中断产物。

## 目录与文档

| 路径 | 用途 |
| --- | --- |
| `benchmark/` | 固定上游子模块，不直接修改 |
| `src/main/java/org/sibench/`、`src/test/java/org/sibench/` | Java 核心与测试，按职责组织 |
| `src/main/native/postgres/` | PG 14.24 最小采集插桩，作用于隔离构建副本 |
| `config/` | 平铺配置，工作负载命名 `<benchmark>_<scale>.yaml` |
| `build/sibench/`、`build/benchmarks/`、`build/services/` | 核心构建、上游构建、本地服务状态 |
| `results/` | 本地数据与实验证据，不纳入源码 |

- [计划](plan.md)：当前实现、未完成项与验收。
- [Trace 契约](docs/contracts/trace.md)：版本、操作、发送及完整性。
- [存储契约](docs/contracts/storage.md)：布局、Rewrite、GC 和测量边界。
- [能力矩阵](docs/benchmarks/capability-matrix.md)：各来源当前覆盖。
- 来源差异：[HyBench](docs/benchmarks/hybench.md)、[YCSB](docs/benchmarks/ycsb.md)、[sysbench](docs/benchmarks/sysbench.md)。
