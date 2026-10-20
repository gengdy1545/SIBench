# SIBench

通过原生 benchmark 执行、真实 Parquet 和确定性索引 Trace，评估 KV Index Service。HyBench 默认采用原生 `runHTAP`：AP → TP → 并发 AT/IQ，保留原生事务比例和业务分支。单会话 TP 子集支持 v5 编译、事务内写后查询及版本化参考验证；完整 HTAP 的 KV 投影、真实 Backend 和 SF1000 正式实验尚未完成。

## 构建

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar inspect-scale
```

使用 JDK 17，`mvn package` 包含测试并生成带依赖 JAR。`--root PATH` 指定仓库，默认为当前目录。配置和运行失败返回非零状态。

## 原生采集与回放

流程：原生数据生成与 PG Load/建索引 → 静止边界导出并独立验证 Baseline → 原生 `runHTAP` 采集 → 同次请求/结果核对、八表最终状态审计、无损原始事件打包。AP/TP/XP 阶段之间不重新加载或恢复数据。

[hybench_sf1.yaml](config/hybench_sf1.yaml) 是默认 HTAP 配置；PG 必须预先准备，`run` 的工作目录包含原生 `Data_1x`，classpath 包含 HyBench classes/lib。完整步骤见 [HTAP SF1 验证](docs/validation/hybench-sf1-htap.md)。

```bash
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc check "$PWD" config/hybench_sf1.yaml
java -Xmx3g -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc audit "$PWD" config/hybench_sf1.yaml results/poc/hybench-htap-sf1/state-audit-check.json
java -Xmx3g -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc pack "$PWD" config/hybench_sf1.yaml results/poc/hybench-htap-sf1/trace-check
```

`pack` 保存原始索引事件及完整 JSON payload，不等于规范 KV Trace；HTAP 输入会被单会话编译器显式拒绝。完整并发快照、全局操作顺序、范围/变长键投影仍待实现。

[hybench_tp_subset_sf1.yaml](config/hybench_tp_subset_sf1.yaml) 保留 TP3/TP5/TP9=35/30/35 诊断负载；[index_replay.yaml](config/index_replay.yaml) 控制下游发送。以下回放既有子集产物，不执行 SQL、不连接 PG：

```bash
java -Xmx3g -jar build/sibench/sibench.jar check-replay config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized
java -Xmx3g -jar build/sibench/sibench.jar replay config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized results/validation/hybench-sf1-snapshot-lookup-20261020/replay-check.json
java -Xmx3g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml results/validation/hybench-sf1-snapshot-lookup-20261020/normalized results/validation/hybench-sf1-snapshot-lookup-20261020/snapshots-check.json
```

报告必须是新文件。`check-replay` 检查 Trace 完整性与有效操作预算；`replay` 是两种 value 的进程内正确性检查；`validate-snapshots` 使用独立行历史 oracle，派生探针不计入原生负载或性能。

- `target.secondary_indexes.enabled` 默认 true；false 过滤全部二级加载、查询和维护，完整 Trace/Parquet 不变。
- `replay.dispatch_mode` 默认 `operation`，逐操作发送；[index_replay_transaction.yaml](config/index_replay_transaction.yaml) 使用 `transaction` 按已提交源事务分组。两者均单线程同步保序，不合并修改，不向索引发送事务控制，也不要求下游事务 API。
- 默认只向下游发送已提交变更，不限定 Index Service 场景。源 commit 后保留事务内查询与修改的原始顺序；写后查询携带源提交时间戳及此前修改位置。回滚修改及修改后的查询排除，修改前的受支持查询保留；原始事实不变，无反向操作或回撤开关。
- [long_replay.yaml](config/long_replay.yaml) 要求 single_pass、666,000,000 个有效操作；SF1 短输入会在预检失败。持续调度和分段供应尚未实现。
- 异常运行保留诊断并拒绝编译；修复后从明确初始状态换新目录重跑。

既有子集证据见 [TP 子集 SF1 报告](docs/validation/hybench-sf1-poc.md)，当前保序发送及写后查询验证见 [已提交变更发送验证](docs/validation/committed-change-delivery.md)。

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
