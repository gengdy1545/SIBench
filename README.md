# SIBench

通过原生 benchmark 执行、真实 Parquet 和确定性索引 Trace，评估 KV Index Service。HyBench 默认采用原生 `runHTAP`：AP → TP → 并发 AT/IQ，保留原生事务比例和业务分支。当前单会话 v7 编译维护八表主键及全部 13 个二级索引，支持索引列值更新、删除重插入、事务内写后查询和多 client 参考验证；完整 HTAP 的访问 / 快照投影、真实 Backend 和 SF1000 正式实验尚未完成。

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

采集与 Baseline export 前自动核对实际 PG 表、完整列和全部索引与固定源码及配置一致；不一致拒绝，不自动补建。`CapturePoc check` 也会连接 PG。三来源可独立检查：

```bash
java -jar build/sibench/sibench.jar check-schema config/hybench_sf1.yaml \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture --user postgres
```

支持范围与 YCSB / sysbench 用法见 [来源 Schema 契约](docs/contracts/source-schema.md)。YCSB / sysbench 已有与 HyBench SF1 同行数（8,706,000）的单会话原生采集（见下），规范 Trace 仍未实现；不支持额外索引类型或运行期动态 DDL。

```bash
make -C src/main/native/postgres install extension
scripts/native-capture.sh config/ycsb_8m.yaml        # 约 48 分钟
scripts/native-capture.sh config/sysbench_8m.yaml    # 约 13 分钟
java -jar build/sibench/sibench.jar access-ledger config/ycsb_8m.yaml results/poc/ycsb-8m/raw REPORT
```

脚本启停本地 PG 并依次执行建库加载、预检、Baseline 导出、原生执行、行状态审计和访问账本；已有数据库或输出目录会被拒绝。`access-ledger` 按 PG 追踪把索引事件划分为访问单元，不连接 PG，REPORT 必须是新文件。`config/*_small.yaml` 是约 15 秒的冒烟配置。证据见 [原生采集验证](docs/validation/ycsb-sysbench-native-capture.md)。

```bash
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc check "$PWD" config/hybench_sf1.yaml
java -Xmx3g -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc audit "$PWD" config/hybench_sf1.yaml
java -Xmx3g -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc pack "$PWD" config/hybench_sf1.yaml results/poc/hybench-htap-sf1/trace-check
```

`pack` 保存原始索引事件及完整 JSON payload，不等于规范 KV Trace；HTAP 输入会被单会话编译器显式拒绝。完整并发快照、全局操作顺序、范围/变长键投影仍待实现。

源行核验统一由 `RowStateAudit` 完成：完整旧 / 新行和最终 PG 状态在同一次审计中核对。默认在 raw 目录保存 `row-state-audit.json` 及已审计变化；相同报告与输入重复调用只复核摘要，不重查 PG。新编译和维护诊断必须先有匹配报告，既有规范产物回放不变。三来源 PK 值 UPDATE（包括回滚尝试）固定拒绝，不影响普通列 / 二级索引列 UPDATE 或同主键删除重插入。

三来源通用入口为 `audit-rows PROFILE BASELINE RAW [REPORT] --jdbc URL --user ROLE`；`CapturePoc audit` 同时支持 HyBench HTAP 和 TP 子集。指定自定义报告时，`CapturePoc compile ROOT CONFIG PG_TRACE OUTPUT [ROW_AUDIT]` 与 `IndexMaintenanceValidation ROOT CONFIG BASELINE RAW REPORT [ROW_AUDIT]` 可显式引用它。审计前业务库必须处于原采集结束的静止状态；历史报告不改写，详情见 [统一行状态审计](docs/validation/row-state-audit.md)。

[hybench_tp_subset_sf1.yaml](config/hybench_tp_subset_sf1.yaml) 保留 TP3/TP5/TP9=35/30/35 诊断负载；[index_replay.yaml](config/index_replay.yaml) 控制下游发送。以下回放既有子集产物，不执行 SQL、不连接 PG：

```bash
java -Xmx16g -jar build/sibench/sibench.jar check-replay config/index_replay_128clients.yaml results/validation/hybench-sf1-secondary-indexes-20261021/normalized-jdk17
java -Xmx16g -jar build/sibench/sibench.jar replay config/index_replay_128clients.yaml results/validation/hybench-sf1-secondary-indexes-20261021/normalized-jdk17 results/validation/hybench-sf1-secondary-indexes-20261021/replay-check.json
java -Xmx16g -jar build/sibench/sibench.jar validate-snapshots config/index_replay_128clients.yaml results/validation/hybench-sf1-secondary-indexes-20261021/normalized-jdk17 results/validation/hybench-sf1-secondary-indexes-20261021/snapshots-check.json
```

回放和快照报告必须是新文件。`check-replay` 检查 Trace 完整性、工作单元身份摘要、过滤后的 client 覆盖与有效操作预算；`replay` 是两种 value 的进程内正确性检查，不代表已连接真实的 128 个网络 client；`validate-snapshots` 使用独立行历史 oracle，派生探针不计入原生负载或性能。

- `target.secondary_indexes.enabled` 默认 true；false 过滤全部二级加载、查询和维护，完整 Trace/Parquet 不变。
- `replay.dispatch_mode` 默认 `operation`，以操作为驱动输入单位；[index_replay_transaction.yaml](config/index_replay_transaction.yaml) 使用 `transaction` 按已提交源事务组织输入。两者最终均逐操作路由、单线程保序发送，不合并修改，不向索引发送事务控制，也不要求下游事务 API。
- `replay.client_count` 默认 1；[64 client](config/index_replay_64clients.yaml) 和 [128 client](config/index_replay_128clients.yaml) 使用同一份 v7 Trace。按源 SQL 请求分配 `workUnitId`，固定路由为 `(workUnitId - 1) % client_count`；同请求的主键/二级展开共用 client，同事务可跨 client。不按索引键或事务绑定，不增加其他路由策略。数量可超过 128，但过滤后的运行期操作必须覆盖全部配置 client，不足则拒绝并继续原生采集。
- 本地发送完成后即可发下一项，不等待执行完成或跨 client 可见。`sent`/`response` 分开确认；`max_in_flight` 默认 4096，`request_timeout_seconds` 默认 60。驱动只在背压及最终排空时等待响应，遇到错误停止并清理，不通过屏障、重试或补偿修复服务端可见性。
- 默认只向下游发送已提交变更，不限定 Index Service 场景。源 commit 后保留事务内查询与修改的原始顺序；写后查询携带源提交时间戳及此前修改位置。回滚修改及修改后的查询排除，修改前的受支持查询保留；原始事实不变，无反向操作或回撤开关。
- [long_replay.yaml](config/long_replay.yaml) 要求 single_pass、666,000,000 个有效操作；SF1 短输入会在预检失败。持续调度和分段供应尚未实现。
- 异常运行保留诊断并拒绝编译；修复后从明确初始状态换新目录重跑。

当前 v7 全部二级索引、真实索引列值更新和多 client 验证见 [全索引 SF1 验证](docs/validation/hybench-sf1-secondary-indexes.md)。[TP 子集 SF1 报告](docs/validation/hybench-sf1-poc.md) 与 [已提交变更发送验证](docs/validation/committed-change-delivery.md) 保留历史证据，不作为当前索引覆盖或操作数量说明。

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
| `scripts/` | 串联已有 Java CLI 阶段的编排脚本，不含来源语义 |
| `config/` | 平铺配置，工作负载命名 `<benchmark>_<scale>.yaml` |
| `build/sibench/`、`build/benchmarks/`、`build/services/` | 核心构建、上游构建、本地服务状态 |
| `results/` | 本地数据与实验证据，不纳入源码 |

- [计划](plan.md)：当前实现、未完成项与验收。
- [Trace 契约](docs/contracts/trace.md)：版本、操作、发送及完整性。
- [来源 Schema 契约](docs/contracts/source-schema.md)：三来源布局、PG 预检及失败边界。
- [YCSB / sysbench 原生采集](docs/validation/ycsb-sysbench-native-capture.md)：原生客户端对账、访问账本与已知差异。
- [存储契约](docs/contracts/storage.md)：布局、Rewrite、GC 和测量边界。
- [能力矩阵](docs/benchmarks/capability-matrix.md)：各来源当前覆盖。
- 来源差异：[HyBench](docs/benchmarks/hybench.md)、[YCSB](docs/benchmarks/ycsb.md)、[sysbench](docs/benchmarks/sysbench.md)。
