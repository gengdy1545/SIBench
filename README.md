# SIBench

通过原生 benchmark 执行、真实 Parquet 和确定性索引 Trace，评估 KV Index Service。HyBench 默认采用原生 `runHTAP`：AP → TP → 并发 AT/IQ，保留原生事务比例和业务分支。v9 规范 Trace 由来源 schema 驱动，使用保序键编码，把每个原生索引访问投影为一组能重现原生结果集的有序查询（区间、带 limit 的主键扫描、二级扫描的前段加钉住点查、BitmapAnd 交集组、`= ANY` 并集组），否则带原因拒绝。历史 v9 记录中，三来源（含并发的完整 HyBench HTAP）已在 8,706,000 行基线上端到端验证：0 拒绝，1/64/128 client 参考回放与独立 oracle 通过。事务内读语义已修正为保留原语句快照并只叠加本事务此前的修改，夹具与小型 RocksDB 回归通过；三来源大 Trace 的重跑尚未执行。通用 Index Service 协议（gRPC）与 RocksDB 参照实现已能回放 Trace，逐查询核对原生期望数和完整 `(PK, version, value)` 摘要。真实 PG 受控反例和完整提交发布 / 全序对照已在远程小库通过；正式大 Trace 与性能实验尚未完成。

## 构建

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
git submodule update --init --recursive
mvn package
java -jar build/sibench/sibench.jar inspect-scale
```

使用 JDK 17，`mvn package` 包含测试并生成带依赖 JAR。`--root PATH` 指定仓库，默认为当前目录。配置和运行失败返回非零状态。

## 原生采集与回放

流程：原生数据生成与 PG Load/建索引 → 静止边界导出并独立验证 Baseline → 原生采集与同次请求/结果核对 → 最终状态审计（完整核验时）→ 规范 Trace 编译（同遍构建访问账本并核对 PG 索引计数器）→ 认证（完整核验时）。三来源同一入口 `scripts/native-capture.sh`；HyBench 的 AP/TP/XP 阶段之间不重新加载或恢复数据。

[hybench_sf1.yaml](config/hybench_sf1.yaml) 是默认 HTAP 配置。每个采集配置以 `postgres.clone_from` 声明静止库：它在 `build/services/postgresql-still/<clone_from>/` 的单独 PG 集群中由脚本的 `base` 阶段只建一次（HyBench 按上游 README 原生 gendata、`ddl_pg.sql`、`load_data_pg.sql` 并建上游二级索引；YCSB / sysbench 以原生客户端装载；随后安装行变化触发器并 `VACUUM (ANALYZE)`），之后停机不再写入。每次采集以 `cp --reflink` 复制停机的集群（需支持 reflink 的文件系统，如 xfs）、把库改名为采集库，成功后删除副本；失败时副本保留以便诊断，下次运行前需手工删除。HyBench 在 `native.work_dir`（含原生 `Data_<sf>`）下运行，装载后只保留运行时读取的两个文件，见 [HyBench 来源](docs/benchmarks/hybench.md)。并发度取原生参数：HyBench 的 `clients` / `xp_transaction_clients` / `xp_query_clients`，YCSB / sysbench 的 `threads`；每个线程一个后端会话，有重叠会话时整次采集按 PostgreSQL 事务完成计数器归并。完整步骤与证据见 [HTAP SF1 验证](docs/validation/hybench-sf1-htap.md)。

采集与 Baseline export 前自动核对实际 PG 表、完整列和全部索引与固定源码及配置一致；不一致拒绝，不自动补建。三来源可独立检查：

```bash
java -jar build/sibench/sibench.jar check-schema config/hybench_sf1.yaml \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture --user postgres
```

支持范围与 YCSB / sysbench 用法见 [来源 Schema 契约](docs/contracts/source-schema.md)。三来源原生采集均按 8,706,000 行基线对齐；不支持额外索引类型或运行期动态 DDL。

```bash
make -C src/main/native/postgres install extension   # 补丁改了 PGPROC，源码树已编过时先 clean
scripts/native-capture.sh config/hybench_sf1.yaml    # 约 50 分钟，并发 HTAP
scripts/native-capture.sh config/ycsb_8m.yaml        # 约 60 分钟
scripts/native-capture.sh config/sysbench_8m.yaml    # 约 15 分钟
java -jar build/sibench/sibench.jar compile-trace PROFILE OUTPUT [ROW_AUDIT] [--certification CERT] [--counters BEFORE AFTER]
java -jar build/sibench/sibench.jar certify-capture PROFILE
java -jar build/sibench/sibench.jar access-ledger PROFILE RAW REPORT     # 编译失败时诊断全部拒绝
java -jar build/sibench/sibench.jar compare-ledgers REPORT_A REPORT_B [OUTPUT]
java -jar build/sibench/sibench.jar snapshot-index-counters PROFILE OUTPUT --jdbc URL --user ROLE
java -jar build/sibench/sibench.jar check-capture-completeness PROFILE LEDGER BEFORE AFTER
```

后端追踪统一写成 zstd 流（`pg-<pid>.jsonl.zst`，需要 `libzstd-dev` 构建 PG），所有读取方透明解压。插桩默认按访问记录汇总（`capture.detail: summary`）：每个索引访问结束时一条 `scan_summary`，bitmap 堆判定按输入集合聚合，结果行记为哈希。`capture.detail: rows` 另保留全部逐行事件，账本逐条核对汇总与逐行事件相等，用于在同一次原生执行上验收汇总格式；两种格式编出的 Trace 除原始文件行号外完全相同，见 [采集完整性验证](docs/validation/capture-completeness.md)。

脚本以 `compute_query_id = on` 启停本地 PG，依次执行建库加载、预检、Baseline 导出、原生执行（结束后并行审计全部会话）、行状态审计与认证（仅完整核验）和 `compile-trace`（输出 `<产物目录>/trace`，内含 `access-ledger.json` 与 `capture-completeness.json`）。核验方式由 `capture.verification` 决定：`full`（默认）做行状态审计，编译后 `certify-capture` 写出 `<产物目录>/certification.json`，记录构建（jar、PG 二进制、触发器库）、来源与索引定义、采集格式及覆盖的访问路径与提交变化类别；`certified` 以 `capture.certification` 指定的认证代替行状态审计，构建或来源不同、出现认证未覆盖的路径或类别即编译失败，见 [Trace 契约](docs/contracts/trace.md)。原生执行前后各取一次 PG 索引计数器快照（等其它会话退出、两次相隔一个统计周期的读数一致后才记录），编译（`--counters`）按 PG14 计数位置核对同遍构建的账本，一致才提交 Trace：`idx_tup_read` 等于候选数，`idx_tup_fetch` 等于经堆判定可见的行（不含 bitmap 与全可见 index-only），`idx_scan` 落在账本推出的树下降上下界内，不一致则编译失败、Trace 保持未完成；`check-capture-completeness` 可对已有账本单独核对。已有采集集群或输出目录会被拒绝；采集结束时删除采集集群副本（终态已由审计读过，`SIBENCH_KEEP_DATABASE=1` 保留以便诊断），完整核验在认证后删除已审计的行变化文件，此后该采集以自己的认证（`--certification`）重编。每个阶段把墙钟、事件数和本次运行的关键事实追加到 `<产物目录>/run-timing.json`，长阶段按秒输出速率，便于比较两次运行并定位热点。任何命令都可用 `--timing FILE` 追加到同一份计时产物。账本按 PG 追踪把索引事件划分为访问单元并给出每个单元的投影决定，模板为 PG `query_id`，不连接 PG；`compile-trace` 对任一不能投影的访问失败，此时独立的 `access-ledger`（REPORT 必须是新文件）给出全部拒绝。`config/*_small.yaml` 是约 15 秒的冒烟配置。回放量以“已提交行写入 ÷ 基线行数”衡量，每个 Trace 的 manifest 给出 `row_write_ratio`；`plan-operations PROFILE CALIBRATION_TRACE REPLAY_CONFIG` 由同来源、同规模的一次校准采集，按回放配置的操作预算（如 `long_replay.yaml` 的 6.66 亿有效运行期操作，与 `check-replay` 同一计数）按同一倍数换算 sysbench 的 `events`、YCSB 的 `operationcount` 或 HyBench 的 TP / XP 阶段时长，用于大数据量（如 SF1000）的正式采集；SF10 等规模的正式采集用 `plan-write-ratio PROFILE CALIBRATION_TRACE RATIO` 按已提交行写入比例（5%）换算。实际结果以按计划采集的 Trace 为准。证据见 [HTAP 验证](docs/validation/hybench-sf1-htap.md) 与 [原生 Trace 验证](docs/validation/ycsb-sysbench-native-trace.md)。

并发 HTAP 按 PostgreSQL 事务完成计数器编译：写事务在持锁自增时记录精确位置，只读事务排在同位置写事务之后，事务大小的片段据此 k 路归并；查询读取其语句快照所见的最后一个写事务，并用快照自己的 xip 逐条交叉验证，计数器或快照缺失、冲突时拒绝，不按 PID、XID 大小、文件顺序或墙钟推断。

源行核验统一由 `RowStateAudit` 完成：完整旧 / 新行和最终 PG 状态在同一次审计中核对。默认在 raw 目录保存 `row-state-audit.json` 及已审计变化；相同报告与输入重复调用只复核摘要，不重查 PG。编译必须先有匹配报告或匹配的认证。三来源 PK 值 UPDATE（包括回滚尝试）固定拒绝，不影响普通列 / 二级索引列 UPDATE 或同主键删除重插入。

三来源通用入口为 `audit-rows PROFILE BASELINE RAW [REPORT] --jdbc URL --user ROLE`；指定自定义报告时，`compile-trace PROFILE OUTPUT ROW_AUDIT` 显式引用它。审计前业务库必须处于原采集结束的静止状态；历史报告不改写，详情见 [统一行状态审计](docs/validation/row-state-audit.md)。

[index_replay.yaml](config/index_replay.yaml) 控制下游发送。参考模型的内存有界：基线条目超过预算时外部排序为磁盘 run，版本历史落入可落盘的有序 map；可用 `-Dsibench.reference.baseline_entries=N`（默认 64,000,000）、`-Dsibench.reference.overlay_entries=N`（默认 500,000）、`-Dsibench.reference.sort_bytes=N`、`-Dsibench.reference.directory=DIR`（每个模型在其下建独占的子目录，关闭时删除）调整，结果与预算无关。`-Dsibench.trace=1` 在失败时打印堆栈。以下回放规范 Trace，不执行 SQL、不连接 PG：

```bash
T=results/poc/regress-20261025/sf1-c8-v22
java -Xmx24g -jar build/sibench/sibench.jar check-replay config/index_replay_128clients.yaml $T
java -Xmx24g -jar build/sibench/sibench.jar replay config/index_replay_128clients.yaml $T REPORT
java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml $T REPORT
java -Xmx24g -jar build/sibench/sibench.jar validate-replay-matrix $T OUTPUT_DIRECTORY
java -Xmx24g -jar build/sibench/sibench.jar check-relocation PROFILE $T OUTPUT_DIRECTORY
```

回放和快照报告必须是新文件。`check-replay` 检查 Trace 完整性、工作单元身份摘要、过滤后的 client 覆盖与有效操作预算；`replay` 是两种 value 的进程内正确性检查，不代表已连接真实的 128 个网络 client；`validate-snapshots` 使用独立行历史 oracle，派生探针不计入原生负载或性能；`validate-replay-matrix` 跑 1/64/128 client × 两种输入模式 × 二级开 / 关，要求操作顺序、状态与结果完全等价；`check-relocation` 搬迁实际修改表的最新存活行，核对精确版本 Locator 搬迁、RowID 不变。

- `target.secondary_indexes.enabled` 默认 true；false 过滤全部二级加载、查询和维护，完整 Trace/Parquet 不变。
- `replay.dispatch_mode` 默认 `operation`，以操作为驱动输入单位；[index_replay_transaction.yaml](config/index_replay_transaction.yaml) 使用 `transaction` 按已提交源事务组织输入。两者最终均逐操作路由、单线程保序发送，不合并修改，不向索引发送事务控制，也不要求下游事务 API。
- `replay.client_count` 默认 1；[64 client](config/index_replay_64clients.yaml) 和 [128 client](config/index_replay_128clients.yaml) 使用同一份 Trace。按源 SQL 请求分配 `workUnitId`，固定路由为 `(workUnitId - 1) % client_count`；同请求的主键/二级展开共用 client，同事务可跨 client。不按索引键或事务绑定，不增加其他路由策略。数量可超过 128，但过滤后的运行期操作必须覆盖全部配置 client，不足则拒绝并继续原生采集。
- 本地发送完成后即可发下一项，不等待执行完成或跨 client 可见。`sent`/`response` 分开确认；`max_in_flight` 默认 4096，`request_timeout_seconds` 默认 60。驱动只在背压及最终排空时等待响应，遇到错误停止并清理，不通过屏障、重试或补偿修复服务端可见性。
- 默认只向下游发送已提交变更，不限定 Index Service 场景。源 commit 后保留事务内查询与修改的原始顺序；写后查询携带原语句快照、本事务 CommitVersion 及此前修改位置，读取时只叠加本事务的这部分写入。回滚修改及修改后的查询排除，修改前的受支持查询保留；原始事实不变，无反向操作或回撤开关。
- [long_replay.yaml](config/long_replay.yaml) 要求 single_pass、666,000,000 个有效操作；SF1 短输入会在预检失败。持续调度和分段供应尚未实现。
- 异常运行保留诊断并拒绝编译；修复后从明确初始状态换新目录重跑。

## Index Service 回放

驱动通过 [Index Service 协议](docs/contracts/index-service.md)（`src/main/proto/index_service.proto`）把规范 Trace 发给 KV 索引服务：每个 client 一条有序 gRPC 流，查询组整体发送，每个查询的返回条数必须等于原生期望数，并逐查询核对绑定查询身份与完整 `(PK, version, value)` 的 SHA-256 摘要。`--reference` 给出同一 Trace 的参考回放报告时复用其摘要 sidecar；没有时在回放前从同一 Trace 生成。

```bash
T=results/poc/sysbench-8m-c8-20261024/trace
java -jar build/sibench/sibench.jar index-server config/index_service_rocksdb.yaml &      # RocksDB 服务，端口 50551
java -jar build/sibench/sibench.jar replay-service config/service_replay_grpc.yaml $T REPORT --reference REFERENCE
java -jar build/sibench/sibench.jar replay-service config/service_replay_rocksdb.yaml $T REPORT  # 进程内 RocksDB，无网络
```

索引目录已存在即拒绝；一个索引只由一份 Trace 的 Baseline 加载一次。`service.ordered: true` 提供 `sequence` 全序诊断；`service.publication: true` 使用事务 lane、`Commit` 标记和完整发布水位，查询等待 `read_version` 发布；两种模式不能同时开启。

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
| `src/main/native/postgres/` | PG 14.24 采集插桩（`sibench_capture.c` 加调用点补丁），作用于隔离构建副本 |
| `src/main/proto/` | Index Service 协议定义 |
| `scripts/` | 串联已有 Java CLI 阶段的编排脚本，不含来源语义 |
| `config/` | 平铺配置，工作负载命名 `<benchmark>_<scale>.yaml` |
| `build/sibench/`、`build/benchmarks/`、`build/services/` | 核心构建、上游构建、本地服务状态 |
| `results/` | 本地数据与实验证据，不纳入源码 |

- [计划](plan.md)：当前实现、未完成项与验收。
- [Trace 契约](docs/contracts/trace.md)：版本、操作、发送及完整性。
- [Index Service 协议契约](docs/contracts/index-service.md)：gRPC 协议、驱动侧核对与 RocksDB 参照实现。
- [来源 Schema 契约](docs/contracts/source-schema.md)：三来源布局、PG 预检及失败边界。
- [YCSB / sysbench 原生 Trace](docs/validation/ycsb-sysbench-native-trace.md)：完整访问投影、v9 Trace、回放与 oracle。
- [按设计并发的原生采集](docs/validation/concurrent-native-capture.md)、[采集性能](docs/validation/capture-performance.md)：并发采集、HyBench SF10 与远程机 PG 配置对照。
- [P0 读取正确性与逐查询结果校验](docs/validation/p0-read-validation.md)：事务内读、夹具反例与摘要负例。
- [P0 原生反例、短流程矩阵与完整提交发布](docs/validation/p0-native-publish-validation.md)：真实 PG 两会话、短 sysbench 新语义 1/64/128 矩阵与发布 / 全序对照。
- [P0 测试复核](docs/validation/p0-test-review.md)：发布模式迟到 / 重复请求缺陷的复核与修复记录。
- [存储契约](docs/contracts/storage.md)：布局、Rewrite、GC 和测量边界。
- [能力矩阵](docs/benchmarks/capability-matrix.md)：各来源当前覆盖。
- 来源差异：[HyBench](docs/benchmarks/hybench.md)、[YCSB](docs/benchmarks/ycsb.md)、[sysbench](docs/benchmarks/sysbench.md)。
