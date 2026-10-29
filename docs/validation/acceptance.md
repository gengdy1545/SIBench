# 三来源完整验收

更新：2026-10-29。当前完整验收为 **acceptance-20261029T053122Z**，三来源的新采集、完整审计、Trace 编译、参考回放、独立 oracle、更新局部性分析，以及本地 RocksDB / rocksdb-cloud COS 六次完整后端运行均通过。所有服务查询摘要不匹配数为 **0**。

## 构建与实验条件

冻结提交 `bbff9a3a3507493420881a3d40df82d96e649366`，jar SHA-256 `238fe58b51c17d7ad1f4c8b3d19b2a678af6c0bedf9f7232efd4a1907055e644`；源码快照、构建和预算分别保存在 `source.tar`、`build.json`、`scope.json`。本地 RocksDB 9.1.1，rocksdb-cloud 上游 `20a0918820ef89d71f4a059da4864db66678c38b`，JNI SHA-256 `5f0e1f5409c90313ddebcd2a62b367883abad3587c08573c927705a1d4eb87ec`。

| 条件 | 本轮配置与记录边界 |
| --- | --- |
| CPU | AMD EPYC 9K65；各 pod 配额 32 CPU，宿主可见 96 个逻辑 CPU，JVM 识别 32 |
| 内存 | 各 pod 上限 64,000,000,000 B（59.60 GiB，含页缓存）；驱动 / 服务分别 `-Xmx16g` |
| 本地盘 | `/data` 为 500 GiB、XFS；磁盘介质、IOPS 和带宽未独立测量 |
| 系统 / 运行时 | TencentOS 3.2，远程 JDK 17.0.20.1，PostgreSQL 14.24 |
| 主机分工 | `sibench_1 / 2 / 3` 分别为 HyBench / YCSB / sysbench，同来源的本地和 COS 使用同机；`sibench_4` 承担 JNI 构建及小输入 / PG 验证 |
| 回放 | row_id、二级开启、operation、64 client、4,096 在途；每次新建索引与 JVM |
| 对象存储 | Rockset 的 rocksdb-cloud 嵌入式引擎接腾讯 COS；未独立测量到 COS 的网络 RTT / 带宽 |

原始主机快照保存在本轮 `environment/`，采集参数和阶段预算见 [采集环境](capture-performance.md#执行环境)。上述 CPU / 内存为容器配额；共享宿主的其他负载未独立控制。

应用主代码冻结不变；本轮另修正 `RowStateAuditIntegrationIT` 的旧文件名、旧 manifest 夹具和采集结束竞态，原有断言不变，见 `test-harness-patch.json`。维护配置同步到本轮原生并发和负载量，`maintained-config-check.json` 核对有效参数一致；不将文档 / 测试修正冒充另一轮应用构建。

三组 Baseline 均为 **87,060,000 行**，复用已验证的静止 PG 基库和摘要匹配的 Baseline。重新执行原生负载、全部会话和完整行状态审计、PG index counter 核对及新认证；不复用旧认证代替本轮 full 审计。未重新测量从零 gendata、load、建索引和全量 Baseline 导出，见 [采集条件](capture-performance.md)。

每来源的 local / COS 在同一主机先后运行，各建全新索引和 JVM；相同 Trace、二级开启、row_id、64 client、operation、4,096 在途、各 16 GiB 驱动 / 服务堆。各来源的后端对照串行，不与其它 COS 实验竞争。输入经过正确性读取，未强制清页缓存；不是全冷缓存对照。云 fork 的默认项差异随 OPTIONS 保存，不能当作只替换存储介质的严格消融。

setup 600 秒、单请求 600 秒、replay 6 小时、最终空闲观测 120 秒、teardown 30 秒分别记录。600 秒是实验保护，不是性能 SLO；首个错误与后续关闭分开诊断，没有追加重试、删除查询或循环 Trace。本轮均在预算内完成。

## 输入与身份

| 来源 | Baseline 索引操作 | Trace 记录 | 业务修改次数 / 比例 | 提交事务 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 312,120,000 | 71,937,488 | 4,553,565 / 5.230% | 153,524 |
| YCSB 87m | 87,060,000 | 81,530,008 | 4,354,218 / 5.001% | 25,823,496 |
| sysbench 87m | 174,120,000 | 30,825,348 | 4,353,000 / 5.000% | 4,072,770 |

Trace 记录包含事务控制记录；业务修改次数不是不同行数，也不是索引展开写数。相同行数不代表相同数据 / 索引字节。HyBench 按时间结束，实际比例允许偏离校准目标；YCSB 无二级，sysbench 的 k 二级主要承担维护写。

Parquet 的文件数、实际大小及生成阶段见 [文件产物](measurement.md#parquet-文件与-trace-产物)，Trace 访问形态与整轮 UPDATE 集中度见 [局部性分析](update-locality.md#本轮修改分布)。本轮不是统一字节大小的数据文件布局实验：PG Baseline 当前按行数分片，YCSB 的典型 Baseline 单文件约 812.70 MiB，不能将配置中的 256 MiB 当作该导出路径的文件上限。

| 来源 | workload SHA-256 | Baseline SHA-256 |
| --- | --- | --- |
| HyBench SF10 | `16c0c46dd3b7ef94481dc3e545d40c757b93382cad91519926961cd60a01dd0e` | `8b95df6906f3ab4f6463751ab5db8fbd461f0ca0039c08417f24d7b588591e6a` |
| YCSB 87m | `a1dab037a2eb0eb09778f67b472a7d8ff23cf1b4c4147337dd23fe2d2620e803` | `0afda377b407ff3866e0d96de0167073903b89ab52698bc5d772c6c3386ace91` |
| sysbench 87m | `c6594f49bc90c2cd8d93d6758b7dd0bc6e5a9360486aecba7490d726fc83b1b3` | `9587cc6f1365f02620f43db96dd69e5e7dc5ff435e3544ea0b10db498c8e11e6` |

三份 manifest 均 `complete_for_declared_projection`，计数器核对均 `consistent`，受支持访问投影完整。来源提交、Schema、Baseline 验证、raw 摘要和新认证在各自 `capture-evidence/` 及原采集目录保存。没有把原生错误、回滚或拒绝计为成功。

## 参考、oracle 与组合覆盖

| 来源 | 全量参考 value | oracle：原生查询 | 派生 PK / 二级 / 范围探针 |
| --- | --- | --- | --- |
| HyBench SF10 | rowid | 59,535,140 | 11,776,141 / 23,750,568 / 3,368 |
| YCSB 87m | rowid | 25,528,798 | 13,345,399 / 0 / 168 |
| sysbench 87m | locator, rowid | 12,709,845 | 14,339,115 / 22,513,155 / 2,622 |

oracle 从 Baseline / 增量 Parquet 独立解码行历史，验证变化键、二级身份与派生区间；派生探针不计入原生负载或服务吞吐。它不能替代原生快照事实，查询组的独立覆盖限成员查询。全量 HyBench / YCSB 为 row_id，sysbench 另完整验证 Locator；两种 value 的组合语义由小输入矩阵补齐。

三来源的 1 / 64 / 128 client × operation / transaction × 二级开关，共 12 个组合、两种 value 及独立 oracle 均通过。HyBench 复用已验证 SF1 Trace，YCSB 复用小输入，sysbench 使用 2,000 行 Trace；矩阵的构建为本轮冻结 jar，来源输入身份单列，不称为本轮新采集。两份复用矩阵输入已迁移到对应主机的 `matrix-inputs/<source>/`，保留原 manifest 和逐文件摘要，当前 jar 的 `check-replay` 通过；只变更必要路径，原矩阵报告保持原样。

JDK 17 根 Maven **319 项**、Python 测量汇总 **4 项**、原生请求延迟分桶 / 溢出测试通过。三个实际 PG 集成测试通过：Schema 的 50 项 catalog 检查、三来源行状态及 PK UPDATE 拒绝、4,000 行原生键序差分。原生两会话反例由同一 jar 的参考、oracle、Locator / RowID RocksDB 重新通过，见 [读取反例](correctness.md#原生两会话反例)。

## 完整真实后端结果

| 来源 | 后端 | 查询摘要核对 | 结果项 | 最大单查询结果项 | 最终发布水位 |
| --- | --- | --- | --- | --- | --- |
| HyBench SF10 | local | 59,535,140 | 13,961,999,125 | 6,004,552 | 153,524 |
| HyBench SF10 | cos | 59,535,140 | 13,961,999,125 | 6,004,552 | 153,524 |
| YCSB 87m | local | 25,528,798 | 94,932,314 | 100 | 25,823,496 |
| YCSB 87m | cos | 25,528,798 | 94,932,314 | 100 | 25,823,496 |
| sysbench 87m | local | 12,709,845 | 234,809,539 | 100 | 4,072,770 |
| sysbench 87m | cos | 12,709,845 | 234,809,539 | 100 | 4,072,770 |

六次合计 **195,547,566 次查询摘要核对、28,583,481,956 个结果项、0 次不匹配**。均使用同轮参考报告的有序 SHA-256 sidecar，逐查询核对原生条数与完整 PK / version / value / 顺序，最终无待返回响应，完成最后 Flush 和关闭。普通查询支持分块传输；本轮最大返回基数见原报告，不扩大为任意查询组容量证明。性能、资源、引擎事件和对象请求统一见 [本轮测量](measurement.md)，静态工作负载分析见 [更新局部性](update-locality.md)。

小输入实际 gRPC 的本地 / COS 回放、同目录新进程重开、COS 全新目录 bucket 冷启动，以及 Flush 后未持久化版本的 SIGKILL 反例均通过，见 [恢复验证](rocksdb-cloud.md)。本轮没有目标规模崩溃恢复，不能把小输入恢复或正常关闭当作它的验收。

## 证据与复现

本地入口：[本轮目录](../../results/validation/acceptance-20261029T053122Z/)，汇总为 `cohort-summary.json`，原始阶段状态为 `cohort-status.json` / 各来源 `pipeline-status.json`。后端的 `replay.json`、参考摘要身份、有效配置、命令、两端 GNU time、应用 timing、引擎 LOG / OPTIONS / stats 和 30 秒索引采样均同轮配对。摘要清单索引所选文件，不改写原报告。

完整输入和查询 sidecar 保留在来源主机：`sibench_1 / 2 / 3` 分别为 HyBench / YCSB / sysbench，工作区 `/data/sibench`，输入 `results/poc/acceptance-20261029T053122Z/<source>/`，控制与报告 `results/validation/acceptance-20261029T053122Z/<source>/`。本地保存报告和选定小输入，未复制三份大 Baseline / raw。复现使用新的输出目录，按保留的 `*-command.json` 和有效配置执行，见 [README](../../README.md)。

文档只保留本轮完整结果，撤下旧构建的优化倍数、发布窗口曲线、旧后端计时和未继续调查的旧超时结论；旧实验产物在本轮通过后清理。仍需使用的原生反例、组合输入、静止库、认证和 HyBench 运行时数据按当前消费者保留或记录路径迁移，不维护旧实现或兼容分支。

清理范围与逐路径记录见 `retirement*.json`、`host-finalization.json` 和 `cos-retirement.json`。7 台来源 / 诊断主机的旧目录已处理，1,749 个确认属于旧实验的 COS 对象已删除，本轮对象的 ETag / 大小核对不变；未识别的前缀保留。字节相同的查询 sidecar 通过硬链接去重，原报告路径与内容保持不变，详见各主机 `query-sidecar-dedup.json`。

清理后，三份完整输入与两份迁移的组合输入再次通过当前 jar 的 `check-replay`，见 `retained-input-check.json` 和各主机 `post-cleanup/`；该检查核对完整性与供应，不重复声称一次新的服务回放。

按此前约定保留的 120 秒首次失败原日志、配置、线程和构建 / 输入身份位于 `retained-failure/request-deadline-120s/`；复制文件逐项与原身份核对。旧失败索引、诊断状态和重复运行不再保留，因此该精简记录不承诺复原旧后端现场，也不混入本轮计时。600 秒配置通过不能证明原超时根因已解决。

单次运行不能给出正式性能排名、稳态成本、长期历史覆盖、Rewrite / GC、目标规模断电恢复、纯远端查询 I/O、SF1000 容量或费用结论。后续工作和验收仍在 [计划](../../plan.md)，本轮没有开展其中的新功能。
