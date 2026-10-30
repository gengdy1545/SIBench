# 三来源完整验收

更新：2026-10-30。最新选择集为 **ycsb-optimization-20261030T010500Z**：本次仅优化并重新完整生成 YCSB；HyBench / sysbench 沿用上轮完整验收。合计 **95,896,781 次查询摘要核对、14,011,397,366 个结果项、0 次不匹配**。

新 YCSB 从原生装载开始到 Trace 编译完成实测 **142.39 分钟**，随后完整参考、独立 Parquet oracle、rocksdb-cloud / COS 回放全部通过。同输入基线导出及 PG 独立验证 **106.45 → 43.28 分钟（−59.34%）**，131 个 Parquet 文件逐字节一致。缓存交接补测及排序空间代价见 [采集记录](capture-performance.md#优化结论与复现)。

## 构建与实验条件

| 角色 | SHA-256 |
| --- | --- |
| YCSB 原生基库生成 / 固定旧比较构建 | `3fbdad4dbf3fb48bc22eaaada7fbd7bbfaec6a7494be678c5d4be08a7da66910` |
| YCSB COPY 导出、采集 / 编译、参考 / oracle、服务 | `5bd4ae41a46bb5ca0705f6bba6746b64434f7abe895c5236e07ac0677bd94c68` |
| 当前构建 / YCSB 最终驱动 / 缓存交接补测 | `8bfb179e8eda0f66141e8aa0bb9b590373c29775b122c9a578440787d053d144` |
| 复用 HyBench / sysbench 的生成、参考 / oracle、服务 | `e966f470ad9ef369a5ffd52f56af4d6688259c8906e03fa44592d507c31b8a24`；最终驱动为 3fbd |
| rocksdb-cloud JNI | `5f0e1f5409c90313ddebcd2a62b367883abad3587c08573c927705a1d4eb87ec` |
| PG 14.24 / 插桩 | `c5e02bffee01d18b6ba5a7b6bec19dd52c3a82e4887014ff47b982bc9638bbe1` / `0c802b68cd168458c554023220cdc1164101397592f9f96ba57ce4b7503ddaa7` |

构建分阶段冻结。5bd 只改变 YCSB 的 PG 导出读取；8bfb 将 PG 验证读取的实际 SHA 缓存交给发布目录及 hardlink，完整 87m 同输入验证 / 发布边界补测通过。其 capture identity 不变，Trace / 回放 / 服务类逐字节未变。新生成耗时按实际 5bd 保留，未扣除后来的缓存补测收益，也未声称用 8bfb 重跑所有阶段。源码、类差异、测试及角色选择见本批次 `comparison-build.json`、`candidate-build.json`、`checksum-candidate-build.json`、`final-driver-selection.json`。

| 条件 | 有效配置 / 边界 |
| --- | --- |
| 主机 | 本次获准 7 台加本机；2 保留原始输入，3 / 4 短对照，5 全量同输入比较，6 全新 YCSB；原 HyBench / sysbench 为 1 / 3 |
| CPU / 内存 | AMD EPYC 9K65；每远程容器 32 CPU、64,000,000,000 B（59.60 GiB，含页缓存） |
| 磁盘 / 系统 | 500 GiB XFS；TencentOS 3.2 / glibc 2.28；JDK 17、PG 14.24 |
| JVM | 采集 32 GiB；参考 / oracle 顺序各 40 GiB；驱动 / 服务各 16 GiB；本机构建、测试及离线分析 |
| 回放 | row_id、二级开启、operation、64 client、4,096 逻辑成员在途；fresh JVM / 索引、独立 COS 前缀 |
| 引擎 | Rockset rocksdb-cloud / RocksDB 9.1.1，上游 `20a0918820ef89d71f4a059da4864db66678c38b`，接腾讯 COS；不是完整 Rockset 服务 |
| 持久化 | ACK_WAL 不 fsync；Baseline 跳过 WAL，到 Flush 持久；云 Flush 落 SST / MANIFEST，WAL 不上传 |
| 保护预算 | 装载 ≤12h，大阶段各 ≤6h，来源总截止 ≤24h；磁盘余量 <20 GiB 或首个失败即停；关闭单独限时 |

同机性能对照隔离竞争实验，独立主机可并行。未强制清页缓存；IOPS、网络 RTT / 带宽及其他租户干扰未独立控制。最终 Flush 后观测 120 秒，不能视为后台维护全部排空。

## 输入与身份

每来源 Baseline **87,060,000 行**，文件目标 **512 MiB、允许超量 5%、每语义流最多一个 EOF 尾文件**。分布见 [测量](measurement.md#parquet-文件与-trace-产物)，原生条件见 [采集](capture-performance.md#本轮生成条件)。

| 来源 | Baseline 索引操作 | Trace 记录 / workload 索引操作 | 业务行写 / 比例 | 提交事务 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 312,120,000 | 69,878,097 / 69,581,401 | 4,488,176 / 5.155268% | 148,348 |
| YCSB 87m | 87,060,000 | 81,528,705 / 29,881,785 | 4,353,398 / 5.000457% | 25,823,460 |
| sysbench 87m | 174,120,000 | 30,825,360 / 22,679,820 | 4,353,006 / 5.000007% | 4,072,770 |

| 来源 / 运行目录 | workload SHA-256 | Baseline 规范流 SHA-256 |
| --- | --- | --- |
| HyBench SF10 / `acceptance-20261029T181200Z` | `f3e5a87eb5dc092ea56becd3a5d4ba30f644e0e7ade53b4d7f067c5ac06b2256` | `27415b48bd905d3ed6c1c836676288bec634df694537a0b70f5e8a49938d2f5c` |
| YCSB 87m / `ycsb-optimization-20261030T010500Z` | `3da73fc417c57595d5012a28455d15c4204bf665a833121e316306c02ac069b9` | `43fd08cd4dc7fcb87bb7cbeda3e9b0834885d7cc4e42355166db945f0c04bee6` |
| sysbench 87m / `acceptance-20261029T170600Z` | `ceca20ade22873443610bafc92aea82cee27f4e6042e833ba5e5049f4a97c46f` | `f4c22afc3b46eec950c0166117b18d541ef0ef985402721b79d885d2164e81ef` |

三份 manifest 均为 `complete_for_declared_projection`、计数器 `consistent`，全行审计及认证通过。不删查询或截断提交凑比例，业务行写与索引展开操作分开计数。

## 完整结果与核验范围

| 来源 | 完整查询摘要核对 | 完整结果项 | 最终发布水位 | 不匹配 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 57,658,549 | 13,681,718,762 | 148,348 | 0 |
| YCSB 87m | 25,528,387 | 94,869,090 | 25,823,460 | 0 |
| sysbench 87m | 12,709,845 | 234,809,514 | 4,072,770 | 0 |

服务核对全部查询的条数及有序 PK / version / value 摘要，响应全部收齐，水位等于提交数，完成 Flush 并正常停止。独立 oracle 从 Parquet 重建行历史，检查键、版本、二级及派生区间；sysbench 另覆盖 Locator。探针不计服务吞吐。

最新根 Maven **338 项，0 失败 / 错误 / 跳过**。真实 PG 小输入将 COPY 与独立 JDBC 解码逐字节比较，覆盖 NULL、空串、Unicode；错误 PG 值被拒绝。缓存回归在旧构建失败、新构建通过，发布前文件变动仍拒绝。历史 [组合矩阵 / 原生反例](correctness.md) 和 [小输入恢复](rocksdb-cloud.md) 保留各自身份，本次未重跑大规模恢复。

发送端 1/64/128 client 诊断及独立 Baseline RPC 证据保留固定旧 Trace，见 [发送端测量](measurement.md#发送端容量与优化对照)。本轮未改变发送逻辑，旧有限前缀不证明新输入长期饱和或服务最大吞吐。

## 证据与复现

入口：[最新机器汇总](../../results/validation/ycsb-optimization-20261030T010500Z/cohort-summary.json)。来源摘要：[HyBench SF10](../../results/validation/acceptance-20261029T181200Z/hybench/source-summary.json)、[YCSB 87m](../../results/validation/ycsb-optimization-20261030T010500Z/ycsb/source-summary.json)、[sysbench 87m](../../results/validation/acceptance-20261029T170600Z/sysbench/source-summary.json)。同输入对照：[COPY](../../results/validation/ycsb-optimization-20261030T010500Z/full-analysis.json)、[缓存交接](../../results/validation/ycsb-optimization-20261030T010500Z/checksum-analysis.json)。命令、effective config、GNU time、资源采样及原始结果同轮保存；汇总拒绝错误构建、阶段未完成、响应 / 水位不符及文件越界。

新 YCSB 的输入 / raw / 认证 / Trace / sidecar 保留在 `sibench_6:/data/sibench/results/poc/ycsb-optimization-20261030T010500Z/ycsb/capture/` 及本批次控制目录；本地同步选定小证据，`collection.json` 绑定压缩包摘要。HyBench / sysbench 保留原目录。旧 YCSB 原始 PG / Baseline 与比较二进制仍支撑同输入性能比较及旧发送诊断，旧结果不再作为当前验收结果。

新验收通过后，已清理 3 / 4 号机短对照的重复 Parquet、PG 恢复副本、重复 dump 和未使用安装包，以及 5 号机全量比较的 PG 副本 / 重复 Parquet。2 号机保留规范原始 PG / Baseline 和唯一 8m dump；清理前核对其 manifest / dump SHA，清理后确认原 PG 仍在、manifest 未变、dump 大小一致。6 号机新证据未动。选定原始报告、日志、manifest 原样保留。路径与逐项清单见 [保留 / 清理记录](../../results/validation/ycsb-optimization-20261030T010500Z/retention.json)；hardlink 别名的逻辑字节不能当实际释放量或性能收益。

持续供给 / 响应端余量、独立重复采集波动、正式排名、目标规模崩溃恢复、Rewrite / GC、远端查询 I/O 稳态及更大规模尚未验证，见 [计划](../../plan.md)。
