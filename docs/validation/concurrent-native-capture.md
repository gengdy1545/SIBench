# 按设计并发的原生采集

2026-10-24 起三来源按各自设计的并发运行原生负载：HyBench 的 TP 与 XP（AT / IQ）阶段用多个 client（AP 按上游 `runAPower` 单 client 跑完各轮），YCSB / sysbench 用多个客户端线程。每个线程是一个后端会话；只要某个 workload 有多个会话，整次采集就记为 `session_order=concurrent`，所有会话按 PostgreSQL 事务完成计数器归并（见 [Trace 契约](../contracts/trace.md)“身份与版本”），查询按其语句快照读取，并逐单元以快照 xip 交叉验证。workload 之间仍依次运行。

以下选用 SF1 / 8,706,000 行的并发路径证据，以及代表规模输入的认证依据。具体客户端数与配置以对应运行记录为准。

## sysbench（8 线程）

产物 `results/poc/sysbench-8m-c8-20261024/`，六个 oltp 脚本依次运行，每个 8 线程。

| 项 | 结果 |
| --- | --- |
| 会话 | 48（6 × 8），`concurrent` |
| 服务端对账 | 每个脚本的语句数、提交数与 8 个后端之和一致；控制语句与零行修改对账通过 |
| 来源失败 | 无（每个脚本的五类失败计数均为 0，未出现死锁或重试） |
| PG 计数器对照 | `consistent` |
| 访问投影 | 181,000 次（equality 149,000、range 32,000），0 拒绝 |
| 快照 | 181,000 个单元全部交叉验证，其中 161,070 个查询读到早于发送位置之前提交的版本（`SNAPSHOT_SEARCH`） |
| 参考回放 / oracle | 128 client、locator 与 RowID 两种 value 通过（3,343,913 个结果项）；`validate-snapshots` 通过 |
| 原生执行 | 32.4 s（单线程 72.2 s） |

没有出现来源失败是这次负载与规模下的事实：8 张各 108 万行的表上均匀随机访问，8 个线程的冲突很少。这不能说明更高并发或更热的访问分布下不会出现死锁；那时按 [计划](../../plan.md) 中的来源失败处理放行，而不是降低并发。

## YCSB（8 线程）

产物 `results/poc/ycsb-8m-c8b-20261024/`，A/B/C/F/D/E 依次运行，每个 8 线程（JDBC 连接各自一个后端）。

| 项 | 结果 |
| --- | --- |
| 会话 | 48（6 × 8），`concurrent` |
| 客户端对账 | 每个 workload 8 个 JDBC 文件的请求数之和等于客户端测得的操作数；声明比例与实测对账通过；workloadf 的 20,105 次 read-modify-write 照实记录 |
| 来源失败 | 无（UNEXPECTED 为 0） |
| PG 计数器对照 | `consistent` |
| 访问投影 | 346,048 次（equality 327,070、limited_range 18,978），0 拒绝 |
| 快照 | 346,048 个单元全部交叉验证，其中 333,492 个查询读早于发送位置的版本 |
| 参考回放 / oracle | 128 client、两种 value 通过；`validate-snapshots` 通过 |
| RocksDB | 进程内 RocksDB 回放 346,048 个查询期望数全部吻合，结果校验和与参考回放相同 |
| 原始体积 | 1.5 GB（单线程逐行格式 5.9 GB） |
| 耗时（ms） | 准备 179,065（批量加载）、导出 919,773、原生执行 112,810、行审计 479,821、账本 17,714、编译 48,164 |

导出与行审计是剩余热点：YCSB 每行 10 个 100 字节文本字段，导出与审计的逐行解析都与行宽成正比。

## HyBench 并发 HTAP（TP 8、XP 8 + 8 client）

产物 `results/poc/hybench-htap-sf1-c8-20261024/`，原生 `runHTAP`：AP 单 client 跑完各轮（62 s），TP 8 client 2 分钟，XP 8 个 AT 与 8 个 IQ client 2 分钟。

| 项 | 结果 |
| --- | --- |
| 会话 | 25（AP 1、TP 8、AT 8、IQ 8），`concurrent` |
| 负载量 | 51,202 个提交事务（单 client 短跑为 379 个），1,357,123 次已提交行修改 |
| 行状态审计 | 通过（修复后，见下） |
| PG 计数器对照 | `consistent` |
| 访问投影 | 38,565,046 次，0 拒绝：limited_equality 36,755,369、equality 1,753,991、stopped_equality 53,973、full_scan 1,603、交集组 1 |
| 规范 Trace | 42,301,207 个运行期操作（5.9 GB）；38,593,423 个查询，其中 37,246,996 个按快照读早于发送位置的版本、877,592 个是事务内写后查询；38,565,046 个单元全部快照交叉验证；`row_write_ratio` 0.156 |
| 编译 | 12 GB 堆，峰值 4.7 GB，1,615 s |
| 参考回放 / oracle | 128 client、locator 与 RowID 两种 value 通过：各 38,593,422 个查询（交集组算一个），核对 1,050,841,025 个结果项，其中 37,246,996 个按快照读；`validate-snapshots` 通过（24 GB 堆，峰值 26 GB，1,936 s） |

这组输入验证了三个当前不变量：

1. 多会话同键变化按事务完成位置建立因果链，不能按会话编号拼接；237,301 个修改键的链连续，其中 121,481 个 savingaccount 键被多个会话修改。
2. 会话归并以前瞻读取器定位事务结尾，再流式读片段内容，不物化整个 AP / IQ 事务。
3. 超过百万操作的源事务按已知结局和身份流式编译；回滚仍仅保留首次修改前查询，不能通过删查询规避容量。

负载量比单 client 大两个数量级，原因有两个：TP / XP 各 8 个 client；HyBench 准备阶段新补的 `VACUUM (ANALYZE)` 使 `applicantid = ? order by timestamp desc limit 1` 一类查询不再沿 timestamp 索引倒扫（见 [HyBench 来源](../benchmarks/hybench.md)）。2026-10-24 之前的 HyBench 采集没有规划器统计，其负载形态与此不同，不作为同一配置比较。

## 代表规模输入

HyBench SF10、YCSB / sysbench 87m 的完整输入和认证身份统一见 [三来源验收](p0-three-source-acceptance.md)、[输入审计](p0-three-source-audit.md)。它们采用各自固定的静止库、按变化比例校准的原生负载及 full / certified 核验；静止库统计信息和影响计划的参数参与身份核对，不将不同计划的采集直接比较。

HyBench 的早期 SF10 校准和 8 client 长采集已由这些输入覆盖，当前不以其阶段耗时或未完成 oracle 判断能力。完整参考 / oracle、本地服务和 COS 结果以所选代表输入的同次报告为准。

## YCSB 87m 32 线程重采（2026-10-26）

选用 `results/poc/ycsb-87m-n22/`：32 线程、`plan-write-ratio` 0.05，同静止库采集，以 `ycsb-87m-yl3` 的 full 认证做 certified 编译。原生配置和身份随运行保存。

| 项 | 结果 |
| --- | --- |
| 原生 | 1,572.1 s；原始采集 9.3 GB |
| 负载 | 81,653,024 个运行期操作；25,862,780 个业务提交事务（同数标签事务另记 utility-only）；295,019 插入、4,064,684 更新，`row_write_ratio` 0.05008 |
| 访问投影 | 25,567,761 次，0 拒绝（equality 24,163,595、limited_range 1,404,166） |
| 审计 | `certified`，认证 `ycsb-87m-yl3`；PG 计数器 `consistent`；未重跑行审计 |
| 编译 | 旧 jar 561.02 s（基线 36.23 / 负载 520.62）；new24 553.71 s（25.31 / 524.29）；两者 16 个 Trace 文件逐字节相同 |
| 体积 | Trace 4.1 GB；Baseline 共享 66 GB |
| 合计 | 原生 + 编译约 35.4 min |

该次采集的 raw 与 Trace 保留；编译阶段的对照细节见 [编译阶段对照](compile-optimization-20261026.md)。

## 输入可用性

开发机保留 `results/poc/regress-20261025/sf1-c8-v22/` 的 Trace / Baseline 与小型证据；代表规模 raw、认证及 Trace 主要在对应远程采集机，规范输入为 hybench-sf10-hl9、sysbench-87m-sl16、ycsb-87m-n22。历史报告中的大小与时间仅属于所记录的构建，不能假定所有旧路径仍可重编。
