# 按设计并发的原生采集

2026-10-24 起三来源按各自设计的并发运行原生负载：HyBench 的 TP 与 XP（AT / IQ）阶段用多个 client（AP 按上游 `runAPower` 单 client 跑完各轮），YCSB / sysbench 用多个客户端线程。每个线程是一个后端会话；只要某个 workload 有多个会话，整次采集就记为 `session_order=concurrent`，所有会话按 PostgreSQL 事务完成计数器归并（见 [Trace 契约](../contracts/trace.md)“身份与版本”），查询按其语句快照读取，并逐单元以快照 xip 交叉验证。workload 之间仍依次运行。

这一轮是 SF1 / 8,706,000 行规模的短跑，目的是在大规模采集前暴露并发路径上的真实问题；配置与默认 SF1 配置的差别只有并发度与产物位置（`/tmp/sibench-profiles/*_c8.yaml`，未纳入仓库）。

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

同一天第一次 8 线程运行在原生执行完成后中断：采集脚本运行期间被改写，bash 按偏移继续读脚本时解析失败；产物原样移为 `results/poc/ycsb-8m-c8-20261024-broken-scriptedit/`，第二次因端口被占用未启动，目录移为 `...-portbusy/`，二者都不作为证据。

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

这次短跑暴露并修复了两个处理管线的问题，没有改动负载：

1. **行状态审计的变化顺序**。审计原来按（表、主键、会话、文件位置）排序同一键的变化；并发会话写同一账户时，后提交的变化可能排在前面，旧像链断开（`Source before-image chain mismatch: savingaccount`，121,481 个 savingaccount 键被多个会话修改）。改为在会话之前按事务完成计数器排序，与编译器归并用同一顺序；按计数器排序后 237,301 个键的链全部连续。
2. **会话归并的内存**。归并原来把每个会话当前的整个事务片段读入内存，单个 IQ / AP 片段即有数 GB，12 GB 堆不足。改为每个会话一个前瞻读取器只找片段结尾的计数器，片段内容在取出时流式读出，内存与片段大小无关。
3. **超大源事务**。8 个 IQ client 各自整个 XP 阶段只是一个事务（只读、连接关闭时 abort），每个含 440 万到 500 万次查询，超过编译器按事务缓冲的 1,000,000 个操作上限。归并的前瞻已经读到片段的结尾记录，所以事务是提交还是回滚、事务身份（结尾记录的事件序号）在读它的事件之前就已知；编译器据此按原顺序写出已结算的前缀（回滚保留首次修改前的查询，提交带已知身份写出 BEGIN、查询与修改），不再整事务缓冲。用改动后的代码重编 sysbench 与 YCSB 的 8 线程采集，所有 Trace 数据文件逐字节相同。

负载量比单 client 大两个数量级，原因有两个：TP / XP 各 8 个 client；HyBench 准备阶段新补的 `VACUUM (ANALYZE)` 使 `applicantid = ? order by timestamp desc limit 1` 一类查询不再沿 timestamp 索引倒扫（见 [HyBench 来源](../benchmarks/hybench.md)）。2026-10-24 之前的 HyBench 采集没有规划器统计，其负载形态与此不同，不作为同一配置比较。

## HyBench SF10 校准（TP 8、XP 8 + 8 client，各 2 分钟）

2026-10-24 至 25。产物 `results/poc/hybench-htap-sf10/`，静止库清单 `results/poc/hybench-sf10/native/base-sibench_sf10_base.json`。

| 项 | 结果 |
| --- | --- |
| 静止库 | `base` 阶段原生 gendata 250 s 生成 `Data_10x`，上游 DDL 与逐表 `COPY` 326 s 装载 87,060,000 行（transfer 60,000,000） |
| 会话 / 负载 | 25 个会话；6,173 个提交事务、401 个回滚，164,841 次已提交行修改，`row_write_ratio` 0.0019 |
| 审计 | 行状态审计通过；PG 计数器对照 `consistent` |
| 访问投影 | 7,498,253 次，0 拒绝（limited_equality 7,241,925、equality 255,999、full_scan 317） |
| 规范 Trace | 7,948,694 个运行期操作；7,498,256 个查询，其中 7,306,934 个按快照读、106,445 个写后查询，全部快照交叉验证 |
| 体积 | 原始采集 341 MB（zstd），Trace 43 GB（`baseline.ops` 约 42 GB），Baseline Parquet 4.8 GB |
| 耗时（s） | base 582、prepare（克隆、二级索引、ANALYZE）287、导出 1,570、原生执行 438、行审计 1,348、账本 122、编译 873（含 Baseline 写出 362） |

同样 2 + 2 分钟，SF10 的写入量只有 SF1 的八分之一（SF1 8 client 为 1,357,123 次）：每个事务的查询扫得更多，阶段按时间截止，完成的事务更少。`plan-write-ratio` 据此把 TP 与 XP 各放大 26.4 倍到 53 分钟，作为 5% 写入比例的正式配置（`config/hybench_sf10.yaml`）。

导出与行审计是这一规模下的主要耗时，都是单线程逐行处理：导出阶段原来把数据读了三遍（写 Parquet、回读算摘要、`PgBaselineVerify` 再逐值比对 PG），核验每行都经 JSON 对象中转；行审计约四分之一时间等 PG 以 `row_to_json` 序列化整表，其余在 JSON 与时间戳解析。之后的运行已去掉回读一遍，并让核验按列直接比较。

## HyBench SF10 正式采集（5% 写入比例，TP / XP 各 53 分钟）

2026-10-25。配置 `config/hybench_sf10.yaml`，产物 `results/poc/hybench-htap-sf10-w5/`，静止库复用校准时建立的 `sibench_sf10_base`（`base` 阶段核对 8 张表行数与清单一致）。

| 项 | 结果 |
| --- | --- |
| 原生执行 | AP 78 s，TP 3,181 s，XP 3,182 s（8 个 TP、8 个 AT、8 个 IQ client） |
| 负载 | 167,545 个提交事务、11,943 个回滚；4,135,983 次已提交行写入，`row_write_ratio` **0.0475**（目标 0.05，由 2 + 2 分钟校准按 26.4 倍推出） |
| 审计 | 行状态审计通过；PG 计数器对照 `consistent` |
| 访问投影 | 191,117,060 次，0 拒绝（limited_equality 185,014,682、equality 6,095,012、full_scan 7,354） |
| 规范 Trace | 202,439,134 个运行期操作；191,117,063 个查询，其中 187,415,485 个按快照读、2,665,674 个写后查询；全部单元快照交叉验证 |
| 体积 | 原始采集 8.5 GB（zstd），Trace 70 GB |
| 耗时（s） | prepare 289、导出 1,146（校准时 1,570）、原生执行 9,307、行审计 6,372、账本 3,342、编译 5,755 |

原生执行之后的离线阶段合计约 4.3 h，比原生执行本身长：行审计（终态逐行比对 8,706 万行加 413 万次变化链）与账本、编译的 JSON 逐行解析（约 6 万事件 / 秒）是剩余热点。审计已改为按列比较 PG 终态（不再经 `row_to_json`），在 sysbench / YCSB 8 线程采集上重审，报告与已审计变化与原结果逐字节相同，耗时分别 81 → 68 s、480 → 380 s；本次正式采集仍用改动前的审计。

原生执行 9,307 s 中 HyBench 本身 6,441 s，其余 2,866 s 是之后逐个会话的客户端 / 服务端配对审计（单个 IQ 会话约 320 s）；之后会话审计已改为并行。

参考回放（128 client，两种 value，`config/index_replay_128clients.yaml`）通过：每种 value 191,117,063 个查询、187,415,485 个快照查询，核对 44,507,577,021 个结果项，locator 10,223 s、row_id 10,430 s，峰值常驻 31.8 GB。快照 oracle（`validate-snapshots`，40 GB 堆）在 63 分钟后内存溢出：每个区间探针各复制一份该表全部变化键的历史，内存为探针数 × 变化键数，属 oracle 实现问题，不涉及 Trace；按计划修复后在远程机的认证采集上验证，不再在这份旧代码产生的 Trace 上重跑。

## YCSB 87m 32 线程重采（2026-10-26）

原 87m YCSB raw 在此前的循环采集清理中删除，无法对 new24 做 compile-only；因此用 `results/profiles/ycsb_87m_n22.yaml`（32 线程、`plan-write-ratio` 0.05）对同一静止库重新采集，并以 `ycsb-87m-yl3` 的完整核验认证做 `certified` 编译。产物 `results/poc/ycsb-87m-n22/`。

| 项 | 结果 |
| --- | --- |
| 原生 | 1,572.1 s；原始采集 9.3 GB |
| 负载 | 81,653,024 个运行期操作；25,862,780 个提交事务（全部 utility-only）；295,019 插入、4,064,684 更新，`row_write_ratio` 0.05008 |
| 访问投影 | 25,567,761 次，0 拒绝（equality 24,163,595、limited_range 1,404,166） |
| 审计 | `certified`，认证 `ycsb-87m-yl3`；PG 计数器 `consistent`；未重跑行审计 |
| 编译 | 旧 jar 561.02 s（基线 36.23 / 负载 520.62）；new24 553.71 s（25.31 / 524.29）；两者 16 个 Trace 文件逐字节相同 |
| 体积 | Trace 4.1 GB；Baseline 共享 66 GB |
| 合计 | 原生 + 编译约 35.4 min |

该次采集的 raw 与 Trace 保留；编译阶段的对照细节见 [编译阶段对照](compile-optimization-20261026.md)。

## 产物清理

2026-10-25 起采集改在远程机运行，开发机只保留四份完整产物作为编译与回放的回归基准：`hybench-htap-sf10-w5`、`hybench-htap-sf1-c8-20261024`、`sysbench-8m-c8-20261024`、`ycsb-8m-c8b-20261024`。其余 `results/poc/*` 删除了全部大于 1 MB 的文件（原始追踪、Trace、Baseline、`Data_<sf>`、运行用 jar 副本），清单、报告与日志保留；开发机 PG 中的采集库与 SF10 静止库一并删除，本文与其他验证文档中引用这些目录的数据文件已不在。
