# 采集完整性对照与有界状态验证

2026-10-23 对三个来源重新采集（同一份 PG 补丁、同一入口 `scripts/native-capture.sh`），在新产物上验证采集完整性对照、区间 oracle 以及有界状态的改动。这是正确性验证，不是性能结论。

## 产物位置

| 来源 | 产物目录 | 采集日志 |
| --- | --- | --- |
| HyBench 并发 HTAP | `results/poc/hybench-htap-sf1/` | `results/poc/native-trace-20261023/hybench_sf1-v9.log` |
| YCSB | `results/poc/ycsb-8m/` | `.../ycsb_8m-v9.log` |
| sysbench | `results/poc/sysbench-8m/` | `.../sysbench_8m-v9.log` |

上一轮（2026-10-23 中午）的同名产物原样改名为 `results/poc/native-trace-20261022/superseded/<名称>-20261023-v9/`，其采集库改名为 `<库名>_20261023_v9`；内容未改写。这些旧产物没有采集前后的计数器快照，不能用于完整性对照，也不用于任何比较。

## 采集完整性对照

原生执行前后各取一次 `pg_stat_user_indexes` 快照（等其它会话退出且两次读数一致后才记录），`check-capture-completeness` 按 PG14 计数位置核对账本。三来源均为 `consistent`，没有不一致的索引。

| 来源 | 参与对照的索引 | `idx_tup_read` = 候选 | `idx_tup_fetch` = 回表可见行 | `idx_scan` 在下降上下界内 |
| --- | ---: | --- | --- | --- |
| HyBench | 21 | 全部相等 | 全部相等 | 全部在界内，一个有数组键的索引落在区间内（18，界 [3, 18]） |
| YCSB | 1 | 1,304,112 | 1,294,028 | 346,372（界内） |
| sysbench | 16 | 全部相等 | 全部相等 | 全部在界内；`k_1`–`k_8` 无访问，按 0 对照 |

HyBench 上 `idx_tup_fetch` 小于候选数且不等于账本的可见行：`idx_loanapps_1` 候选 2,070,231、回表可见行 1,710,231；纯 bitmap 路径的索引（如 `idx_transfer_3`，17,408,201 个候选）`idx_tup_fetch` 为 0。bitmap 堆检查不经过 `index_fetch_heap`，所以比较对象是账本的 `heap_fetch_visible`，而不是 `visible_rows`。

边界：PG14 统计经 UDP 上报，丢失只会表现为计数低于账本；本轮未出现。尚未做故障注入（人为丢弃事件）与插桩前后开销对比，所以只能说明在本次负载下独立计数与账本一致，不能说明各类事件丢失都能被发现。

## 区间 oracle

`validate-snapshots` 在新产物上通过，全部使用 JDK 17。

| 来源 | 修改主键 | 区间探针（索引 × 区间） | 区间探针执行次数 / 模式 | 原生查询 / 模式 |
| --- | ---: | ---: | ---: | ---: |
| HyBench | 4,636 | 874 | 3,268 | 3,004,751 |
| YCSB | 41,649 | 42 | 158 | 345,827 |
| sysbench | 48,751 | 672 | 2,623 | 181,000 |

Locator 与 RowID 两种模式的结果相同。HyBench 的 3,004,751 个原生查询、23,676,588 个被核对的结果项。

## 重编译回归

用改动后的代码对同一份原始采集重新编译，与采集时的 Trace 逐文件比较。

| 来源 | 结果 |
| --- | --- |
| sysbench | `baseline.ops`、`workload.ops`、`work-units.jsonl` 与全部增量 Parquet 逐字节一致 |
| HyBench（并发） | 同上，包括 11 个文件 |

两者的 `manifest.json` 仅多出 `snapshot_crosscheck.commit_window`（窗口上限与淘汰数）一项，其余字段相同。

## 同遍账本、计数器核对与认证

2026-10-25。编译改为同遍构建账本并在提交前核对 PG 计数器，编译开头不再重跑会话审计（只核对运行清单中已记录的审计与文件摘要），Trace 文件在写入时计算摘要。用改动后的代码重编三份按设计并发的采集（`--counters` 取各自的计数器快照），与原产物比较：

| 来源 | Trace 数据文件 | 账本（不计输入摘要字段） | 计数器核对 | 编译耗时（s，原 → 现） |
| --- | --- | --- | --- | --- |
| sysbench 8 线程 | 11 个逐字节一致 | 相同 | 一致，与原报告相同 | 账本 8 + 编译 48 → 41 |
| YCSB 8 线程 | 4 个逐字节一致 | 相同 | 一致，与原报告相同 | 账本 18 + 编译 117 → 109 |
| HyBench SF1 并发（rows） | 11 个逐字节一致 | 仅一个模板的显示文本不同 | 一致，与原报告相同 | 账本 986 + 编译 1,615 → 1,080 |

HyBench 的差异来自同一 `query_id` 的两种文本写法：单独的账本按文件名顺序读、编译按完成计数器顺序读，先见到的文本不同；显示文本已改为取字典序第一个，与读取顺序无关。编译的负载阶段因同遍构建账本多 6–27%，编译开头的核验从 686 s 降到 154 s（剩余为输入摘要）。

单元测试覆盖：编译产出的账本与独立账本相同；计数器不一致时 Trace 保持未完成并写出核对报告；认证由完整核验的采集写出，同一采集凭认证（不带行审计）编译出相同的 `workload.ops`，删去一条访问路径或换一个构建即编译失败。真实采集上的认证流程待远程机的校准采集验证。

## 有界状态

| 改动 | 验证 |
| --- | --- |
| 行审计：按（表、主键、会话、位置）外部排序，与基线、PG 终态三路归并 | sysbench 真实采集重跑：提交 / 回滚数、各表计数、61,999 条变化（按集合比较）与旧报告一致，峰值内存约 2.1 GB（`-Xmx12g` 下）；单测中预算 1 字节与默认预算的报告和变化文件逐字节相同，终态不一致仍被发现 |
| 编译：行身份落盘、提交位置有界窗口、工作单元只计数、增量 Parquet rollover | 单测：预算 1 与默认预算的 Trace 逐字节相同；窗口外的快照明确报错；rollover 产出 3 个唯一 FileID 的文件并通过回放与 oracle |
| 参考模型：基线外部排序为磁盘 run，版本历史落入有序可落盘 map | 单测：与全内存模型在随机读写、区间、降序、limit 下逐项相同；真实 sysbench Trace（181,000 个查询）强制落盘（基线预算 300 万条、历史预算 2,000 条）回放，所有校验和与全内存回放逐项相同，耗时约 2.4 倍 |
| Locator 为（FileID, 行位置）两个 int64 | 单测：两个字都超过原 32 位打包宽度仍区分，relocate 与更新保留 |

## 汇总采集格式

2026-10-24 插桩改为按访问记录（`capture.detail: summary`，见 [Trace 契约](../contracts/trace.md)）。验收方法：同一次原生执行用 `capture.detail: rows` 采集，PG 同时写出逐行事件与汇总，账本用逐行事件计算并要求每条 `scan_summary`、`bitmap_verdicts` 及每次执行返回的行数与之完全相等；再从这份采集删去逐行事件得到只含汇总的副本（`/tmp/sibench-tools/derive_summary_capture.py`，副本不是运行证据），两份分别审计、建账本、核对 PG 计数器并编译，比较 Trace。

| 来源 | 产物目录 | 结果 |
| --- | --- | --- |
| sysbench | `results/poc/sysbench-8m-rows-20261024/`（副本 `raw-summary-derived/`、`trace-summary-derived/`） | 账本逐条核对通过，181,000 次投影访问、0 拒绝；两份的 PG 计数器对照均 `consistent`；`baseline.ops` 与 8 个增量 Parquet 逐字节相同；`workload.ops` 438,998 条记录除 `sourceEvent` 与 `transactionId`（原始文件行号，后者一一对应于 58,000 个事务）外逐字段相同；`work-units.jsonl` 除 `source_event` 外相同；行审计的变化集合除原始位置外相同 |
| YCSB | `results/poc/ycsb-8m-rows-20261024/` | 账本逐条核对通过，346,138 次投影访问、0 拒绝；两份计数器对照均 `consistent`；`baseline.ops` 与增量 Parquet 逐字节相同；`workload.ops` 1,105,329 条记录、350,082 个事务除原始行号外逐字段相同 |
| HyBench 并发 HTAP | `results/poc/hybench-htap-sf1-rows-20261024/`（副本 `raw-summary-derived/`、`trace-summary-derived/`，逐行格式的 Trace 在 `trace-rows/`） | 账本逐条核对通过：103,108,136 个事件、3,412,523 次投影访问（含 23 个 BitmapAnd 交集组、并集组、二级提前停止扫描），0 拒绝；两份计数器对照均 `consistent`；`baseline.ops` 与 8 个增量 Parquet 逐字节相同；`workload.ops` 3,456,924 条记录、587 个事务除原始行号外逐字段相同，402,417 次快照读与 3,412,523 个单元的快照交叉验证一致；原始体积 38 GB → 3.1 GB |

另以 `capture.detail: summary` 独立采集一次 sysbench（`results/poc/sysbench-8m-summary-20261024/`），计数器对照 `consistent`、0 拒绝。各阶段对比（毫秒）：

| 采集 | 原始事件 | 原始体积 | 原生执行 | 行审计 | 账本 | 编译 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10-23 逐行（旧插桩） | 10,756,381 | 3.5 GB | 232,423 | 184,397 | 50,624 | 49,623 |
| 10-24 rows | 10,939,380 | 3.7 GB | 215,305 | 163,306 | 51,763 | 53,804 |
| 10-24 summary | 1,525,974 | 484 MB | 72,245 | 80,656 | 7,897 | 7,844 |

两次独立执行的 Trace 不逐字节相同：sysbench 以 8 线程并行 prepare，sbtest3 / sbtest4 的基线行摘要在两次运行间不同，负载里的 `k` 值随之不同；所以格式等价只以同一次执行的 rows 与派生副本对照为准。

2026-10-24 起后端追踪默认写成 zstd 流（`capture.compression: zstd`）。验收：把 sysbench 8 线程采集的 48 个明文后端文件用 zstd 命令行压成副本（484 MB → 47 MB），账本除输入文件清单外与明文完全相同；`sysbench_small` 以插桩直接写 zstd 采集一次，审计、账本、计数器对照（`consistent`）与编译全部通过（`results/poc/sysbench-small-zstd-20261024/`）。帧未结束的文件读取失败由 `ServerTraceTest` 覆盖。

逐行采集只用于验收：HyBench rows 采集原始体积 38 GB，默认 12 GB 堆在编译的会话归并处内存不足（一个 AP 事务片段含上百万逐行事件），以 36 GB 堆重编；summary 采集不受影响。这次对照还发现并修复了一个问题：并发归并为每个事务片段新建访问划分，原先只有文件的第一个片段带格式声明，后续片段被当作逐行格式而拒绝汇总事件；现在格式按文件延续，`IndexTraceCompilerTest.concurrentSummaryCapturesCompileLikeTheirRows` 覆盖该路径。YCSB 派生副本仍有 3.5 GB，因为客户端 JDBC 文件是原样复制的（含每行列文本），实际 summary 采集的客户端只记哈希。

## 产物清理记录

2026-10-23 重新采集后，旧产物不再用于任何比较（缺少计数器快照，且格式已从 v8 变为 v9），按用户确认回收其重量级载荷：

- `results/poc/native-trace-20261022/`（含 `superseded/`）与 `results/poc/sysbench-small-20261021/`：删除全部 `raw*` / `trace*` 目录及 `*.jsonl`、`*.parquet`、`*.ops`、`*.jar`、`*.tsv`、`*.bin`；共 639 个文件，回收 327 GiB。
- `results/poc/native-trace-20261023/recompile-sysbench`、`recompile-hybench`：重编译回归副本，已逐字节比对完毕（结论见上文「重编译回归」），整目录删除。
- 保留：每份归档运行的 `manifest.json`、`trace-summary.json`、`access-ledger-summary.json`、`row-state-audit-summary.json`、`run-summary.json`、`run-timing.json`、`schema-preflight.json`、`verified-manifest.json`、`baseline.log`、`*.props`、`native/` 下除 jar 外的文件，以及 `native-trace-20261022/` 下的全部日志与 yaml。这些共约 6 MiB，仍能说明每次运行的事实与结果。
- 未改动：当前三份产物（`hybench-htap-sf1`、`ycsb-8m`、`sysbench-8m`）、HyBench 原生工作目录 `hybench-sf1`，以及 `sibench_sf1_baseline_check` 采集源库。

清单与日志保留、只删载荷，是因为 [仓库约定](../../AGENTS.md) 要求历史日志与清单按记录保留。删除不影响 git：`/results/` 已在 `.gitignore` 中。

## 不代表

- 不是容量验收：落盘路径只在 SF1 规模的输入上与内存路径对照，没有在超过内存的输入上跑过。
- 编译的提交窗口默认 400 万个写事务；更早的快照会直接失败，而不是静默读到「无提交」。
- 分段续编译、Checkpoint 与恢复尚未接入编译与回放；`SpillingMap` 只提供由文件清单恢复的能力。
- 汇总格式之后，Baseline 导出（sysbench 约 300 s、HyBench 约 400 s）仍未优化；行审计只在完整核验（认证）时运行。
