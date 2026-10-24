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

## 有界状态

| 改动 | 验证 |
| --- | --- |
| 行审计：按（表、主键、会话、位置）外部排序，与基线、PG 终态三路归并 | sysbench 真实采集重跑：提交 / 回滚数、各表计数、61,999 条变化（按集合比较）与旧报告一致，峰值内存约 2.1 GB（`-Xmx12g` 下）；单测中预算 1 字节与默认预算的报告和变化文件逐字节相同，终态不一致仍被发现 |
| 编译：行身份落盘、提交位置有界窗口、工作单元只计数、增量 Parquet rollover | 单测：预算 1 与默认预算的 Trace 逐字节相同；窗口外的快照明确报错；rollover 产出 3 个唯一 FileID 的文件并通过回放与 oracle |
| 参考模型：基线外部排序为磁盘 run，版本历史落入有序可落盘 map | 单测：与全内存模型在随机读写、区间、降序、limit 下逐项相同；真实 sysbench Trace（181,000 个查询）强制落盘（基线预算 300 万条、历史预算 2,000 条）回放，所有校验和与全内存回放逐项相同，耗时约 2.4 倍 |
| Locator 为（FileID, 行位置）两个 int64 | 单测：两个字都超过原 32 位打包宽度仍区分，relocate 与更新保留 |

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
- 采集耗时未优化：本轮 `run-timing.json` 中导出与行审计仍远超编译与账本。
