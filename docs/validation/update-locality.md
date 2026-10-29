# 更新局部性与静态文件重叠

更新：2026-10-29。仅分析 **acceptance-20261029T053122Z** 的三份新 Trace，应用与输入身份见 [完整验收](acceptance.md)。输出为直接分布与静态模型估算；真实引擎 flush / compaction 的成本见 [测量结果](measurement.md)，不从本页推导引擎写放大。

## 布局、窗口与计数

采用 [update_locality.yaml](../../config/update_locality.yaml)：每个来源主键或 `(二级值, PK)` 按保序键排序，每 256 条目一个 block、每 4,096 block 一个等条目数 L1 文件。这些不是 Parquet row group、数据文件或真实 SST 边界。Baseline 域外的新键单列 overflow，不扩写静态域。

每窗 10,000 次业务行修改，UPDATE 计一次；不同主键数由 64 位指纹估计，不把它等同累计修改数。二级旧键失效、版本刷新等维护操作另计。L0 批次累计 10,000 次所有索引维护，完整维护单元不拆开，可能略超目标；不按同键去重，也没有模拟真实 flush 的压缩字节。

每个 index-batch 同时报实际命中 block / 文件并集与 `min..max` 键范围覆盖，二者不能替换；下表平均以 CSV 的 index-batch 行等权计算。热力图是 block 范围 bin 聚合，不能恢复精确 SST 的边界。

## 本轮修改分布

| 来源 | 有效索引操作 | 查询操作 | 维护操作 / 比例 | 变化主键 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 71,630,440 | 59,535,140 | 12,095,300 / 16.89% | 2,767,452 |
| YCSB 87m | 29,883,016 | 25,528,798 | 4,354,218 / 14.57% | 2,318,947 |
| sysbench 87m | 22,679,808 | 12,709,845 | 9,969,963 / 43.96% | 3,378,320 |

有效索引操作排除事务控制记录；维护比例是逻辑索引操作比例，包含版本刷新和旧键失效，不是 PG 物理索引写比例。变化主键来自独立 Parquet oracle；同一主键多次更新 / 删除重插入只计一个键。因此相同的 5% 累计业务修改目标不代表相同的不同键覆盖或索引写比例。

Trace 的主要访问形态：HyBench 为多表主键 / 二级等值及 LIMIT 读，另有少量大扫描；YCSB 为主键点读 / 更新及带 LIMIT 的范围扫描，无二级；sysbench 为主键点 / 范围查询与主键 / k 索引维护，原生键抽样配置为 uniform。`workload.ops` 是这些索引操作与事务控制的二进制记录，工作单元保存来源请求身份，增量 Parquet 保存行版本；它们不恢复完整 SQL 端到端执行或原始到达时序。

| 来源 | 业务 INSERT / UPDATE / DELETE | 二级 INSERT / UPDATE / DELETE | 窗口数 / 尾窗修改数 |
| --- | --- | --- | --- |
| HyBench SF10 | 1,486,981 / 3,066,584 / 0 | 4,431,832 / 3,095,139 / 14,764 | 456 / 3,565 |
| YCSB 87m | 294,698 / 4,059,520 / 0 | 0 / 0 / 0 | 436 / 4,218 |
| sysbench 87m | 912,862 / 2,527,926 / 912,212 | 912,862 / 2,527,926 / 2,176,175 | 436 / 3,000 |

三组累计业务修改均达到 Baseline 的 5%，但只表示修改次数，不表示有 5% 不同行被修改；10% / 50% 检查点均未达到，不循环 Trace 补量。1% / 5% 的实际到达窗口见 `summary.json` 的 progress_checkpoints，尾窗保留实际计数。

YCSB 仅主键，二级维护为零；sysbench 有主键 / k 维护，但原生标准脚本不按 k 查询；HyBench 的多表插入、索引列更新与新增域外键形成另一种分布。不能按来源总行数推定形态均衡。

## 整轮 UPDATE 集中度

从 `blocks.csv.gz` 选主键（index=0）的 `business_update`，跨全部窗口按 `(table, block)` 汇总，再在所有表的基线主键 block 中按 UPDATE 次数排序。每 block 为 256 条基线记录；最热 1% / 10% 取 `ceil(基线 block 总数 × 比例)`，分母为能映射到该静态域的 UPDATE 总数。各表未命中 block 仍计入总域，空闲 block 的次数为零。

| 来源 | 基线主键 block 数 | 纳入统计的 UPDATE | 最热 1% block 承担比例 | 最热 10% block 承担比例 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 340,081 | 3,066,531 | 62.80% | 99.63% |
| YCSB 87m | 340,079 | 4,059,520 | 35.56% | 51.15% |
| sysbench 87m | 340,080 | 2,527,926 | 2.11% | 16.87% |

HyBench 共 3,066,584 次 UPDATE，其中 53 次不在静态基线域内，未纳入上表分母。UPDATE 的 96.55% 落在 savingaccount；仅在该表内部排序，最热 1% block 承担 49.27% 的 UPDATE。因此全来源集中度同时反映表间倾斜和表内热点。YCSB 的热点较明显，sysbench 更新更分散；事后选择最热 block，即使 uniform 也不要求最热 1% 恰好承担 1% 的样本。

该指标只统计 UPDATE，排除 INSERT / DELETE 和二级维护；是整轮事后选择的 block 集中度，不是精确热点键占比、原生热点配置值、每窗 top share 的平均值或相邻窗口 `hot_overlap`。它也不等同约 5% 的累计业务修改比例。原始 CSV / summary 的 SHA-256、各表统计和整数分子保存在本轮 `report-details.json`，可用 `summarize_report_details.py` 中的独立 Python 汇总复核，未重复原生采集或服务回放。

## 静态 L0→L1 重叠

| 来源 | L0 批次 | index-batch 行 | 平均命中 / 范围 block | 平均命中 / 范围文件 | 范围文件多于命中文件的行 |
| --- | --- | --- | --- | --- | --- |
| HyBench SF10 | 1,210 | 25,147 | 186.10 / 35,603.25 | 6.19 / 9.27 | 6,379 |
| YCSB 87m | 436 | 436 | 7,618.66 / 339,985.88 | 83.97 / 83.97 | 0 |
| sysbench 87m | 997 | 15,952 | 485.87 / 40,851.17 | 10.61 / 10.61 | 0 |

代表索引：

| 来源 | 索引 | 平均命中 block | 平均范围 block | 平均命中文件 | 平均范围文件 |
| --- | --- | --- | --- | --- | --- |
| HyBench SF10 | idx_transfer_1 | 21.69 | 202,678.25 | 14.11 | 50.48 |
| YCSB 87m | usertable_pkey | 7,618.66 | 339,985.88 | 83.97 | 83.97 |
| sysbench 87m | sbtest1_pkey | 427.60 | 39,347.73 | 10.23 | 10.23 |
| sysbench 87m | k_1 | 541.09 | 42,354.14 | 11.00 | 11.00 |

这些观测说明，命中 block 较少时，最小到最大键仍可跨越多数文件；文件级重叠不能从 block 命中比例直接推出。YCSB 的静态主键域有 340,079 个 block、84 个文件；sysbench 每张表的索引为 42,510 个 block、11 个文件，粗文件粒度本身会压低区分度。HyBench 各索引的域大小和 overflow 见完整 CSV，不能把某一个索引的结果推广到全部索引。

`UpdateLocalityTest` 的手算反例独立于真实负载：16 个 block、4 个文件，两端远隔热点只命中 2 个 block / 2 个文件，但跨度覆盖 16 个 block / 4 个文件；固定热点相邻窗口重合度为 1、移动热点为 0。空 workload 只写表头，不制造空窗口或 L0 批次。这些断言保留在本轮通过的根测试中。

## 分析资源与产物

| 来源 | wall 秒 | CPU 秒 | RSS GiB | 分析目录采样峰值 GiB | 分析收尾输出 MiB |
| --- | --- | --- | --- | --- | --- |
| HyBench SF10 | 353.05 | 799.61 | 22.25 | 11.30 | 28.19 |
| YCSB 87m | 74.50 | 203.19 | 17.16 | 3.53 | 13.89 |
| sysbench 87m | 209.15 | 415.31 | 16.77 | 5.70 | 66.89 |

GNU time 为单次分析，包含重新派生 Baseline 和排序；RSS 为该 JVM 峰值。目录峰值来自应用 5 秒采样，包含排序临时文件，可能漏过短暂峰值；收尾输出是 CSV / JSON，不含随后绘图。它们不是服务端 compaction 的时间、内存或 SST 字节。

[本轮目录](../../results/validation/acceptance-20261029T053122Z/) 中各来源的 `locality/` 保留 `windows.csv`、`blocks.csv.gz`、`heatmap.csv.gz`、`heatmap-kind.csv.gz`、`l0-batches.csv`、`summary.json`，以及对应 `locality.time` / `locality-timing.json`。图表由 [plot_update_locality.py](../../scripts/plot_update_locality.py) 从这些输出生成，文档选用代表索引，完整数值仍可重新绘图。

```bash
java -Xmx24g -jar build/sibench/sibench.jar analyze-locality \
  TRACE_DIRECTORY config/update_locality.yaml OUTPUT_DIRECTORY --timing TIMING_JSON
python3 scripts/plot_update_locality.py OUTPUT_DIRECTORY --out OUTPUT_DIRECTORY/plots
```

完整 Trace 在各来源主机，输出目录必须新建；本地已同步分析数据。代表图为 HyBench `idx_transfer_1`、YCSB `usertable_pkey`、sysbench `sbtest1_pkey` / `k_1` 的热力图、覆盖曲线与范围重叠图。

代表范围重叠图：[HyBench](../../results/validation/acceptance-20261029T053122Z/hybench/locality/plots/l0-overlap-idx_transfer_1.png)、[YCSB](../../results/validation/acceptance-20261029T053122Z/ycsb/locality/plots/l0-overlap-usertable_pkey.png)、[sysbench 主键](../../results/validation/acceptance-20261029T053122Z/sysbench/locality/plots/l0-overlap-sbtest1_pkey.png)与 [k 二级](../../results/validation/acceptance-20261029T053122Z/sysbench/locality/plots/l0-overlap-k_1.png)。相同目录保留对应 SVG，完整 CSV 可重绘其他索引。

## 解释边界

静态布局没有真实 SST 的压缩、block cache、动态 L1、compaction 选择与输出布局，不计算引擎实测写放大、回收收益或费用。时间窗口仅按固定业务修改数，没有恢复可靠原始墙钟；`hot_overlap` 为相邻修改窗口的集合重合，不是时间稳定性。来源输入、版本深度和数据分布均限本轮，长期历史、Rewrite / GC 和更大规模仍按 [计划](../../plan.md) 单独验收。
