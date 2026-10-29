# 更新局部性与静态 L0→L1 重叠估算

更新：2026-10-28。对已有 HyBench SF10、YCSB 87m、sysbench 87m Trace 做静态分析；不重采 PG，不模拟真实 LSM compaction，也不把静态 block / 文件估算当作引擎读写字节省。

## 输入与身份

| 来源 | Trace | Baseline 条目 | 逻辑身份 | 业务修改 | 分析窗口 |
| --- | --- | ---: | --- | ---: | ---: |
| HyBench SF10 | `sibench_1:/data/sibench/results/poc/hybench-sf10-hl9/trace-p0` | 312,120,000 | `workload=7e06ba49…` / `baseline=8b95df69…` | 5,431,550 | 544 |
| YCSB 87m | `sibench_2:/data/sibench/results/poc/ycsb-87m-n22/trace` | 87,060,000 | `workload=9ec9cdc9…` / `baseline=0afda377…` | 4,359,703 | 436 |
| sysbench 87m | `sibench_3:/data/sibench/results/poc/sysbench-87m-sl16/trace-current` | 174,120,000 | `workload=b8b9045e…` / `baseline=9587cc6f…` | 4,352,482 | 436 |

分析 jar `8517f6c5…`（与最终源码构建 `cfa47f0d…` 主代码相同，只差测试辅助）。每份分析都从 Trace manifest 读取 `(table,index)` 目录，使用 `BaselineOperations` 重新派生 Baseline 并校验其摘要；输入 Trace / Baseline 的身份与 [三来源预检](p0-three-source-audit.md) 一致。YCSB 只有 PK 索引，secondary 统计为 0；HyBench / sysbench 分别有 21 / 16 个索引。

## 静态布局与窗口规则

- **布局**：每个 `(table,index)` 独立按逻辑键排序，`entries_per_block=256` 个条目一个 block，`blocks_per_file=4096` 个 block 一个文件；首尾不足按实际条目处理。这里只是等条目数近似，不是 Parquet row group、真实 SST 字节或压缩边界。布局参数在 [config/update_locality.yaml](../../config/update_locality.yaml)。
- **窗口**：按已提交行修改（业务修改）固定 10,000 次一个窗口；主键维护记录计一次业务修改，同一业务修改的索引展开不拆开。窗口边界只由业务修改数决定，不按提交序号换算时间。
- **业务 / 索引**：`business` 是主索引维护（每行业务修改一次）；`secondary_index` 是二级索引维护。`all_index_operations = business + secondary_index`，主索引不会在两张表里重复相加。二级改键产生旧键 `secondary_index_delete` 和新键 `secondary_index_insert/update`。
- **越界**：新键小于 baseline 最小值计入 `overflow_left`，大于最大值计入 `overflow_right`；已有区间内的新键按声明的有序 range 规则落入对应 block。HyBench transfer 的新增主键就是实际越界样本（约 3,231 次/首窗）。
- **唯一键**：窗口内按 64-bit `KeyHash` 指纹计数，不保存全部键字节；这是近似唯一数，不是精确字节集合。
- **热点重叠**：每个窗口取**实际命中 block** 中修改次数最高的 1%（按次数降序、block 升序并列），与上一窗口同一 index 的集合计算 Jaccard；首个窗口为 `-1`（无前窗）。累计覆盖按该 index 的 block 位图计算。
- **L0 批次**：按全部索引维护操作的连续条目数形成假定 L0 文件，初版 10,000 条；达到阈值后在下一次源维护单元开始前收批，保证一个维护单元不被拆开；不做同键去重，也没有真实 flush 的排序 / 压缩字节。每条记录同时给“实际命中文件并集”和“min..max 键范围覆盖文件数”，两者不能互相替代。
- **热力图**：`heatmap.csv.gz` 是 business / secondary_index 的 bin 聚合；`heatmap-kind.csv.gz` 保留操作类型的 bin 聚合；`blocks.csv.gz` 保留原始 block / 类型计数。bin 把有序 block range 等分，不能还原为精确 SST 边界。

## 2.1 修改分布

| 来源 | 业务 INSERT / UPDATE / DELETE | 二级 INSERT / UPDATE / DELETE | 1% 检查点 | 5% 检查点 | 10% / 50% |
| --- | --- | --- | --- | --- | --- |
| HyBench SF10 | 1,774,420 / 3,657,130 / 0 | 5,288,559 / 3,690,588 / 17,328 | window 88，880,000 次 | window 436，4,360,000 次 | N/A |
| YCSB 87m | 295,019 / 4,064,684 / 0 | 0 / 0 / 0 | window 88，880,000 次 | window 436，4,359,703 次 | N/A |
| sysbench 87m | 912,753 / 2,527,624 / 912,105 | 912,753 / 2,527,624 / 2,175,917 | window 88，880,000 次 | N/A（总修改 4,352,482 < 4,353,000） | N/A |

检查点按累计业务修改 / Baseline 行数计算；1% 和 5% 是“累计行修改次数”达到 1% / 5%，不是 1% / 5% 的不同行被修改。三份输入都没有达到 10% / 50%，因此没有用 Trace 循环或重复修改补量。

每个 source 的 `windows.csv` 给出逐窗口的命中 block / 文件比例、覆盖 50% / 90% 修改所需最少 block、Top 1% / 10% block 承载比例、溢出量、相邻窗口热点重合度和累计 block 覆盖。例如：

- HyBench `idx_transfer_1` 平均每窗命中 14.14 / 58 个 L1 文件，但 `min..max` 范围平均覆盖 50.75 / 58 个文件；每窗平均命中 21.7 个 block，范围跨度约 203,884 个 block。
- YCSB `usertable_pkey` 平均命中 7,629 / 340,079 个 block（约 2.2%），却触达 83.94 / 84 个 L1 文件；436 个 L0 批次中 0 个满足“范围文件数少于命中文件数”。
- sysbench `sbtest1_pkey` 平均命中 427.6 / 42,510 个 block，范围跨度约 39,350 个 block；`k_1` 平均命中 541.1 个 block，范围约 42,352 个 block。L1 只有 11 个文件，因此文件级重叠没有区分度，block 级局部性不能直接推导为 compaction 文件范围小。

## 2.2 静态 L0→L1 重叠

每来源的 L0 批次数和重叠摘要：

| 来源 | L0 批次数 | 每批平均命中 block / 范围 block | 每批平均命中文件 / 范围文件 | 范围文件多于命中文件的 index-batch 行 |
| --- | ---: | --- | --- | ---: |
| HyBench SF10 | 1,443 | 186.3 / 35,664.8 | 6.20 / 9.29 | 7,654 / 29,944 |
| YCSB 87m | 436 | 7,629.2 / 339,983.6 | 83.94 / 83.94 | 0 / 436 |
| sysbench 87m | 997 | 485.8 / 40,850.4 | 10.61 / 10.61 | 0 / 15,952 |

- HyBench 的二级索引是最清晰的“命中少、范围宽”样本：`idx_transfer_1` 平均命中 14.14 个文件，范围覆盖 50.75 个文件，最大范围覆盖全部 58 个 L1 文件。这说明“命中 block 少”不能推出“L0 只与少数 L1 文件重叠”。
- YCSB 每批 10,000 条随机更新命中约 7,600 个 block，但 min..max 几乎必然跨越全部 84 个 L1 文件；实际命中文件也已经接近全部，文件级 compaction 无法因 block 局部性缩小。
- sysbench 的 L1 只有 11 个文件，范围与命中文件都接近全部；在更细的 SST 文件布局下会表现出与 YCSB 类似的结论，但本轮没有真实文件布局，不能外推具体字节数。

**受控反例**：`UpdateLocalityTest` 用 16 个 block、4 个文件的手算布局验证了均匀、固定热点、移动热点、两端远隔热点和新增越界，并用空 workload 验证空窗口 / 空 L0 批次只写表头。两端远隔热点两个命中 block 的跨度 16 个 block、范围覆盖 4 个文件，而实际命中只有 2 个文件；固定热点相邻窗口重合度为 1，移动热点为 0。受控数据不计入真实负载证据。

## 能耗与资源

| 来源 | wall | CPU user+sys | 峰值 RSS | 分析目录峰值 | 最终分析字节 |
| --- | ---: | ---: | ---: | ---: | ---: |
| HyBench SF10 | 346.93 s | 741.86 s | 24.01 GB | 12.13 GB | 35.2 MB |
| YCSB 87m | 76.28 s | 201.99 s | 15.56 GB | 3.79 GB | 14.6 MB |
| sysbench 87m | 208.28 s | 399.31 s | 22.19 GB | 6.12 GB | 70.1 MB |

时间为单次静态分析（重新派生 Baseline 并排序）；峰值磁盘来自 `RunTiming` 对输出目录的 5 秒采样，含排序临时文件，最终目录只保留 CSV / JSON / 图。它不是服务端 compaction 时间，也不代表真实 SST 字节。

## 复现

```bash
java -Xmx24g -jar build/sibench/sibench.jar analyze-locality \
  results/poc/<trace> config/update_locality.yaml results/poc/update-locality-<source> \
  --timing results/poc/update-locality-<source>/timing.json
python3 scripts/plot_update_locality.py results/poc/update-locality-<source>/analysis \
  --out results/poc/update-locality-<source>/analysis/plots
```

输出：`windows.csv`、`blocks.csv.gz`、`heatmap.csv.gz`、`heatmap-kind.csv.gz`、`l0-batches.csv`、`summary.json` 和 PNG / SVG 图。完整结果：

- [HyBench SF10](../../results/poc/update-locality-hybench-sf10/analysis/summary.json)
- [YCSB 87m](../../results/poc/update-locality-ycsb-87m/analysis/summary.json)
- [sysbench 87m](../../results/poc/update-locality-sysbench-87m/analysis/summary.json)

## 边界

- 静态布局是等条目数近似；没有真实 SST 的 key 分布、压缩、block cache、L1 动态演化、compaction 选择、输出布局或后台字节。本分析是工作负载特征描述，不再与引擎实测对照；真实 flush / compaction 作为后端维护成本在服务回放中记录（每次 Flush 后的 `<index>.engine-stats.jsonl`，见 [rocksdb-cloud 验证](rocksdb-cloud.md)）。
- 只报告“直接分布 / 静态模型估算”，不报告引擎实测写放大；命中 block 少不能推导 compaction 次数或写字节少。
- HyBench 的 transfer 新主键越界、YCSB 的纯随机更新、sysbench 的 11 文件 L1 是不同形态；不能把某一来源的局部性结论推广到其他来源或二级索引。
- 时间窗口只支持固定业务修改数；没有保留可靠原始墙钟，因此不添加墙钟窗口。