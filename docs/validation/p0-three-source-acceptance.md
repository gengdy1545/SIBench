# P0 三来源完整验收与容量结论

更新：2026-10-29。保留三来源验收所需的输入身份、完整结果、独立 oracle、组合矩阵与容量边界。数值绑定记录中的实验构建，不把小测试或另一构建的结果冒充当前全量实测。

## 服务验收与输入身份

### 身份与证据复用

- jar `e126693f678d6b659dfa15c837d4ca43bd6d97c1ba9c8b2a4c090b63d84d88bd`，本地 RocksDB 9.1.1。普通查询分块响应、Baseline 批量写与最小 index manifest 已合入。该构建的根 Maven 测试为 315 项通过。
- 输入与原生采集认证保持不变；三来源完整参考回放、独立 Parquet oracle 与小输入组合矩阵已通过，报告在 `p0-accept-final-20261028`、`p0-small-matrix-20261028`，不因 adapter 变化重采 PG 或重跑未改变的参考模型。
- 每来源新建索引，完整提交发布、64 client、operation、二级开启、row_id、4,096 在途、120 s 请求超时，逐查询摘要取自已验收 sidecar。每个运行目录保存 jar / manifest / 参考报告身份、有效配置、两端 `/usr/bin/time -v`、驱动 `--timing`、30 s 索引 / 服务 RSS 采样和 index manifest。
- 证据：[最终本地目录](../../results/poc/p0-final-20261028/)。HyBench 原报告与 sidecar 在 `sibench_1:.../p0-accept-final-20261028/hybench/`，sysbench / YCSB 同理。后续观测补充构建只改请求统计，语义证据按未改变的边界复用，详情见 [对象存储测量](rocksdb-cloud-scale.md)。

### 本地服务验收

| 来源 | 主机 | Baseline 秒 | workload 秒 / ops/s | 摘要核对 / 不匹配 | 发布水位 | 结论 |
| --- | --- | ---: | ---: | --- | ---: | --- |
| sysbench 87m | `sibench_4` | 235.49 | 640.70 / 35,394 | 12,708,328 / 0 | 4,072,283 | passed |
| sysbench 87m 重复 | `sibench_4` | 219.77 | 543.37 / 41,734 | 12,708,328 / 0 | 4,072,283 | passed |
| YCSB 87m | `sibench_2` | 146.62 | 869.74 / 34,410 | 25,567,761 / 0 | 25,862,780 | passed |
| HyBench SF10 | `sibench_1` | 842.25 | 3,781.96 / 21,746 | 67,813,021 / 0 | 181,388 | passed |

| 来源 | 驱动 wall / CPU / 峰值 RSS | 服务 CPU / 峰值 RSS | 索引采样峰值 / 最终 |
| --- | --- | --- | --- |
| sysbench | 16:52.01 / 2,733 s / 11.07 GB | 9,158 s / 1.65 GB | 3.45 / 2.90 GB |
| sysbench 重复 | 14:52.60 / 2,570 s / 10.75 GB | 7,867 s / 1.66 GB | 2.77 / 2.73 GB |
| YCSB | 18:55.34 / 3,762 s / 8.66 GB | 6,179 s / 1.57 GB | 1.89 / 1.86 GB |
| HyBench | 1:20:51 / 10,835 s / 12.87 GB | 110,582 s / 9.04 GB | 4.65 / 4.29 GB |

GB 按 10^9 B，`time` 的 RSS KiB 先乘 1,024；30 s 采样可能漏掉短峰值。服务 CPU 只属于本次 load 进程，重开核对单独计量。初始索引 / JVM 为新建，输入页缓存未强制清冷；不能把加载后的查询当作冷读。服务正常停止后 `time` 的 exit 143 是 SIGTERM 收尾，业务是否成功以驱动和报告为准。

sysbench 首轮 workload 的 query p50 / p95 / p99 为 117 / 155 / 194 ms，重复样本 p50 为 102 ms；workload 接口调用数均为 26,749,387，包含 4,072,283 个 commit marker，而 `operations_per_second` 用 22,677,104 条操作计。两种口径不混用。 当前三来源二级开启，`filtered_secondary_operations=0`。HyBench / YCSB / sysbench 的规范 Trace 操作分别为 82,603,822 / 81,653,024 / 30,821,670，减去 362,776 / 51,725,560 / 8,144,566 个事务控制记录，恰为表中有效操作；目标调用再加 181,388 / 25,862,780 / 4,072,283 个 commit marker。全部完成响应与目标调用相等，pending 为 0。性能波动与旧构建对照见 [测量与回归](p0-measurement.md)，不以第二个较快样本证明所有退化疑问已消失。

### Flush、后台与重开

sysbench 首轮 Baseline / workload Flush 为 0.355 / 0.333 s，workload 最后 Flush 时仍有 59,952,416 B 待 compaction、1 个运行中的 compaction；原 load 进程在随后 1.66 s 内完成最后一个已观测 compaction。原 load 日志有 240 次完整 flush、39 次完整 compaction，输出 SST 分别为 3,376,033,859 / 9,204,638,117 B。重开进程的同编号 job 未与 load 混配。

sysbench 本轮 `restart.status=0`，报告逐查询核对全部可重放快照读，事务内读按定义单列不核对；输入与发布水位绑定随 Flush 保存。YCSB 最后 Flush 时待 compaction、运行中的 compaction / flush 均为 0；日志有 134 次 flush、35 次 compaction，输出 SST 为 2,150,778,798 / 1,900,424,955 B。这些事件之和不等于端到端 wall，也不把 `Flush` 返回本身当作后台已排空的证明。

事件汇总脚本为 [summarize_index_run.py](../../scripts/summarize_index_run.py)。各成功运行只保留 `index-measurement.json` 作为派生汇总，绑定原报告、LOG 与分析脚本 SHA-256；sysbench 只使用原 load 的日志。错误或重复派生摘要已移除，所选原始服务报告与日志保持不变。

HyBench 最终本地运行完整通过，核对 15,467,768,568 个结果项，每查询最多 6,005,423 条；最后两次 Flush 为 0.380 / 0.112 s，原进程日志有 437 次 flush、75 次 compaction，输出 SST 为 5,811,821,740 / 30,392,609,927 B，最后 Flush 后未观察到 compaction。大结果的完整传输与摘要现有目标规模证据，不能扩大成查询组容量或 SF1000 结论。至此三来源本地 P0 代表配置验收完成；COS HyBench 的 600 s 完整验收及原 120 s 失败诊断见 [云后端目标规模](rocksdb-cloud-scale.md)，不把本地通过替代 COS 通过。

## 参考、独立 oracle 与组合矩阵

三份输入分别为 `hybench-sf10-hl9/trace-p0`、`ycsb-87m-n22/trace`、`sysbench-87m-sl16/trace-current`，详见 [输入审计](p0-three-source-audit.md)。HyBench / YCSB 的参考与 oracle 在 `p0-accept-final-20261028`，sysbench 完整参考复用 `p0-acceptance-20261028/sysbench`。参考 / oracle 构建、有效配置和运行身份随报告保留；服务变化不要求重新采集 PG。

### 完整参考回放

| 来源 | 主机 | wall | CPU | 峰值 RSS | workload 操作 | 查询 | 结果项 | 结论 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| HyBench SF10 | `sibench_1` | 1:37:48 | 14,442 s | 34.4 GB | 82,241,046 | 67,813,021 | 15,467,768,568 | passed |
| YCSB 87m | `sibench_2` | 1:31:25 | 5,863 s | 20.9 GB | 29,927,464 | 25,567,761 | 95,104,831 | passed |
| sysbench 87m | `sibench_3` | 29:27（两模式） | 3,107 s | 26.3 GB | 22,677,104 | 12,708,328 | 234,781,525 | passed，复用 |

sysbench 复用同一 Trace 与读取语义下已通过的 locator / rowid 完整参考结果。HyBench 的 workload 阶段 5,256 s，YCSB 5,302 s；参考模型版本跳过的同机完整比较见 [测量与回归](p0-measurement.md)。

### 独立 Parquet oracle（row_id）

| 来源 | 主机 | wall | 峰值 RSS | changed 主键 | 主键 / 二级历史探针 | 范围探针 | 结果项 | 结论 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| HyBench SF10 | `sibench_5` | 1:16:53 | 44.1 GB | 3,252,743 | 13,948,638 / 28,233,365 | 3,368 | 269,978,502 | passed |
| YCSB 87m | `sibench_6` | 1:31:33 | 27.4 GB | 2,321,974 | 13,362,337 / 0 | 168 | 12,777,567 | passed |
| sysbench 87m | `sibench_3` | 19:44 | 44.0 GB | 3,377,952 | 14,337,477 / 22,510,449 | 2,633 | 25,953,582 | passed |

oracle 由独立解码的 Baseline / 增量 Parquet 推出派生探针的期望，核对参考模型；派生探针不是原生负载。YCSB 只有主键索引。

### 小输入组合矩阵

1 / 64 / 128 client × operation / transaction × 二级开 / 关，每组两种 value；另以两种二级设置运行独立 Parquet oracle。当前组合比较使用完整逐查询摘要 SHA-256，另核对状态、选中版本和操作顺序；不保留结果 XOR 作为比较路径。

| 来源 | 采集 | Baseline / workload 操作 | 工作单元 / 事务内查询 | 矩阵 |
| --- | --- | --- | --- | --- |
| sysbench | `p0-sysbench-8m-short` | 17,412,000 / 24,300 | — / 1,000 | passed（见 [原生验证](p0-native-publish-validation.md)） |
| YCSB | `sibench_2`，`config/ycsb_small.yaml` | 1,000 / 4,737 | 1,495 / 0 | passed，2.2 s |
| HyBench | `sibench_3`，`config/hybench_sf1.yaml`（静止库新建 47 s，原生执行 379 s） | 31,212,000 / 8,146,958 | 432,562 / 263,551 | passed，35:25，峰值 RSS 21.7 GB；12 组合 × 两种 value 与两份 oracle |

## 当前整理的验证边界

协议 2 使用完整提交发布；显式参考报告缺少逐查询摘要会被拒绝。当前 [协议 2 验证](../../results/validation/index-service-20261029b/validation.json) 包含 319 项根测试、12 组小输入矩阵、真实 gRPC 回放与新进程重开，另保留跨连接提交等待与分块结果测试。Trace v9、键编码、事务快照与发布语义未变，上述完整输入证据在对应边界上复用；不把原构建的计时记到当前二进制名下。

剩余容量与故障验收要求见 [计划](../../plan.md)。
