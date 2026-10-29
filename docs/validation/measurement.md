# 测量口径与本轮结果

更新：2026-10-29。只报告 **acceptance-20261029T053122Z** 的冻结应用和同轮输入，构建身份、正确性及复现入口见 [完整验收](acceptance.md)。本地与 COS 每来源各一次，没有重复分布、无插桩对照或性能排名结论；不沿用旧构建的加速倍数。

远程各 pod 为 AMD EPYC 9K65、32 CPU 配额、59.60 GiB 内存限额及 500 GiB XFS；驱动 / 服务各 16 GiB 堆。完整机器 / 软件条件与未测项见 [实验条件表](acceptance.md#构建与实验条件)。下文 COS 指 Rockset 的 rocksdb-cloud 嵌入式索引引擎接腾讯 COS，未测试 Rockset 完整数据库服务。

## 索引加载与回放

| 来源 | 后端 | Baseline 秒 / Flush 秒 | workload 秒 / Flush 秒 | 有效操作 / 秒 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | local | 781.89 / 0.17 | 5,342.05 / 0.08 | 13,408.80 |
| HyBench SF10 | cos | 945.20 / 0.80 | 2,911.17 / 0.24 | 24,605.37 |
| YCSB 87m | local | 92.78 / 0.44 | 821.21 / 0.08 | 36,389.13 |
| YCSB 87m | cos | 138.48 / 0.90 | 832.22 / 0.47 | 35,907.74 |
| sysbench 87m | local | 259.31 / 0.26 | 521.88 / 0.35 | 43,457.59 |
| sysbench 87m | cos | 252.22 / 0.90 | 445.83 / 0.82 | 50,870.70 |

Baseline / workload 秒来自服务回放报告的各阶段，Flush 另列。workload 吞吐按实际派发的查询和维护操作计，不包含事务控制记录；commit marker、逻辑源操作、Trace 记录和 RPC 消息数分别保留在原报告，不能互换。Baseline 批量写的 target_calls / completed_responses 按成员计，不能当作 gRPC 消息数。该 single_pass 没有目标到达率和持续调度，不证明长期稳态。

这些吞吐不是来源 SQL TPS 或纯查询 QPS。本轮单次观测中，HyBench 的 COS 回放较快、加载较慢，YCSB 回放接近，sysbench 的 COS 回放较快；没有交错重复、清页缓存或默认参数消融，不能将差异唯一归因于存储介质，也不能据此给出后端排名。

| 来源 | 后端 | 查询 p50 / p95 / p99 毫秒 | 查询 max 毫秒 | Result 字节 GiB |
| --- | --- | --- | --- | --- |
| HyBench SF10 | local | 272 / 574 / 3393 | 171,976.36 | 234.15 |
| HyBench SF10 | cos | 192 / 298 / 360 | 78,092.65 | 234.15 |
| YCSB 87m | local | 58 / 70 / 74 | 154.17 | 3.06 |
| YCSB 87m | cos | 59 / 70 / 74 | 153.70 | 3.06 |
| sysbench 87m | local | 95 / 129 / 151 | 211.23 | 3.94 |
| sysbench 87m | cos | 78 / 103 / 115 | 236.20 | 3.94 |

查询延迟包含驱动发出后排队、等待发布、服务、传输和核对边界，不是引擎内部执行时间。分辨率为 1 毫秒，max 按实际采样；写入和提交延迟分布见原报告。`request_bytes` 是 Request 序列化字节，`result_bytes` 是 Result 子消息字节，不是 TCP 字节；完整网络流量与 wire 消息数为 N/A。相同输入、布局、确认层级及资源预算下比较，不能把输入页缓存已热写成冷启动。

HyBench 本地版最大单查询约 172 秒，超过此前的 120 秒保护期限；本轮使用经确认的 600 秒期限，未触发保护。COS 本轮最大约 78 秒。单次通过不能证明此前超时根因已解决，原 120 秒首次失败记录单独保留。

## 资源与真实引擎事件

| 来源 | 后端 | 服务 CPU 秒 / RSS GiB | 驱动 CPU 秒 / RSS GiB | 索引采样峰值 / 最后 GiB |
| --- | --- | --- | --- | --- |
| HyBench SF10 | local | 161,525.60 / 9.45 | 10,036.67 / 11.94 | 4.35 / 3.90 |
| HyBench SF10 | cos | 84,063.00 / 10.01 | 9,597.51 / 10.37 | 4.35 / 3.95 |
| YCSB 87m | local | 6,076.68 / 1.50 | 3,422.77 / 8.08 | 1.90 / 1.68 |
| YCSB 87m | cos | 5,223.48 / 1.45 | 3,428.51 / 8.30 | 1.77 / 1.73 |
| sysbench 87m | local | 6,857.67 / 1.53 | 2,622.53 / 9.76 | 2.63 / 2.60 |
| sysbench 87m | cos | 6,114.02 / 1.65 | 2,565.80 / 9.06 | 2.74 / 2.48 |

服务 GNU time 覆盖启动、等待驱动、加载、回放、最终 120 秒空闲观测和正常停止；驱动 GNU time 包含自身预检与完整回放。两者 CPU 不能按 workload 秒拆分。RSS 为各进程峰值，峰值时刻不同，不相加冒充同一时刻总内存。索引字节为 30 秒采样的目录逻辑文件大小，可能漏过短暂峰值，最后采样也不是独立完整文件清单；包含本地 WAL / manifest，COS 同样保留本地文件。

| 来源 | 后端 | flush：完成 / 不完整 / 输出 GiB | compaction：完成 / 不完整 / 输出 GiB |
| --- | --- | --- | --- |
| HyBench SF10 | local | 434 / 0 / 5.38 | 76 / 0 / 27.79 |
| HyBench SF10 | cos | 436 / 0 / 5.38 | 76 / 0 / 27.74 |
| YCSB 87m | local | 132 / 0 / 2.01 | 31 / 0 / 3.48 |
| YCSB 87m | cos | 134 / 0 / 2.00 | 34 / 0 / 2.06 |
| sysbench 87m | local | 241 / 0 / 3.14 | 51 / 0 / 8.99 |
| sysbench 87m | cos | 242 / 0 / 3.14 | 41 / 0 / 7.98 |

事件仅取同次加载进程实际 LOG，不混配重开进程的相同 job id。输出字节分别为 flush / compaction 生成 SST 的累计值，不是最终 live SST；事件耗时重叠，累加不是 wall。原报告区分 memtable 非压缩输入与 SST 输入，不用二者混算写放大。日志缺失和未完成 job 单列，不能写成零工作量。Flush 不等待 compaction；120 秒是观测窗口，不保证所有维护排空。最终 Flush 的 pending bytes / running jobs 见 `index.engine-stats.jsonl`，它的时间点早于空闲观测结束。

共同 OPTIONS、云 fork 独有项和默认差异随运行保存。当前 `max_manifest_file_size` 为本地 1 GiB、cloud 4 MiB；没有为了本轮修改冻结实现的默认值。两者同为 RocksDB 9.1.1 基线、共用 Java 版本 / 发布语义，但 fork、JNI 路径与默认项不同，不能把耗时差唯一归因为 COS。

## 对象请求

| 来源 | 上传调用 | 上传 GiB | 失败 | 延迟 p50 / p95 / p99 毫秒上界 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 1,363 | 33.15 | 0 | 524.288 / 4,194.304 / 4,194.304 |
| YCSB 87m | 386 | 3.97 | 0 | 262.144 / 2,097.152 / 4,194.304 |
| sysbench 87m | 668 | 11.13 | 0 | 524.288 / 2,097.152 / 4,194.304 |

上表为 COS 进程**截至最终 Flush**的累计上传调用 / 字节，含加载与 workload；不含其后尚未采样的后台请求。请求延迟分位数是固定桶上界，成功与失败均入桶，不是精确分位数或端到端耗时；不同请求可并发，累计微秒不能当 wall。read / list / info / delete 等分类和直方图样本数见同一统计文件，SDK 内部 HTTP 重试次数为 N/A。建库探测失败计数不能独立证明一次服务错误。

对象字节包括 compaction 输出，不等于新增业务字节或最终活跃对象大小；旧 SST 延迟回收、本地缓存 / WAL 和对象清单观察边界见 [云后端验证](rocksdb-cloud.md)。本轮未计算费用、未验证纯远端查询 I/O 稳态。

## Parquet 文件与 Trace 产物

以下为同轮来源主机完整文件清单的逻辑字节统计，仅计 `.parquet`，不含 manifest / 审计元数据；不是采样或新分配空间。Baseline 复用已验证文件，增量为本轮 Trace 编译生成。逐文件字节、输入摘要及统计脚本保存在本轮 `report-details.json` / `summarize_report_details.py`，作为原报告的补充，不改写原清单。

| 来源 | Baseline 数量 / 总 GiB | Baseline 单文件最小 / 中位 / 最大 MiB | 增量数量 / 总 MiB | 增量单文件最小 / 最大 MiB |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 86 / 1.53 | 0.54 / 19.47 / 24.46 | 8 / 64.35 | 0.13 / 41.01 |
| YCSB 87m | 84 / 65.89 | 21.85 / 812.70 / 812.72 | 11 / 2,757.87 | 53.10 / 271.78 |
| sysbench 87m | 88 / 7.87 | 36.72 / 97.07 / 97.10 | 8 / 336.39 | 41.89 / 42.18 |

1 MiB = 2²⁰ B，1 GiB = 2³⁰ B。后文目录总量包含元数据，故 YCSB Baseline 目录四舍五入为 65.90 GiB，与仅 Parquet 的 65.89 GiB 不矛盾。相同基线行数不意味着相同数据字节或文件布局。

当前三来源 `data_layout` 的目标文件大小为 256 MiB，row group 上限 1,048,576 行 / 64 MiB 编码缓冲，Zstd level 3、1 MiB page。但原生 PG Baseline 导出按 `row_group_max_rows` 预先分片，一片一个文件，未用 writer 的大小滚动信号重新拆片；每片内部仍按 row group 限制编码。YCSB 宽行因此形成约 812.70 MiB 的典型 Baseline 文件。增量编译按 row group 写出后的实际字节滚动，允许超过目标一个组及 footer，尾文件可较小。现有产物不证明 256 MiB 是已统一实现或已验证合理的布局；精确职责见 [数据文件契约](../contracts/storage.md#数据文件)。

| 产物 | 生成阶段 | 边界由谁控制 |
| --- | --- | --- |
| Baseline Parquet | 原生 PG 静止边界导出；本轮复用 | SIBench 的 PG 导出分片与 `data_layout` |
| `workload.ops` / `work-units.jsonl.zst` | 采集后的 `compile-trace` | SIBench 的访问投影、事务 / 工作单元与 Trace 编码 |
| `<table>-versions*.parquet` | 同次 `compile-trace`，保存 INSERT / UPDATE 的行版本 | SIBench 的 `data_layout`；DELETE 为索引失效，不新增完整行版本 |
| 索引 SST / WAL / MANIFEST 与 COS 对象 | 后端加载、回放及后台维护 | RocksDB / rocksdb-cloud 的 memtable、flush、compaction 和实际 OPTIONS |

两种后端复用同一份已编译输入，服务回放不再生成增量 Parquet。修改 RocksDB 的 SST / memtable 参数不会改变这些 Parquet；修改数据文件布局则须重建相应 Locator / Physical Trace。当前目标规模服务使用 row_id，SST 保存索引键、版本和值，本轮未测完整业务行回表性能。Parquet 字节、SST 输出字节和 COS 上传字节分别计量，不能互相替代。

## 生成与正确性模型成本

| 来源 | 采集 wall 秒 | CPU 秒（不含 PG daemon） | 最大子进程 RSS GiB | raw / Trace / Baseline GiB |
| --- | --- | --- | --- | --- |
| HyBench SF10 | 1,606.58 | 3,362.57 | 30.36 | 1.71 / 0.73 / 1.53 |
| YCSB 87m | 3,372.29 | 7,291.16 | 33.58 | 9.24 / 4.06 / 65.90 |
| sysbench 87m | 954.19 | 1,717.31 | 18.21 | 3.04 / 0.82 / 7.87 |

生成复用了静止库和 Baseline，raw 为认证后保留文件，字节是逻辑大小；共享 hard link 不重复代表新分配空间。采集 GNU time 不包含独立 PG daemon 的 CPU，max RSS 为最大子进程而不是所有进程之和。原生执行、行审计与编译阶段见 [采集环境](capture-performance.md)；不能用这些秒数评价后端性能。

| 来源 | 参考 wall 秒 / RSS GiB | oracle wall 秒 / RSS GiB | work unit / workload.ops MiB |
| --- | --- | --- | --- |
| HyBench SF10 | 5,778.00 / 37.29 | 4,554.00 / 42.15 | 50.71 / 629.79 |
| YCSB 87m | 423.20 / 24.29 | 1,305.23 / 30.55 | 290.45 / 1,106.36 |
| sysbench 87m | 1,262.83 / 34.44 | 1,145.82 / 42.04 | 128.97 / 372.70 |

sysbench 全量参考包含 Locator 和 RowID 两种 value，HyBench / YCSB 全量参考只运行 row_id，因此参考 wall 不能跨来源直接比较。oracle 使用独立 Parquet 行历史和派生探针，它的耗时不是服务查询耗时。临时落盘和目录峰值以应用 timing 的观测范围为准；共享 `/data` 前后差值和 cgroup 历史 HWM 都不能单独归因到本轮。

工作单元目录保留源请求区间、编号 / 数量和身份摘要，当前消费者为编译器与回放预检，承担防截断和替换职责，不能仅因它占 workload 的一定比例就删除。读取只保留首事件 long[]，不是把全部 JSON 解压常驻内存；远超本轮工作单元数的容量没有实测。Baseline / 增量 Parquet、Trace、SST、分析 block 和网络结果为不同计量对象。

## 可复现性与限制

[本轮证据](../../results/validation/acceptance-20261029T053122Z/) 的 `cohort-summary.json` 从原报告提取并核对三个来源的六次后端：参考摘要身份、查询数、结果项、最终水位和零不匹配必须一致。`index-measurement.json` 由 [summarize_index_run.py](../../scripts/summarize_index_run.py) 从原服务报告与 LOG 生成，保留报告 / 日志 / 脚本摘要。每个结果只与本次命令、配置和 timing 配对。

目标确认层级保持一致：ACK_WAL 不 fsync，Baseline 跳过 WAL、到 Flush 才持久；COS 的 Flush 提供其声明的远端持久边界。生成、调度 / 背压、发送 / 完成、服务、结果核对、Flush 和后台维护分别解释，未直接测量的内部阶段写 N/A。驱动、传输和结果校验没有独立饱和余量曲线，因此本轮吞吐不能宣称索引上限。

没有新的 before / after 性能对照、缓存冷暖矩阵、至少 30 分钟持续到达率实验、SF100+ / SF1000 容量、Rewrite / GC、目标规模崩溃恢复或正式后端排名。旧比较产物和数值结论随本轮替换而撤下，仍有效的正确性不变量保留在测试与契约中；剩余验收见 [计划](../../plan.md)。
