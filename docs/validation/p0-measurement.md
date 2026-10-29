# 测量口径与性能对照

更新：2026-10-29。定义可比口径，保留支持当前结论的测量与比较。三来源本地完整服务验收与 COS 目标规模均已通过；HyBench COS 使用用户批准的 600 s 请求保护，原 120 s 超时失败与诊断保留，详见 [云后端目标规模](rocksdb-cloud-scale.md)。未验证的边界不扩大为已完成能力。

## 全量回归与比较范围

语义构建为 `e126693f…`，输入为已认证的三来源 Trace。Baseline 批量写、分块响应和最小 index manifest 已合入；逐查询摘要继续复用原参考 sidecar。完整结果、两端资源和输入绑定见 [P0 验收](p0-three-source-acceptance.md)。Rewrite / GC 和正式长时实验的剩余要求见 [计划](../../plan.md)。

### Baseline 批量写与最终回归

同一已验证 sysbench 短输入、64 client、4,096 在途、相同确认与完整摘要校验，在 `p1-baseline-batch-20261028` 保留一组两次前后对照：

| 变体 | 两次 Baseline 秒 | 两次 workload 秒 | 摘要核对 |
| --- | --- | --- | --- |
| 单写请求 `7193caf7…` | 337.06 / 332.53 | 3.91 / 3.52 | 各 10,700 / 0 不匹配 |
| Baseline 批请求 `23039907…` | 110.99 / 111.13 | 3.42 / 4.01 | 各 10,700 / 0 不匹配 |

保留批量写；短输入 Baseline 约快 3 倍，workload 没有批量合并。目标规模成本与 sysbench 重复波动统一见 [完整验收](p0-three-source-acceptance.md)，不从不同构建或主机的单样本推导后端排名。批量后的 `target_calls` / `completed_responses` 仍按 INSERT 成员计，在途额度逐成员占用，不能当作 gRPC 消息数。`request_bytes` 计 Request 序列化字节，`result_bytes` 只计 Result 子消息，均不是 TCP 字节；wire 消息数和完整传输字节未直接计量，为 N/A。

### 发布窗口与 JFR

`sibench_3:/data/sibench/results/poc/p1-publication-window-20261028/`，相同 sysbench 87m、40 GiB 以下的两端总堆预算（各 16 GiB）、新索引、64 client，4,096 / 8,192 / 16,384 在途依次运行；不是循环 Trace。jar `63d00048…`，与最终构建共享批量写 / 发布路径，不含后续请求统计补充。4,096 档开启 JFR，其余不启用，故差异包含观测开销。三档均已结束，统一汇总曲线，不按单样本挑最佳窗口。

三档各为 12,708,328 摘要 / 0 不匹配。workload 吞吐 / query p50 分别为 40,955 / 100 ms、41,392 / 203 ms、43,545 / 373 ms；p95 为 141 / 272 / 511 ms，p99 为 170 / 352 / 610 ms。扩大四倍窗口仅增加约 6.3% 吞吐，而延迟近四倍，收益小于本轮独立样本的波动范围；保留 4,096，不将加大排队视作优化。曲线限制只适用于这份完整负载，不能证明服务本身的绝对吞吐上限。仅截取 Baseline Flush 与 workload Flush 之间的 JFR：2,521 个 Java execution sample、27,555 个 native residency sample；无超过 JFR 阈值的 monitor-enter 事件，378 次 GC 累计停顿约 0.737 s。应用 Java 采样分布于 protobuf 序列化、发布调度和查询执行，没有仅由这些采样证明单个发布锁是吞吐上限。native residency 中 RocksDB iterator seek 较多，但 residency 包含阻塞线程，不能当作 CPU 百分比；epoll 等待不算 CPU 热点。原 JFR 与脱敏字段汇总 `jfr-workload-samples.json` 绑定在同一运行目录。

### 参考模型版本跳过

`HistoryScan` 对同一 entry 连续跳过 8 个旧版本后定位下一 entry；小输入双向扫描、内存 / 强制落盘均有独立预期。保留同机完整对照，早期重复输出不再单独保存。

`sibench_6:.../p1-reference-compare-20261028/` 补一组同机旧 / 新：旧构建 `fc8c718a…` 最多 7,200 s、新构建 `1042aeb8…` 最多 1,800 s，均为 row_id、40 GiB 堆、Baseline 1 亿 / overlay 2,000 万 entries、512 MiB 排序预算，同一已验证 Trace；任一失败即停止该组。两份完整摘要都必须匹配原结果，计时与资源只在各自运行内配对。实际两轮均 exit 0 / passed，SHA-256 都为 `63bf29f1…`：

| 同机变体 | wall | CPU | 峰值 RSS | 摘要 |
| --- | ---: | ---: | ---: | --- |
| 旧构建 `fc8c718a…` | 1:04:20 | 4,472.70 s | 28.22 GB | 一致 |
| 新构建 `1042aeb8…` | 7:25.14 | 715.73 s | 17.52 GB | 一致 |

固定完整输入的 wall 约快 8.7 倍，CPU 约降至 16%，峰值 RSS 下降约 38%；内存数字是此次 JVM RSS，不声称模型固有状态量按同样比例减少。两次均新进程和新模型目录，输入页缓存未清冷且各自预检先读取输入；没有同机竞争实验。保留版本跳过优化。固定旧二进制仅供复现实验对照。

### 引擎事件和对象请求分布

[summarize_index_run.py](../../scripts/summarize_index_run.py) 只读取原服务报告和实际引擎 `EVENT_LOG_v1`，输出分阶段发送 / 完成、延迟、背压、Flush，以及逐个 flush / compaction 的时间与 SST 文件元数据；源报告、日志与分析脚本均绑定摘要。不同进程日志的 job id 不交叉配对，不完整 job 单列，不把无日志当作零成本。四项 Python 测试覆盖跨进程同编号 job、缺失边界、压缩前后字节区分、损坏事件及错误 / 缺少结果的拒绝。

云后端增加固定 32 桶的请求延迟计数，不改请求或发布语义。新 jar `1042aeb8…`、JNI `5f0e1f54…`：根 Maven 测试与原生手算分桶 / 溢出测试通过；短输入 COS 10,700 次摘要 / 0 不匹配，重开与仅从 bucket 冷启动各核对 9,700 个快照查询 / 0 不匹配（各另有 1,000 个事务内查询按定义不核对）。分位数是桶上界，成功与失败均计入，直方图样本数单列；原长运行没有该分布，写 N/A。细节、适配成本的两组交错微测与对象回收见 [rocksdb-cloud 目标规模](rocksdb-cloud-scale.md)。

## 三来源生成基线

2026-10-28 用固定比较构建 `6676ed3e…` 从三份既有 raw 重编，未重采 PG。`/usr/bin/time -v` 记录子进程 CPU / 峰值 RSS；JVM 未显式设置 `-Xmx`，在 64 GB cgroup 下 `MaxHeapSize` 为 16 GB。输出 Trace 的逻辑摘要与既有 Trace 相同。

| 来源 / 输入 | wall | workload 子阶段 | CPU user+sys | 峰值 RSS | Trace 最终字节 | 身份 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| HyBench SF10 `hybench-sf10-hl9/raw` | 397.18 s | 334.798 s | 1,265.01 s | 15.13 GiB | 902,910,703 | `workload=7e06ba49…` / `baseline=8b95df69…` |
| YCSB 87m `ycsb-87m-n22/raw` | 556.88 s | 518.083 s | 1,704.27 s | 12.59 GiB | 4,364,569,656 | `workload=9ec9cdc9…` / `baseline=0afda377…` |
| sysbench 87m `sysbench-87m-sl16/raw` | 266.89 s | 228.671 s | 781.93 s | 13.50 GiB | 878,641,867 | `workload=b8b9045e…` / `baseline=9587cc6f…` |

- 计时原文与 `--timing` 在 `sibench_1/2/3:/data/sibench/results/poc/p0-baseline-20261028/`；三份 `capture_completeness=consistent`，源事件 / 规范操作数分别为 174,366,939 / 82,603,822、238,503,582 / 81,653,024、106,146,237 / 30,821,670。
- `workload` 是编译子阶段，不能与 baseline、ledger、report 简单相加得到 wall；wall 还含 JVM 与阶段收尾。
- 峰值临时磁盘没有在旧构建的计时中采样；上表只给最终 Trace 字节。主机 `/data` 为共享资源，`df` 前后差值不能单独归因于本次编译；该缺口保留，不写成“空间不变”。

## 驱动与容量边界

YCSB 87m 的 4 GiB `check-replay` 已通过（`p0-capacity-20261028/ycsb-check-4g.*`），只证明预检，不是完整参考回放。工作单元目录流式读取，只保留首事件 long[]；25,862,780 个单元约 207 MB。已完成 lane 释放、事务身份去重使用位图；5.76 亿单元、远超内存数据和大查询组未实测。

空目标、进程内、本机 / 远程 gRPC 的驱动分解保留在 `p0-transport-rocksdb9-20261028`，发布窗口的完整输入曲线见上文。它们用于识别解码 / RPC / 排队影响，不能由空目标速率推出真实服务上限。普通查询分块后的 HyBench 完整约 154.7 亿结果项已通过；查询组仍整体物化。当前服务按完整提交发布执行。

### 工作单元目录的保留理由

消费者只有 `IndexTraceCompiler`（写出并把明文行摘要记入 `work_units_sha256`）与 `ReferenceReplay.check`（所有回放、oracle、服务回放的预检）。它承担三项独立职责：把每个 workload 操作绑定到其源 SQL 请求的事件区间、核对工作单元数与连续编号、以 manifest 摘要防截断 / 替换；另保留每个请求的进程与应用身份作为来源。读取为一次流式遍历，只保留每单元首事件的 `long[]`（YCSB 2,586 万单元约 207 MB），4 GiB 堆下 YCSB 预检已通过。

| 来源 | 目录 | `workload.ops` | 目录 / workload |
| --- | ---: | ---: | ---: |
| HyBench SF10 | 63.5 MB | 758.0 MB | 8% |
| YCSB 87m | 307.1 MB | 1,161.4 MB | 26% |
| sysbench 87m | 135.0 MB | 390.8 MB | 35% |

结论：保留，不迁出或删除；相对 Baseline Parquet（YCSB 66 GB、sysbench 7.9 GB）体积小，且没有替代机制。

## 阶段

下表定义阶段口径；未提供的分项为 N/A。未测量的内部阶段不得用端到端差值冒充，端点间计时须说明时钟与观测边界。

| 阶段 | 起止 | 指标 |
| --- | --- | --- |
| 生成 / 采集 | 原生执行、raw、Baseline 导出、Trace 编译 | wall、CPU、峰值 RSS、输入 / 输出字节、逻辑操作、展开操作、拒绝 / 过滤 |
| 解码与派生 | 读取 Trace、Baseline、work unit、参考模型或 oracle 的初始化 | wall、CPU、RSS、读取字节、外部排序 / 落盘字节、峰值临时磁盘 |
| 调度排队 | 驱动接收操作到交给 client / RPC | 排队条数、排队时间、背压时间、在途峰值；不把已发送请求算作已执行 |
| RPC / 传输 | 交给 client 到服务接收 / 响应返回 | 请求数、结果项 / 字节、发送率、完成率、网络时间；没有测量时写 N/A |
| 后端执行 | 服务接收请求到后端返回 | 操作数、查询 / 写入分项吞吐、延迟分布、后端 CPU / RSS、flush / compaction |
| 结果返回与校验 | 响应到逐查询摘要核对结束 | 核对查询数、结果项、摘要失败、校验 CPU / wall |
| 持久化与排空 | Flush、drain、后台维护 | `published_version`、flush 时间、未完成响应、后台积压 / 排空时间 |

端到端必须明确包含哪些阶段；生成端与消费端分开报告，不能把采集耗时当作索引服务性能，也不能只报 `ops/s` 或压缩率。相同确认层级（`write_ack`、`baseline_durable_only_at_flush`、`flush_makes_durable`）才可比较；baseline 加载、workload、Flush 和 drain 分开计时。

## 计数

- 源操作、逻辑访问、索引展开操作、发送请求、提交标记、结果项 / 字节分别计数；查询组按一个请求计，成员结果项按实际返回计。
- 查询与写入分开报吞吐与延迟；报告目标到达率、实际发送率、完成率，不能只统计已发请求。
- 记录 cache / page cache 冷热条件；预检读取不得算作冷启动预热。
- 重复运行报告波动、顺序与资源预算；不同输入 / 构建 / 配置不得拼接。

## 证据位置与复现

完整本地验收位于 `results/poc/p0-final-20261028/`，对象存储目标规模位于 `p1-cloud-scale-20261028/` 与 `p1-hybench-cloud-final-4-20261028/`，当前接口验证位于 `results/validation/index-service-20261029b/`。独立反例、选定性能对照、发布窗口曲线和原 120 s 超时诊断各保留必要输入、原报告 / 日志、有效配置及构建身份；派生清单 evidence.sha256 只索引所选文件，不改变原始 identity / manifest 内容。

必要比较二进制各保留一份，集中在 `results/builds/<完整 SHA-256>.jar` 或 `.so`：`7193caf7…` / `23039907…` 为批量写对照，`fc8c718a…` / `1042aeb8…` 为参考回放对照，`e126693f…` 为本地完整验收及 120 s 失败；JNI 为 `6ac08918…` / `5f0e1f54…`。复现历史命令中的 before.jar / after.jar 时，将集中副本放入新运行目录；已有输出目录不重跑。当前构建入口为根 Maven，输出 build/sibench/。

可重建索引仅保留代表微测状态与必要诊断状态；原 120 s 失败索引及控制文件快照保留。原始完整实验属于各自绑定构建，当前接口测试不替代目标规模计时。格式已失效、无保留结论消费者的早期试验和重复派生不承担复现职责。

YCSB 对照共享的查询 sidecar 规范副本为 `sibench_6:/data/sibench/results/poc/p0-accept-final-20261028/ycsb/reference.json.rowid.queries`，SHA-256 `63bf29f17c8a72d3ce061b8bfdbdc0560abd442a2ab9b4e1200facffbda7c2c0`。对照目录的 deduplicated-queries.json 记录位置；使用历史报告回放前，将规范副本恢复到报告声明的 query_digest_file 路径。
