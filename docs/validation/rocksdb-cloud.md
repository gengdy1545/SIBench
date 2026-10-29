# rocksdb-cloud 首个对象存储后端验证

更新：2026-10-29。记录 rocksdb-cloud 的构建身份、对象存储核验及短输入在本地 S3 与 COS 上的功能 / 重启 / 冷启动验证。本页只证明功能与语义正确，不含性能比较；未完成项列在末尾。

## 构建与身份

- 上游与依赖固定在 [index/versions.yaml](../../index/versions.yaml)：rocksdb-cloud `20a09188…`（RocksDB 9.1.1 基线），AWS SDK C++ 1.8.8（s3 / transfer / kinesis / core）。构建脚本 [build-rocksdb-cloud.sh](../../scripts/build-rocksdb-cloud.sh)，在 `sibench_4`（TencentOS 3.2、glibc 2.28、gcc 8.5）构建。
- 上游在 gcc 8 下有 unused-parameter 警告，构建以 `DISABLE_WARNING_AS_ERROR=1` 通过；不修改上游工作区（构建脚本校验 `git status` 为空）。首次试构建曾用 sed 改上游 `build_detect_platform` 指定 SDK 路径，已还原并改为把 SDK 装入上游默认的 `/usr/local`。
- JNI 绑定 `src/main/native/index/rocksdb_cloud/`：只导出 `Java_*` 符号（version script），只转发字节；键布局、版本选择、事务内读与发布语义沿用 Java 的 `RocksDbBackend` / `PublishedIndex`，与本地 RocksDB 共用一份实现。剥离后 `librocksdb_cloud_jni.so` 约 10.8 MB，动态依赖为系统 libcurl / OpenSSL 1.1 / 压缩库，因此只能在 sibench 机器加载（开发机 Ubuntu 22.04 无 OpenSSL 1.1）。
- 本地 RocksDB 参照统一为 Maven `rocksdbjni` 9.1.1（与 rocksdb-cloud 的 RocksDB 基线相同；原 10.2.1 已移除，本地 308 项测试在 9.1.1 下通过）。rocksdb-cloud 是该基线上的 fork，云层以外的引擎补丁差异未逐项核对。本页 jar `e3b47e46…` 至 `c3708387…` 内含的本地 RocksDB 仍为 10.2.1，但这些运行只使用 rocksdb-cloud 后端。

## 对象存储核验（开发机，aws CLI）

`config/storage.yaml` 的 `cos` profile：`ap-guangzhou`、bucket `dannygeng-cos-1257943044`、prefix `sibench/`、virtual-hosted。

| 项 | 结果 |
| --- | --- |
| 公网域名 `cos.ap-guangzhou.myqcloud.com` | 全部 `AccessDenied`（签名被接受）：平台子账号密钥只能用内网域名 |
| 内网域名 `cos-internal.ap-guangzhou.tencentcos.cn` | PUT / HEAD / GET 往返、range GET、list v2、COPY、DELETE、删除后 list 为空、读取已删对象失败：通过 |
| 5 MiB + 1 KiB 分片上传 | 关闭客户端默认请求校验和（`AWS_REQUEST_CHECKSUM_CALCULATION=when_required`）后通过；默认 aws-chunked 校验和被拒（`MissingContentLength`） |
| `If-None-Match: *` 条件创建 | **不支持**：已有对象被覆盖 |
| 版本控制 | 未开启 |

rocksdb-cloud 使用 AWS SDK 1.8.8，早于默认请求校验和；它在单 writer 模式下不依赖条件写。COS 原生 `x-cos-forbid-overwrite` 未测。上述为首次手工核验。`scripts/check_storage.py` 已自动核验 COS / 本地 S3 的全部必需操作；COS 条件创建仍覆盖已有对象，本地 S3 则支持。凭据只从环境变量读取，探针在独立前缀下创建并清理，报告见 `results/poc/p1-check-storage-20261028/`。`sibench_4` 可访问内网域名，rocksdb-cloud 经它读写见下文。

## 本地 S3 功能验证（`sibench_4`）

本地 S3 服务为 SeaweedFS 4.51（MinIO 社区二进制下载地址已返回 410），`local_s3` profile，测试凭据仅用于该本机服务。输入为 `p0-sysbench-8m-short`（Baseline 8,706,000 行 / 17,412,000 索引操作，workload 18,500 操作 / 10,700 查询），64 client、完整发布、row_id、逐查询摘要对照 `matrix/secondary-true-operation-64.json`。证据在 `sibench_4:/data/sibench/results/poc/p0-cloud-local-s3-20261028/`。

| 运行 | jar / JNI | 结果 |
| --- | --- | --- |
| `publication`（首次） | `e3b47e46…` / `08e2adc7…` | 驱动预检失败：`sibench_4` 缺少 `benchmark/sysbench`；服务端已建库并上传 CLOUDMANIFEST / IDENTITY / MANIFEST。同步子模块后换目录重跑 |
| `publication-2-2`（目录名由脚本替换误叠为 `-2-2`） | 同上 | 通过：baseline 197.8 s，workload 1.28 s（14,470 ops/s），10,700 / 0 摘要不匹配，`published_version=2900`；本地 308 MB，bucket 逻辑 794 MB |
| `lifecycle/load` | `94c9f6e8…` / `67b06954…` | 通过：baseline 199.9 s，workload 1.29 s（14,363 ops/s），10,700 / 0，`published_version=2900`；结束后 `kill -9` 服务进程 |
| `lifecycle/restart`、`cold` | 同上 | 失败：`verify-service` 摘要顺序错误。原因是快照核对模式把事务内查询的跳过提前于同事务尚未发送的快照查询；`ReplayProjectionTest.snapshotVerificationSendsAndSkipsSearchesInTraceOrderWithoutWrites` 在修复前复现（`[skip 3, send 2, send 4]`），修复为该模式立即发送 |
| `lifecycle/restart2`（同一本地目录重开） | `24eb2a21…` / `67b06954…` | 通过：恢复水位 2900；9,700 个快照查询逐查询一致、0 不匹配；1,000 个事务内查询按定义未核对；从启动到可服务 2.45 s（含 JVM），服务峰值 RSS 0.61 GB |
| `lifecycle/cold2`（新本地目录，仅从 bucket） | 同上 | 通过：同上 9,700 / 0；从 bucket 下载后本地 231 MB；从启动到可服务 2.47 s |

`load` 后的 bucket 逻辑大小约 1.02 GB，高于本地 238 MB：该快照中 compaction 替换的旧 SST 尚未删除。`run_purger=false` 不关闭普通文件的默认 1 h 延迟删除，后续完整观测见下文目标规模与回收证据；不能由这个即时快照断言旧 SST 不会被清理。重启前进程被 `kill -9`，最后一次 `Flush` 已记录水位 2900 并上传；冷启动没有本地 WAL，验证的是“最后一次 Flush 前远端持久”的声明。事务内查询无法在事后复现，重启核对不覆盖它们。

## COS 功能、重启与冷启动（`sibench_4`）

同一短输入与配置，profile `cos`（内网域名、virtual-hosted），`object_path` 为 `sibench/p0-cloud-cos-20261028/lifecycle`。jar `c3708387…`、JNI `6ac08918…`（加入每次 Flush 的引擎统计与对象存储请求计数）。COS 凭据从开发机环境经 ssh 标准输入注入远端进程环境，不写入远端磁盘或命令行。证据：本机 `results/poc/p0-cloud-cos-20261028/lifecycle/`（索引目录除外）。

| 步骤 | 结果 |
| --- | --- |
| 加载 + 回放后 `kill -9` | baseline 200.9 s（86,687 ops/s），Flush 1.46 s（本地 S3 为 0.26 s）；workload 1.24 s（14,879 ops/s），Flush 0.19 s；10,700 / 0；`published_version=2900`；服务 CPU 760 s、峰值 RSS 1.31 GB |
| 重启（同一本地目录） | 水位 2900；9,700 个快照查询 / 0 不匹配，1,000 个事务内查询未核对；启动到可服务 4.7 s |
| 冷启动（新本地目录，仅从 COS） | 同上 9,700 / 0；本地 231 MB；启动到可服务 5.6 s |

对象存储请求（`index.engine-stats.jsonl` / `index-cold.engine-stats.jsonl`，均为进程内累计）：

| 阶段 | 写 | 读 | list / info | 失败 |
| --- | --- | --- | --- | --- |
| 加载结束 | 64 次、739.8 MB | 2 次、0 B | 6 / 2 | 读 2、info 1 |
| 重启后 Flush | 5 次、9 KB | 3 次、106 B | 7 / 35 | 0 |
| 冷启动后 Flush | 9 次、231.2 MB | 11 次、238.7 MB | 7 / 35 | 0 |

- 加载上传 739.8 MB，而最终 SST 238.5 MB：compaction 产生的每个输出文件都上传，对象存储写入约为最终数据的 3.1 倍。
- 加载阶段的 2 次读失败与 1 次 info 失败发生在建库期间，推测是对尚不存在对象的探测（404），未逐条核对。
- 冷启动下载全部 SST 后又上传 231.2 MB，原因已由冷启动目录的 RocksDB `LOG` 确认：加载结束时 L0 已有 4 个文件（达到 `LevelL0FilesNum` 触发阈值），加载进程在该 compaction 完成前被 `kill -9`；冷启动打开后执行这次 L0→L6 compaction（输入 238.7 MB，输出 4 个文件 231.1 MB，8.2 s），输出按 rocksdb-cloud 机制上传。它是加载遗留的后台 compaction 债，不是冷启动固定成本；`Flush` 不等待 compaction，排空需单独测量。
- 本地 S3 与 COS 只各跑一次，耗时差异不作性能结论。

## 持久化声明的反向验证（`sibench_3`，本地 S3）

声明是“最后一次 `Flush` 前远端持久”。方法：新建 rocksdb-cloud 索引并回放短输入；Baseline 阶段的 `Flush` 记录水位 0 并写出 `engine-stats.jsonl` 首行，检测到该行 0.5 s 后在 workload 中途 `kill -9` 服务（未到 workload 末的第二次 `Flush`）；随后重新打开并运行 `verify-service`，由其拒绝信息读出发布水位，由打开后 `Flush` 的 `rocksdb.estimate-num-keys` 读出条目数。jar `eee833b9…`（RocksDB 9.1.1）、JNI `6ac08918…`；证据 `results/poc/p0-cloud-durability-20261028/`。

| 运行 | kill 位置 | 首个打开者 | 水位 | 条目数（Baseline Flush 时 17,412,002） | 结论 |
| --- | --- | --- | --- | --- | --- |
| `kill-0.5` | workload 第 8,708 次调用 | 重启（同一本地目录） | 0：`Index publishes version 0, the trace ends at 2900` | 17,413,739（+1,737，本地 WAL 回放） | Flush 后写入在本地 WAL 中保留，但不发布、不可读 |
| `cold-first-kill-0.5` | 第 6,400 次调用 | 冷启动（新本地目录） | 0 | 17,412,003 | Flush 后写入不在 bucket 中 |

两轮的第二个打开者都受前一次打开影响，不计入结论：`kill-0.5` 的重启在读水位时 `Flush` 了回放出的条目并上传，随后冷启动看到 17,413,740；`cold-first-kill-0.5` 的冷启动在 bucket 写入新的 CLOUDMANIFEST 世代后，再用旧本地目录重启只剩 17,412,004 条，即本地未上传的 WAL 被丢弃。后者说明 bucket 是权威状态，同一 object path 只应有一个打开者，重启前不应有其他进程从 bucket 打开。条目数为 RocksDB 估计值。

## 同引擎同机短对照（`sibench_7`）

本地 RocksDB 与 rocksdb-cloud 均为 RocksDB 9.1.1（jar `eee833b9…`、JNI `6ac08918…`），同一主机、gRPC、64 client、完整发布、row_id、同一短输入，两轮交错（`results/poc/p0-cloud-compare-20261028/`）。全部 10,700 / 0 摘要不匹配。

| 后端 | baseline | baseline Flush | workload | 对象存储写（累计） | 服务 CPU / RSS |
| --- | ---: | ---: | ---: | --- | --- |
| 本地 RocksDB | 252.5 / 252.6 s | 0.14 / 0.19 s | 11,875 / 12,630 ops/s | N/A | 860 / 846 s，1.3 GB |
| rocksdb-cloud + 本地 S3 | 273.3 / 274.4 s | 0.09 / 0.07 s | 15,226 / 15,485 ops/s | 74 次 759 / 756 MB，3.8 / 3.6 s | 875 / 882 s，1.3 GB |
| rocksdb-cloud + COS | 278.8 / 280.3 s | 0.34 / 1.41 s | 14,957 / 15,342 ops/s | 74 次 757 / 733 MB，24.6 / 56.0 s | 898 / 886 s，1.3 GB |

- 对象存储使 Baseline 加载慢约 8%（本地 S3）/ 10%（COS）；最终 SST 约 235 MB，上传约 3.2 倍（每个 compaction 输出都上传）。COS 上传累计耗时为本地 S3 的 7–15 倍，但上传在后台并行，前台差约 2%。
- workload 阶段 rocksdb-cloud 高于本地 RocksDB（两轮一致），原因未定位。两者 `OPTIONS` 文件除云相关键与 `max_manifest_file_size`（1 GiB / 4 MiB）外相同，已排除引擎配置；待验证的推测是 Java 访问路径：本地后端每次查询经 RocksJava 新建 `ReadOptions` / `Slice` / 迭代器等 native 对象，薄 JNI 游标只有一次调用。不作为后端性能结论。

## 后台排空

`Flush` 只把 memtable 落盘，不等待 compaction。测量方法：回放与最终 `Flush` 后服务空闲 120 s 再停止，从索引目录的 RocksDB `LOG` 事件计算最后一次 flush 之后的 compaction。

- 短输入（`sibench_3`，`results/poc/p0-drain-20261028/`）：本地 RocksDB 与 rocksdb-cloud（本地 S3）在最终 flush 后都没有 compaction（L0 为 3 / 2 个文件，低于触发值 4），排空 0 s；加载过程中各 6 次 compaction，写 422.9 / 458.7 MB，耗时 14.1 / 9.6 s。
- 是否留下债务取决于最终 flush 时 L0 是否达到触发值：COS 生命周期运行中 L0 恰为 4，被 `kill -9` 后债务转到冷启动（见上）。
- 所选目标规模本地 RocksDB 见 [P0 验收](p0-three-source-acceptance.md)：sysbench 87m workload 末 Flush 后约 60 MB 待 compaction，最后一次已观测 compaction 在 1.66 s 后结束。

## 当前目标规模与剩余边界

目标规模结果、JNI 请求延迟分布与引擎旧 SST 延迟删除的最新证据统一见 [rocksdb-cloud 目标规模](rocksdb-cloud-scale.md)：YCSB 87m 本地 / COS，sysbench 87m 本地 / 本地 S3 / COS 均已通过；HyBench SF10 COS 原 120 s 查询超时失败已归档，三组冻结状态诊断均通过；用户授权的 600 s 保护期限全量运行现已通过：67,813,021 次摘要 / 0 不匹配、约 154.7 亿结果项，最长查询 165.207 s；本地 SF10 同样完整通过。原 120 s 配置未通过，不把扩大实验保护写成根因已修复。引擎日志的实际 flush / compaction 事件已由 `scripts/summarize_index_run.py` 自动汇总，未提供的用户键范围等指标为 N/A。

后续观测补充 jar `1042aeb8…`、JNI `5f0e1f54…` 通过短输入 COS 回放、正常停止后重开及仅从 bucket 的冷启动，分位数按请求延迟桶的上界解释；历史旧 JNI 的累计微秒数不能恢复其延迟分布。它们只新增观测与旧 JNI 格式的早期拒绝，不改变本页历史运行的语义或结果。

仍未证明正式性能排名、单次短 workload 速率差的唯一原因、目标规模崩溃后恢复或远端查询 I/O 的稳态成本。各路径和规模的验证不能相互替代；费用本轮未计量。
