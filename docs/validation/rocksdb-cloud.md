# rocksdb-cloud 构建、存储与恢复验证

更新：2026-10-29。本轮证据为 `results/validation/acceptance-20261029T053122Z/`；完整输入的结果见 [三来源验收](acceptance.md)，计量边界见 [测量口径](measurement.md)。比较对象为 Rockset 的 rocksdb-cloud 嵌入式引擎，不是 Rockset 数据库服务。

## 构建与运行条件

版本固定在 [index/versions.yaml](../../index/versions.yaml)：rocksdb-cloud `20a0918820ef89d71f4a059da4864db66678c38b`，RocksDB 9.1.1 基线，AWS SDK C++ 1.8.8。本地参照使用 Maven rocksdbjni 9.1.1；云 fork 的非云层补丁差异未逐项排除，因此不是只改变存储介质的严格消融。

冻结应用提交 `bbff9a3`，jar SHA-256 `238fe58b51c17d7ad1f4c8b3d19b2a678af6c0bedf9f7232efd4a1907055e644`。本轮在 `sibench_4` 以 [构建脚本](../../scripts/build-rocksdb-cloud.sh) 重新构建 JNI，复用固定上游的静态依赖；剥离后 10,793,680 字节，SHA-256 `5f0e1f5409c90313ddebcd2a62b367883abad3587c08573c927705a1d4eb87ec`。原生延迟分桶和溢出测试通过。构建身份见 `build.json`、`native-build.json`。

JNI 只转发字节和引擎调用，键布局、版本选择、事务内读及完整提交发布由两种后端共用的 Java 实现负责。JNI 依赖目标环境的 libcurl / OpenSSL 1.1 / 压缩库，本轮在 TencentOS 3.2、glibc 2.28 的 `sibench_1–4` 核验动态依赖；不声称该二进制可直接在开发机 Ubuntu 加载。

## COS 接口核验

使用 [storage.yaml](../../config/storage.yaml) 的 `cos` profile：`ap-guangzhou`、内网域名、virtual-hosted、项目 prefix `sibench/`。凭据仅从环境读取，远程实验通过 SSH 标准输入注入子进程环境，不写入配置、日志或命令行。每个实验使用独立 object path，单 writer 打开。

本轮 [check_storage.py](../../scripts/check_storage.py) 的结果保存在 `storage-conformance.json`：

| 操作 | 结果 |
| --- | --- |
| PUT / HEAD / GET、range GET、list v2 | 通过 |
| multipart、COPY、DELETE、删除后空清单 | 通过 |
| `If-None-Match: *` 阻止覆盖 | 不支持：已有对象被覆盖 |
| bucket 版本控制 | 未开启 |

九项必需操作均通过。探针在独立前缀中创建并清理对象；CLI 使用 `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`。当前单 writer 路径不依赖条件创建，不把接口探针当作一致性或多 writer 正确性证明。

## 小输入回放、重开与冷启动

`sibench_4` 上复用已验证的 2,000 行 sysbench Trace，路径迁移与摘要等价核对见 `small/input-relocation.json` 和 `small/input-relocation-check.json`。冻结应用先通过 12 个组合 × Locator / RowID 及独立 Parquet oracle，再以 64 client、4,096 在途、operation、二级开启、row_id 运行真实 gRPC 服务。

| 后端 / 操作 | 核对查询 | 不匹配 | 发布水位 |
| --- | ---: | ---: | ---: |
| 本地 RocksDB，完整加载与回放 | 2,050 | 0 | 500 |
| 本地 RocksDB，新进程重开 | 1,850 | 0 | 500 |
| COS，完整加载与回放 | 2,050 | 0 | 500 |
| COS，新进程重开 | 1,850 | 0 | 500 |
| COS，全新本地目录，仅从 bucket 冷启动 | 1,850 | 0 | 500 |

完整回放各核对 39,760 个结果项；重开和冷启动各核对 39,560 个结果项，另有 200 个事务内查询按定义跳过，不能在事后复现本事务写入前缀。加载进程完成最终 Flush 后正常停止，再依次由新进程打开；各进程报告与引擎日志分别保留。`small/local/`、`small/cos/` 为本轮证据。

## Flush 持久化的正反验证

声明为“最后一次 Flush 之前远端持久”，不是每次 ACK 远端持久。COS 仍保留本地 SST 和 WAL；WAL 不上传，bucket 为恢复权威状态。同一 object path 不允许多个打开者并发使用。

本轮 `durability/` 使用手算的小输入，调用当前 `PublishedIndex` 和后端 API：提交 version 1 / RowID 11 并 Flush，再提交 version 2 / RowID 22 而不 Flush；ready 标记确认现场后由父进程 SIGKILL。随后使用全新本地目录，直接从同一 bucket 打开，避免同目录 WAL 和前一次重开造成污染。

`durability/check.json` 结果通过：恢复水位为 1，持久版本返回 RowID 11，对 version 2 的读取继续等待，底层也不存在未 Flush 的 version 2。`ready.json`、`kill.json`、writer / cold 日志记录同一次实验。该探针同时区分“最后 Flush 可恢复”和“Flush 后写入尚未远端持久”，没有在 kill 前执行隐式 close / Flush。

## 观测与边界

Flush 将 memtable 落盘，不等待 compaction。完整输入运行在最终 Flush 后空闲 120 秒再停止，保留引擎事件、OPTIONS、进程资源和索引目录采样；120 秒是观测窗口，不能据此宣称所有后台任务已排空。对象旧 SST 的延迟删除由引擎负责，本轮没有等待完整删除周期，不报告回收率。

每次 Flush 的 `*.engine-stats.jsonl` 给出引擎属性、对象请求次数 / 字节 / 累计耗时和延迟桶。分位数为桶上界，样本数与失败数单列；SDK 内部 HTTP 重试次数未直接计量。对象请求计数包括建库时的探测，不能仅凭失败总数推定一次服务失败。日志事件可由 [summarize_index_run.py](../../scripts/summarize_index_run.py) 汇总，不把不同进程的同编号 job 混配。

当前未验证目标规模崩溃恢复、断电 / 机器故障、multi-writer、纯远端查询 I/O、正式稳态性能或费用。小输入恢复、存储 API 核验与完整输入正确性承担不同职责，不能相互替代。
