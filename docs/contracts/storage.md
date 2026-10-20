# 存储契约

## 数据文件

- 默认目标 256 MiB；row group 最多 1,048,576 行，编码缓冲达到 64 MiB 可提前封闭。缓冲阈值不是 JVM 内存硬上限。
- parquet-java 1.15.2、Zstd level 3、单线程压缩、1 MiB page/dictionary，启用 dictionary；codec 支持 zstd/snappy/none，后两者 level=0。
- 完整组写出后按实际字节封闭文件，允许超过目标一个组及 footer，尾文件可不足目标；最多 32,767 组。行号是文件内绝对位置，不含 RowGroupID。
- 固定 schema、源顺序、参数和 FileID 分配。按表/桶流式写，当前派生分桶按源序号取模，不额外排序；不引入 Small-File Compaction、Sort 或 Z-Order。
- 清单记录行数、大小、SHA256、组信息和身份。文件以 `.partial` 发布，最终 manifest 完成后移除 `INCOMPLETE`；失败换新目录，不覆盖或续写。
- 磁盘保留默认 20 GiB，是停止阈值，不是全量容量校准。文件生成、SQL 和结果数据读取不计入索引性能。

原生 SF1 的全行核对及增量重复编译已通过；各来源派生大规模产物尚未校准。更改布局需重建 Physical Trace，不能复用旧 Locator。

## 删除与 GC

业务 DELETE 必须发布版本化逻辑失效，不能关闭，也不能物理删除最新条目让旧版本复活。二级失效受二级发送开关控制。后台延迟删除、批量、内部 tombstone 和 compaction 由 Backend 决定，但需满足可见性和确认契约。

`versions.gc.owner` 的责任约定：

| 值 | 责任 |
| --- | --- |
| none | 不物理清理，逻辑删除照常生效；当前唯一可运行选项 |
| benchmark | 安全保留边界后发送精确版本清理，点/批量 API 待实现 |
| backend | 服务提供安全保留边界并自行清理，不发送逐键清理；待接入 |

历史版本保留与是否启用历史查询分开；关闭查询不能自动清理。报告分别统计逻辑删除、清理条目、API 请求、积压、排空时间和存储量，不能漏掉延迟维护成本。

## Rewrite 规则

目标为 Validity-Based Rewrite + Deterministic Bin-Packing，当前仅有独立强制搬迁控制，尚未实现自然触发。

`Validity = RetainedRows / TotalRows`，`<=70%` 触发。RetainedRows 是当前版本与仍受保护历史点所需版本的去重集合；不能把受保护旧版本算作可回收，行比例也不等于字节比例。

在固定逻辑边界检查，按确定顺序在同 Table/Partition/Bucket 内装箱，使用相同文件、组及压缩规则。生成完整键、确切版本、旧/新 Locator 的 relocation；Locator 更新位置，RowID 不写索引。发布屏障完成且旧位置不再被在途操作引用后才回收旧文件。全部可回收时无需替代文件。

Rewrite 不改变键、CommitVersion、RowID 或逻辑可见性。保留窗口、检查周期、装箱单位及并发失败处理仍需固定；所有 Backend 复用相同布局和发布序列。

## 真实索引测量

声明索引、WAL、缓存位置、资源预算、协议和确认层级，区分已接收、本地持久化与远端持久化。加载、就绪、缓存设置、预热、前台测量、后台排空和结果核对分别计量。

记录吞吐/延迟、索引存储量、读写放大、维护、CPU/内存/GC、实际本地/EFS/S3 I/O；缺失指标为 N/A。上传 Trace/Parquet 不能证明索引存储能力。连接中断或写结果未知时保留诊断，不自动重试非幂等请求；恢复按 Backend 声明验证。

当前无真实 Backend、对象存储性能或故障恢复验收。
