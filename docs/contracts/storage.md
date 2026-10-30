# 存储契约

## 数据文件

- 当前维护的测试配置统一以 512 MiB（536,870,912 字节）作为 `target_file_size_bytes`；参数支持其它正整数字节目标，不限于预设档位。`file_size_max_overshoot_percent` 默认为 5，允许 1–100 的整数百分比；这是允许超量，不是最优布局结论。正常文件的完整字节数须在目标与 `target + floor(target × percent / 100)` 之间。row group 最多 1,048,576 行、编码缓冲最多 64 MiB，接近文件目标时提前封闭较小的组；缓冲阈值不是 JVM 内存硬上限。
- parquet-java 1.15.2、Zstd level 3、单线程压缩、1 MiB page/dictionary，启用 dictionary；codec 支持 zstd/snappy/none，后两者 level=0。
- 原生 PG Baseline 导出按表并发，每表从同一导出快照读取一个主键有序流，按共享字节策略滚动文件，row group 不再直接成为文件。RowID 按 schema / 主键顺序连续分配；FileID 为 `表序号（从 1 开始） + 文件序号（从 0 开始） × 表数`，唯一、确定且不受调度影响。文件清单仍按 schema / 主键顺序排列。单表编码目前为串行，尚无目标规模导出性能对照。
- YCSB 的预检 Schema 全为 varchar/text，使用 PG binary COPY 读取整表的主键有序流，允许 PG 并行扫描 / 排序，避免分段读取反复扫描无序宽堆。COPY 保留 NULL、空串和 UTF-8 值，协议头、列数、长度、截断及结束边界均核验；仍调用相同 Parquet 写入器并执行独立 PG 逐值验证。单次宽排序可能使用接近全表大小的临时空间，必须保留磁盘停止阈值；87,060,000 行同输入对照及全新 YCSB 完整验收通过，时间收益与临时空间代价见 [采集报告](../validation/capture-performance.md#优化结论与复现)。JDBC 非零 fetch count 会使 PG 在执行时禁用并行 worker，见 [PG 14 官方说明](https://www.postgresql.org/docs/14/when-can-parallel-query-be-used.html)。
- 其他来源的无序堆大表先仅对主键确定区间，再依次读取并排序区间内的完整行，以免一次宽行排序占满磁盘。区间数按 PG 表字节与默认 8 GiB 的 `sibench.baseline.sort_range_bytes` 估算，该参数不控制 YCSB 的 COPY 整表读取；它不是临时空间硬上限，偏斜 / 排序开销仍受磁盘停止阈值保护。多个区间共用同一表写入器和快照，不改变文件边界或产生分片尾文件；代价是可能多次扫描堆，时间与空间须共同验收。
- 基线、派生生成与增量共用 `ParquetDataFile`：在有界内存中编码一个 row group，通过 parquet-java 的公开 API 复制压缩页并保留页索引，不重复压缩；按真实元数据序列化计算关闭后的完整大小，包含页索引和 footer。达到目标即滚动，关闭后再次核对实际字节和超量上限；超差拒绝完成发布。单行过大、元数据过多或不适合的组 / page 配置可能使目标无法满足，显式报错，不填充、不拆行、不静默放宽误差。每文件最多 32,767 组，未达目标便触及该限制时拒绝；行号仍是文件内绝对位置。
- 每个 Table / Partition / Bucket 语义写入流结束时允许一个未达目标的尾文件，仍不得超过上限；文件清单的 `file_size` 记录策略、目标、上限、实际字节和是否在流结束时关闭。并行任务分片不产生额外尾文件。达到目标恰好位于流末尾的文件计为正常文件；EOF 刷出剩余组时才达到目标的文件仍记录为流尾。
- 固定 schema、源顺序、参数和 FileID 分配。直接派生生成按表/桶流式写，分桶按源序号取模，不额外排序；原生 PG 导出按主键排序。不引入 Small-File Compaction 或 Z-Order。
- 清单记录行数、组信息及校验 / 身份依据。`prepare` 逐文件以 `.partial` 发布；原生导出与 Trace 编译以完成 manifest 和目录 `INCOMPLETE` 防止消费未完成产物。失败换新目录，不覆盖或续写。
- 磁盘保留默认 20 GiB，是停止阈值，不是全量容量校准。离线文件生成、来源 SQL 和全行审计单列；回放阶段实际发生的 Baseline 解码、调度、传输和结果核对按 [测量边界](../validation/measurement.md) 计入，不能把端到端吞吐当作纯引擎执行速率。

`<table>-versions*.parquet` 在采集后的 Trace 编译中生成，保存已提交 INSERT / UPDATE 的行版本；DELETE 通过版本化索引失效表示。后端服务回放复用相同输入，不重新生成这些 Parquet。RocksDB / rocksdb-cloud 回放时生成的 SST / WAL / MANIFEST 属于索引状态，由引擎的 memtable、flush、compaction 和 OPTIONS 控制，不使用 `data_layout` 作为 SST 大小参数；当前服务配置也未提供这些引擎参数的通用调优接口。

调整原生采集配置的 `data_layout.target_file_size_bytes` 后，重新编译的增量 Parquet 使用新目标；修改 replay 配置不会重切已有 Parquet。共享 Baseline 的布局身份包含目标、允许超量及 `compressed_bytes_v1` 策略版本；旧布局不匹配时明确拒绝，不自动覆盖，旧输入和有效配置按原验收身份保留。布局变化必须重建相应 Locator / Physical Trace；RowID 不含文件位置，其完整解析契约仍须单独定义。

当前字节策略已通过三来源 87,060,000 行规模的 Baseline 新导出、增量编译、完整参考 / 独立 oracle 和 COS 回放，正常 / 尾文件及 row group 分布见 [产物测量](../validation/measurement.md#parquet-文件与-trace-产物)。192 MiB 受控验证说明目标并非写死为 512 MiB；其它目标按配置及本契约执行。数据文件 Rewrite 生命周期和大小敏感性实验仍按 [计划](../../plan.md) 单独验收。

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

拟采用 `Validity = RetainedRows / TotalRows`，有效率阈值及最小可回收字节为待验证的策略参数。RetainedRows 是当前版本与仍受保护历史点所需版本的去重集合；不能把受保护旧版本算作可回收，行比例也不等于字节比例。

在固定逻辑边界检查，按确定顺序在同 Table/Partition/Bucket 内装箱，使用相同文件、组及压缩规则。生成完整键、确切版本、旧/新 Locator 的 relocation；Locator 更新位置，RowID 不写索引。发布屏障完成且旧位置不再被在途操作引用后才回收旧文件。全部可回收时无需替代文件。

Rewrite 不改变键、CommitVersion、RowID 或逻辑可见性。保留窗口、检查周期、装箱单位及并发失败处理仍需固定；所有 Backend 复用相同布局和发布序列。

`check-relocation`（`RelocationCheck`）是 manifest 驱动的受控诊断：选择全部实际修改表的最新存活行，分配高于已有文件的 FileID，写回真实 Parquet 并更新全部源索引的精确版本 Locator；RowID 验证不产生索引搬迁写。该诊断目前每表仅允许一个输出文件，需要滚动时明确拒绝；它不保留或搬迁受保护旧版本，不自然触发、不回收旧文件，不能据此宣称 Rewrite 生命周期完成。Locator 由 FileID 与文件内行号两个 int64 表示，FileID 必须为正、行号非负；参考模型、搬迁与查询摘要均保留完整两个值。

## 真实索引测量

声明索引、WAL、缓存位置、资源预算、协议和确认层级，区分已接收、本地持久化与远端持久化。加载、就绪、缓存设置、预热、前台测量、后台排空和结果核对分别计量。

记录吞吐/延迟、索引存储量、读写放大、维护、CPU/内存/GC、实际本地/EFS/S3 I/O；缺失指标为 N/A。上传 Trace/Parquet 不能证明索引存储能力。连接中断或写结果未知时保留诊断，不自动重试非幂等请求；恢复按 Backend 声明验证。

对象存储后端目前只有 rocksdb-cloud：小输入结果核对、正常重开、仅从 bucket 冷启动，以及最后一次 Flush 边界的 SIGKILL 正反验证已通过（[rocksdb-cloud 验证](../validation/rocksdb-cloud.md)），持久化层级见 [Index Service 契约](index-service.md#rocksdb-cloud-后端)。每次 Flush 后的引擎统计与对象存储请求计数写入 `<index>.engine-stats.jsonl`。三来源目标规模与成本见 [完整验收](../validation/acceptance.md)；正式性能排名、目标规模崩溃恢复和断电级耐久性未证明。
