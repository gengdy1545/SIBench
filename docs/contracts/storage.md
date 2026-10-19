# 存储与测量边界

## 当前 Baseline 输出

generation.output_directory 配置本地 results/datasets/ 下的新目录。生成器拒绝覆盖已有目录，数据文件先写 .partial，完成后原子发布；min_free_disk_bytes 是磁盘保留阈值，不代表已完成全量容量校准。

Baseline 不需要数据库、EFS、S3 或 Index Backend 配置。没有可用后端接口时，不创建带占位 endpoint 的所谓可运行配置。实际文件写入与恢复行为尚未验收。

## 索引存储实验契约

后续真实 Backend 接入遵循 [计划](../../plan.md)：

- 声明索引、WAL、缓存位置、资源预算、协议与持久化确认层级。
- 同一实验固定数据产物、Trace、并发及缓存条件；不同确认语义分组报告。
- 分别测量加载、预热、前台操作和后台排空；记录持久化、compaction 与物理 I/O。
- 文件生成、数据 Rewrite 读写、Lookup 后的数据读取不计入 Index Service 性能。
- RowID 模式暂不执行额外物理位置映射；Rewrite 不产生该模式的索引写入。
- 未提供的观测指标标为 N/A；Trace 或 Parquet 上传对象存储不能替代真实索引 I/O 证据。
- 故障恢复按 Backend 声明验证；结果未知时不自动重试非幂等操作。

当前未实现真实索引适配、对象存储性能对比或故障恢复验证。
