# Baseline 产物与 Trace 边界

当前 Java Baseline 实现由工作负载 YAML 驱动，接口和参数说明见 [README](../../README.md#baseline-生成配置)。增量 Logical/Physical Trace 以 [计划](../../plan.md) 为目标，尚未实现。

## Baseline 产物

- resolved-config.json：生成时的完整生效配置。
- files.jsonl：按发布顺序记录 FileID、相对路径、表、桶、行数、实际字节数、SHA256、row group 信息及源序号规则。
- Parquet：保留业务列、主键列、_sib_row_id 与 _sib_timestamp。
- manifest.json：记录完成状态、总行数、文件数、清单与配置摘要、源码版本及运行环境。
- INCOMPLETE：生成未提交的标记，存在时不得视为完整数据集。

Baseline 仅有 INSERT，不执行 Rewrite。读取主键、版本和 RowID 列，结合文件身份与文件内绝对行号，即可构造两种 value 模式的加载条目。文件清单不是已实现的逐请求增量 Trace。

## 身份与定位

IndexKey 为 (TableNamespace, PrimaryKey, TimestampDescending)。Physical Value 为 (FileID, FileRowPosition)，Logical Value 为 RowID。FileRowPosition 从 0 开始，不需要将 RowGroupID 加入默认 value。

RowID 在同一数据集内按固定表顺序及源序号分配；FileID 按生成顺序分配。不同配置对应的数据集必须隔离。当前使用 JSON 数值表达 int64，消费者须采用无损整数解析，不能经 IEEE-754 double 转换。

## 完成条件与待验证项

消费者需同时检查 manifest.json 存在、INCOMPLETE 不存在及校验值匹配。当前中断产物不可续写，必须换新输出目录从头重建。

实际 Parquet 写入、读取和确定性重建尚未执行验收；Checkpoint、并发事件调度、Lookup 开关及 Rewrite relocation 的增量协议仍待实现。
