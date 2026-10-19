# 数据来源与当前能力

本矩阵描述当前实现，不引用已清理的原生运行验证。来源细节见 [HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。

| 维度 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 原生规模参数 | sf 对应各表行数 | recordcount、fieldcount、fieldlength | tables、table-size |
| 当前配置 | 1000x | 10 亿行，10 × 100 字节字段 | 100 表 × 1000 万行 |
| 来源适配 | 确定性派生，读取参数及词表 | 确定性派生，保留键与字段规则 | 确定性派生，保留表结构与 uniform 分布 |
| Baseline Java 实现 | 已实现 | 已实现 | 已实现 |
| 配置与 schema dry-run | 已通过 | 已通过 | 已通过 |
| 实际 Parquet 写入及重建验证 | 未执行 | 未执行 | 未执行 |
| 容量及吞吐校准 | 未执行 | 未执行 | 未执行 |
| 增量 Trace、Rewrite、Checkpoint | 待实现 | 待实现 | 待实现 |
| Index Backend 回放 | 待实现 | 待实现 | 待实现 |

配置校验与 dry-run 不生成记录或数据文件。各来源独立选择规模，不要求记录数或字节量精确相同。FileID + FileRowPosition 与 RowID 两种 value 模式及版本语义以 [计划](../../plan.md) 为准。

后续增量适配必须处理真实键与范围、版本时间点、业务修改的前置查询及结果依赖；不能把数据库 SQL 数或事务数直接当成索引操作数。
