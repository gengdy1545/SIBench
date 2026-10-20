# 当前能力

来源说明：[HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。各来源独立选规模，不要求行数或字节量相等。

| 能力 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 派生 Baseline / dry-run | 已实现 / 通过 | 已实现 / 通过 | 已实现 / 通过 |
| 候选大规模配置 | 1000x，87.06 亿行 | 10 亿行，10 × 100 字节 | 100 表 × 1000 万行 |
| 全量容量校准 | 未执行 | 未执行 | 未执行 |
| 原生执行采集 | SF1 单会话 TP3/TP5/TP9 | 未实现 | 未实现 |
| Parquet 核对 | SF1 原生全行与增量重复编译通过 | 来源产物未验证 | 来源产物未验证 |
| 规范 Trace | v4，当前/历史点及 transfer 二级等值 | 未实现 | 未实现 |
| 参考回放 | 两种 value、二级/合并开关、历史 oracle | 未实现 | 未实现 |
| 真实 Backend | 未接入 | 未接入 | 未接入 |
| 自然 Rewrite / GC / Checkpoint | 未实现 | 未实现 | 未实现 |

SF1 原生修改为两表 UPDATE/INSERT，没有 AP、DELETE 或历史点 SQL；DELETE/重插入仅受控测试。历史主键与二级等值查找已完成，完整多表 CRUD、历史范围、并发可见性和持续调度未完成。

详细结果见 [SF1 验证](../validation/hybench-sf1-poc.md)，统一版本及发送语义见 [Trace 契约](../contracts/trace.md)，剩余工作见 [计划](../../plan.md)。
