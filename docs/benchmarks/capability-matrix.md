# 当前能力

来源说明：[HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。各来源独立选规模，不要求行数或字节量相等。

| 能力 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 派生 Baseline / dry-run | 已实现 / 通过 | 已实现 / 通过 | 已实现 / 通过 |
| 候选大规模配置 | 1000x，87.06 亿行 | 10 亿行，10 × 100 字节 | 100 表 × 1000 万行 |
| 全量容量校准 | 未执行 | 未执行 | 未执行 |
| 原生执行采集 | SF1 默认 runHTAP，AP → TP → 并发 AT/IQ，八表触发器 | 未实现 | 未实现 |
| Parquet / 行状态核对 | SF1 基线 8,706,000 行；HTAP 最终八表 8,707,767 行通过；TP 子集增量重复编译通过 | 来源产物未验证 | 来源产物未验证 |
| 原始 Trace | HTAP 多会话无损事件打包，非全局提交排序 | 未实现 | 未实现 |
| 规范 Trace | TP 子集 v4，当前/历史点及 transfer 二级等值；HTAP 投影未完成 | 未实现 | 未实现 |
| 默认提交变更筛选 | 单会话 v4 支持；回滚修改仅保留原始事实，不发送或补偿；写入后回滚已受控验证 | 未实现 | 未实现 |
| 参考回放 | TP 子集两种 value、二级/合并开关、历史 oracle | 未实现 | 未实现 |
| 真实 Backend | 未接入 | 未接入 | 未接入 |
| 自然 Rewrite / GC / Checkpoint | 未实现 | 未实现 | 未实现 |

默认 HTAP 已启用 AP 和多表事务，旧 TP 子集的原生修改仅为两表 UPDATE/INSERT。默认 runHTAP 不运行 freshness DELETE；DELETE/重插入仅受控测试。历史主键与二级等值查找已在 TP 子集完成，HTAP 的多表规范投影、历史范围、并发可见性和持续调度未完成。

详细结果见 [HTAP SF1 验证](../validation/hybench-sf1-htap.md) 和 [TP 子集验证](../validation/hybench-sf1-poc.md)，统一版本及发送语义见 [Trace 契约](../contracts/trace.md)，剩余工作见 [计划](../../plan.md)。
