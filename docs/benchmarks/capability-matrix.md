# 当前能力

来源说明：[HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。各来源独立选规模，不要求行数或字节量相等。

| 能力 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 派生 Baseline / dry-run | 已实现 / 通过 | 已实现 / 通过 | 已实现 / 通过 |
| 来源 schema / PG 预检 | 八表完整列、8 PK + 13 二级；导出 / run 前自动核对 | JDBC TEXT 字段布局与字符串 PK；独立预检 | 配置表数、id PK + k 二级、SERIAL 默认值；独立预检 |
| 候选大规模配置 | 1000x，87.06 亿行 | 10 亿行，10 × 100 字节 | 100 表 × 1000 万行 |
| 全量容量校准 | 未执行 | 未执行 | 未执行 |
| 原生执行采集 | SF1 默认 runHTAP，AP → TP → 并发 AT/IQ，八表触发器 | 未实现 | 未实现 |
| Parquet / 行状态核对 | SF1 基线 8,706,000 行；HTAP 最终八表 8,707,767 行通过；TP 子集增量重复编译通过 | 来源产物未验证 | 来源产物未验证 |
| 原始 Trace | HTAP 多会话无损事件打包，非全局提交排序 | 未实现 | 未实现 |
| 规范 Trace | 单会话 v7：八表主键及全部 13 二级维护、索引列值更新 / 删除重插入、单列等值、写后查询和源 SQL 工作单元；完整 HTAP 访问 / 快照投影未完成 | 未实现 | 未实现 |
| 默认提交变更筛选 | 单会话支持；回滚修改及修改后查询仅保留原始事实，不发送或补偿；受控验证通过 | 未实现 | 未实现 |
| 保序发送 | 一个全局发送线程，按工作单元路由多 client，同事务可跨 client；两种输入单位最终均逐项发送，异步响应及在途管理；无下游事务 API | 未实现 | 未实现 |
| 参考回放 | TP 子集两种 value、二级开关、两种输入模式、1/64/128 client、历史 oracle；进程内非网络验证 | 未实现 | 未实现 |
| 真实 Backend | 未接入 | 未接入 | 未接入 |
| 自然 Rewrite / GC / Checkpoint | 未实现 | 未实现 | 未实现 |

默认 HTAP 已启用 AP 和多表事务，TP3/5/9 子集原生修改仅为 savingaccount UPDATE / transfer INSERT，不含已有索引列值 UPDATE。默认 runHTAP 不运行 freshness DELETE；DELETE / 重插入仍只有受控测试证据。全索引维护逻辑与完整 HTAP 访问投影分开：后者的范围、快照、跨会话可见性及持续调度尚未完成。原生逐行因果维护审计不声称恢复了全局提交顺序。

详细结果见 [HTAP SF1 验证](../validation/hybench-sf1-htap.md) 和 [TP 子集验证](../validation/hybench-sf1-poc.md)，统一版本及发送语义见 [Trace 契约](../contracts/trace.md)，剩余工作见 [计划](../../plan.md)。

Schema 预检范围见 [来源 Schema 契约](../contracts/source-schema.md)。固定源码解析、元数据差异 / JDBC 测试及 PG14.24 的 51 项集成检查已通过，包含既有 HyBench SF1、三来源独立布局及采集前失败边界，见 [预检验证](../validation/source-schema-preflight.md)。YCSB / sysbench 使用独立空表布局测试，不能解释为它们的原生生成、执行或采集已通过。
