# 当前能力

来源说明：[HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。原生采集验证按 HyBench SF1 的 8,706,000 行对齐基线行数；索引条目数和字节量不相等，正式实验的容量校准另行进行。

| 能力 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 派生 Baseline / dry-run | 已实现 / 通过 | 已实现 / 通过 | 已实现 / 通过 |
| 来源 schema / PG 预检 | 八表完整列、8 PK + 13 二级；导出 / run 前自动核对 | JDBC TEXT 字段布局与字符串 PK；独立预检 | 配置表数、id PK + k 二级、SERIAL 默认值；独立预检 |
| 候选大规模配置 | 1000x，87.06 亿行 | 10 亿行，10 × 100 字节 | 100 表 × 1000 万行 |
| 全量容量校准 | 未执行 | 未执行 | 未执行 |
| 原生执行采集 | SF1 默认 runHTAP，AP → TP → 并发 AT/IQ，八表触发器 | 8,706,000 行，A/B/C/F/D/E 共 349,959 个请求，单会话原生 JDBC | 8 表 × 1,088,250 行，六个 oltp 脚本共 194,000 次执行，单会话 libpq |
| Parquet / 行状态核对 | SF1 基线 8,706,000 行；HTAP 最终八表 8,707,767 行通过；TP 子集增量重复编译通过 | 8,706,000 行 Baseline 逐值核验与采集后行状态通过；派生大规模产物未验证 | 8,706,000 行 Baseline 逐值核验与采集后行状态通过；派生大规模产物未验证 |
| 原始 Trace / 访问账本 | HTAP 多会话无损事件打包，非全局提交排序；TP 子集账本 v2 | 账本 v2：按 `query_id` 的模板、计划变体、访问路径与投影决定 | 同左 |
| 规范 Trace | TP 子集 v8：130,092 点查（含 BitmapOr 两侧），0 拒绝；并发 HTAP 拒绝 | v8：字符串主键保序编码，327,076 点查、19,030 带 limit 区间，0 拒绝 | v8：149,000 点查、32,000 区间、`k` 维护，0 拒绝 |
| 默认提交变更筛选 | 单会话支持；回滚修改及修改后查询仅保留原始事实，不发送或补偿；受控验证通过 | 同一编译器；本轮无回滚 | 同一编译器；本轮无回滚 |
| 保序发送 | 一个全局发送线程，按工作单元路由多 client，同事务可跨 client；两种输入单位最终均逐项发送，异步响应及在途管理；无下游事务 API | 同左 | 同左 |
| 参考回放 | v8 TP 子集两种 value、两种输入模式、1/64/128 client、oracle；进程内非网络验证 | v8 1/64/128 client 两种 value、oracle 通过 | v8 128 client 两种 value、oracle 通过 |
| 真实 Backend | 未接入 | 未接入 | 未接入 |
| 自然 Rewrite / GC / Checkpoint | 未实现 | 未实现 | 未实现 |

默认 HTAP 已启用 AP 和多表事务，TP3/5/9 子集原生修改仅为 savingaccount UPDATE / transfer INSERT，不含已有索引列值 UPDATE。默认 runHTAP 不运行 freshness DELETE；DELETE / 重插入仍只有受控测试证据。全索引维护逻辑与完整 HTAP 访问投影分开：后者的范围、快照、跨会话可见性及持续调度尚未完成。原生逐行因果维护审计不声称恢复了全局提交顺序。

详细结果见 [YCSB / sysbench 原生 Trace 验证](../validation/ycsb-sysbench-native-trace.md)、[TP 子集 v8 验证](../validation/hybench-sf1-tp-trace.md) 和 [HTAP SF1 验证](../validation/hybench-sf1-htap.md)，统一版本及发送语义见 [Trace 契约](../contracts/trace.md)，剩余工作见 [计划](../../plan.md)。

Schema 预检范围见 [来源 Schema 契约](../contracts/source-schema.md)。固定源码解析、元数据差异 / JDBC 测试及 PG14.24 的 51 项集成检查已通过，包含既有 HyBench SF1、三来源独立布局及采集前失败边界，见 [预检验证](../validation/source-schema-preflight.md)。YCSB / sysbench 使用独立空表布局测试，不能解释为它们的原生生成、执行或采集已通过。
