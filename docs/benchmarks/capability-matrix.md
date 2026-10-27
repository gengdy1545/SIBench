# 当前能力

来源说明：[HyBench](hybench.md)、[YCSB](ycsb.md)、[sysbench](sysbench.md)。原生采集验证按 HyBench SF1 的 8,706,000 行对齐基线行数；索引条目数和字节量不相等，正式实验的容量校准另行进行。

| 能力 | HyBench | YCSB | sysbench |
| --- | --- | --- | --- |
| 派生 Baseline / dry-run | 已实现 / 通过 | 已实现 / 通过 | 已实现 / 通过 |
| 来源 schema / PG 预检 | 八表完整列、8 PK + 13 二级；导出 / run 前自动核对 | JDBC TEXT 字段布局与字符串 PK；导出 / run 前自动核对 | 配置表数、id PK + k 二级、SERIAL 默认值；导出 / run 前自动核对 |
| 候选大规模配置 | 1000x，87.06 亿行 | 10 亿行，10 × 100 字节 | 100 表 × 1000 万行 |
| 全量容量校准 | 未执行 | 未执行 | 未执行 |
| 原生执行采集 | SF1 `runHTAP`：AP → TP → 并发 AT/IQ，4 个会话，八表触发器；从静止已验证库克隆 | 8,706,000 行，A/B/C/F/D/E 依次运行，单线程原生 JDBC | 8 表 × 1,088,250 行，六个 oltp 脚本依次运行，单线程 libpq |
| Baseline / 行状态核对 | 八表 8,706,000 行逐值核验，采集后八表状态审计通过 | 逐值核验与采集后行状态审计通过 | 逐值核验与采集后行状态审计通过 |
| 访问账本 | v3：可见行身份、数组键元素、bitmap 组合树、BitmapAnd 分轮成组 | v3：按 `query_id` 的模板、计划变体、访问路径与投影决定 | 同左 |
| 规范 Trace（v9） | 并发会话按事务完成位置归并；全部访问投影，含 BitmapAnd 交集组、数组并集组、二级前段加钉住点查；全部单元通过快照交叉验证 | 字符串主键保序编码；主键点查与带 limit 区间，全部投影 | 主键点查与区间、`k` 维护，全部投影 |
| 回滚与写后查询 | 回滚修改及修改后查询仅保留原始事实，不发送或补偿；已提交事务的写后查询保留原语句快照并只叠加本事务此前修改（原生 PG 两会话反例、参考 / 服务回放见 P0 验证文档） | 同一编译器 | 同一编译器 |
| 保序发送 | 一个全局发送线程，按工作单元路由多 client，同事务可跨 client；两种输入单位最终均逐项发送，查询组整体发送或过滤；无下游事务 API | 同左 | 同左 |
| 参考回放 | 1/64/128 client、两种输入模式、两种 value 与独立 oracle 通过；重复编译逐字节一致；进程内非网络验证 | 同左 | 同左 |
| 真实 Backend | 未接入 | 未接入 | 未接入 |
| 自然 Rewrite / GC / Checkpoint | 未实现 | 未实现 | 未实现 |

投影原则：原生执行是确定的，每个访问投影成一组能重现原生结果集的 KV 查询，每个期望数都是原生事实，见 [Trace 契约](../contracts/trace.md) 的"访问投影"。只在事实缺失时拒绝（有损页不带 TID、堆扫描提前停止未判定部分候选、全可见 index-only 行没有主键），或因超出固定来源范围拒绝（嵌套 bitmap 组合、行比较、复合键、非 C collation 的文本区间等）。账本按原因计数（`projection.rejected`），任一拒绝使整次编译失败；三来源当前负载均无拒绝。

HyBench 的索引列值更新来自 `loanapps.timestamp`（AT3）与 `loantrans.timestamp`（TP14/15/16、AT4）。默认 runHTAP 不运行 freshness DELETE；DELETE / 重插入的原生覆盖来自 sysbench `oltp_read_write` 与 `oltp_delete`。YCSB 只有主键索引，不产生二级维护或访问组形态。

详细结果见 [HTAP SF1 验证](../validation/hybench-sf1-htap.md) 与 [YCSB / sysbench 原生 Trace 验证](../validation/ycsb-sysbench-native-trace.md)，统一版本及发送语义见 [Trace 契约](../contracts/trace.md)，剩余工作见 [计划](../../plan.md)。

Schema 预检范围见 [来源 Schema 契约](../contracts/source-schema.md)；离线差异 / JDBC 测试与 PG14.24 集成检查见 [预检验证](../validation/source-schema-preflight.md)。
