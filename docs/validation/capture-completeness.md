# 原生采集与完整性验证

更新：2026-10-30。三来源使用同一 `scripts/native-capture.sh` 入口，采集后再编译规范 Trace。当前应用、输入和完整结果统一见 [三来源验收](acceptance.md)，资源条件见 [采集环境](capture-performance.md)。

## 当前采集路径

| 来源 | 原生执行 | 投影与维护 |
| --- | --- | --- |
| HyBench | 固定上游 `runHTAP`，同一克隆库依次 AP / TP / XP，阶段之间不恢复数据；TP 和 XP 保留原生并发 | 八表、8 主键和 13 二级；包含数组并集、bitmap 交集和提前停止扫描的受支持投影 |
| YCSB | 固定 JDBC binding，A / B / C / F / D / E，原生线程与 Return 计数 | 字符串主键点查与带 limit 区间；没有二级索引，不虚构二级维护 |
| sysbench | 固定 libpq 和六个 oltp Lua 脚本，原生线程与事件数 | 多表主键、k 二级、DELETE / 同主键重插入 |

静止基库由原生生成 / 装载并验证，采集使用新克隆。复用 Baseline 必须匹配原生库、schema、参数与验证身份；输入身份不匹配即拒绝。HyBench 运行时数据文件属于来源输入，不能作为旧实验输出删除。固定来源与配置说明见 [HyBench](../benchmarks/hybench.md)、[YCSB](../benchmarks/ycsb.md)、[sysbench](../benchmarks/sysbench.md)。

## 相互独立的核验职责

1. **Schema 与 Baseline**：采集前核对完整来源定义和实际 PG catalog；Baseline 核对原生全列、主键顺序、连续 RowID 和 version 0。见 [Schema 验证](source-schema-preflight.md)。
2. **请求与会话**：关联 JDBC / libpq 请求、结果摘要、事务结局和 PG 后端会话；缺事件、未完成流或非预期来源错误拒绝。异常、原生重试和业务回滚不是同一种结局，未分类错误维持 fail-closed。
3. **行状态**：从 Baseline、已提交旧 / 新行和原生 PG 终态三路核对，排除回滚修改，保留同会话 / 同键因果链；PK UPDATE（包括回滚尝试）拒绝。见 [行审计](row-state-audit.md)。
4. **访问账本与计数器**：每个访问投影或明确拒绝；原生执行前后等会话退出和统计稳定后记录 `pg_stat_user_indexes`，与同遍账本比较。一致才提交 Trace。
5. **提交与快照**：并发会话按 PG 事务完成计数器归并，快照自己的 xip 独立交叉核对；不按 PID、XID 大小、文件顺序或墙钟猜测。事务内读保留原语句快照并只叠加本事务此前写入。
6. **产物与结果**：Trace / work unit / Parquet 身份、防截断和来源绑定由预检核对；参考模型、独立 Parquet oracle 与真实服务逐查询摘要分别验收，模型间相等不能替代原生事实。

PG14 的 `idx_tup_read` 比较候选数；`idx_tup_fetch` 比较经 index heap fetch 可见的行，不包含 bitmap 堆检查和全可见 index-only 的可见行；`idx_scan` 比较从访问推导出的树下降上下界。PG14 统计通过 UDP 上报，丢失可能造成偏低；本轮没有人为丢统计事件的故障注入。

最新三来源均重新执行完整审计，`capture_completeness=consistent`、受支持投影完整、Trace 完成并生成新的认证；不以旧认证代替本次 full 审计。原生数据、会话、行审计、计数器和 manifest 在各来源主机 `/data/sibench/results/poc/<来源运行目录>/<source>/capture/`，具体目录、摘要和 YCSB Baseline 重试复用边界见 [最新完整验收](acceptance.md#输入与身份)，选定报告按相同运行目录同步本地。

## 当前格式与有界状态

后端写 `pg-<pid>.jsonl.zst`；summary 默认逐访问汇总扫描与 bitmap 判定，客户端保留结果哈希。rows 模式用于逐行核对 summary，不另造兼容读取路径。`IndexTraceCompilerTest.concurrentSummaryCapturesCompileLikeTheirRows` 保留跨事务片段的格式延续反例，`ServerTraceTest` 拒绝截断 zstd 流。当前测试验证格式与结果不变量；已撤下历史格式迁移的体积和加速结论。

| 状态 / 边界 | 当前验证职责 |
| --- | --- |
| 行审计外排 | 小预算与默认预算报告 / 变化等价，错误旧值与错误终态仍拒绝 |
| 编译落盘与提交窗口 | 小预算与默认预算 Trace 等价；窗口外快照明确拒绝，rollover 的 FileID 唯一且可由 oracle 解码 |
| 参考基线 / 版本历史落盘 | 内存与强制落盘模型在点查、范围、降序、limit、更新删除下逐项相等 |
| Locator | FileID 和行位置为两个 int64，高位变化仍可区分，搬迁保持版本 |
| 认证 | 绑定 jar、PG、触发器、来源定义、文件摘要和覆盖类别；替换构建、删除访问事实或出现认证未覆盖路径必须失败 |

完整审计后的变化文件由管线按规则清理，此后重编使用该次采集自己的认证；必要 raw、Baseline、身份和认证仍保留。认证是已核验输入的有界复用，不是证明任何未来负载都完整。

## 边界与复现

构建与命令见 [README](../../README.md)。当前采集配置为 `config/hybench_sf10.yaml`、`config/ycsb_87m.yaml`、`config/sysbench_87m.yaml`；重跑使用新的输出目录，实际运行以证据内有效配置为准。短输入、受控 SQL 和 full 输入各证明自己的边界，不冒充正式长时性能或 SF1000 容量。

尚未支持分段续编、Checkpoint 或结果未知时的服务续发。版本、历史深度和查询组超出本轮输入的累计资源风险仍须另验；计划中的失败分类、Rewrite / GC 与正式重复实验见 [剩余工作](../../plan.md#剩余工作)。
