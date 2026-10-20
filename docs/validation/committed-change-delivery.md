# 已提交变更发送验证

默认只向下游发送已提交变更，不限定 Index Service 场景。原始 Trace 保留修改尝试和事务结果；单会话 v4 编译器在 commit 时生成增量 Parquet 和索引修改，在 abort 时丢弃待发布修改，不生成反向补偿，也不增加回撤开关。查询遵循独立可见性契约。

## 受控回滚验证

`IndexTraceCompilerTest` 使用独立验证的两行基线和配对 JDBC/PG 测试事件，不改变 HyBench 原生选择器，也不计入原生 SF1 覆盖。

| 测试 | 验证结果 |
| --- | --- |
| 查询后 UPDATE、INSERT、DELETE，再显式回滚 | 三次修改均排除；原始 Trace 不变；回滚前受支持 SEARCH 保留 |
| 回滚后继续 UPDATE 和 INSERT，并提交 | CommitVersion 从 1 开始；账户原 RowID 保留；新 transfer RowID 为 3，回滚插入不占用身份 |
| 查询已回滚 transfer 的二级键 | 实际结果为 0；无回滚写入或反向补偿条目 |
| 只读回滚与后续只读提交 | 回滚不推进版本；只读提交正常推进版本；后续查询读取点正确 |
| 三次行修改后关闭连接并 abort，整次无 commit | 增量 Parquet 行数、规范操作数和提交版本数均为 0；参考回放通过 |
| abort XID 不匹配、事务或扫描未完成 | 拒绝编译，不发布完成 manifest |
| 修改后读取自身未提交数据，随后回滚 | 仍拒绝规范转换，不伪装成已提交版本 SEARCH |
| 相同输入重复编译 | 两份 Trace、两个增量 Parquet 和 manifest 字节一致 |
| 二级索引与事务合并的四种组合 | Locator 和 RowID 参考回放均通过 |

主回滚用例统计为：原始行修改 5、回滚排除 3、已提交行修改 2、展开索引写入 4、SEARCH 4、commit 2、abort 3，规范记录共 12。PG 索引插入尝试单独统计，不等同于下游版本写入。

`RequestCaptureAuditTest` 和 `CaptureRunTest` 核对单会话及 HTAP 的 rollback/abort 请求身份，接受配对业务回滚和关闭 abort；拒绝未完成请求、缺失事务结束、错误回滚身份、异常与文件改动。`ReplayProjectionTest` 验证写入在 commit 前不发送，报告 `change_delivery=committed_only`。

## SF1 离线回归

2026 年 10 月 20 日，使用既有原生 TP 子集采集 `results/poc/hybench-sf1/raw/pg-1475807.jsonl` 及其配对 JDBC 日志，重新执行请求审计、v4 编译和默认发送配置下的参考回放，不重新执行 SQL 或启动 PG。

| 项目 | 结果 |
| --- | ---: |
| 配对请求 / 未关联服务端事件 | 150,463 / 0 |
| 原始行修改 / 已提交行修改 | 76,881 / 76,881 |
| commit / abort / 回滚排除行修改 | 73,582 / 0 / 0 |
| Baseline 索引记录 | 20,706,000 |
| 规范运行期记录，含事务控制 | 422,244 |
| SEARCH / 索引版本写入 | 146,945 / 128,135 |
| 默认合并后发送量，排除控制 | 275,079 |
| 合并省略量 | 1 |
| Locator / RowID 参考回放 | 均通过 |

两份 Trace 和两个增量 Parquet 的 SHA256 与 `results/validation/hybench-sf1-snapshot-lookup-20261020/normalized/` 一致。新 manifest 增加提交变更筛选统计，不要求与旧 manifest 字节一致。

产物位于 `results/validation/hybench-sf1-committed-changes-20261020/`：`request-audit.json`、`normalized/` 和 `replay.json`。`normalized-interrupted-build-20261020/` 保留构建替换运行中 JAR 导致的失败产物，不作为验证成功证据，不复用该目录。

JDK 17 离线 `mvn -o -q package` 和 65 项测试通过。上述 SF1 输入无回滚，证明既有提交路径无回归；写入后回滚由受控测试验证。既有原生 HTAP 的 13 次业务回滚发生在写入前，详见 [HTAP SF1 报告](hybench-sf1-htap.md)。

既有完整 HTAP 的四会话输入也通过更新后的摘要和配对审计。随后单会话编译器按预期拒绝 HTAP 投影，未创建规范输出目录；正常业务回滚不使原始采集失效，也不因此放宽并发可见性限制。

## 支持边界

完整 HTAP 仍只保存无损原始 Trace 并审计八表状态，不能使用单会话 v4 编译器。并发快照、范围或变长键、多表索引维护、事务内读取自身修改、来源预期 SQL 错误与重试转换仍待实现；真实 Index Service adapter 尚未接入。发送规则见 [Trace 契约](../contracts/trace.md)，剩余任务见 [计划](../../plan.md)。
