# 已提交变更发送验证

更新：2026-10-20。默认只向下游发送已提交变更，不限定 Index Service 场景。当前单会话 v5 编译在源 commit 后生成该事务的有序查询与修改组，在 abort 时排除修改及修改后的查询；原始 Trace 保留全部事实，不生成反向补偿或回撤开关。

`operation` 默认逐项发送，`transaction` 按源事务分组；两者均同一线程同步执行，不合并修改或重排查询。源 BEGIN/COMMIT 仅由驱动校验，不发送给索引。参考索引逐项应用版本写入，按 ReadTimestamp 查找，不暂存事务或实现事务 API。

## 受控验证

`IndexTraceCompilerTest` 使用独立验证的两行基线和配对 JDBC/PG 测试事件，不改变 HyBench 原生选择器，也不计入原生 SF1 覆盖。

| 测试 | 验证结果 |
| --- | --- |
| 查询后 UPDATE、INSERT、DELETE，再显式回滚 | 三次修改均排除；原始 Trace 不变；回滚前受支持 SEARCH 保留 |
| 回滚后继续 UPDATE 和 INSERT，并提交 | CommitVersion 从 1 开始；账户原 RowID 保留；新 transfer RowID 为 3，回滚插入不占用身份 |
| 查询已回滚 transfer 的二级键 | 实际结果为 0；无回滚写入或反向补偿条目 |
| 只读回滚与后续只读提交 | 回滚不推进版本；只读提交正常推进版本；后续查询读取点正确 |
| 三次行修改后关闭连接并 abort，整次无 commit | 增量 Parquet 行数、规范操作数和提交版本数均为 0；参考回放通过 |
| abort XID 不匹配、事务或扫描未完成 | 拒绝编译，不发布完成 manifest |
| 修改后查询，随后回滚 | 修改与修改后查询均不发送；查询排除量独立统计，原始事实保留 |
| 查询、UPDATE、查询、再次 UPDATE、查询，最终 commit | 查询依次看到基线、第一次更新位置、第二次更新位置；修改不合并 |
| INSERT transfer 后查 sourceid，再 DELETE account 后查主键，最终 commit | 插入后查询得到新条目，删除后查询为空；二级展开连续且查询不插入其中 |
| 相同输入重复编译 | 两份 Trace、两个增量 Parquet 和 manifest 字节一致 |
| 二级开/关 × operation/transaction | Locator 和 RowID 的查询与修改顺序均通过 |

主回滚用例统计为：原始行修改 5、回滚排除 3、已提交行修改 2、展开索引写入 4、SEARCH 4、commit 2、abort 3，规范记录共 12。PG 索引插入尝试单独统计，不等同于下游版本写入。

写后查询用例保留 4 次行修改、6 项索引写入和 5 次查询，另有两个源控制记录，共 13 条；查询携带同一源 transactionId，此前修改位置依次为 0、1、2、3、4。首次查询 ReadTimestamp=0，后四次为源 CommitVersion=1。Locator 依次读取基线位置、第一次更新位置和第二次更新位置；RowID 始终不变。原始日志摘要和重复编译产物一致。

`RequestCaptureAuditTest` 和 `CaptureRunTest` 核对单会话及 HTAP 的 rollback/abort 请求身份，接受配对业务回滚和关闭 abort；拒绝未完成请求、缺失事务结束、错误回滚身份、异常与文件改动。

`ReplayProjectionTest` 验证源 COMMIT 前不发送任何组内操作、目标不接收控制记录、两种模式操作序列相同、跨线程调用拒绝、无效版本/事务/查询位置和展开顺序拒绝、空过滤组不调用目标、超过 1,000,000 项的源组拒绝，以及目标失败后停止后续发送。`IndexOperationTraceTest` 覆盖 v5 写后查询位置往返和旧格式只读兼容。配置测试验证默认 operation、transaction 配置和未知选项拒绝。

## SF1 离线回归

2026 年 10 月 20 日，使用既有原生 TP 子集采集 `results/poc/hybench-sf1/raw/pg-1475807.jsonl` 及其配对 JDBC 日志，重新执行请求审计、v5 编译、两种发送模式回放和独立历史 oracle 验证。不重新执行 SQL 或启动 PG，不修改原生负载。

| 项目 | 结果 |
| --- | ---: |
| 配对请求 / 未关联服务端事件 | 150,463 / 0 |
| 原始行修改 / 已提交行修改 | 76,881 / 76,881 |
| commit / abort / 回滚排除行修改 | 73,582 / 0 / 0 |
| Baseline 索引记录 | 20,706,000 |
| 规范运行期记录，含事务控制 | 422,244 |
| SEARCH / 索引版本写入 | 146,945 / 128,135 |
| 事务内修改后查询 | 25,627 |
| 两种模式发送量，排除控制 | 均为 275,080 |
| 两种模式的 Locator / RowID 参考回放 | 均通过 |
| 修改主键数 | 72,870 |
| 每种 value 的派生主键 / 二级探针 | 299,500 / 512 |

| 发送模式 | 运行期索引操作 | adapter 调用数 | 结果、最终状态、历史版本 |
| --- | ---: | ---: | --- |
| operation | 275,080 | 275,080 | 通过 |
| transaction | 275,080 | 73,582 | 通过且与 operation 一致 |

以上使用二级开启配置；adapter 调用数不是网络请求数。每种 value 核对 992,036 个查询结果项、保留 20,834,134 个索引版本。相同 value 在两种发送模式下的查询结果、选中版本和最终主键状态摘要一致。25,627 个原生扫描处于此前已有行修改的事务内位置，因此使用 TRANSACTION_LOCAL_SEARCH；这不等于 25,627 次都读取同一事务刚修改的同一个键。

增量 Parquet 与历史 v4 产物摘要一致：account 为 `8d7769279fd27cf847b97bd3c312a6d95cdb7200ef3f5ea3de247d573a699b38`，transfer 为 `04a2f9535d8a9c54469999830adebcf06f6c04d0bdc45a778c6b464f261acbb5`。新 Trace 使用 v5 及组内查询位置，不能要求与旧二进制摘要一致。旧 v4 产物使用当前 operation 配置的 Locator/RowID 回放也通过。

两次独立 JVM 编译的 baseline/workload Trace 与两个增量 Parquet 均 SHA256 一致；manifest 的所有字段和值一致，但 Map 的 JSON 字段输出顺序不同，因此 manifest 文件摘要不同。此处验证数据和 Trace 字节确定性，不声称跨 JVM 的 manifest 文本排序确定性。

当前产物位于 `results/validation/hybench-sf1-ordered-replay-20261020/`：`request-audit.json`、`normalized/`、`normalized-repeat/`、`replay-operation.json`、`replay-transaction.json`、两份 snapshots 报告及 `legacy-v4-replay.json`。此前 `results/validation/hybench-sf1-committed-changes-20261020/` 和其他历史日志、manifest、失败产物均保留原记录，不作为当前配置说明。

JDK 17 离线 `mvn -o -q package` 和 74 项测试通过。上述 SF1 输入无回滚，证明已有提交路径无回归；写入后回滚由受控测试验证。既有原生 HTAP 的 13 次业务回滚发生在写入前，详见 [HTAP SF1 报告](hybench-sf1-htap.md)。本次不重新运行完整 HTAP。

## 支持边界

已实现的是单会话已提交源事务内的保序写后查询，以及两种不要求索引事务 API 的发送模式。完整 HTAP 仍只保存无损原始 Trace 并审计八表状态，不能使用单会话编译器；并发快照、跨会话全局顺序、范围或变长键、多表索引维护、来源预期 SQL 错误与重试转换仍待实现。真实 Index Service adapter 尚未接入，单线程参考模型不证明网络服务吞吐、持久化或大规模状态能力。发送规则见 [Trace 契约](../contracts/trace.md)，剩余任务见 [计划](../../plan.md)。
