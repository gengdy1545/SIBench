# 正确性反例与验证依据

更新：2026-10-29。本文保留能区分正确与错误行为的原生事实、手算反例和受控交错。读取语义见 [Trace 契约](../contracts/trace.md#身份与版本)，发布、结果摘要及持久化语义见 [Index Service 契约](../contracts/index-service.md)。本轮构建、测试数量和完整输入结果统一见 [完整验收](acceptance.md)。

## 原生两会话反例

来源为固定 sysbench `3ceba0b1e115f8c50d1d045a4574d8ed643bd497`，Baseline 为 8 表 × 1,088,250 行，共 8,706,000 行。受控 SQL 在独立克隆上执行，保留 PG 后端事件与原始结果，不写共享静止库。

1. T1 更新 `sbtest1.id=1` 的 `c`，随后在原语句快照中读取 `k=302261`。
2. T2 在 T1 快照后把 `id=2` 的 `k` 改为 `400001`，插入 `id=2000001,k=302261` 并提交。
3. T1 提交。原生 T1 查询返回 **id=2**，行审计终态包含 T1 更新、T2 改键和新增行。

规范查询为 `TRANSACTION_LOCAL_SEARCH`，`readTimestamp=0`、本事务 `version=2`、`mutationOrdinal=1`。正确读取保留原语句快照，只叠加本事务此前修改；若改用本事务提交版本作为读点，会错误返回 **id=2000001@v1**。两者条数同为 1，因此只核对结果数量不能发现问题。对应回归为 `IndexTraceCompilerTest.transactionLocalSearchKeepsItsSnapshotAndOnlyItsOwnWrites`。

原始事实采集于 2026-10-27；本轮 `acceptance-20261029T053122Z` 使用冻结提交 `bbff9a3` 重新验证，未重跑原生 SQL。证据为 [native-read-counterexample](../../results/validation/acceptance-20261029T053122Z/native-read-counterexample/)：

| 证据 | 作用 |
| --- | --- |
| `original/raw/`、`original/trace/` | 原始两会话事件、请求 / 行审计和编译身份，保持原样 |
| `original/old-semantics.txt` | 同条数错主键的反事实观测，不是保留的旧实现 |
| `input-relocation.json` | 原 manifest 与迁移后 manifest 摘要；Baseline 验证摘要匹配，数据与逻辑摘要不变 |
| `reference.json`、`oracle.json` | 当前 Locator / RowID 参考回放各 3 个查询、两种 value 的独立 Parquet 行历史校验均通过 |
| `locator.json`、`row_id.json` | 当前进程内 RocksDB，各 3 次逐查询摘要核对、0 不匹配 |
| `status.json`、`*-command.json`、`*.time`、`*.log` | 同轮退出状态、命令与资源 |

完整可执行输入在 `sibench_4:/data/sibench/results/validation/acceptance-20261029T053122Z/native-read-counterexample/input/`，包含 Baseline 和 Trace；本地保留选定报告与小型原生事实。逻辑 Baseline SHA-256 为 `7dc20a21756b20f780f5f8c3b8551aa86765f2f5596c9d90ac8f866589587505`，workload SHA-256 为 `50a248725276c699d77d404cb333f682a509f7946abf3b011e197f7b84964c37`。

该手工事实没有 PG index counter 前后快照，`capture_completeness=not_checked`；请求配对、会话和行审计通过。它验证快照与本事务写入前缀，不能代替完整原生采集认证或性能对照。

## 读取与结果校验反例

| 场景 | 预期与独立依据 | 测试 |
| --- | --- | --- |
| 事务内多次改键、删除 / 重插入、主键与二级历史读 | 原语句快照叠加本事务前缀，旧值不复活，重插入保留新身份；由原生事实与 Parquet 行历史交叉验证 | `RocksDbBackendTest`、`IndexTraceCompilerTest`、独立 oracle |
| 事务内查询缺失本事务版本 | 拒绝歧义输入 | `IndexReferenceSearchTest.oldV9LocalSearchFormWithoutItsOwnVersionIsRejected` |
| 同条数错 PK、version、value、重复、倒序、跨查询交换 | 手算摘要必须失败并定位源事件；覆盖 XOR 相同的错集合及 Locator 两个 int64 的变化 | `TraceRequestsTest` |
| 普通查询分块、丢块、错 request id、最终状态失败 | 完整结果匹配才算完成，中途失败不能计为成功 | `IndexServerTest`、`PublishedIndexTest` |
| 缺少摘要、旧协议或不支持的发布能力 / 配置 | 显式参考报告缺少当前摘要即拒绝；不支持的输入不能进入另一套执行路径 | `TraceRequestsTest`、`IndexServerTest`、`ServiceConfigTest` |

组合矩阵除查询摘要外，分别核对最终状态、选中版本与操作顺序；覆盖范围及输入身份见 [完整验收](acceptance.md#参考oracle-与组合覆盖)。模型间一致不能替代原生事实或独立 oracle。

## 发布边界的受控交错

下列手算预期和交错均在 [PublishedIndexTest](../../src/test/java/org/sibench/adapters/PublishedIndexTest.java) 中验证。拒绝后必须检查原状态，不能只检查响应状态。

| 场景 | 预期及测试 |
| --- | --- |
| 已发布事务再次写入 | 事务 800 写 PK=1 / version=1 / RowID=11 并提交；迟到写 99 必须拒绝，原读仍为 11。`rejectedLateWriteCannotChangeAnAlreadyPublishedSnapshot`。 |
| 另一事务复用已发布版本 | 接触后端前拒绝。`lateWriteUnderAnotherTransactionCannotReuseAPublishedVersion`。 |
| pending / executing 位置重复 | 事务 900 的 operation=2 首值 22 不被重复值 99 替换，原请求最终正常执行。`duplicatePendingPositionCannotReplaceTheOriginalWrite`。 |
| commit 先到或前一版本缺失 | 等全部声明操作与连续水位，不以最大已见版本发布。`commitArrivingBeforeOperationsWaitsForEveryDeclaredWrite`、`laterCompleteVersionCannotPublishAcrossAMissingCommit`。 |
| 查询等待事务前缀 / 提交 | 暂存请求不占执行线程，独立事务仍能推进；提交者暂停也不丢失唤醒。`transactionOperationsExecuteInPositionOrderWhenTheyArriveOutOfOrder`、`parkedOperationRunsWithoutItsSubmitterAndPublishes`。 |
| 已完成事务状态 | lane 释放且迟到请求仍拒绝；5,000 个事务不累积 lane。`completedTransactionLanesAreReleasedButLateOperationsStayRefused`。 |
| 重开索引 | 恢复最后 Flush 水位，不暴露未发布版本。`reopenedIndexResumesItsRecordedWatermark`。 |

跨连接读取、分块流控和关闭边界由 `IndexServerTest` 验证。真实 gRPC 的加载、重开、COS 冷启动及 Flush 后 SIGKILL 正反验证见 [后端恢复证据](rocksdb-cloud.md)，不以短回放偶然成功代替上述不变量。

## 复现与边界

```bash
rtk mvn -Dtest=IndexTraceCompilerTest,IndexReferenceSearchTest,TraceRequestsTest,RocksDbBackendTest,PublishedIndexTest,ReplayProjectionTest,IndexServerTest,ServiceConfigTest,ReferenceReplayTest test
rtk mvn test
```

这些测试不证明真实网络重传、巨大查询组、目标规模崩溃恢复或性能上限。复用证据须保持其输入和对应语义有效；三来源完整结果与资源统一在 [验收](acceptance.md) 和 [测量](measurement.md) 中记录。
