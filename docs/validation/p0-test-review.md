# 发布与结果校验的不变量

更新：2026-10-29。本文保留仍需防止回归的具体反例和当前测试职责。原生与全量证据见 [原生反例](p0-native-publish-validation.md)、[三来源验收](p0-three-source-acceptance.md)。

## 发布与拒绝必须发生在正确边界

| 场景 | 预期及测试 |
| --- | --- |
| 已发布事务再次写入 | 事务 800 写 PK=1 / version=1 / RowID=11 并提交；迟到写 99 必须拒绝，原读仍为 11。`rejectedLateWriteCannotChangeAnAlreadyPublishedSnapshot`。 |
| 另一事务复用已发布版本 | 在接触后端前拒绝，不能靠事后返回 INVALID 掩盖已发生修改。`lateWriteUnderAnotherTransactionCannotReuseAPublishedVersion`。 |
| pending / executing 位置重复 | 事务 900 的 operation=2 首个值 22 不被重复值 99 替换；原请求最终正常执行。`duplicatePendingPositionCannotReplaceTheOriginalWrite`。 |
| commit 先到或前一版本缺失 | 必须等全部声明操作与连续水位；最大已见版本不代表已发布。`commitArrivingBeforeOperationsWaitsForEveryDeclaredWrite`、`laterCompleteVersionCannotPublishAcrossAMissingCommit`。 |
| 查询等事务前缀 / 提交 | 暂存请求不占执行线程，独立事务仍能推进；原提交者暂停也不能丢失唤醒。`transactionOperationsExecuteInPositionOrderWhenTheyArriveOutOfOrder`、`parkedOperationRunsWithoutItsSubmitterAndPublishes`。 |
| 已完成事务状态 | lane 释放且迟到请求仍被拒绝；5,000 个事务不累积 lane。`completedTransactionLanesAreReleasedButLateOperationsStayRefused`。 |
| 重开索引 | 恢复最后 Flush 水位，不暴露未发布版本。`reopenedIndexResumesItsRecordedWatermark`。 |

上述测试在 [PublishedIndexTest](../../src/test/java/org/sibench/adapters/PublishedIndexTest.java)。合法性预留、实际写入与发布之间的受控交错需要保留；不能删掉失败断言或只检查响应状态。

## 当前服务与强结果依据

服务只有完整提交发布路径。`IndexServerTest` 验证跨连接读取等待 commit、大结果流控与分块、关闭边界，以及旧协议 / 缺少完整发布能力的拒绝。`ServiceConfigTest` 拒绝移除的 ordered / publication 开关，不为旧配置提供另一套执行语义。

`TraceRequestsTest` 用手算错值、PK、version、Locator 两个 int64、顺序和跨查询身份反例验证逐查询摘要。组合矩阵使用 query_digest_sha256，最终状态、选中版本和调度顺序仍各自有独立检查。显式参考报告没有当前摘要时直接失败。

原生反例、Parquet oracle 和不支持输入的拒绝测试共同覆盖当前不变量。

```bash
rtk mvn -Dtest=PublishedIndexTest,TraceRequestsTest,ReplayProjectionTest,IndexServerTest,ServiceConfigTest,ReferenceReplayTest test
rtk mvn test
```

测试不证明真实网络重传、目标规模崩溃恢复或性能上限。实际服务确认条件和剩余边界见 [Index Service 契约](../contracts/index-service.md)。

当前验证：[协议 2 验证记录](../../results/validation/index-service-20261029b/validation.json)。JDK 17 根测试 319 项及 Python 汇总测试 4 项通过；2,000 行小输入的 12 个组合、两种 value 和两份独立 Parquet oracle 通过。64 client 的真实 gRPC 回放核对 2,050 次摘要 / 0 不匹配；停止旧服务后用新进程重开，核对 1,850 个快照查询 / 0 不匹配，200 个事务内查询按定义不核对，水位 500。新报告不含结果 XOR，汇总脚本读取该报告及实际 LOG 通过。未重跑长捕获或目标规模实验。
