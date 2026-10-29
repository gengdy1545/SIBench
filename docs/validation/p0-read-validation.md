# 读取正确性与逐查询结果校验

更新：2026-10-29。本文记录当前读取语义与独立反例；完整输入结果见 [三来源验收](p0-three-source-acceptance.md)，协议与持久化边界见 [Index Service 契约](../contracts/index-service.md)。

## 当前语义

事务内读保留原语句 `readTimestamp`，以 `SEARCH.version` 指定本事务 CommitVersion，并只叠加查询之前的 `mutationOrdinal` 前缀。其它处于两个读点之间的提交不可见。读取器要求本地读 version>0、readTimestamp<version；缺失本事务版本的歧义形式被拒绝。

服务回放逐查询核对原生条数和有序 SHA-256，涵盖来源身份、范围、读取点、事务位置、PK、version 和完整 value。期望 sidecar 绑定输入和过滤条件；响应按 request id 核对，可乱序完成。显式参考报告缺少摘要会失败，没有全局 XOR 校验回退。

## 独立证据

| 场景 | 当前测试 / 证据 | 预期 |
| --- | --- | --- |
| T1 更新 A 后读 B；T2 在快照后移出 B 并插入同值 C | `IndexTraceCompilerTest.transactionLocalSearchKeepsItsSnapshotAndOnlyItsOwnWrites`、[原生 PG 反例](p0-native-publish-validation.md) | 返回 B；把读点替换为 T1 提交版本会错误返回 C，两者条数同为 1。 |
| 事务内多次改键、删除 / 重插入、主键与二级历史读 | `RocksDbBackendTest`、`IndexTraceCompilerTest`、Parquet oracle | 原语句快照与本事务前缀一致，旧值不复活，重插入保留新身份。 |
| 缺失本事务版本 | `IndexReferenceSearchTest.oldV9LocalSearchFormWithoutItsOwnVersionIsRejected` | 明确拒绝。 |
| 同条数的错键、错版本、错 value、重复、顺序颠倒、跨查询交换 | `TraceRequestsTest` | 摘要失败并定位源事件；包括 XOR 相同的错集合和 Locator 高位变化。 |
| 普通查询分块、丢块、错 id、最终状态失败 | `IndexServerTest`、`PublishedIndexTest` | 完整摘要匹配；中途失败不能算完成。 |
| 迟到 / 重复请求、缺少提交、跨连接读取 | [发布不变量](p0-test-review.md) | 非法请求不改状态；只在连续完整水位下可读。 |

三来源本地完整服务验收与 COS 目标规模已通过。等待中的查询不占执行线程，发布水位随 Flush 恢复；不能继续把这些能力列为未完成。大规模查询组、目标规模崩溃恢复与正式性能排名仍未证明。

## 复现

```bash
rtk mvn -Dtest=IndexTraceCompilerTest,TraceRequestsTest,RocksDbBackendTest,PublishedIndexTest,IndexServerTest test
rtk mvn test
```

根测试检查当前协议与实现；既有原生 PG、Parquet 和全量证据只在输入及对应语义未改变时复用，不用模型间一致替代独立事实。
