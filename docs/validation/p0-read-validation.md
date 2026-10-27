# P0 读取正确性与逐查询结果校验

更新：2026-10-27。工作区验证，不是远程大 Trace 或正式性能实验。

## 结论

事务内读不再把查询的读取点替换成本事务的 CommitVersion。规范 Trace 现在表达：

```text
原语句快照 (readTimestamp)
+ 本事务 CommitVersion (SEARCH.version)
+ 查询前已发生的修改数 (mutationOrdinal)
+ TRANSACTION_LOCAL_SEARCH
```

参考模型和 RocksDB 先取快照 `<= readTimestamp` 的状态，再只叠加该事务 CommitVersion 中已发送的写入；两个位置之间提交的其它事务不可见。旧 v9 的 `version=0`、`readTimestamp=本源 CommitVersion` 形式仍可读，但新编译不再生成；旧 Trace 与新 Trace 不应混在同一次比较结论中。

服务回放还新增逐查询强摘要：查询身份、范围 / 钉住键、读取点、事务位置、结果顺序及完整 `(PK, version, value)` 都进入 SHA-256。期望摘要由参考回放生成一次，头部绑定 workload / baseline Trace 摘要、`operation_trace_version`、value 模式和二级过滤配置；服务回放按请求 id 核对，响应仍可乱序完成。

## 证据

| 检查 | 位置 | 结果 |
| --- | --- | --- |
| 两会话夹具反例，T2 先提交移出 B 并插入同值 C，T1 读 B | `IndexTraceCompilerTest.transactionLocalSearchKeepsItsSnapshotAndOnlyItsOwnWrites` | 新语义返回 B0；旧单时间戳语义返回 C，条数同为 1 |
| 主键 / 二级、删除 / 重插入、同键多次修改 | `IndexTraceCompilerTest.allIndexesAndValueChangesPreserveIdentityHistoryQueriesAndVariants`、`abortDoesNotAllocateVersionsOrRowIdsAndPrewriteReadsSurvive` | 参考回放与 oracle 通过 |
| 同条数错集合、错版本、错主键、重复、顺序颠倒、跨查询交换、`1^2 == 4^7` | `TraceRequestsTest` | 全部拒绝并定位 source event / work unit |
| 参考模型与 RocksDB 的事务内读 | `RocksDbBackendTest.transactionLocalReadSeesOwnWritesAndKeepsTheStatementSnapshot` | 结果一致；快照外版本不可见 |
| 旧 v9 事务内读形式 | `IndexReferenceSearchTest.oldV9LocalSearchFormStillReadsItsOwnVersion` | 兼容读取，不声明新语义 |
| 进程内 RocksDB `replay-service` 端到端 | `IndexTraceCompilerTest` 同一夹具 | `status=passed`，逐查询摘要检查 1 次 |
| 乱序 / 缺失位置 / 重复位置 / 超时 / 未知结果 | `ReplayDispatcherTest`、`IndexServerTest` | 后续位置等待、重复拒绝、未知写入不自动重试 |

全部 `mvn test` 在当前工作区通过（296 tests，0 failures / errors / skipped）；发布模式迟到 / 重复请求的复核记录见 [P0 测试复核](p0-test-review.md)。本页的单元测试是夹具和进程内服务验证；真实 PG 两会话、短 sysbench 矩阵和 gRPC 发布 / 全序对照见 [P0 原生反例、短流程矩阵与完整提交发布](p0-native-publish-validation.md)。

## 原生证据与发布模式

真实 PG 两会话反例已在 `sibench_3` 的独立小库中完成：T1 更新 A 后读 B（`k=302261`），T2 在 T1 快照之后把 B 移出该值并插入同值 C，T1 提交。原生结果是 B（`id=2`）；规范 Trace 的本地读为 `read=0/version=2/ordinal=1`；参考 / Parquet / RocksDB / gRPC 回放均返回 B，旧单时间戳反事实返回 C（`id=2000001`）。完整命令、哈希、短 sysbench 新语义 1/64/128 矩阵及发布 / 全序对照见 [P0 原生反例、短流程矩阵与完整提交发布](p0-native-publish-validation.md)。

## 未完成

- SF10 `hl9` 已重编并完成结果-only 影响比较：3,530,167 个分组本地查询 changed=0；3,530,323 个本地搜索是可能受影响上界，不是错误率。
- 87m 级完整服务回放仍未执行；短 sysbench / 原生小 Trace 的 sidecar 复用与 gRPC 回放已通过。
- 完整提交发布目前是正确性参照：等待查询占用服务线程，未做非阻塞调度、恢复水位和高并发扩展性证明。

## 复现

```bash
mvn -q -Dtest=IndexTraceCompilerTest#transactionLocalSearchKeepsItsSnapshotAndOnlyItsOwnWrites test
mvn -q -Dtest=TraceRequestsTest,RocksDbBackendTest,ReplayDispatcherTest,IndexServerTest test
mvn -q test
```

历史 Trace 未改写；旧 `result_checksum` 仅保留为历史回归定位字段。