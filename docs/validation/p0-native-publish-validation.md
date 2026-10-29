# 原生读取反例与完整提交发布验证

更新：2026-10-29。保留直接支持当前读取语义、发布不变量与组合正确性的证据。已有实验按记录中的构建解释；当前服务协议 2 只提供完整提交发布，不维护旧执行器。配置与命令细节由所选运行的快照保存，当前使用入口见 [README](../../README.md)。

## 原生两会话反例

在独立克隆 `sibench_sysbench_8m_base` 的小库中运行，不写共享静止库。输入与脚本：

- 来源：`sysbench` pinned `3ceba0b1e115f8c50d1d045a4574d8ed643bd497`
- 静态库：8 表 × 1,088,250 行；Baseline 已验证（`baseline_trace_sha256=7dc20a21756b20f780f5f8c3b8551aa86765f2f5596c9d90ac8f866589587505`）
- 场景：T1 `UPDATE sbtest1 SET c='p0-a1' WHERE id=1`，随后读 `k=302261`；T2 在 T1 快照之后把 `id=2` 的 `k` 改为 `400001` 并插入 `id=2000001,k=302261`，提交；T1 再提交。
- 原生 PG 结果：T1 的读返回 `id=2`（B0）；行审计最终为 `id=1` 的 `c=p0-a1`、`id=2` 的 `k=400001`、新增 `id=2000001`。
- 规范 Trace：1 个事务内查询，`readTimestamp=0`、`SEARCH.version=2`、`mutationOrdinal=1`、`TRANSACTION_LOCAL_SEARCH`。
- 新语义回放：参考模型、Parquet oracle、RocksDB `replay-service`（locator / row_id）均 `passed`，3/3 查询。
- 旧单时间戳反事实：同一 Trace 按旧读取点 `readTimestamp=2` 返回 `[2000001@v1]`（C），与新语义返回 `id=2` 不同。见 `old-semantics.txt`。

证据目录：`/data/sibench/results/poc/p0-native-counterexample-20261027/`（小型 raw / trace / 报告已复制到本地 `results/poc/p0-native-counterexample-20261027/`；Baseline 未复制）。关键文件：

| 文件 | 说明 |
| --- | --- |
| `raw/capture-run.json` | 2 会话，均 `passed`，`session_order=concurrent` |
| `raw/pg-378258.jsonl.zst`、`raw/pg-378263.jsonl.zst` | T1 / T2 原生事件；T1 读的 `csn=1`，T2 提交 `csn=2` |
| `trace/manifest.json` | `workload_operations=14`、`transaction_local_searches=1`、`search_operations=3` |
| `trace/workload.ops` | 本地读 `event=38`、`read=0`、`version=2`、`ordinal=1` |
| `replay.json`、`snapshots.json` | 参考回放 / 独立 Parquet oracle `passed` |
| `service-locator.json`、`service-rowid.json` | 进程内 RocksDB 3/3 查询摘要通过 |
| `service-publication.json` | gRPC 完整发布 3/3 查询摘要通过 |
| `old-semantics.txt` | 旧读取点返回 `2000001@v1` 的反事实 |

该手工实验没有 PG index counter 前后快照，`capture_completeness=not_checked`；行审计、请求配对和会话审计通过。它证明快照与本事务写入语义，不能替代 PG 计数器完整性认证。

## sysbench 短流程与组合矩阵

用新 jar 从 `sibench_sysbench_8m_base` 复跑一个短 sysbench 流程，未重采集 SF10：

- 配置：历史一次性 profile 已归档到 [p0-sysbench-8m-short/config](../../results/poc/p0-sysbench-8m-short/config/p0_sysbench_8m_short.yaml)；6 个固定 sysbench 脚本，事件数 500/500/500/200/200/1000。
- `run` 73.4 s；行审计 28.2 s；编译 0.25 s。
- `baseline_rows=8,706,000`，`workload_operations=24,300`，`source_work_units=11,400`，`transaction_local_searches=1,000`，`search_operations=10,700`。
- `baseline_trace_sha256=7dc20a21756b20f780f5f8c3b8551aa86765f2f5596c9d90ac8f866589587505`，`workload_trace_sha256=c85751aac0e7a0fe054116c5ca9811fe671cebf98bb5fda32f01a3b00507420a`，`access_ledger_sha256=1b06c64fa03cba343c5be6e1d6211e2f639ccca761262af0b6fe30590d66baa9`，`capture_completeness=consistent`。
- `validate-replay-matrix`：`status=passed`，12 个变体（1/64/128 × operation/transaction × 二级开/关），Locator / RowID 逐查询摘要和 Parquet oracle 通过。耗时约 7 分钟；矩阵报告在 `matrix/matrix.json`。

证据目录：`/data/sibench/results/poc/p0-sysbench-8m-short/`（本地同名 `results/poc/` 保留 Trace、矩阵、报告与 Baseline 副本）。

## SF10 结果影响

用 `hybench-sf10-hl9` 的完整 raw / 87,060,000 行 Baseline / 认证重编新语义 `trace-p0`，不覆盖原 `trace`：

- 运行期操作 82,603,822；事务内查询 3,530,323（占 search 操作约 5.2%，只是可能受影响上界）。
- 对旧 `trace` 与新 `trace-p0` 分别用同一参考模型、row_id 模式计算本地查询的 result-only 摘要（PK、version、value、顺序），避免 read point 本身进入摘要。
- 分组本地查询记录 3,530,167 条（156 个额外成员属于同一搜索组，组结果已合并核对）；**changed=0，missing=0**。该样本没有触发结果变化；原生小反例仍证明旧语义会返回另一主键。
- 证据：`/data/sibench/results/poc/hybench-sf10-hl9/impact-compare.txt`，本地小副本见 `results/poc/p0-native-counterexample-20261027/impact-compare.txt`。这不是错误率，也不是正式性能结论。

## 发布与回归证据

原生小 Trace 的 operation / transaction 完整发布回放各核对 3 次摘要、0 不匹配，提交标记数与发布水位均为 2。短 sysbench 的 64 client 完整发布也通过，水位 2,900，10,700 次摘要 / 208,500 个结果项。三来源目标规模与最终重开证据见 [完整验收](p0-three-source-acceptance.md)。

有意义的失败保留在 `p0-publication-timeout-20261027T2332`：首次失败为 dispatch 13,750 的 version 550 提交超时，水位停在 549，4,096 个请求线程阻塞在 lane / 发布等待，后续连接关闭栈不是首因。当前实现只把就绪请求交给固定线程池，等待请求挂起且不占线程；受控交错验证在 [PublishedIndexTest](../../src/test/java/org/sibench/adapters/PublishedIndexTest.java)。重复小输入偶然通过、忽略结果的 workload-only 试验和混合来源的 `current` 报告不再作为保留结论或验收依据。

关闭不等于请求执行完成；提交、读取、流控与关闭的测试职责见 [发布不变量](p0-test-review.md)。保留独立反例与原始首次失败诊断，不继续维护旧序号调度或阻塞式执行路径。
