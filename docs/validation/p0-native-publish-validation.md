# P0 原生反例、短流程矩阵与完整提交发布

更新：2026-10-27。远程 `sibench_3`（PG 14.24 插桩 runtime，32 CPU cgroup）上的小型实验；不是正式性能实验，也不是 SF10 规模结论。

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
| `service-publication.json`、`service-ordered2.json` | gRPC 完整发布与全序诊断 3/3 查询摘要通过 |
| `old-semantics.txt` | 旧读取点返回 `2000001@v1` 的反事实 |

命令（在 `/data/sibench`，新 jar 为 `native/sibench-run.jar` / `sibench-p0-perf.jar`）：

```bash
java -jar .../sibench-run.jar compile-trace config/p0_native_counterexample.yaml .../trace
java -Xmx8g -jar .../sibench-run.jar replay config/index_replay.yaml .../trace .../replay.json
java -Xmx8g -jar .../sibench-run.jar validate-snapshots config/index_replay.yaml .../trace .../snapshots.json
java -Xmx8g -jar .../sibench-run.jar replay-service config/p0_native_service_locator.yaml .../trace .../service-locator.json
java -Xmx8g -jar .../sibench-run.jar replay-service config/p0_replay_publication.yaml .../trace .../service-publication.json
java -Xmx8g -jar .../sibench-run.jar replay-service config/p0_replay_ordered.yaml .../trace .../service-ordered2.json
```

`capture_completeness=not_checked`：该手工小实验没有取 PG index counter 前后快照；行审计、请求配对和 session audit 已通过。它证明快照 / 本事务写入语义，不建立 PG 计数器完整性。

## 新语义短流程与 1/64/128 矩阵

用新 jar 从 `sibench_sysbench_8m_base` 复跑一个短 sysbench 流程，未重采集 SF10：

- 配置：`config/p0_sysbench_8m_short.yaml`；6 个固定 sysbench 脚本，事件数 500/500/500/200/200/1000。
- `run` 73.4 s；行审计 28.2 s；编译 0.25 s。
- `baseline_rows=8,706,000`，`workload_operations=24,300`，`source_work_units=11,400`，`transaction_local_searches=1,000`，`search_operations=10,700`。
- `baseline_trace_sha256=7dc20a21756b20f780f5f8c3b8551aa86765f2f5596c9d90ac8f866589587505`，`workload_trace_sha256=c85751aac0e7a0fe054116c5ca9811fe671cebf98bb5fda32f01a3b00507420a`，`access_ledger_sha256=1b06c64fa03cba343c5be6e1d6211e2f639ccca761262af0b6fe30590d66baa9`，`capture_completeness=consistent`。
- `validate-replay-matrix`：`status=passed`，12 个变体（1/64/128 × operation/transaction × 二级开/关），Locator / RowID 逐查询摘要和 Parquet oracle 通过。耗时约 7 分钟；矩阵报告在 `matrix/matrix.json`。

证据目录：`/data/sibench/results/poc/p0-sysbench-8m-short/`（trace、matrix 与 64 client 报告已复制到本地同名 `results/poc/`；8.45 GB Baseline 留在远程）。

## SF10 结果影响

用 `hybench-sf10-hl9` 的完整 raw / 87,060,000 行 Baseline / 认证重编新语义 `trace-p0`，不覆盖原 `trace`：

- 运行期操作 82,603,822；事务内查询 3,530,323（占 search 操作约 5.2%，只是可能受影响上界）。
- 对旧 `trace` 与新 `trace-p0` 分别用同一参考模型、row_id 模式计算本地查询的 result-only 摘要（PK、version、value、顺序），避免 read point 本身进入摘要。
- 分组本地查询记录 3,530,167 条（156 个额外成员属于同一搜索组，组结果已合并核对）；**changed=0，missing=0**。该样本没有触发结果变化；原生小反例仍证明旧语义会返回另一主键。
- 证据：`/data/sibench/results/poc/hybench-sf10-hl9/impact-compare.txt`，本地小副本见 `results/poc/p0-native-counterexample-20261027/impact-compare.txt`。这不是错误率，也不是正式性能结论。

## 完整提交发布 vs 全序诊断

协议新增 `Commit`、`transaction_id` / `transaction_operation`、`complete_commit_publication`、`FlushResponse.published_version`。`PublishedIndex` 以事务 lane 执行，版本只在提交标记到达且全部写入应用后连续发布；查询等待 `read_version`，事务内查询还等待同一事务前缀。

原生小 Trace 结果：

| 模式 | 状态 | 查询 / 结果项 | 逐查询摘要 | `published_version` | commit markers |
| --- | --- | ---: | ---: | ---: | ---: |
| `sequence` 全序诊断 | passed | 3 / 3 | 3 | 0（未声明） | 0 |
| 完整提交发布，operation 输入 | passed | 3 / 3 | 3 | 2 | 2 |
| 完整提交发布，transaction 输入 | passed | 3 / 3 | 3 | 2 | 2 |

短 sysbench Trace 的 64 client 完整发布回放已通过（`service-publication64c.json`）：64/64 client 使用，2,900 个提交标记，`published_version=2900`，10,700 个查询请求、208,500 个结果项、10,700 个逐查询摘要全部通过；driver 复用 1-client 矩阵报告的期望摘要 sidecar，避免再次在驱动内构建完整参考模型。基线加载 225.7 s / 77k ops/s，负载 10.6 s / 1.75k ops/s（含 64 client 与等待发布；不是稳态性能）。全序 64 client 对照 `service-ordered64.json` 同样 `passed`：64/64 client、10,700 查询、208,500 结果项、10,700 摘要，0 个 commit marker；两种模式的逐查询摘要一致。该 64 client 报告构建早于迟到 / 重复请求防御修复；正常输入结果不变，最终构建的远端复验仍待执行，最终代码由 296 项本地测试覆盖。实现为正确性参照：等待查询占用服务线程，未做非阻塞调度、恢复或高并发吞吐证明。

## 未覆盖

- SF10 / 87m 级本地结果-only 摘要影响比较已完成（changed=0），但 87m 级完整服务回放尚未执行。
- 迟到 / 重复请求防御修复后的最终构建尚未重跑远端 64 client；现有 64 client 报告是修复前构建。
- 生产规模提交发布、恢复后的水位、网络断连 / 未知写入诊断仍只做小型测试。
- 原生小实验使用单机克隆和短流程，不等于三来源大 Trace 或正式性能。