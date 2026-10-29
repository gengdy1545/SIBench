# P0 三来源身份与当前语义预检

更新：2026-10-29。所选预检构建 jar `6676ed3e250dda24d4f44f083ad391cd145fee9bbee6cf040c80c2c73ed07ac9`，使用 `check-replay`（单 client 预检，不执行参考回放），不重采 PG。

## 结果

| 来源 | 输入 Trace | 当前语义 | 预检 | 说明 |
| --- | --- | --- | --- | --- |
| HyBench SF10 | `hybench-sf10-hl9/trace-p0` | `transaction_local_read_model=statement_snapshot_plus_own_writes_up_to_mutation_ordinal`；3,530,323 个事务内读 | `trace_integrity_and_supply_valid` | 82,603,822 个 workload 操作、5,712,367 个 work unit；认证来自 `hybench-sf10-w5c32v2/certification.json`，`capture_completeness=consistent`。 |
| YCSB 87m | `ycsb-87m-n22/trace` | 无事务内读（0），旧 `transaction_local_read_model` 缺失不影响读取 | `trace_integrity_and_supply_valid` | 81,653,024 个 workload 操作、25,862,780 个 work unit；认证来自 `ycsb-87m-yl3/certification.json`，`capture_completeness=consistent`。 |
| sysbench 87m | 旧 `sysbench-87m-sl16/trace` → 新 `sysbench-87m-sl16/trace-current` | 旧 Trace 1,123,388 个事务内读为旧 `version=0` 形式，当前读取器拒绝 | 旧：`Invalid normalized operation`；新：`trace_integrity_and_supply_valid` | 从 `sysbench-87m-sl16/raw` 用当前 jar、`sysbench-87m-w5v2/certification.json` 和该采集的 PG 计数器重编；30,821,670 个 workload 操作、13,621,081 个 work unit、1,123,388 个事务内读（当前形式）。编译 wall 261 s，负载 216 s。 |

## 证据

- 本机 `results/poc/p0-three-source-audit-20261028/`：三份 `check-*.json`、sysbench 旧 Trace 拒绝、`trace-current/manifest.json` 与编译 timing。
- 远端：
  - `sibench_1:/data/sibench/results/poc/hybench-sf10-hl9/trace-p0`
  - `sibench_2:/data/sibench/results/poc/ycsb-87m-n22/trace`
  - `sibench_3:/data/sibench/results/poc/sysbench-87m-sl16/trace-current`
- 新 sysbench Trace 的 `workload_trace_sha256=b8b9045eebf6982be7d0599d16cbb6a2c182ce7e0d5673bcd99d41ca037366ee`；baseline 仍为 `9587cc6f…`。

## 验证范围

这些预检证明读取器接受输入、Baseline 索引覆盖与 work unit 供应，不单独证明查询结果。三来源完整参考、独立 Parquet oracle、本地服务与 COS 代表配置此后均已通过，见 [完整验收](p0-three-source-acceptance.md) 与 [测量证据](p0-measurement.md)；不再保留按早期短测外推的未完成状态。

认证与原生采集证据按 [Trace 契约](../contracts/trace.md) 的覆盖边界复用，不因服务协议或弱校验清理重新采集 PG。源输入身份和所选原始报告保持不变。
