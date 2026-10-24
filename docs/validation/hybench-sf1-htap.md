# HyBench SF1 原生 HTAP 验证

原生 `runHTAP`（AP → TP → 并发 AT/IQ）的多会话采集，按 PostgreSQL 事务完成位置编译为 v9 规范 Trace，参考回放与独立 oracle 通过。这是正确性与可复现性验证，不是性能结论。

## 采集

日期：2026-10-23。准备步骤（克隆静止库 `sibench_sf1_baseline_check`、安装上游二级索引与行变化触发器）由 `scripts/native-capture.sh` 统一编排，产物在 `results/poc/hybench-htap-sf1/`。

| 项目 | 生效值 |
| --- | --- |
| HyBench | eaad91b52956fddd3d48ce49f792928056b7b360；子模块未修改，构建副本源码一致 |
| JDK / PG | JDK 17；PG 14.24 隔离插桩构建 |
| 模式 | 原生 runAP() → runTP() → runXP(0)，不调用 freshness 路径 |
| 客户端 | AP=1、TP=1；XP 的 AT=1 与 IQ=1 并发 |
| 时长 | AP 1 分钟、TP 2 分钟、XP 1 分钟；原生计时器与退出规则 |
| 比例 | 上游 pg.props 的 AT 比例、TPClient 的默认 TP 比例，不覆盖 |
| 隔离 | Read Committed；UTC；禁用 PG 并行查询、并行建索引、autovacuum |
| 行变化触发器 | customer/company/savingaccount/checkingaccount/transfer/checking/loanapps/loantrans |

AP 按完整查询轮结束，实际耗时可能超过配置时间；TP / XP 使用原生退出规则。INSERT / UPDATE / 预期业务回滚完全由原生事务选择，未手工填齐覆盖；`runHTAP` 不调用 freshness DELETE 路径，不宣称 DELETE 覆盖。

| 阶段 | 耗时 |
| --- | ---: |
| prepare（克隆 8,706,000 行库、13 个二级索引、触发器） | 24 s |
| export（Baseline 导出与独立核验） | 401 s |
| run（AP 75 s → TP 121 s → 并发 AT/IQ 72 s） | 616 s |
| audit-rows | 721 s |
| access-ledger | 320 s |
| compile-trace | 308 s |

| 结果 | 数值 |
| --- | ---: |
| 会话 | 4（AP、TP、XP-AT、XP-IQ），`session_order=concurrent` |
| 请求 / 已提交事务 / 回滚 | 17,728 / 425 / 24 |
| 业务变更 | INSERT 5,565、UPDATE 11,390 |
| 访问单元投影 | 0 拒绝；带 limit 的主键等值 3,198,843、等值 12,667、二级提前停止 21、BitmapAnd 交集组 13、数组并集 3 |
| 查询（访问 / 记录） | 3,211,564 / 3,211,610 |
| 快照交叉验证 | 3,211,577 个单元全部校验，0 个快照不可用，0 个冲突 |
| 按快照读较早版本的查询 | 601,609 |
| 事务内写后查询 | 11,065 |

八表状态审计（Baseline 与 PG 最终状态逐行对账）：

| 表 | 基线行数 | 最终行数 | 提交行修改 | 修改主键数 |
| --- | ---: | ---: | ---: | ---: |
| customer | 300,000 | 300,000 | 23 | 23 |
| company | 2,000 | 2,000 | 6 | 6 |
| savingaccount | 302,000 | 302,000 | 11,088 | 5,688 |
| checkingaccount | 302,000 | 302,000 | 185 | 185 |
| transfer | 6,000,000 | 6,005,482 | 5,482 | 5,482 |
| checking | 600,000 | 600,051 | 51 | 51 |
| loanapps | 600,000 | 600,020 | 56 | 56 |
| loantrans | 600,000 | 600,012 | 64 | 45 |

三种访问组形态都在原生负载里出现并通过：二级索引被提前停止的扫描（`order by timestamp desc limit` 一类与关联查询的半连接）投影为已读完的前段区间加边界值上被返回行的钉住点查；transfer 上的 BitmapAnd 投影为交集组，组期望数是堆判定可见的交集行；`= ANY` 数组投影为并集组。

客户端 / 服务端关联审计记录了两类原生现象，均不改变提交结果：AP 与 IQ 会话最后一个请求在关闭连接时没有 `executor_end`；XP-AT 会话有 2 次客户端 commit 发生在事务已以 abort 结束之后、且该请求未改动任何行（`client_commits_ended_by_server_abort`）。

| 回放 / oracle | 结果 |
| --- | --- |
| 参考回放 1 / 64 / 128 client、transaction 输入模式，locator 与 RowID 两种 value | 全部 passed；每种 value 校验 3,211,585 个查询，其中 601,611 个按快照读较早版本；发送 3,256,723 个操作（编译 3,257,573 减 425 个事务的 850 条控制记录） |
| 独立 Parquet oracle | passed |
| 同一输入重复编译 | 全部产物逐字节一致，记录见 `results/poc/native-trace-20261022/recompile-20261023.log` |

复现（需要静止的已验证 SF1 库 `sibench_sf1_baseline_check` 和原生 `Data_1x`，路径见 [hybench_sf1.yaml](../../config/hybench_sf1.yaml)）：

```bash
make -C src/main/native/postgres install extension   # 头文件结构变化后先 make -C build/benchmarks/postgresql-14.24 clean
mvn package
scripts/native-capture.sh config/hybench_sf1.yaml
java -Xmx24g -jar build/sibench/sibench.jar replay config/index_replay_128clients.yaml results/poc/hybench-htap-sf1/trace REPORT
java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml results/poc/hybench-htap-sf1/trace REPORT
```

边界：短跑负载（AP 1 分钟、TP 2 分钟、XP 1 分钟），不是容量或性能结论；oracle 的二级谓词仍为每索引最多 64 个等值抽样；全局丢事件的独立旁证与区间 oracle 见 [采集完整性与有界状态验证](capture-completeness.md)（本页数字为 2026-10-23 中午的采集，后者为同日重新采集的结果）；未做故障注入。

## 产物迁移记录

- 2026-10-22：为按当前插桩重新采集让出目录名，`results/poc/hybench-htap-sf1`（2026-10-20）移为 `results/poc/native-trace-20261022/superseded/hybench-htap-sf1-20261020`。同日未完成运行分别移为 `.../hybench-htap-sf1-20261022-nocounter`（abort 记录缺少完成计数器）、`.../hybench-htap-sf1-20261022-audit`（客户端/服务端关联审计失败）、`.../hybench-htap-sf1-20261022-stalebinary`（PG 未重建）和 `.../hybench-htap-sf1-20261022-v8`（v8 投影止步于 50 个拒绝单元）。2026-10-23 的 `.../hybench-htap-sf1-20261023-racycounter` 用锁外读取的计数器采集，写事务位置可能有竞态，已作废。仅改名，内容未改写。
