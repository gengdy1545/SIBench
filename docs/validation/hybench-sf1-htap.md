# HyBench SF1 原生 HTAP 验证

日期：2026-10-20。默认负载改为原生 `runHTAP`；此报告与 [历史 TP 子集 v4 报告](hybench-sf1-poc.md) 分开，不用旧子集回放结论替代 HTAP 验证。

## 环境与运行

| 项目 | 生效值 |
| --- | --- |
| HyBench | eaad91b52956fddd3d48ce49f792928056b7b360；子模块未修改，构建副本源码一致 |
| JDK / PG | JDK 17；PG 14.24 隔离插桩构建 |
| 模式 | 原生 runAP() → runTP() → runXP(0)，不调用 freshness 路径 |
| 客户端 | AP=1、TP=1；XP 的 AT=1 与 IQ=1 并发 |
| 时长 | AP 1 分钟、TP 2 分钟、XP 1 分钟；原生计时器与退出规则 |
| 比例 | 上游 pg.props 的 AT 比例、TPClient 的默认 TP 比例；未设置 TP 子集权重 |
| 数据库 | 127.0.0.1:55433 / sibench_htap_sf1_capture |
| 隔离 | Read Committed；UTC；禁用 PG 并行查询、并行建索引、autovacuum |
| 行变化触发器 | customer/company/savingaccount/checkingaccount/transfer/checking/loanapps/loantrans |
| 文件布局 | 256 MiB 目标；行组最多 1,048,576 行；Zstd level 3 |

Baseline 复用原生 SF1 CSV 输入，在独立、静止的原生 PG Load 上核对全部 8,706,000 行和身份。本轮数据库由该静止数据库 `sibench_sf1_baseline_check` 克隆，安装完整上游 `create_index_pg.sql` 的 13 个二级索引、8 个已有主键及 4 个自增序列。AP/TP/XP 之间不重载数据；XP 查询同一数据库上的 TP 和 AT 更新。

AP 按完整查询轮结束，因此实际耗时可能超过配置时间。TP/XP 使用原生退出规则，当前方法返回和连接清理也会延长耗时。INSERT/UPDATE/预期业务回滚完全由原生事务选择，未手工填齐覆盖。`runHTAP` 不调用已被注释的 freshness DELETE 路径，不宣称 DELETE 或指定历史点 SQL 覆盖。

## 本轮采集

原生开始：2026-10-20 15:24:38 +08:00。执行阶段总计 258.28 秒；包括完成审计在内于 15:32:54 发布 `complete`。

| 阶段 | 实际耗时（秒） | 方法调用 | SQL 请求 | 结果消费行 | 提交 / 显式回滚 |
| --- | ---: | ---: | ---: | ---: | ---: |
| AP | 66.102 | AP 39，13 种各 3 次 | 42 | 3 | 0 / 0 |
| TP | 123.645 | TP 156，18 种均非零 | 5,474 | 1,822 | 156 / 0 |
| XP-AT | XP 合计 68.536 | AT 74，6 种均非零 | 298 | 160 | 61 / 13 |
| XP-IQ | 与 AT 并发 | IQ 23，5 种非零 | 23 | 0 | 0 / 0 |
| 总计 | 258.282 | 不混合不同方法口径 | 5,837 | 1,985 | 217 / 13 |

TP1–TP18 调用数：`5/5/4/11/8/3/5/3/12/11/10/12/5/8/13/7/13/21`。

AT1–AT6：`27/10/15/14/7/1`；IQ1–IQ6：`4/4/4/4/0/7`。**IQ5 已启用，但本次短跑没有被原生随机抽中**；因此不宣称全部 IQ 方法已实跑验证。保留真实随机结果，不补造调用或拼接其他运行。

四会话配对审计通过，未关联服务端事件 0，索引相关事件 35,759,735。13 次显式业务回滚与服务端 abort 对应；AP/IQ 连接关闭各产生一次只读 abort。5,837 个请求均有服务端 `executor_run_end`，其中 5,835 个有 `executor_end`：后者是执行器清理事件，原生 AP/IQ 的最后请求在关闭连接时没有此事件，不改变原生提交行为。

## 验证边界

同次请求核对包括 SQL 身份、客户端实际消费行、执行器返回、提交及回滚/关闭身份，不为观察补跑查询。多会话输入的完整清单及每对 JDBC/PG 文件 SHA256 在消费前复核。`REAL`、timestamp、CHAR 等按实际 JDBC 类型规范化比较，不引入任意数值容差。

八表状态核验使用已验证 Baseline 和提交后的 before/after 行变化链，丢弃 abort 的修改，逐行比较最终 PG 状态。按键的变化因果链不推定跨会话全局提交顺序；新增 ID 必须遵守原生序列，位于基线 ID 之后。

八表全量核验通过，最终 8,707,767 行；共 5,408 次提交行修改、3,706 个修改主键，未修改的基线行也全部核对。13 次业务回滚在写入前发生，本轮丢弃的回滚行修改数为 0。

| 表 | 基线行数 | 最终行数 | 提交行修改 | 修改主键数 |
| --- | ---: | ---: | ---: | ---: |
| customer | 300,000 | 300,000 | 12 | 12 |
| company | 2,000 | 2,000 | 1 | 1 |
| savingaccount | 302,000 | 302,000 | 3,507 | 1,820 |
| checkingaccount | 302,000 | 302,000 | 75 | 75 |
| transfer | 6,000,000 | 6,001,728 | 1,728 | 1,728 |
| checking | 600,000 | 600,020 | 20 | 20 |
| loanapps | 600,000 | 600,012 | 32 | 32 |
| loantrans | 600,000 | 600,007 | 33 | 18 |

`CapturePoc pack` 保存全量 PG 原始 JSON 及 CRC32C 的 64 字节 access 记录。文件名排序只保证各文件内顺序，不代表全局事务排序。**这不是完整 HTAP KV 回放**：并发快照、范围/变长键、事务内可见性及残余过滤的规范转换仍待实现，HTAP 输入被旧单会话 v4 编译器显式拒绝。

打包验证通过：51,111,329 条原始事件、12,479 个 CRC32C 块；`access.bin` 为 3,271,274,820 字节，完整 `payload.jsonl` 为 16,832,115,136 字节。事件计数与状态审计完全一致，解码数量与 CRC 全部通过；SHA256 分别为 `5492eccc988b072caa2945ab6cd9bc1b6cc1fb23f71dd406e6de2847cd041a14`、`c0a8faef1f28464d31b465dde9f62bed11f6e768ae5737eee8d1831a5fba72a8`。

原始量、短跑时间和解码速度均不是 Index Service 性能指标；本次未接入真实 Backend，也未做 SF1000、自然 Rewrite/GC 或长时回放。

## 入口

配置：[hybench_sf1.yaml](../../config/hybench_sf1.yaml)。PG/数据必须预先准备，`export`/`run` 不自动启动服务。新采集需新数据库和新 artifacts/raw 目录，不覆盖或续写失败运行。

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn package
make -C src/main/native/postgres install extension
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc check "$PWD" config/hybench_sf1.yaml
```

PG 启动时指定与配置相同的 `SIBENCH_CAPTURE_DIR`；安装 `install-row-capture.sql` 时通过 psql `capture_library` 指向 `build/services/postgresql-capture/row_capture.so`。`Data_1x` 必须包含原生 CSV 和风控辅助序列化文件；本次工作目录为 `results/poc/hybench-htap-sf1/native/`，其中数据指向既有原生 SF1 输入。运行中的 JAR 使用不可变副本，不覆盖它进行重新构建。

```bash
java -Xmx3g -cp "$ROOT/build/sibench/sibench.jar:$ROOT/build/benchmarks/HyBench-2024/target/classes:$ROOT/build/benchmarks/HyBench-2024/lib/*" \
  org.sibench.capture.CapturePoc run "$ROOT" config/hybench_sf1.yaml
java -Xmx3g -cp "$ROOT/build/sibench/sibench.jar" \
  org.sibench.capture.CapturePoc audit "$ROOT" config/hybench_sf1.yaml "$ROOT/results/poc/hybench-htap-sf1/state-audit-check.json"
java -Xmx3g -cp "$ROOT/build/sibench/sibench.jar" \
  org.sibench.capture.CapturePoc pack "$ROOT" config/hybench_sf1.yaml "$ROOT/results/poc/hybench-htap-sf1/trace-check"
```

`ROOT` 指向仓库绝对路径，`run` 在含 `Data_1x` 的目录执行。报告必须为新文件，打包目录必须不存在。旧 TP 子集采用 [hybench_tp_subset_sf1.yaml](../../config/hybench_tp_subset_sf1.yaml)，继续支持既有 v4 编译及回放。

## 证据与修复

本轮证据保存在 `results/poc/hybench-htap-sf1/`：

- `baseline/verified-manifest.json`：八表独立基线核对及摘要。
- `native/capture-resolved.props`、`native/sibench-htap-runner.jar`：本轮属性和不可变运行 JAR。
- `raw/capture-run.json`：阶段方法计数、四会话配对审计及文件 SHA256；PG/JDBC 原始文件同目录。
- `native/sibench-audit.jar`：增加完整清单和精确事务身份检查的离线审计 JAR。
- `state-audit.json`：严格输入复核、按阶段访问索引、事件计数和八表逐行状态核对通过。
- `trace/manifest.json`、`trace/access.bin`、`trace/payload.jsonl`：全部原始事件打包、摘要与 CRC 验证通过。

保留两次失败运行的原始 manifest、日志、属性及数据库：`raw-failed-completion-20261020` 对应 AP 关闭时缺少 executor_end，新增执行器返回事件修复；`raw-failed-result-identity-20261020` 对应原生 TP 交错读取旧 ResultSet 时误关联最新请求，改为结果集固定原始请求身份，并补齐普通 Statement 路径。不改失败 manifest、不拼接前缀。中断的 Baseline 导出保留于 `baseline-interrupted-20261020`，之后以不可变 JAR 重导出并独立核验。

JDK 17 离线 Maven 构建和 59 项测试通过，涵盖多会话、跨线程异常、延迟 ResultSet、普通 Statement、回滚身份、类型规范化、行变化链和新事件打包，以及旧 TP 编译/回放回归。全局事件丢失与插桩成本未证明。

验证结束后停止本次启动的专用 PG，恢复原先停止状态；所有数据库和本地证据保留。重新执行 `audit` 前需启动该服务，`check` 和已完成产物的离线检查不需要 PG。
