# 统一行状态审计

更新：2026-10-21。

## 职责

`RowStateAudit` 统一负责三来源的源行状态核验。来源核验一次，转换产物和 Backend 结果各自验收，不在后续阶段重复匹配源行 before-image。

- 按 `SourceSchema` 核对全部列、可空性和类型；CHAR、VARCHAR / TEXT、REAL、date / timestamp 分别按源类型比较。
- 从已验证 Baseline 连接已提交旧 / 新行，检查普通列；提交 / 回滚和未完成事务单独处理。
- 单会话保留来源顺序；多会话因果链保留各会话的同键顺序，不声称获得全局提交或快照顺序。
- 全表最终 PG 对账支持删除、重插入和插入键与基线交错；不要求新增整数 ID 高于基线最大值，字符串主键保留尾部空格。
- PK 值 UPDATE 属于固定范围外负载，提交及回滚尝试均拒绝，不生成删除旧 PK / 插入新 PK 的补偿事件。
- 成功报告及变化文件与输入摘要绑定。相同输入复用报告，不查询 PG；缺失、陈旧或损坏报告拒绝。失败残留不当作成功报告。

## 使用

采集前 PG schema 预检、静止初始边界 Baseline 验证和采集请求关联仍各自负责不同事实。完成原生采集后，在业务库保持采集结束状态时执行：

```bash
java -Xmx3g -jar build/sibench/sibench.jar audit-rows \
  config/hybench_sf1.yaml results/poc/hybench-htap-sf1/baseline \
  results/poc/hybench-htap-sf1/raw \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture --user postgres
```

默认生成 `RAW/row-state-audit.json` 和 `RAW/row-state-audit.json.changes.jsonl`。可指定第四个位置参数 REPORT；编译 / 维护诊断使用自定义报告时需显式传入其路径。`CapturePoc audit ROOT CONFIG [REPORT]` 从 HyBench 配置读取相同审计所需的路径和连接。审计历史输入时必须提供与原采集结束状态一致的 PG；新报告使用独立路径，历史证据保持原样。

`PgBaselineVerify --config ROOT PROFILE JDBC_URL USER BASELINE_DIR` 按来源 schema 验证基线全值和身份；要求 Parquet 按主键有序、RowID 从 1 连续、版本为 0，`locators.tsv` 存在。字符串对账显式使用 C 排序，不代表原生查询 collation 投影已实现。

## 验证范围

默认离线测试和需显式启用的 PG 集成测试分别执行。实库证据保存于 `results/validation/row-state-audit-20261021/`，原生 SF1 数据库仅用于只读对账。

JDK17 根 Maven 构建通过 110 项测试，零失败、零错误、零跳过；日志为 `package.log`。最终不可变构建 `sibench-audit.jar` 的 SHA256 为 `41421562f166b6a169cebfde487b0806cd77592585048bcf13d5091b620771a8`，与 `build/sibench/sibench.jar` 一致。运行命令应使用 `$JAVA_HOME/bin/java`；系统默认 Java 23 不满足本项目 Parquet 运行要求。

原生 HTAP 的 `htap-jdk17.json` 全量对账通过：8,706,000 基线行、8,707,767 最终行、5,408 次提交行修改、3,706 个修改主键，四会话的同键因果链与最终八表状态一致。217 次提交、15 次 abort 中包含 13 次业务回滚及 2 次只读连接关闭；该次原生运行回滚行修改为 0。

`maintenance.json` 复用该 HTAP 报告和已审计变化通过：31,212,000 个基线索引条目、34 次真实二级索引值改写（loanapps 15、loantrans 19）、36,020 次 Locator / RowID 可见性探针。未重新匹配源行 before-image。

原生 TP 子集的 `tp-subset-jdk17.json` 全量对账通过：8,706,000 基线行、8,731,627 最终行、76,881 次提交行修改、72,870 个修改主键；八表包含未修改的基线行均核对。当前 v8 TP 子集的审计见 [TP 子集 v8 验证](hybench-sf1-tp-trace.md)。

`RowStateAuditTest` 使用真实 Parquet 与独立 JDBC 行结果 fixture，覆盖三来源全部配置表 / 字段、字符串主键、非连续 int64 主键、DELETE / 重插入、低于和位于基线键之间的新插入、回滚排除、普通列旧值错误、最终值错误、双会话同键因果链及其持久化顺序、摘要复用及损坏变化文件。`IndexTraceCompilerTest` 验证一次审计后维护诊断、重复编译及各回放变体复用报告，并核对缺失报告与 PK 改写的拒绝边界。

`RowStateAuditIntegrationIT` 在 PG14.24 插桩构建上使用三个随机临时库：HyBench 执行固定上游 DDL，YCSB / sysbench 使用独立声明的来源布局。基线 Parquet 与实际 JDBC 结果核对，变更通过 `CaptureDriver` 和 PG 行触发器采集。9 项检查全部通过，报告为 `bound-parameters/integration-summary.json`：三来源各验证完整行状态与离线复用、回滚 PK 改写拒绝、提交 PK 改写拒绝。接受的变更覆盖所有配置表的普通列 / 二级索引列更新、同 PK 删除重插入、低于基线键的新增行和写入后回滚；HyBench 覆盖 REAL、CHAR、date 和微秒 timestamp，YCSB 保留主键尾部空格。

集成测试需要 PG 已启动，且其 `SIBENCH_CAPTURE_DIR` 与测试提供的目录一致。使用新证据目录运行：

```bash
export SIBENCH_ROW_TEST_JDBC=jdbc:postgresql://127.0.0.1:55433/postgres
export SIBENCH_ROW_CAPTURE_DIR="$PWD/results/validation/row-state-audit-20261021/live-capture"
mvn -o -Dtest=RowStateAuditIntegrationIT \
  -Dsibench.row.evidence=results/validation/row-state-audit-check test
```

`SIBENCH_ROW_TEST_USER` 默认 postgres，密码使用 `PGPASSWORD`。测试创建并自动删除自身的临时库；原始采集、基线、审计和拒绝诊断保留在证据目录。

验证结束确认无残留测试库，隔离 PG 已恢复停止状态。停止 PG 后，HTAP / TP 各自再次调用 `audit-rows` 均成功，`htap-reuse-offline.json` / `tp-reuse-offline.json` 中的报告内容与原成功报告一致，证明复用路径不依赖数据库。前者标准输出含 JVM 性能计数器警告，原始输出保留，JSON 内容从首个对象开始解析。

受控 SQL 不代表原生客户端执行；三来源的原生采集审计见各自的 Trace 验证文档。多会话行因果链不证明全局事件无丢失或查询快照等价；审计仍缓存修改集合，未验收 SF1000 有界状态。
