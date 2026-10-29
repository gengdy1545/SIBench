# 统一行状态审计

更新：2026-10-23。

## 职责

`RowStateAudit` 统一负责三来源的源行状态核验。来源核验一次，转换产物和 Backend 结果各自验收，不在后续阶段重复匹配源行 before-image。

- 按 `SourceSchema` 核对全部列、可空性和类型；CHAR、VARCHAR / TEXT、REAL、date / timestamp 分别按源类型比较。
- 从已验证 Baseline 连接已提交旧 / 新行，检查普通列；提交 / 回滚和未完成事务单独处理。
- 单会话保留来源顺序；多会话因果链保留各会话的同键顺序。审计只证明行状态，全局提交顺序与查询快照由编译器按事务完成位置建立。
- 全表最终 PG 对账支持删除、重插入和插入键与基线交错；不要求新增整数 ID 高于基线最大值，字符串主键保留尾部空格。
- PK 值 UPDATE 属于固定范围外负载，提交及回滚尝试均拒绝，不生成删除旧 PK / 插入新 PK 的补偿事件。
- 成功报告及变化文件与输入摘要绑定。相同输入复用报告，不查询 PG；缺失、陈旧或损坏报告拒绝。失败残留不当作成功报告。

## 使用

采集前 PG schema 预检、静止初始边界 Baseline 验证和采集请求关联各自负责不同事实。`scripts/native-capture.sh` 在原生执行后、业务库保持采集结束状态时调用审计；单独执行时：

```bash
java -Xmx12g -jar build/sibench/sibench.jar audit-rows \
  config/hybench_sf1.yaml results/poc/hybench-htap-sf1/baseline \
  results/poc/hybench-htap-sf1/raw \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture --user postgres
```

默认生成 `RAW/row-state-audit.json` 和 `RAW/row-state-audit.json.changes.jsonl`。可指定第四个位置参数 REPORT；编译使用自定义报告时以 `compile-trace PROFILE OUTPUT ROW_AUDIT` 显式传入。审计历史输入时必须提供与原采集结束状态一致的 PG；新报告使用独立路径，历史证据保持原样。

Baseline 验证由采集编排的 `export` 阶段在导出后执行（`PgBaselineVerify`）：按来源 schema 验证基线全值和身份，要求 Parquet 按主键有序、RowID 从 1 连续、版本为 0。字符串对账显式使用 C 排序，不代表原生查询 collation 投影已实现。

## 验证范围

原生采集：三来源的当前采集均通过完整行状态审计，包括完整 HyBench HTAP 的四会话同键因果链与最终八表对账，见 [HTAP 验证](hybench-sf1-htap.md) 与 [原生 Trace 验证](ycsb-sysbench-native-trace.md)。

`RowStateAuditTest` 使用真实 Parquet 与独立 JDBC 行结果 fixture，覆盖三来源全部配置表 / 字段、字符串主键、非连续 int64 主键、DELETE / 重插入、低于和位于基线键之间的新插入、回滚排除、普通列旧值错误、最终值错误、双会话同键因果链及其持久化顺序、摘要复用及损坏变化文件。`IndexTraceCompilerTest` 验证编译复用审计报告且不改写它，并核对缺失报告与 PK 改写的拒绝边界。

`RowStateAuditIntegrationIT` 在 PG14.24 插桩构建上使用三个随机临时库：HyBench 执行固定上游 DDL，YCSB / sysbench 使用独立声明的来源布局。基线 Parquet 与实际 JDBC 结果核对，变更通过 `CaptureDriver` 和 PG 行触发器采集；三来源各验证完整行状态与离线复用、回滚 PK 改写拒绝、提交 PK 改写拒绝。接受的变更覆盖所有配置表的普通列 / 二级索引列更新、同 PK 删除重插入、低于基线键的新增行和写入后回滚；HyBench 覆盖 REAL、CHAR、date 和微秒 timestamp，YCSB 保留主键尾部空格。

集成测试需要 PG 已启动，且其 `SIBENCH_CAPTURE_DIR` 与测试提供的目录一致。使用新证据目录运行：

```bash
export SIBENCH_ROW_TEST_JDBC=jdbc:postgresql://127.0.0.1:55433/postgres
export SIBENCH_ROW_CAPTURE_DIR="$PWD/results/validation/row-state-audit-check/live-capture"
mvn -o -Dtest=RowStateAuditIntegrationIT \
  -Dsibench.row.evidence=results/validation/row-state-audit-check test
```

`SIBENCH_ROW_TEST_USER` 默认 postgres，密码使用 `PGPASSWORD`。测试创建并自动删除自身的临时库；原始采集、基线、审计和拒绝诊断保留在证据目录。

受控 SQL 不代表原生客户端执行。行因果链不证明全局事件无丢失；行审计以外部排序归并修改，落盘等价证据见 [采集完整性](capture-completeness.md)；SF1000 容量尚未验收。
