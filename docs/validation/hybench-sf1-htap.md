# HyBench 原生 HTAP 验证

HyBench 原生 runHTAP 按 AP → TP → 并发 AT/IQ 执行，经请求核验、行状态审计和事务完成位置归并形成规范 Trace。来源语义见 [HyBench 说明](../benchmarks/hybench.md)，当前支持范围见 [能力矩阵](../benchmarks/capability-matrix.md)。

## 代表输入与证据

选用带 PG 计数器快照的 SF1 采集 `results/poc/hybench-htap-sf1/`，采集日志为 `results/poc/native-trace-20261023/hybench_sf1-v9.log`。同次输入的计数器、完整参考和区间 oracle 结果见 [采集完整性](capture-completeness.md)，不混入同目录名在早期运行中的统计。

- 固定上游提交 `eaad91b52956fddd3d48ce49f792928056b7b360`，JDK 17、PG 14.24；八表共 8,706,000 行，8 主键、13 二级索引。
- AP / TP / XP 使用原生选择器、比例、计时与退出规则；AP 按完整轮结束，实际耗时可超配置时长。runHTAP 不执行 freshness DELETE，不宣称这类覆盖。
- 四会话事实按完成位置归并，保留语句快照、事务内操作和回滚排除；原生 INSERT / UPDATE 与预期业务回滚由上游负载决定。
- 原生访问包含点 / 范围、数组并集、BitmapAnd 交集和提前停止的二级扫描。二级扫描投影为已读完前段及边界值的钉住点查；原生可见行数与访问成本分别记录。
- Baseline 逐值核验、最终八表行状态对账、逐访问账本、PG 计数器和独立 Parquet oracle 各自证明不同事实。

按设计提高并发及 SF10 的输入有效性见 [并发采集](concurrent-native-capture.md)，SF10 完整参考 / oracle / 本地服务结果见 [完整验收](p0-three-source-acceptance.md)，COS 结果见 [对象存储验收](rocksdb-cloud-scale.md)。事务内读的独立 PG 两会话反例见 [读取验证](p0-native-publish-validation.md)。

## 复现入口

需使用配置匹配的静止库与原生 Data 文件；已有数据库和输出目录会被拒绝，新运行使用独立身份和目录。

```bash
rtk mvn package
rtk proxy scripts/native-capture.sh config/hybench_sf1.yaml
rtk proxy java -Xmx24g -jar build/sibench/sibench.jar replay config/index_replay_128clients.yaml TRACE REPORT
rtk proxy java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml TRACE REPORT
```

原生依赖、静止库准备和分阶段命令见 [README](../../README.md)。旧实验的配置与构建以各自证据为准，当前配置重新采集不保证复现同一随机执行。短输入证明语义覆盖，不证明容量、性能排名或全部故障路径。
