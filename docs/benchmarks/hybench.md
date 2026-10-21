# HyBench 来源与适配

固定提交：`eaad91b52956fddd3d48ce49f792928056b7b360`，位于 `benchmark/HyBench-2024/`，运行前核对干净工作区。

## 原生路径

[hybench_sf1.yaml](../../config/hybench_sf1.yaml) 默认 `runhtap`。SIBench 调用未修改的 `HyBench.runAP()`、`runTP()`、`runXP(0)`，使用原生 SQL、随机选择、计时器和客户端；阶段之间只等待连接排空，不恢复数据。流程见 [HTAP SF1 验证](../validation/hybench-sf1-htap.md)。

| 阶段 | 原生负载 | 默认选择 |
| --- | --- | --- |
| AP | AP1–AP13 | 原生每轮随机顺序，按完整轮结束 |
| TP | TP1–TP18 | 权重依次为 5/3/5/5/3/3/3/3/7/7/7/7/6/4/4/4/12/12 |
| XP | AT1–AT6 与 IQ1–IQ6 并发 | AT 权重 35/25/15/15/7/3；IQ 使用原生随机选择 |

属性以固定上游 `conf/pg.props` 为底，仅覆盖连接、客户端数和阶段时长；不手工构造事务或补齐随机分支。行变化采集触发器覆盖八张表，但某表没有实际写入时不会补造修改。AT-00 有 DELETE 模板，原生默认 runner 的 execFresh3 调用被注释，`runHTAP` 不调用该路径；不能据模板宣称 DELETE 覆盖。

[hybench_tp_subset_sf1.yaml](../../config/hybench_tp_subset_sf1.yaml) 显式保留 TP3/TP5/TP9=35/30/35 诊断。该子集只有 savingaccount UPDATE、transfer INSERT 和相关查询，不包含已有索引列值 UPDATE。当前 v7 编译维护八表主键及固定 DDL 全部 13 二级索引；维护逻辑支持八表提交修改、索引列值更新和 DELETE / 重插入，不再只展开 transfer。二级默认启用，开关对全部二级加载 / 查询 / 维护统一生效。

既有完整 HTAP 原始输入的逐键因果维护审计确认 34 次真实提交改键：loanapps.timestamp 15 次、loantrans.timestamp 19 次，见 [全索引 SF1 验证](../validation/hybench-sf1-secondary-indexes.md)。这里是已建索引的列值改变，不是从一列换到另一列建索引，也不是修改 CommitVersion。完整 HTAP 的并发快照、全局顺序和范围访问投影仍未实现；该审计不产生完整 HTAP 规范 Trace。

发送设置独立于原生采集，默认只向下游发送已提交变更。原始 Trace 保留业务回滚及修改尝试；单会话编译排除回滚修改及修改后的查询，不生成反向操作，不改变原生事务比例。已提交事务保留查询与修改的原始相对顺序，支持写后查询；operation/transaction 两种驱动输入模式最终均逐项发送，不要求索引事务 API。一个源 SQL 请求对应一个工作单元，按编号轮询路由；默认 1 client，同一 Trace 可用于 64/128 及更多 client，同事务可跨 client，过滤后覆盖不足拒绝。一个全局线程只保证本地发送顺序，不等待跨 client 可见性；服务端自行实现可见性，响应独立核对。受控测试独立于原生覆盖，详见 [Trace](../contracts/trace.md) 和 [发送验证](../validation/committed-change-delivery.md)。长时采用连续原生采集，不循环；SF1000 原生入口尚未实现。

## 派生 Baseline

[hybench_sf1000.yaml](../../config/hybench_sf1000.yaml) 是独立 `deterministic_derived` 生成配置，不是原生采集配置：

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/hybench_sf1000.yaml
```

| 表 | SF1000 初始行数 |
| --- | ---: |
| customer | 300000000 |
| company | 2000000 |
| savingaccount / checkingaccount | 各 302000000 |
| transfer | 6000000000 |
| checking / loanapps / loantrans | 各 600000000 |

合计 8,706,000,000 行；`1000x` 不表示压缩后 1 TB。loantrans 与 loanapps 配对，账户覆盖客户和企业。

派生规则读取上游 TOML 与词表，使用固定种子和 UTC 日期替代原生随机与墙钟；ID 为 int64、金额 double。blocked 使用每账户 Bernoulli，区别于原生有放回 ID 抽样；ID 抽样覆盖全部有效边界；checking 枚举保留原生实际结果，贷款配对共享随机域，通过时合同日期为次日。不做 SQL CHAR 截断/补齐，不输出风控辅助采样文件，不运行 TP/AP/AT。

原生 PG transfer.id 的 int32 无法容纳 SF1000，正式采集前需显式适配并验证，不直接复用 SF1 DDL。

## 源码入口

[参数](../../benchmark/HyBench-2024/src/main/resources/parameters.toml)、[生成器](../../benchmark/HyBench-2024/src/main/java/com/hybench/load/DataGenerator_RiskControlling.java)、[随机工具](../../benchmark/HyBench-2024/src/main/java/com/hybench/util/RandomGenerator.java)、[事务](../../benchmark/HyBench-2024/src/main/java/com/hybench/workload/TPClient.java)、[PostgreSQL 模板](../../benchmark/HyBench-2024/conf/stmt_postgres.toml)。
