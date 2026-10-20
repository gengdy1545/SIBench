# HyBench 来源与适配

固定提交：`eaad91b52956fddd3d48ce49f792928056b7b360`，位于 `benchmark/HyBench-2024/`，运行前核对干净工作区。

## 原生路径

[hybench_sf1.yaml](../../config/hybench_sf1.yaml) 使用原生数据生成、DDL、索引和 TPClient 事务方法。SIBench 反射调用原生方法并采集实际执行，当前为单会话 TP3/TP5/TP9=35/30/35，不是完整混合比例。流程与参数见 [SF1 验证](../validation/hybench-sf1-poc.md)。

运行期只有 savingaccount UPDATE、transfer INSERT 和相关主键/二级查询；没有 AP 或 DELETE。AT-00 有 DELETE 模板，但原生默认 runner 的 execFresh3 调用被注释，不能据模板宣称运行覆盖。完整多表 CRUD、索引列变更和其他二级索引仍待适配。

发送设置独立于原生采集，详见 [Trace](../contracts/trace.md)。长时采用连续原生采集，不循环；SF1000 原生入口尚未实现。

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
