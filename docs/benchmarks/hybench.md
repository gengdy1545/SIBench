# HyBench 数据来源与适配

固定源码提交：`eaad91b52956fddd3d48ce49f792928056b7b360`。上游位于 `benchmark/HyBench-2024/`，由生成前检查核对提交、定义文件摘要及干净工作区。

## 当前入口

使用 [sf1000.yaml](../../config/workloads/hybench/sf1000.yaml) 配置 Java Baseline：

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/workloads/hybench/sf1000.yaml
```

去掉 `--dry-run` 会实际生成数据。参数选择、产物格式及未验证范围见 [README](../../README.md#baseline-生成配置)。

## 数据规模

原生 `sf=1000x` 从 parameters.toml 读取各表规模，不等于压缩后 1 TB。

| 表 | 初始行数 |
| --- | ---: |
| customer | 300000000 |
| company | 2000000 |
| savingaccount | 302000000 |
| checkingaccount | 302000000 |
| transfer | 6000000000 |
| checking | 600000000 |
| loanapps | 600000000 |
| loantrans | 600000000 |

总计 8706000000 行。loantrans 与 loanapps 配对生成；两个账户表均覆盖客户与企业。

## 适配边界

- 主项目使用确定性派生生成器，读取固定源码中的参数与词表，不修改 benchmark。
- ID 使用 int64，金额使用 double；原生 PostgreSQL transfer.id 的 int32 不足以覆盖 SF1000。
- 固定 UTC 日期范围与随机种子，区别于原生 Random、ThreadLocalRandom 和部分墙钟日期逻辑。blocked 状态和有效 ID 抽样边界的差异见 README。
- Baseline 仅产生初始文件与 INSERT 身份信息，不运行原生 TP/AP/AT 事务，也不输出其风控辅助采样文件。
- 原生 TP1–4 可直接定位 PK；非 PK 谓词、连接及查询反馈决定的后续操作，需要在增量适配中明确处理，不能仅凭 SQL 模板推断完整访问键集合。
- 原生 TP 模板不包含所有删除场景；不得据 Baseline 宣称实现完整原生操作负载。

## 源码入口

[参数](../../benchmark/HyBench-2024/src/main/resources/parameters.toml)、[生成器](../../benchmark/HyBench-2024/src/main/java/com/hybench/load/DataGenerator_RiskControlling.java)、[随机工具](../../benchmark/HyBench-2024/src/main/java/com/hybench/util/RandomGenerator.java)、[TP 客户端](../../benchmark/HyBench-2024/src/main/java/com/hybench/workload/TPClient.java)、[PostgreSQL 模板](../../benchmark/HyBench-2024/conf/stmt_postgres.toml)。
