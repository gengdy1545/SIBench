# HyBench 使用与索引负载分析

本报告针对子模块提交 `eaad91b52956fddd3d48ce49f792928056b7b360`。已完成原始 Java 源码编译、缩小数据生成、8 表加载、索引与序列初始化，以及1分钟 TP 运行。验证仅覆盖 TP1 点查和 TP9 转账的派生组合；不代表全部 TP、AP、AT、freshness 模式通过。

## 构建与运行

本环境的 `mvn package` 被 SSJ 传递依赖 `dsol:dsol-xml:1.6.9` 的旧 HTTP 仓库阻断。没有禁用 Maven 全局 HTTP blocker。替代构建使用 Java 17 的 `javac --release 16` 编译当前全部 Java 源码，依赖为仓库 `lib/*` 与 POM 声明的 `ssj-2.5.jar`；classes 放在 bundled jar 之前。具体实现见 [build.sh](../../scripts/benchmarks/build.sh)。这验证了编译和本次路径，但不表示 Maven 完整依赖构建已修复。

按 [环境步骤](../validation/native-smoke.md#复现步骤) 准备后执行：

```bash
python3 scripts/validation/native_smoke.py --benchmark hybench
```

底层原生流程：

1. `java -cp '<资源目录>:<源码classes>:<lib/*>' com.hybench.HyBench -t gendata -c <props>` 生成 `Data_<sf>/`。
2. psql 执行 `conf/ddl_pg.sql`，使用 `conf/load_data_pg.sql` 中的 `\copy` 加载8表。
3. psql 执行 `conf/create_index_pg.sql`。该文件除二级索引外还创建并初始化 transfer、checking、loanapps、loantrans 的自增序列；不能因“不测二级索引”就整段跳过。
4. `java ... com.hybench.HyBench -t runtp -c <props> -f conf/stmt_postgres.toml`。
5. 删除本次专属数据库；生成数据保留在运行目录便于核验。

`-f` 在运行阶段指定 SQL 模板。`gendata` 的规模参数来自 classpath 中的 `parameters.toml`，不是 `-f`。验证使用 [parameters.toml](../../config/workloads/hybench/parameters.toml) 的 `[smoke]` 段覆盖资源，生成和执行必须使用同一份资源。

## 规模与工作负载参数

### SIBench SF1000 初始规模配置

[sf1000.yaml](../../config/workloads/hybench/sf1000.yaml) 固定本报告对应源码版本的 `sf=1000x`。各表为 customer 300000000、company 2000000、savingaccount/checkingaccount 各 302000000、transfer 6000000000、checking/loanapps/loantrans 各 600000000，共 **8706000000 行**。此处 SF1000 不是压缩后 1 TB 的承诺。

该配置仅定义初始数据规模及 Parquet 生成参数，不代表生成器已经实现。统一文件目标 256 MiB、每个 row group 最多 1048576 行；先显式选用 Zstd level 3 作为可配置的生成基线，不作压缩算法性能比较。

原生生成器混用未固定种子的随机源，并在部分时间戳生成中读取当前时间。`requested_seed` 是 SIBench 适配器必须实现的要求，不会自动使原生代码可复现。SF1000 的 transfer 主键达到 6000000000，生成器记录类型是 long，但原生 PostgreSQL DDL 为 int，不能直接全量加载；Parquet 适配器需使用 int64 并记录类型转换。

规模检查命令为 `java -jar build/sibench/sibench.jar inspect-scale`（先使用 JDK 17 执行 `mvn package`）。它只核验配置、源码版本和定义文件摘要，输出容量信息及未满足的生成条件，不生成数据。完整生成前需要使用覆盖全规模键空间和真实字段分布的样本校准 Parquet 大小、生成速率与峰值空间，预算须包括 Baseline、增量、Rewrite、保留文件和临时产物。

### 原生参数

| 维度 | 配置 | 含义 |
| --- | --- | --- |
| 数据规模 | `sf`＋`parameters.toml` 相应段 | 读取具体各表行数；不能直接使用任意浮点 SF |
| 运行时长 | `tpRunMins`、`apRunMins`、`xpRunMins` | 整数分钟；0 不应当作可靠的空测试 |
| 并发 | `tpclient`、`apclient`、`xtpclient`、`xapclient` | 对应 TP/AP/混合模式 |
| 事务比例 | `tp1_percent`…`tp18_percent`、`at1_percent`…`at6_percent` | 每组要求合计100 |
| 风控状态 | `risk_rate`、`contention_num`、生成参数 `prob_blocked` | 影响状态及风险相关数据；缩小数据后需保持有效 |
| 查询模板 | `-f conf/stmt_postgres.toml` | 决定具体 SQL、谓词、字段及方言 |

当前资源包含 1x/10x/100x/1000x 段。1x 包含 customer 300000、company 2000、transfer 6000000、checking 600000、loanapps 600000；两个账户表各为302000。生成器在同一 loanapps 循环写 loantrans，实际行数由 `loanapps_number` 决定，不能假设修改 `loantrans_number` 会独立改变它。

本次 smoke 使用 customer=1000、company=10、两账户表各1010、transfer=10000、checking/loanapps/loantrans各1000，总计16030行。`sf=smoke` 是本项目派生配置，不是标准 SF。TP1/TP9 各50%，其余 TP 为0，运行1分钟。

`dist`、`power_alpha` 虽出现在 pg.props，但本次源码检索未发现工作负载读取这两个属性，不能承诺它们可控制分布。RandomGenerator 混用 Random/ThreadLocalRandom，尚无统一可配置随机种子。

## 主键与运行反馈

| 表 | PK | 生成／访问特点 |
| --- | --- | --- |
| customer | custid | 初始1…customer_number |
| company | companyid | 从 customer_number+1 起 |
| savingaccount/checkingaccount | accountid | 覆盖客户和企业，userid 为另一列 |
| transfer/checking | id | 初始显式整数；运行 INSERT DEFAULT 使用序列 |
| loanapps/loantrans | id | 初始生成；后续涉及状态选择和生成键 |

TP1–4 可直接定位 PK。TP5–8 涉及 sourceid/targetid/applicantid 等非 PK 谓词；不能从这些参数宣称得到实际访问 PK 范围。TP9 转账先读余额，余额不足可回滚，否则更新两个账户并插入 transfer。TP10、TP13–16 等还根据 ResultSet 决定后续键或业务分支；完整请求流需参考数据库反馈，不能仅通过静态模板离线展开。

`runtp` 的 TP1–18 模板未包含 delete；删除出现在 AT-00/freshness 相关路径。本次验证不把该模式扩展为“TP 删除已覆盖”。业务回滚不能只依赖日志中的事务总数判断，采集器需记录 commit/rollback 与受影响行数。

## 已确认的运行注意事项

- 账户 CSV 以追加方式打开；重复 gendata 必须换新目录，避免 PK 重复。
- 运行会读取 `Related_transfer_bids`、`Related_checking_bids` 序列化辅助文件；即使选择简单 TP，也应保留生成器完整产物。
- bundled Log4j 配置有无效的 `appender.error.level` 属性。验证脚本只在运行目录副本移除该行，不改变子模块。
- ConfigLoader 会打印全部配置，连接错误也可能打印密码。当前仅在本机隔离的无密码测试实例运行；采集真实连接前必须增加日志脱敏。
- 默认启动脚本的 classpath 通配符未妥善引用；报告和验证脚本使用显式 Java argv。

## 源码依据

[HyBench 主入口](../../benchmark/HyBench-2024/src/main/java/com/hybench/HyBench.java)、[ConfigReader](../../benchmark/HyBench-2024/src/main/java/com/hybench/load/ConfigReader.java)、[数据生成器](../../benchmark/HyBench-2024/src/main/java/com/hybench/load/DataGenerator_RiskControlling.java)、[TPClient](../../benchmark/HyBench-2024/src/main/java/com/hybench/workload/TPClient.java)、[Client](../../benchmark/HyBench-2024/src/main/java/com/hybench/workload/Client.java)、[RandomGenerator](../../benchmark/HyBench-2024/src/main/java/com/hybench/util/RandomGenerator.java)、[PostgreSQL 模板](../../benchmark/HyBench-2024/conf/stmt_postgres.toml)、[索引和序列脚本](../../benchmark/HyBench-2024/conf/create_index_pg.sql)。
