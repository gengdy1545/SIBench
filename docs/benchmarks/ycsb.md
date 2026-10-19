# YCSB 使用与索引负载分析

本报告针对子模块提交 `66302f301b13f60d4bcb2f29f478586bb1d6f2e0`。已验证 JDBC → PostgreSQL 的加载及读写流程；完整结果见 [验收报告](../validation/native-smoke.md)。YCSB 是三者中最直接对应 PK 键值接口的起点。

## 构建与运行

在仓库根目录执行 [环境与构建步骤](../validation/native-smoke.md#复现步骤)，再运行：

```bash
python3 scripts/validation/native_smoke.py --benchmark ycsb
```

脚本创建独立数据库 `sibench_ycsb`，建表、加载、运行、核对行数后删除该数据库。如果数据库已存在则失败，不覆盖已有数据。配置来源为 [smoke.properties.in](../../config/workloads/ycsb/smoke.properties.in)，`@PORT@` 由脚本替换。

原生构建命令为 `mvn -pl site.ycsb:jdbc-binding -am -DskipTests package`。本次使用 Java 17，构建版本为 `0.18.0-SNAPSHOT`；解压 JDBC 分发包后使用其中 `lib/*`、`jdbc-binding/lib/*` 和 PostgreSQL JDBC jar。只加 `jdbc/target/dependency/*` 会缺少 Core 传递依赖，例如 HTrace。

原生流程是先建表，再 `bin/ycsb.sh load jdbc -P <配置>`，之后 `bin/ycsb.sh run jdbc -P <配置>`；PostgreSQL 驱动需加入 classpath。验证脚本等价地直接调用 `site.ycsb.Client -load` 和 `-t`，完整 argv 写入每次运行的 `result.json`。

表为 `usertable(YCSB_KEY varchar(255) primary key, FIELD0 text, …, FIELD9 text)`；验证显式使用 `COLLATE "C"`。字段数量与 `fieldcount` 对齐。JDBC 的 cleanup 是连接收尾，不是删除数据；测试数据库由脚本单独清理。

## 参数与生成方式

### 大规模初始数据配置

[large.yaml](../../config/workloads/ycsb/large.yaml) 使用候选档位 `recordcount=1000000000`、`insertstart=0`、`insertcount=1000000000`。保留 10 个字段、每字段 100 字节的固定长度配置，以及 hashed 键生成规则。10 亿条是本项目选择的整洁规模档位，不是上游标准规模，也不要求与 HyBench SF1000 的记录数或字节量精确相等。

字段内容合计 1000000000000 字节（1 TB，十进制），不包含键、编码和存储开销；这不是 Parquet 落盘大小估计。配置的 calibration_status 为 pending，生成代表性样本后再评估与其他来源的规模差距及磁盘预算，必要时调整记录数档位；不通过改变原生行形态精确配平。报告分别列出记录数、逻辑内容字节、Parquet 字节和索引大小。

该配置是初始规模配置，尚未指定 A-F 的增量负载，不是可以直接运行的原生 properties 文件。`requested_seed` 需由确定性适配器落实。文件目标、row group 行数及 Zstd level 3 与 HyBench 配置一致；所有 Backend 必须复用同一生成产物。使用 `java -jar build/sibench/sibench.jar inspect-scale` 核验配置及生成条件。

### 原生参数

| 维度 | 原生配置 | 含义 |
| --- | --- | --- |
| 初始数据 | `recordcount`、`insertstart`、`insertcount` | 记录数及加载编号区间 |
| 运行长度 | `operationcount`、`maxexecutiontime` | 操作数／秒级时间限制；同时设置时任一限制可结束 |
| 并发与限速 | `threadcount` 或 `-threads`、`-target` | 工作线程数、目标总操作速率 |
| 内容大小 | `fieldcount`、`fieldlength`、`fieldlengthdistribution` | 字段数与字段长度；10×100 约为 1000 字节内容，非磁盘行大小 |
| 操作比例 | `readproportion`、`updateproportion`、`insertproportion`、`scanproportion`、`readmodifywriteproportion` | 加权随机选择，短运行不保证精确比例 |
| 键分布 | `requestdistribution` | uniform、zipfian、latest、hotspot、sequential、exponential |
| 键编码 | `insertorder`、`zeropadding` | 默认 hashed；ordered 也需补零才能在等宽范围内与数值顺序一致 |
| 扫描 | `minscanlength`、`maxscanlength`、`scanlengthdistribution` | 以起始键＋条数上限表达，不是数值区间宽度 |

标准配置 A 为 50% read/50% update；B 为 95% read/5% update；C 为纯 read；D 为 95% read/5% insert，使用 latest；E 为 95% scan/5% insert；F 为 50% read/50% read-modify-write。本次样例采用 A 的操作比例，但将分布改为 uniform、键改为 ordered＋8 位补零，因此明确属于派生验证案例。

`load` 直接经 binding 插入，不预先生成操作文件。`run` 运行时选择操作及键。CoreWorkload 使用线程随机生成器，没有本项目已验证的统一 seed 配置；固定配置并不等于固定请求序列。

## 索引语义及采集位置

- `read/update/insert` 的实际字符串键在 `CoreWorkload.buildKeyName` 生成，经 DB API 交给 binding。统计必须保存此字符串，不能只保存 hash 前的计数器。
- `scan` 在 JDBC 中形成 `WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY FETCH FIRST ? ROWS ONLY`；排序依赖数据库 collation。未知结束键保留为空，不能推算为 `start + count`。
- CoreWorkload 标准 A–F 不生成 delete；虽然 `DB` 和 JDBC binding 有 delete 接口，删除工作负载需扩展并标为派生案例。
- F 的 read-modify-write 是两次 API 调用，不能默认具有数据库事务原子性。模拟 value 回放只重现约定的键值访问成本。
- 采集优先在 DB/binding 边界记录实际 key、field、status；要统计 JDBC 扫描返回键/行数则需在 ResultSet 层补充。当前 JDBC `scan` 仅在 `fields != null` 时填充结果容器，不能将容器为空当作扫描未命中。

## 源码依据

- [CoreWorkload](../../benchmark/YCSB/core/src/main/java/site/ycsb/workloads/CoreWorkload.java)：`init`、`buildKeyName`、`doInsert`、`doTransaction*`。
- [Client](../../benchmark/YCSB/core/src/main/java/site/ycsb/Client.java) 与 [DB API](../../benchmark/YCSB/core/src/main/java/site/ycsb/DB.java)：线程、运行限制、binding 接口。
- [JdbcDBClient](../../benchmark/YCSB/jdbc/src/main/java/site/ycsb/db/JdbcDBClient.java) 与 [DefaultDBFlavor](../../benchmark/YCSB/jdbc/src/main/java/site/ycsb/db/flavors/DefaultDBFlavor.java)：SQL、状态及扫描行为。
- [原生 workload 文件](../../benchmark/YCSB/workloads) 与 [JDBC 说明](../../benchmark/YCSB/jdbc/README.md)。
