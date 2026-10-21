# 来源 Schema 与 PG 预检

## 范围

`SourceSchema` 只适配项目固定的 HyBench、YCSB JDBC 和 sysbench PostgreSQL 布局。源码提交必须是已审阅版本，子模块必须干净；报告保存相关定义文件 SHA256。HyBench 用有限、逐 token 的 DDL 语法读取表、索引及序列声明；YCSB / sysbench 用固定版本建表工具 / Lua 逻辑的明确适配规则解析配置，不尝试解析任意 Java、Lua 或 SQL。

| 来源 | 表与列 | 索引 |
| --- | --- | --- |
| HyBench | `conf/ddl_pg.sql` 的八表，完整列顺序、类型 / 宽度、可空性和默认值；索引脚本中的四个 ID 序列默认值 | 八个单列整数主键、全部 13 个普通非唯一单列二级索引；二级类型为 integer、timestamp without time zone、CHAR、VARCHAR |
| YCSB JDBC | `usertable`：`YCSB_KEY VARCHAR PRIMARY KEY`，`FIELD0` 至配置 `fieldcount-1`，普通字段 TEXT | 只有字符串主键；不额外创建字段二级索引 |
| sysbench PG | `sbtest1` 至配置 `tables`：id INTEGER / SERIAL、k INTEGER、c CHAR(120)、pad CHAR(60) | 每表 id 主键、k 上的普通非唯一单列 B-tree 索引 `k_N` |

YCSB 固定使用 `JdbcDBCreateTable`，不是普通字段为 VARCHAR 的 SQL 模板。`fieldcount` 默认配置为 10，可按配置解析，当前最多 1024。sysbench 固定 `secondary=false`、`create-secondary=true`，`auto-inc` 默认 true，也允许 false；Redshift、主键替换模式和自定义建表选项拒绝。这些是源布局约束，不是下游二级发送开关。

只有单列主键自身的唯一性需要支持；额外 UNIQUE、复合、表达式、partial、INCLUDE、自定义 operator class / collation / 排序及其它索引访问方法不在这三种目标布局内，明确拒绝。运行期动态 DDL 不在范围内。固定标准负载不改写已有行的主键值：PK 值 UPDATE 明确拒绝，包括随后回滚的尝试，不作为待实现功能，也不补造此类负载。这不排除普通列 / 二级索引列 UPDATE、新版本主键索引写入或同主键 DELETE / 重插入。Schema catalog 和公共行审计支持不意味着 YCSB / sysbench 的原生采集、字符串主键 Trace 或范围投影已完成。

## 核对与失败

`PgSchemaCheck` 在只读 Repeatable Read 事务中读取实际 `pg_class`、`pg_attribute`、`pg_attrdef`、`pg_index`、operator class / collation 及约束元数据。检查不会执行 benchmark SQL、读写业务行、创建索引或修复数据库。

- 指定 schema 的表 / 视图清单必须完全一致；默认 `public`，可用 `postgres.schema` 指定。序列、采集函数与触发器不作为额外业务表；schema 内额外业务表或视图拒绝，其它 schema 的无关表不计入清单。
- 每张表必须是普通持久表，无继承 / 分区或 RLS；不能将视图、临时表或 unlogged 表当作原生表。
- 完整列顺序、名称、类型及宽度、可空性、默认值必须一致；额外 / 缺失 / dropped 列、生成列、identity 列和自定义列 collation 拒绝。CHAR / VARCHAR / TEXT 要求数据库默认 collation，并记录实际 `lc_collate` / `lc_ctype`，不据此声称已实现查询排序等价性。
- 索引按名称、列、主键 / 唯一属性、B-tree 方法、单列宽度、默认内置 operator class、列 collation 及排序标记核对；必须 valid / ready / live / immediate。额外索引、缺失索引、同名错列索引、额外约束及不支持定义拒绝。
- 序列默认值同时核对 `nextval` 引用与 `pg_get_serial_sequence` 的列所有权；不验证当前计数或历史 `setval` 是否执行，这属于数据状态 / 初始化验收。
- 原生 SQL 使用非限定表名，额外检查同一角色的 `search_path` 是否将这些名称解析到目标 schema。配置声明了数据库、角色、版本、端口、隔离级别或并行参数时，同时核对连接实际值。

成功报告为 `schema_verified`，差异为 `schema_mismatch`，包含期望与实际定义、环境、源码摘要和具体差异。检查是采集前的时点事实，不是整个运行期 DDL 漂移检测；实验环境应禁止外部 DDL，不宣称持有跨运行发布屏障。

HyBench `CapturePoc check` 连接 PG 并执行预检。Baseline export 在创建输出目录前检查，保存 `schema-preflight.json`；原生 run 在打开记录 JDBC 会话和调用 benchmark 方法前检查，在 `capture-run.json` 保存结果。失败 run 保留诊断，但没有业务采集会话。

YCSB / sysbench 的单会话原生采集入口（`NativeCapture`，见 [原生采集验证](../validation/ycsb-sysbench-native-capture.md)）在采集前调用相同检查并保存结果；独立预检通过不替代采集验收。构建副本中参与执行的上游源码须与固定子模块逐字节一致，否则在建库前拒绝。

## 使用

```bash
java -jar build/sibench/sibench.jar check-schema config/hybench_sf1.yaml \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture --user postgres
java -jar build/sibench/sibench.jar check-schema config/ycsb_large.yaml \
  --jdbc jdbc:postgresql://127.0.0.1:55433/ycsb_capture --user postgres
java -jar build/sibench/sibench.jar check-schema config/sysbench_large.yaml \
  --jdbc jdbc:postgresql://127.0.0.1:55433/sysbench_capture --user postgres
```

密码通过 `PGPASSWORD` 提供，不写入报告。独立检查输出 JSON；定义不一致退出码为 1，报告仍包含实际差异；连接或配置错误退出码也为 1。命令不隐式启动 PG，不创建或加载数据库。
