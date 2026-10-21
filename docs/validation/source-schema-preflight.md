# 来源 Schema 预检实现验证

日期：2026-10-21。范围以 [来源 Schema 契约](../contracts/source-schema.md) 为准。

## 离线验证

JDK 17，根 Maven 工程离线构建：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn -o -q package
java -jar build/sibench/sibench.jar --help
java -jar build/sibench/sibench.jar prepare --dry-run config/ycsb_large.yaml
java -jar build/sibench/sibench.jar prepare --dry-run config/sysbench_large.yaml
git diff --check
```

104 项测试零失败 / 错误 / 跳过；新增 CLI 显示 `check-schema`，两个 Baseline dry-run 通过且不生成数据。

| 测试域 | 已覆盖 |
| --- | --- |
| 固定来源定义 | HyBench 八表完整列 / 13 二级 / ID 序列 / 历史索引编号；YCSB 配置字段数、TEXT 字段、字符串主键且无二级；sysbench 多表命名空间、k 索引、auto-inc 两种选择 |
| 来源拒绝 | 不支持的提交、sysbench 主键替换 / 禁用源二级 / Redshift / 自定义选项；DDL UNIQUE、复合、表达式、partial、INCLUDE、非 B-tree、排序、截断及未知列 |
| 元数据比较 | 缺表 / 多表、视图、unlogged、继承、RLS、额外约束；完整列清单、名称 / 顺序、宽度、可空性、默认值、生成 / identity / dropped、collation、序列引用 / 所有权命名空间 |
| 索引比较 | 同名错列、主键 / 唯一属性、访问方法、key / INCLUDE 宽度、valid / ready / live / immediate、exclusion / 表达式 / partial、operator class、collation、排序、缺失 / 额外索引 |
| JDBC 与环境 | 三来源查询参数和元数据字段读取；search_path 遮蔽；配置数据库 / 角色 / 版本 / 端口 / 隔离 / 并行参数；读取错误回滚；源码 / 实际定义及差异 JSON |
| 采集失败证据 | 预检差异保存到失败 manifest，没有记录 JDBC 业务会话；既有采集审计、Trace 与发送测试保持通过 |

## 实际 PG 集成验证

经用户授权启动已有隔离 PG14.24，监听 `127.0.0.1:55433`。既有数据库 `sibench_htap_sf1_capture` 只运行只读预检，不修改 SF1 业务行、表或索引。三个独立、随机命名的测试数据库由测试创建，验证后自动删除；失败采集 manifest 与逐项报告保留。

最终扩展运行 **51 项检查全部通过**，证据目录为 `results/validation/schema-preflight-20261021/catalog-boundaries/`，总报告为 `integration-summary.json`；第一次 48 项运行保留在上级目录，不覆盖原始证据。

最终 JDK17 打包 CLI 再次核对 SF1，通过报告为 `packaged-cli-sf1.json`；全量离线构建仍为 104 项测试零失败 / 错误 / 跳过，两个来源 Baseline dry-run 和 `git diff --check` 通过。验证结束确认无残留测试库，隔离 PG 恢复为验证前的停止状态。

| 实际验证 | 结果 |
| --- | --- |
| HyBench SF1 | 完整八表、8 主键、13 二级及默认值 / 序列所有权与固定源码一致；报告 `hybench-sf1.json` |
| 来源布局 | HyBench 直接执行固定 `ddl_pg.sql` / `create_index_pg.sql`；YCSB 独立布局为 VARCHAR 主键 + 10 TEXT 字段，也核验配置 3 字段；sysbench 独立布局为 100 表、100 主键 + 100 k 二级及 SERIAL，也核验配置 2 表 / auto-inc=false |
| 表 / 列差异 | 缺失 / 额外表、视图、quoted 额外表名、缺失 / 额外 / dropped 列、错类型、可空性、默认值、序列失去所有权 / 错引用、生成 / identity 列、额外约束、unlogged、RLS 均拒绝并输出诊断 |
| 索引差异 | 缺失、同名错列、额外索引、unique 二级、复合、partial、表达式、INCLUDE、hash、降序、自定义 collation / 非默认 operator class 均拒绝；sysbench 最后一张表缺 k 索引拒绝 |
| 命名与环境 | 非 public schema 通过；无关 schema 不影响清单；search_path 遮蔽拒绝；数据库 / 角色 / 版本 / 端口 / 隔离 / 并行配置不一致拒绝 |
| CLI 与入口边界 | 正常 CLI 返回 0，差异返回 1 且输出 JSON；导出失败时输出目录尚未创建；原生 run 在加载 benchmark 类和创建记录 JDBC 会话前拒绝，失败 manifest 包含预检报告，raw 内仅有 manifest，未生成 native properties |

集成测试在 `src/test/java/org/sibench/schema/PgSchemaIntegrationIT.java`，不包含在默认离线测试模式中，必须显式选择并提供允许创建测试数据库的 PG 连接。重新运行使用新的证据路径，不覆盖已有报告：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SIBENCH_SCHEMA_TEST_JDBC=jdbc:postgresql://127.0.0.1:55433/postgres
export SIBENCH_SF1_JDBC=jdbc:postgresql://127.0.0.1:55433/sibench_htap_sf1_capture
mvn -o -q -Dtest=PgSchemaIntegrationIT \
  -Dsibench.schema.evidence=build/sibench/schema-preflight test
```

`SIBENCH_SCHEMA_TEST_USER` 默认 postgres；密码使用 `PGPASSWORD`。`SIBENCH_SF1_JDBC` 可选，省略时没有 SF1 检查证据。独立测试库按 PostgreSQL catalog 完成实际类型绑定、默认表达式反解析及序列检查；YCSB / sysbench 建表是与固定来源一致的独立空表 fixture，不是执行原生客户端 / Lua 的证据。valid / ready / live 等状态异常由离线元数据测试覆盖，未通过直接修改 PG 系统表制造异常。

## 验收边界

本次完成来源 schema 与实际 PG 的采集前一致性检查，不验证初始数据规模、序列当前计数、原生加载是否正确、查询 collation / 范围等价性或运行期外部 DDL 漂移。YCSB / sysbench 原生生成、负载执行、采集入口预检和 Trace 转换仍待接入，不属于本次通过证据。
