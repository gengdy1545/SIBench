# sysbench 使用与索引负载分析

本报告针对子模块提交 `3ceba0b1e115f8c50d1d045a4574d8ed643bd497`。已验证 PostgreSQL 的 prepare、混合事务 run、cleanup；使用源码构建的 sysbench 1.1.0。

## 构建与运行

依赖及统一构建见 [验收报告](../validation/native-smoke.md#复现步骤)。原生构建步骤为：

```bash
bash autogen.sh
./configure --without-mysql --with-pgsql
make -j4
```

只构建 PostgreSQL 驱动；LuaJIT 与 concurrency kit 使用仓库自带版本。验证脚本使用导出的源码副本，设置 `LUA_PATH=<副本>/src/lua/?.lua;;`，运行：

```bash
python3 scripts/validation/native_smoke.py --benchmark sysbench
```

原生命令结构是 `sysbench [options] <脚本路径> prepare|run|cleanup`。`prepare` 在目标库创建表和数据；`run` 在线生成 SQL；`cleanup` 删除表。数据库由外层验证脚本创建和删除。

配置来源 [smoke.conf.in](../../config/workloads/sysbench/smoke.conf.in)。**当前版本不能依赖 `--config-file` 设置 Lua 参数**：第一次测试指定 table-size=1000 却加载 10000，源码 `sb_register_arg_set` 注册默认值会覆盖提前读入的选项。验证脚本保留配置文件，但将各项转换为 `--key=value` 传入，并断言实际行数。本结论适用于当前固定版本，不推断所有版本均如此。

## 参数与数据结构

### 大规模初始数据配置

[large.yaml](../../config/workloads/sysbench/large.yaml) 使用候选档位 `tables=100`、`table-size=10000000`，合计 1000000000 行。每表 ID 保持在原生 int32 范围；100 张表和每表 1000 万行是本项目的整洁规模选择，不是上游标准规模。表数影响命名空间与访问分布，必须在可比实验间固定；不要求与 HyBench 的行数或字节量精确相等。

原生生成的 c/pad 分别是 119/59 个 ASCII 字符，SQL 列宽为 CHAR(120)/CHAR(60)。仅生成文本共 178000000000 字节（178 GB，十进制），不含 id、k、CHAR 补齐、编码、压缩及数据库开销；不能将它作为 Parquet 实际大小。calibration_status 为 pending，须用样本判断规模和容量是否合适。报告分别列出记录数、逻辑内容字节、Parquet 字节和索引大小，不宣称不同 benchmark 按字节等价。

配置显式设定 `rand-seed=20261019` 与 uniform 分布；uniform 为派生选择。原生支持种子不等于并发全局操作序列确定。初始数据的 Parquet 适配器尚未实现，增量事务组合另行配置。文件目标、row group 行数及 Zstd level 3 与其他两套配置一致。使用 `java -jar build/sibench/sibench.jar inspect-scale` 核验配置；固定版本的原生 Lua 参数仍需由适配器转换为命令行参数。

### 原生参数

| 维度 | 原生参数 | 含义 |
| --- | --- | --- |
| 数据规模 | `--tables`、`--table-size` | 总初始行数为两者乘积 |
| 运行长度 | `--events`、`--time` | 事件数／秒数；0 代表该项不限制 |
| 并发限速 | `--threads`、`--rate` | 工作线程数与目标总事件速率 |
| 分布 | `--rand-type`、`--rand-seed` | uniform/gaussian/special/pareto/zipfian；seed=0 使用时间 |
| 查询组合 | `--point-selects`、各 `--*-ranges`、`--index-updates`、`--non-index-updates`、`--delete-inserts` | 每事件执行次数，不是各类操作概率 |
| 范围 | `--range-size` | SQL BETWEEN 的闭区间宽度 |
| 事务 | `--skip-trx` | 默认 false，显式 BEGIN/COMMIT |
| 建表 | `--auto-inc`、`--secondary`、`--create-secondary` | 主键生成与索引设置；PK 测试保持 secondary=false |

默认表有 `id`、`k`、`c CHAR(120)`、`pad CHAR(60)`；`id` 为 PK，默认另建 `k` 索引。`--secondary` 是用普通索引替换原主键约束，不是“开启额外 k 索引”。默认字段长度由 Lua 固定，并无通用 value-size 参数；SIBench 模拟值大小应在后续投影层独立配置。

标准 `oltp_read_write` 每事件默认执行 10 次点查、4 次范围读、2 次 update、1 次 delete 和1次 insert，另有 BEGIN/COMMIT：共20条 SQL。原生 event/transaction 数不能等同为索引操作数。

## 索引操作解释

- `oltp_update_index` 更新的是 `k=k+1 WHERE id=?`，改变二级索引列；`oltp_update_non_index` 更新 `c WHERE id=?`。两者在第一版都投影为 PK→模拟 value 更新，保留原始标签以免误称二级索引维护测试。
- `execute_delete_inserts` 删除某 id 后重新插入同 id，通常不扩大初始键空间。独立 `oltp_insert` 在 PostgreSQL 默认自增模式下由数据库生成 id，客户端 SQL 不含该键，采集需获取生成键。
- 范围为 `[id, id+range_size-1]`，上界可能超过现有数据最大 id；记录请求端点，实际行数另计。
- SUM、DISTINCT、ORDER BY 查询可保留 PK 区间访问意图，但 PK 扫描投影不重现聚合和排序计算成本。
- 随机 seed 可控制原生 RNG；多线程调度、重试及数据库反馈仍不保证全局请求顺序一致。

采集点为 `oltp_common.lua` 的 `execute_*`、绑定参数设置，以及 `sysbench.sql` 执行/错误钩子；必须保留 BEGIN/COMMIT、SQL 错误、重连与事件重试，不只在随机数生成处记一次 key。

## 源码依据

- [oltp_common.lua](../../benchmark/sysbench/src/lua/oltp_common.lua)：配置、建表、绑定参数、执行函数。
- [oltp_read_write.lua](../../benchmark/sysbench/src/lua/oltp_read_write.lua)、[oltp_insert.lua](../../benchmark/sysbench/src/lua/oltp_insert.lua)、[oltp_delete.lua](../../benchmark/sysbench/src/lua/oltp_delete.lua)。
- [sb_options.c](../../benchmark/sysbench/src/sb_options.c)：`read_config_file`、`sb_register_arg_set`；[sysbench.c](../../benchmark/sysbench/src/sysbench.c)：运行、限速与统计。
- [原生 README](../../benchmark/sysbench/README.md)。
