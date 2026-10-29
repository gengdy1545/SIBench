# sysbench 数据来源与适配

固定源码提交：`3ceba0b1e115f8c50d1d045a4574d8ed643bd497`。上游位于 `benchmark/sysbench/`，由生成前检查核对提交、定义文件摘要及干净工作区。

## 当前入口

原生采集使用 [sysbench_87m.yaml](../../config/sysbench_87m.yaml)：8 表 × 10,882,500 行，共 87,060,000 行，8 线程运行六个固定 oltp 脚本。采集入口与完整性职责见 [原生采集验证](../validation/capture-completeness.md)。

[sysbench_large.yaml](../../config/sysbench_large.yaml) 是另一个 Java 派生 Baseline 入口：

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/sysbench_large.yaml
```

去掉 `--dry-run` 会实际生成数据。参数选择和产物格式见 [README](../../README.md#baseline-生成配置)。

## 数据规模与内容

- 100 张表，每表 10000000 行，共 1000000000 行；每表主键保持在 int32 范围内。
- 保留 id、k、c、pad；c/pad 分别生成 119/59 个 ASCII 字符，原生 SQL 列宽为 CHAR(120)/CHAR(60)。
- 仅生成文本共 178 GB（十进制），不含整数列、编码和存储开销，不是实际 Parquet 大小。
- 显式使用 uniform 分布与固定 generation.seed，rand-seed 必须与之相同。Java 使用按记录寻址的派生随机流，不执行 SQL CHAR 补齐、数据库建表或二级索引创建。
- 表数影响命名空间与访问分布，必须在可比实验间固定；该档位不宣称是上游统一标准。

## 增量适配边界

来源 catalog 按配置表数覆盖 id 主键及全部 k 二级索引；源布局固定 `secondary=false`、`create-secondary=true`，支持 `auto-inc=true/false`，与下游二级发送开关独立。采集与导出前执行只读 PG 预检，见 [来源 Schema 契约](../contracts/source-schema.md)。

原生 oltp_read_write 的 event 包含多条查询和修改，event 数不等于索引操作数。范围为 BETWEEN 闭区间，可能超出已有键范围；聚合与排序不能计入索引查询时间。index update 修改 k，non-index update 修改 c；两者必须保留来源标签。delete-insert 删除后重新插入同键，需要保留顺序及身份规则。

Java `prepare` 只生成派生 Baseline，10 亿行档位容量尚未验收。真实采集使用原生 libpq prepare / run，oltp_read_write / update_index / update_non_index / delete / insert / point_select 依次运行，各脚本内部多会话按 PG 事务完成计数器归并。服务端对账保留脚本实际调用与 `--skip-trx` 控制边界，另完成 Baseline 核验、行状态审计和访问账本。规范 Trace 中点查与 DELETE / UPDATE 的定位为主键点查，`BETWEEN`（含 `sum`、`ORDER BY c`、`DISTINCT`）为主键闭区间，返回行与可见行分别保留；`k` 索引列 UPDATE、`c` 普通列 UPDATE、DELETE / 重插入展开为完整维护。代表输入的完整结果见 [三来源验收](../validation/acceptance.md)。标准脚本不按 `k` 查询，二级索引只有维护写。

## 源码入口

[公共 OLTP 逻辑](../../benchmark/sysbench/src/lua/oltp_common.lua)、[混合读写](../../benchmark/sysbench/src/lua/oltp_read_write.lua)、[插入](../../benchmark/sysbench/src/lua/oltp_insert.lua)、[删除](../../benchmark/sysbench/src/lua/oltp_delete.lua)。
