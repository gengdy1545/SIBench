# sysbench 数据来源与适配

固定源码提交：`3ceba0b1e115f8c50d1d045a4574d8ed643bd497`。上游位于 `benchmark/sysbench/`，由生成前检查核对提交、定义文件摘要及干净工作区。

## 当前入口

使用 [sysbench_large.yaml](../../config/sysbench_large.yaml) 配置 Java Baseline：

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

原生 oltp_read_write 的 event 包含多条查询和修改，event 数不等于索引操作数。范围为 BETWEEN 闭区间，可能超出已有键范围；聚合与排序不能计入索引查询时间。index update 修改 k，non-index update 修改 c；两者必须保留来源标签。delete-insert 删除后重新插入同键，需要保留顺序及身份规则。

当前只实现派生 Baseline；该来源产物与容量尚未验证，原生增量采集与索引回放待实现。

## 源码入口

[公共 OLTP 逻辑](../../benchmark/sysbench/src/lua/oltp_common.lua)、[混合读写](../../benchmark/sysbench/src/lua/oltp_read_write.lua)、[插入](../../benchmark/sysbench/src/lua/oltp_insert.lua)、[删除](../../benchmark/sysbench/src/lua/oltp_delete.lua)。
