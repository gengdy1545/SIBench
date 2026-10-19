# YCSB 数据来源与适配

固定源码提交：`66302f301b13f60d4bcb2f29f478586bb1d6f2e0`。上游位于 `benchmark/YCSB/`，由生成前检查核对提交、定义文件摘要及干净工作区。

## 当前入口

使用 [large.yaml](../../config/workloads/ycsb/large.yaml) 配置 Java Baseline：

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/workloads/ycsb/large.yaml
```

去掉 `--dry-run` 会实际生成数据。参数选择和产物格式见 [README](../../README.md#baseline-生成配置)。

## 数据规模与内容

- recordcount 与 insertcount 均为 1000000000，insertstart 为 0。
- 保留 10 个字段、每字段 100 字节，字段长度分布为 constant。
- 字段内容合计 1 TB（十进制），不含键、编码和存储开销，不是 Parquet 落盘大小估计。
- 使用原生 FNV64 hashed key 与 zeropadding 规则；字符串排序不等于源序号顺序。
- 字段使用原生字节掩码分布，但随机流由主项目确定性派生生成器提供；不声称输出与原生随机数据逐字节相同。
- 10 亿条为项目的大规模档位，不是上游统一标准，也不要求与其他 benchmark 精确配平。

## 增量适配边界

原生 A–F 的 read、update、insert、scan、read-modify-write 比例及分布需要在增量实现中保留或明确标为派生负载。标准 A–F 不产生 delete。scan 使用起始键与数量上限，不能将结束键推导为数值起点加数量。read-modify-write 是多个 API 调用，不能默认具有事务原子性。

当前只实现 Baseline 入口，实际 Parquet 写入尚未验证，增量 Trace 与索引回放待实现。

## 源码入口

[CoreWorkload](../../benchmark/YCSB/core/src/main/java/site/ycsb/workloads/CoreWorkload.java)、[键散列](../../benchmark/YCSB/core/src/main/java/site/ycsb/Utils.java)、[随机字段](../../benchmark/YCSB/core/src/main/java/site/ycsb/RandomByteIterator.java)、[原生负载配置](../../benchmark/YCSB/workloads)。
