# YCSB / sysbench 原生 Trace 验证

YCSB JDBC 与 sysbench PG 共用原生采集、Baseline 核验、行审计、账本、PG 计数器及 Trace 编译入口；读取与版本语义见 [Trace 契约](../contracts/trace.md)。原生客户端和负载脚本不被手工 SQL 替换。

## 来源与核验职责

| 项目 | YCSB | sysbench |
| --- | --- | --- |
| 原生负载 | JDBC A/B/C/F/D/E；read-modify-write 保留为非原子访问 | 六个固定 oltp 脚本，保留 event 内多条查询和修改 |
| 索引布局 | 字符串主键，无二级索引 | 每表 id 主键及 k 二级索引 |
| 请求核验 | JDBC / PG 身份、SQL、绑定参数、消费行及客户端返回值对账 | 服务端请求序号、返回行及事务边界，与原生 read / write / other / transactions 计数对账 |
| 行状态 | Baseline 全值比较、提交变化链及 PG 最终状态 | 同左，区分 k 索引列 UPDATE、c 普通列 UPDATE、DELETE / 同 PK 重插入 |
| 访问投影 | 主键点查和带 limit 的升序区间 | BETWEEN 闭区间、点查与 k 维护；聚合 / 排序不计为 KV 执行成本 |

YCSB workload E 在固定 JDBC binding 上使用原生属性 readallfields=false，避免 readallfields=true 的空 scan / 除零行为，配置随运行保存。YCSB autocommit 的业务请求每次推进一个版本，采集标签提交单列；sysbench 每 event 提交，连接预备语句提交单列。原生失败的已验证分类边界见契约，未分类错误拒绝。

## 选用证据

带 PG 计数器的 8,706,000 行输入分别在 `results/poc/ycsb-8m/` 与 `results/poc/sysbench-8m/`，日志在 `results/poc/native-trace-20261023/`；计数器对照、区间 oracle 和落盘回归见 [采集完整性](capture-completeness.md)。这组单线程、顺序会话的样本不代表多会话性能。

| 核对事实 | YCSB | sysbench |
| --- | ---: | ---: |
| 规范查询 | 345,827 | 181,000 |
| 推进版本的已提交事务 | 349,888 | 58,000 |
| 业务行修改 | 58,935 | 61,999 |
| 计数器与账本 | consistent | consistent |

YCSB 的采集行审计包含标签事务，其提交计数不直接作为规范版本数。sysbench 标准脚本不按 k 查询；k 二级主要承担维护写，不能由索引数量推断查询形态覆盖。Bitmap / 数组及二级提前停止由 [HyBench](hybench-sf1-htap.md) 覆盖。

并发原生覆盖见 [并发采集](concurrent-native-capture.md)；87m 代表规模的参考、独立 oracle、本地服务和 COS 结果分别见 [完整验收](p0-three-source-acceptance.md)、[对象存储验收](rocksdb-cloud-scale.md)。单个样本的计数不外推到其他采集身份。

## 复现入口

```bash
rtk proxy scripts/native-capture.sh config/sysbench_8m.yaml
rtk proxy scripts/native-capture.sh config/ycsb_8m.yaml
rtk proxy java -Xmx24g -jar build/sibench/sibench.jar validate-replay-matrix TRACE OUTPUT_DIRECTORY
rtk proxy java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml TRACE REPORT
```

静止库、依赖和阶段命令见 [README](../../README.md)。新运行使用独立数据库和目录；参考矩阵 / oracle 是进程内语义验证，实际服务吞吐和恢复须有单独证据。
