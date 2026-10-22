# YCSB / sysbench 原生 Trace 验证

更新：2026-10-22。YCSB 和 sysbench 在 8,706,000 行基线上从原生客户端执行一路得到 v8 规范 Trace：插桩 PG 采集 → 请求对账 → Baseline 逐值核验 → 行状态审计 → 访问账本 → 编译 → 参考回放和独立 Parquet oracle。全部访问单元均被投影，没有拒绝项。这是正确性与可复现性验证，不是性能结论。

## 范围

| 项目 | YCSB | sysbench |
| --- | --- | --- |
| 原生负载 | workload A、B、C、F、D、E 依次运行，60,000 / 100,000 / 50,000 / 40,000 / 60,000 / 20,000 次操作 | `oltp_read_write`、`oltp_update_index`、`oltp_update_non_index`、`oltp_delete`、`oltp_insert`、`oltp_point_select`，8,000 / 10,000 / 10,000 / 5,000 / 5,000 / 20,000 个 event |
| 会话 | 6 个，单线程，按 workload 顺序 | 6 个，单线程，按脚本顺序 |
| 基线 | 8,706,000 行；Trace 基线 8,706,000 条主键 INSERT | 8 表 × 1,088,250 行；17,412,000 条主键 + `k` INSERT |
| 配置 | 与 [ycsb_8m.yaml](../../config/ycsb_8m.yaml) 相同，仅产物目录与数据库名改为本轮 | 与 [sysbench_8m.yaml](../../config/sysbench_8m.yaml) 相同，同上 |

入口为 [native-capture.sh](../../scripts/native-capture.sh)，在原有阶段后增加 `compile-trace`；采集服务以 `compute_query_id = on` 启动，`run` 前确认该设置。本轮使用新数据库（`sibench_<来源>_<规模>_trace`）和新产物目录 `results/poc/native-trace-20261022/`（含所用 profile 副本），不复用、不删除已有采集库或证据。`config/*_small.yaml` 的同流程冒烟运行也在该目录。

## 采集对账

两个来源的客户端形态不同，对账依据也不同；任何一项不一致都使采集失败，不生成成功清单。

| 检查 | YCSB（有 JDBC 记录层） | sysbench（libpq，无 JDBC 层） |
| --- | --- | --- |
| 请求身份 | JDBC 与 PG 请求逐条配对，SQL、绑定参数、客户端消费行与服务端结果核对 | 服务端请求序号连续，执行返回、结果行与事务边界自洽 |
| 客户端自报数量 | Operations 与 JDBC 请求数；所有 `Return=` 必须是 OK | `read + write + other − 控制语句` 与服务端执行数；`transactions` 与服务端提交数 |
| 提交 | autocommit：每个请求在服务端有标签与请求两次提交，逐请求核对 | 每 event 一次；会话前的预备语句提交单独计数 |
| 会话顺序 | 每个 workload 一个 JVM，退出且无采集后端残留后启动下一个 | 每个脚本一个进程，同上 |
| 基线 / 行状态 | Parquet 全部行与 PG 逐值比较；`RowStateAudit` 通过 | 同左 |

workload E 在固定 JDBC binding 上 `readallfields=true` 会使 scan 结果为空并除零退出，采集对 E 使用文档化的原生属性 `readallfields=false`，记录在运行清单。`index_insert_attempt` 是物理写尝试，每次更新的次数随页内空间变化，不等于逻辑键维护；sysbench 标准脚本不按 `k` 查询，`k_N` 只有维护写。

## 访问投影

| 指标 | YCSB | sysbench |
| --- | ---: | ---: |
| 顶层请求 | 350,046 | 194,000 |
| 无索引访问的请求 | 3,940（INSERT） | 13,000（INSERT） |
| 投影：点查 | 327,076 | 149,000 |
| 投影：区间 | 0 | 32,000 |
| 投影：带 limit 区间 | 19,030 | 0 |
| 拒绝 | 0 | 0 |
| 排除：规划期端点探测 | 500 | 1,871 |
| 模板（有索引访问） | 12 | 64 |
| `plan_variants > 1` 的模板 | 0 | 0 |

- YCSB workload E 的 `ycsb_key >= $1 ORDER BY ycsb_key FETCH FIRST $2` 投影为主键升序区间、limit 等于已消费的可见行：候选 966,805、可见 964,120、返回 964,120。小规模冒烟中有一个 E 请求走 seq scan（账本显示该模板两种计划序列、1 个请求无访问单元），Trace 中不为它生成索引查询。
- sysbench 的 `BETWEEN`（含 `sum(k)`、`ORDER BY c`、`DISTINCT`）投影为主键闭区间，期望数取可见行：例如 sbtest1 的 `sum(k)` 可见 95,600、返回仅 956。
- 候选多于可见行是死版本的索引条目：YCSB 1,301,245 对 1,291,196，sysbench 3,364,995 对 3,345,277；Trace 使用可见行，参考模型从已提交写入得到相同数量。
- 本轮规模下两来源都只出现普通 index scan；bitmap 与 index-only 的可见性事件只在受控测试中覆盖。

## 版本与转换

| 指标 | YCSB | sysbench |
| --- | ---: | ---: |
| 提交事务（推进版本） | 350,046 | 58,000 |
| 不执行请求的事务（不分配版本） | 350,046 | 112 |
| 行版本写入（INSERT / UPDATE / DELETE） | 58,862（3,940 / 54,922 / 0） | 61,999（13,000 / 36,000 / 12,999） |
| 展开后的索引写入 | 58,862 | 141,998 |
| 事务内写后查询 | 0 | 16,000 |
| Trace 运行期记录（含控制） | 1,105,060 | 438,998 |

YCSB 的 autocommit 每个请求恰好推进一个版本；采集标签 `SET application_name` 的 350,046 个提交单列，不进入 Trace。sysbench 的 112 个是连接建立时的预备语句提交。sysbench 有一次 DELETE 影响 0 行（原生行为，不产生行变化），所以 DELETE 比 INSERT 少一次。

## 回放与 oracle

| 检查 | YCSB | sysbench |
| --- | --- | --- |
| `replay`，1 / 64 / 128 client，Locator 与 RowID | 通过；346,106 个查询、1,291,196 个结果项；三种 client 数的发送顺序校验和相同 | 128 client 通过；181,000 个查询、3,343,991 个结果项 |
| `validate-snapshots`（独立读取 Parquet） | 通过；41,611 个修改主键、200,902 个历史主键探针 | 通过；48,751 个修改主键、205,497 个主键探针、512 个 `k` 谓词共 322,255 个二级探针 |
| 重复编译 | 最终 JAR 对四次采集重编译，Trace、工作单元和增量 Parquet 与采集脚本产物逐字节相同；manifest 内容相同、字节一致性由最终 JAR 的重复编译确认 | 同左 |

小规模冒烟（YCSB 1,479 个查询；sysbench 2,050 个查询）同样通过 1 / 64 / 128 client 回放，sysbench 通过 oracle。

## 不代表

- 不是性能结论：单线程、`shared_buffers` 256 MB、进程内参考模型，没有真实 Backend。
- 不是并发结论：会话依次运行；HTAP 等并发会话仍被编译器拒绝。
- 二级索引上被提前停止的扫描（重复值的 heap TID 顺序与 KV 的 PK 顺序不同）、BitmapAnd/Or、非 C collation 的文本区间仍显式拒绝；本轮负载未出现。
- 账本证明事件被恰好归属一次，不证明没有事件丢失。
- HyBench 见 [TP 子集 v8 验证](hybench-sf1-tp-trace.md)。本轮 YCSB / sysbench 采集时的 bitmap 堆事件为后来替换的旧形式；两者在规模下均无 bitmap 单元，Trace 不受影响。

## 复现

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
make -C src/main/native/postgres install extension
mvn package
scripts/native-capture.sh results/poc/native-trace-20261022/sysbench_8m.yaml   # 约 16 分钟
scripts/native-capture.sh results/poc/native-trace-20261022/ycsb_8m.yaml       # 约 50 分钟
java -Xmx24g -jar build/sibench/sibench.jar replay config/index_replay_128clients.yaml \
  results/poc/native-trace-20261022/ycsb-8m/trace REPORT
java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml \
  results/poc/native-trace-20261022/sysbench-8m/trace REPORT
```

数据库或产物目录已存在时脚本拒绝；重跑使用新的目录和数据库名。
