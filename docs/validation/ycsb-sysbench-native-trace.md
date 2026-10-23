# YCSB / sysbench 原生 Trace 验证

更新：2026-10-23。YCSB 和 sysbench 在 8,706,000 行基线上从原生客户端执行一路得到 v9 规范 Trace：插桩 PG 采集 → 请求对账 → Baseline 逐值核验 → 行状态审计 → 访问账本 → 编译 → 参考回放和独立 Parquet oracle。全部访问单元均被投影，没有拒绝项。这是正确性与可复现性验证，不是性能结论。

## 范围

| 项目 | YCSB | sysbench |
| --- | --- | --- |
| 原生负载 | workload A、B、C、F、D、E 依次运行，60,000 / 100,000 / 50,000 / 40,000 / 60,000 / 20,000 次操作 | `oltp_read_write`、`oltp_update_index`、`oltp_update_non_index`、`oltp_delete`、`oltp_insert`、`oltp_point_select`，8,000 / 10,000 / 10,000 / 5,000 / 5,000 / 20,000 个 event |
| 会话 | 6 个，单线程，按 workload 顺序 | 6 个，单线程，按脚本顺序 |
| 基线 | 8,706,000 行；Trace 基线 8,706,000 条主键 INSERT | 8 表 × 1,088,250 行；17,412,000 条主键 + `k` INSERT |
| 配置 | [ycsb_8m.yaml](../../config/ycsb_8m.yaml) | [sysbench_8m.yaml](../../config/sysbench_8m.yaml) |
| 产物 | `results/poc/ycsb-8m/` | `results/poc/sysbench-8m/` |

入口为 [native-capture.sh](../../scripts/native-capture.sh)；采集服务以 `compute_query_id = on` 启动，`run` 前确认该设置。每阶段耗时写入产物目录的 `run-timing.json`。

## 采集对账

两个来源的客户端形态不同，对账依据也不同；任何一项不一致都使采集失败，不生成成功清单。

| 检查 | YCSB（有 JDBC 记录层） | sysbench（libpq，无 JDBC 层） |
| --- | --- | --- |
| 请求身份 | JDBC 与 PG 请求逐条配对，SQL、绑定参数、客户端消费行与服务端结果核对 | 服务端请求序号连续，执行返回、结果行与事务边界自洽 |
| 客户端自报数量 | Operations 与 JDBC 请求数；非 OK 返回按来源失败分类，未分类即失败 | `read + write + other − 控制语句` 与服务端执行数；`transactions` 与服务端提交数 |
| 控制语句 / 声明 | workload 文件声明的比例、请求分布、scan 上限与字段读写和实测对账；read-modify-write 记为非原子 | 控制语句数由脚本实际 `begin()` / `commit()` 与运行时 `--skip-trx` 推出，并与服务端残差对账 |
| 提交 | autocommit：每个请求在服务端有标签与请求两次提交，逐请求核对 | 每 event 一次；会话前的预备语句提交单独计数 |
| 会话顺序 | 每个 workload 一个 JVM，退出且无采集后端残留后启动下一个 | 每个脚本一个进程，同上 |
| 基线 / 行状态 | Parquet 全部行与 PG 逐值比较；`RowStateAudit` 通过 | 同左 |

workload E 在固定 JDBC binding 上 `readallfields=true` 会使 scan 结果为空并除零退出，采集对 E 使用文档化的原生属性 `readallfields=false`，记录在运行清单。`index_insert_attempt` 是物理写尝试，每次更新的次数随页内空间变化，不等于逻辑键维护；sysbench 标准脚本不按 `k` 查询，`k_N` 只有维护写。`ChangeKinds` 在 sysbench 上区分 `oltp_update_index` 的 `k` 索引列 UPDATE、`oltp_update_non_index` 的 `c` 普通列 UPDATE，以及 `oltp_read_write` 的 DELETE 后重插入。

## 阶段耗时

| 阶段 | sysbench | YCSB |
| --- | ---: | ---: |
| prepare | 38 s | 756 s |
| export | 302 s | 1370 s |
| run | 236 s | 304 s |
| audit-rows | 186 s | 529 s |
| access-ledger | 52 s | 40 s |
| compile-trace | 49 s | 96 s |

## 访问投影

| 指标 | YCSB | sysbench |
| --- | ---: | ---: |
| 顶层请求 | 349,990 | 194,000 |
| 无索引访问的请求 | 3,895（INSERT） | 13,000（INSERT） |
| 投影：点查 | 327,077 | 149,000 |
| 投影：区间 | 0 | 32,000 |
| 投影：带 limit 区间 | 19,018 | 0 |
| 拒绝 | 0 | 0 |
| 排除：规划期端点探测 | 515 | 1,862 |
| 有索引访问的模板 | 12 | 64 |
| `plan_variants > 1` 的模板 | 0 | 0 |

- YCSB workload E 的 `ycsb_key >= $1 ORDER BY ycsb_key FETCH FIRST $2` 投影为主键升序区间、limit 等于已消费的可见行。
- sysbench 的 `BETWEEN`（含 `sum(k)`、`ORDER BY c`、`DISTINCT`）投影为主键闭区间，期望数取可见行而不是聚合后的返回行。
- 候选多于可见行是死版本的索引条目：YCSB 1,294,500 对 1,284,597，sysbench 3,364,986 对 3,345,280；Trace 使用可见行，参考模型从已提交写入得到相同数量。
- 两来源只出现普通 index scan，没有 bitmap、数组键或被提前停止的二级扫描；这些访问组形态由 [HTAP 验证](hybench-sf1-htap.md) 覆盖。

## 版本与转换

| 指标 | YCSB | sysbench |
| --- | ---: | ---: |
| 提交事务（推进版本） | 349,990 | 58,000 |
| 不执行请求的事务（不分配版本） | 349,990 | 112 |
| 行版本写入（INSERT / UPDATE / DELETE） | 58,668（3,895 / 54,773 / 0） | 61,999（13,000 / 36,000 / 12,999） |
| 展开后的索引写入 | 58,668 | 141,998 |
| 事务内写后查询 | 0 | 16,000 |
| Trace 运行期记录（含控制） | 1,104,743 | 438,998 |

YCSB 的 autocommit 每个请求恰好推进一个版本；采集标签 `SET application_name` 的提交单列，不进入 Trace。sysbench 的 112 个是连接建立时的预备语句提交。sysbench 有一次 DELETE 影响 0 行（原生行为，不产生行变化），所以 DELETE 比 INSERT 少一次。sysbench 以固定种子运行，结果在重复采集间完全一致；YCSB 客户端的随机选择不固定种子，重复采集的数量有几十个的差异。

## 回放与 oracle

| 检查 | YCSB | sysbench |
| --- | --- | --- |
| `replay`，1 / 64 / 128 client 与 transaction 模式，Locator 与 RowID | 通过；346,095 个查询 | 通过；181,000 个查询 |
| `validate-snapshots`（独立读取 Parquet） | 通过；41,534 个修改主键、200,369 个历史主键探针 | 通过；48,751 个修改主键、205,497 个主键探针、512 个 `k` 谓词共 322,255 个二级探针 |
| 同一输入重复编译 | 全部产物逐字节一致 | 同左 |

## 不代表

- 不是性能结论：单线程、进程内参考模型，没有真实 Backend。
- 不是多会话结论：两来源的客户端都是单线程，会话依次运行。
- oracle 的二级谓词只抽样等值（每索引最多 64 个）。
- 账本证明事件被恰好归属一次，不证明没有事件丢失。

## 复现

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
make -C src/main/native/postgres install extension
mvn package
scripts/native-capture.sh config/sysbench_8m.yaml   # 约 15 分钟
scripts/native-capture.sh config/ycsb_8m.yaml       # 约 60 分钟
java -Xmx24g -jar build/sibench/sibench.jar validate-replay-matrix results/poc/ycsb-8m/trace OUTPUT_DIRECTORY
java -Xmx24g -jar build/sibench/sibench.jar validate-snapshots config/index_replay.yaml results/poc/sysbench-8m/trace REPORT
```

数据库或产物目录已存在时脚本拒绝；重跑使用新的目录和数据库名。

## 产物迁移记录

- 2026-10-23：v8 产物 `results/poc/sysbench-8m`、`results/poc/ycsb-8m`、`results/poc/sysbench-small`（2026-10-22）移入 `results/poc/native-trace-20261022/superseded/` 下的 `*-20261022-v8`。
- 2026-10-22：为重新采集让出目录名，`results/poc/sysbench-8m` 与 `results/poc/ycsb-8m`（2026-10-21）移入 `results/poc/native-trace-20261022/superseded/` 下的 `sysbench-8m-20261021`、`ycsb-8m-20261021`；`results/poc/sysbench-small`（2026-10-21）移为 `results/poc/sysbench-small-20261021`。仅改名，内容未改写。
