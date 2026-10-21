# YCSB / sysbench 原生采集验证

更新：2026-10-21。YCSB 和 sysbench 与 HyBench SF1 采用相同的基线行数（8,706,000 行），在原生客户端、插桩 PG14.24 上完成单会话采集，并新增访问账本。这是流程与审计验证，不是规范 Trace、性能或多会话结论。

## 范围

| 项目 | YCSB | sysbench |
| --- | --- | --- |
| 固定源码 | `66302f30…`，JDBC binding | `3ceba0b1…`，Lua 脚本 |
| 客户端 | 构建副本中的原生 `site.ycsb.Client`，每个 workload 一个独立 JVM；`JdbcDBCreateTable` 建表，原生 `-load` 以 16 线程加载（未采集） | 构建副本中的原生 `sysbench` 二进制和未修改 Lua，每个脚本一个独立进程；原生 `prepare` 以 8 线程并行建表加载（未采集） |
| 基线 | 8,706,000 行，10 × 100 字节字段；PG 表加主键索引约 10.2 GB，Parquet 约 7.1 GB（随机字段不可压缩） | 8 表 × 1,088,250 行 = 8,706,000 行，每表 `k_N` 二级索引；PG 约 2.2 GB，Parquet 约 0.84 GB |
| 负载 | workload A、B、C、F、D、E 依次作用于同一张增长的表；操作数 60,000 / 100,000 / 50,000 / 40,000 / 60,000 / 20,000 | `oltp_read_write`、`oltp_update_index`、`oltp_update_non_index`、`oltp_delete`、`oltp_insert`、`oltp_point_select`；event 数 8,000 / 10,000 / 10,000 / 5,000 / 5,000 / 20,000，`--rand-seed` 固定 |
| 采集会话 | 6 个，单线程 | 6 个，单线程 |
| 配置 | [ycsb_8m.yaml](../../config/ycsb_8m.yaml) | [sysbench_8m.yaml](../../config/sysbench_8m.yaml) |

行数与 HyBench SF1 对齐（8,706,000），索引条目数分别约为 870 万（YCSB，仅主键）、1740 万（sysbench）、3121 万（HyBench）。**索引条目数不相等**：HyBench 有 13 个二级索引，YCSB 没有，因此"基线规模相同"指行数，不指索引量或字节量。[ycsb_small.yaml](../../config/ycsb_small.yaml)（1,000 行）和 [sysbench_small.yaml](../../config/sysbench_small.yaml)（2 × 1,000 行）保留为约 15 秒的冒烟配置，用来快速回归各阶段，不作为验证证据。

执行入口 [native-capture.sh](../../scripts/native-capture.sh) 只启停本地 PG 并串联阶段，语义在 Java CLI：`prepare`（新库、原生建表加载、行变化触发器、`VACUUM (ANALYZE)`）→ `check`（来源 schema 预检）→ `export`（Baseline 导出并独立核验）→ `run`（原生执行、客户端与服务端对账）→ `audit-rows`（源行状态审计）→ `access-ledger`。数据库或产物目录已存在即拒绝，不复用。构建副本中参与执行的上游源码须与固定子模块逐字节一致，否则在建库前拒绝。证据在 `results/poc/ycsb-8m/` 和 `results/poc/sysbench-8m/`。

## 采集对账

两个来源的客户端形态不同，对账依据也不同；任何一项不一致都使采集失败，不生成成功清单。

| 检查 | YCSB（有 JDBC 记录层） | sysbench（libpq，无 JDBC 层） |
| --- | --- | --- |
| 请求身份 | JDBC 与 PG 请求逐条配对，SQL、绑定参数、客户端消费行与服务端结果核对 | 服务端请求序号连续，执行返回、结果行与事务边界自洽 |
| 客户端自报数量 | Operations 与 JDBC 请求数；所有 `Return=` 必须是 OK | `read + write + other − 控制语句` 与服务端执行数；`transactions` 与服务端提交数 |
| 提交 | autocommit 连接：每个请求 2 次提交（见下），逐请求核对 | 每 event 一次；会话前的预备语句提交单独计数 |
| 基线 | 22 个 Parquet 文件，8,706,000 行全部与 PG 逐值比较通过 | 8 个表的全部行同样通过 |
| 行状态 | `RowStateAudit` 通过 | `RowStateAudit` 通过 |

| 来源 | 请求 / 执行 | 提交行修改 | 修改键 | 行数变化 |
| --- | ---: | ---: | ---: | --- |
| YCSB | 349,959 个 JDBC 请求（A 60,000、B 100,000、C 50,000、F 59,959、D 60,000、E 20,000） | 58,802 | 41,280 | 8,706,000 → 8,709,986 |
| sysbench | 194,000 次服务端执行，58,000 次提交 | 61,999 | 8 表各 7.5k–7.9k 次修改 | 各表 −51 … +66，合计 8,706,000 → 8,706,001 |

YCSB 的 workload F 声明 40,000 次操作，其中 19,959 次 read-modify-write 各含一次读和一次更新，语句数为 59,959；YCSB 的键选择未固定随机种子，重复运行的数量会不同。

sysbench 的数量核对依据来自固定源码：pgsql 驱动仅在修改影响至少一行时计 `write`，否则计 `other`；预备的 `BEGIN` / `COMMIT` 也计 `other`，但属于不进入执行器的实用语句。因此 `other` 被拆为两部分并分别核对：控制语句数由固定 Lua 脚本是否使用显式事务推出（推断，不是服务端观察），影响零行的修改数来自服务端执行返回。`oltp_delete` 的 5,000 次中有 1 次命中已删除的 id，两边独立得出 1。

## 访问账本

`access-ledger` 把原始 PG 索引事件划分为访问单元：一次索引重扫对应一个单元，嵌套循环内表的每次重扫各是一个单元。每个原始事件恰好归入访问单元、写维护、生命周期、事务边界、结果行、行变化或排除类别之一；总数必须与输入事件数相等，否则失败。候选条目、堆可见行、SQL 返回行是同一访问的三个属性，从不相加。

| | YCSB | sysbench |
| --- | ---: | ---: |
| 原始事件 | 7,037,963 | 10,756,510 |
| 访问单元 | 346,525 | 182,893 |
| 单列等值 | 327,018 | 149,000 |
| 范围（不投影） | 18,955 | 32,000 |
| 规划期端点探测（排除并单列） | 552 | 1,893 |
| 索引候选条目 | 1,295,062 | 3,365,017 |
| 堆可见行 | 1,285,090 | 3,345,293 |
| 无任何索引访问的请求 | 3,986（INSERT） | 13,000（INSERT） |
| 索引写尝试 | 43,479 | 94,178 |

打包校验是独立路径：`PgTraceBlocks` 的事件总数及按类型计数应与账本一致；小规模冒烟运行已核对相同（32,038 与 126,558 个事件），大规模运行尚未重复该核对。

### 在规模下才出现的现象

- **候选、可见行、返回行三者确实不同。** sysbench 的 `select sum(k) … between` 平均每个请求有约 100 个候选条目、约 100 个可见行，但只返回 1 行；若以返回行数当作索引命中数，会低估约 100 倍。YCSB workload E 的 scan 是 `order by 主键 … fetch first N rows`，候选（960,633）与返回行（958,072）几乎相等，因为 LIMIT 在索引扫描上提前结束。
- **候选多于可见行是已死版本的索引条目。** 点查模板上 sysbench 候选 13,651 对可见 12,397（同一表），YCSB 总计 1,295,062 对 1,285,090。
- **规划期探测只在个别请求出现。** YCSB 的 552 个探测全部在 workload E，约占其 18,955 次 scan 的 3%，其余 workload 为 0。PG 只在范围谓词的边界值落到统计信息之外时才读索引端点；E 会插入新键，因此与此相符，但本次没有逐条验证机制。
- **索引写尝试是物理写，不是逻辑键维护。** 每次更新产生的尝试数随页内空间变化：YCSB 为 0.72 次 / 更新（小规模 0.46），sysbench 为 1.89 次 / 更新（小规模 1.08，其中 `set c=…` 在小规模仅 0.17）。两个规模的差异还混有一个配置变化——大规模运行在加载后执行了 `VACUUM (ANALYZE)`，小规模运行只执行了 `ANALYZE`——所以不能把差异归因于规模或 HOT 比例，也不能用它估计下游的键维护次数；逻辑维护仍由行变化展开得到。
- **sysbench 的 `k_N` 二级索引没有任何读访问。** 标准脚本不按 `k` 查询，二级索引只有维护写。

## 采集插桩的变化

- PG 事件增加请求序号 `req` 和执行器嵌套深度 `depth`，使 SPI / 触发器内部查询归属外层请求；新增 `plan_scan` 事件（事件码 17，1–16 不变），把每个索引扫描描述符绑定到计划节点（index / index_only / bitmap）。补丁见 `src/main/native/postgres/node-attribution.patch`，由 Makefile 按序应用。
- 规划期的索引端点探测（`get_actual_variable_range`，`IS NOT NULL` 键）发生在 `executor_start` 之前，其快照不是 MVCC，旧代码读取 `xmin` 等字段会越界并使 PG 崩溃；现仅对 MVCC 快照输出这些字段，探测被识别并单列，不计作查询。
- 客户端对 autocommit 连接额外记录 `autocommit` 事件。采集用 `SET application_name` 给请求加标签，在 autocommit 连接上这条标签语句本身会提交一次，所以 YCSB 每个请求在服务端有 2 次提交，其中一次是空事务。**这是采集标签的副作用，不是来源负载。**
- 无 JDBC 层的原生客户端通过 `PGAPPNAME=sibench:…` 启用采集；其会话在清单中没有 `jdbc_file`，由服务端审计验证。外部客户端进程的采集失败写入 `capture-failure-<pid>.json`，控制进程据此拒绝。
- `PgBaselineExport` 改为由来源 schema 驱动（原仅 HyBench 八表），文本主键使用与 `PgBaselineVerify` 相同的 C 排序。回归：同一 HyBench SF1 数据库分别用原版导出器（旧 JAR）和新版导出，8 个 Parquet 文件与 `locators.tsv` 逐字节相同，`files.jsonl` 仅 JSON 键顺序不同（两者都序列化 `Map.of`，顺序每次运行不同）。证据在 `results/validation/native-small-captures-20261021/hybench-export-regression/`。注意该数据库是采集后状态，不是原始基线。

## 验证结果

- JDK 17 完整构建 113 项测试，零失败、零错误；精简重复测试后，逐测试覆盖对比显示全套件覆盖的行 / 分支单元由 7,945 降至 7,936，减少的 9 个均为 `NativeCaptureConfig` 的默认值访问器。新增覆盖：账本划分与重复 / 孤立 / 未识别事件拒绝、嵌套循环重扫、bitmap 与计划绑定、语句失败后的扫描清理、无 `req` 追踪的拒绝、模板字面量归一化、服务端审计的提交 / 序号 / 重叠拒绝、sysbench 和 YCSB 输出解析、构建副本逐字节一致检查、autocommit 提交核对及绑定参数开关、`plan_scan` 打包解码，以及单会话编译器接受 `plan_scan`（移除该允许项后测试失败，已验证）、两个 8m 配置的行数与 HyBench SF1 一致（改动 sysbench 表大小后测试失败，已验证）。
- 重建后的 PG 上，`RowStateAuditIntegrationIT` 的 9 项检查全部通过。该测试使用受控 SQL，不是原生客户端执行。
- 既有 SF1 TP 子集追踪（2,177,565 事件，146,945 个访问单元，全部为单列等值）和 SF1 HTAP 追踪（51,111,329 事件，4 个会话，2,803,419 个访问单元）经同一划分通过，事件数与既有打包结论一致；这两个追踪采集于请求序号加入之前，当时用应用标签识别请求；该兼容路径随后已移除，账本现在要求 `req` / `depth`，因此这两份报告只作为已记录的历史证据，不能用当前代码复现。报告在 `results/validation/native-small-captures-20261021/legacy-ledgers/`。
- SF1 HTAP 的账本给出了完整访问投影还要覆盖的范围：2,803,383 个单列等值之外，有 3 个范围、3 个数组 / 行比较和 30 个无键扫描单元；8 个扫描在语句失败后随事务中止结束；242 个 bitmap 扫描单元的候选为 TID，不含可见性。
- 失败运行保留在 `results/validation/native-small-captures-20261021/superseded/`：YCSB workload E 在固定客户端上因 `readallfields=true` 导致 scan 结果为空而除零退出，改为该 workload 文档化的原生属性 `readallfields=false`（偏离上游 workload 文件，记录在运行清单）；sysbench 的两次对账失败分别暴露预备语句提交和零行删除的计数规则；其余是修复事件兼容性之前的两次小规模成功运行。

## 不代表

- 不是规范 Trace：单会话编译器仍只针对 HyBench 固定 DDL，没有 YCSB 字符串主键或 sysbench 表的投影，没有范围、变长键保序编码。账本的"单列等值"只描述谓词形态，不表示可编译。
- 不是性能结论：单线程；`shared_buffers` 为 256 MB，YCSB 表装不进缓存；没有真实 Backend。
- 访问账本证明所有已采集事件被恰好归属一次，不证明没有事件丢失；跨会话全局丢事件检测、residual filter 和 recheck 剔除的逐节点归因仍未完成。
- 唯一性检查发生在索引 `aminsert` 内部，不经过扫描钩子，只作为写维护尝试计入，没有单独观察。
- 多线程 / 多会话下的采集未做；`threads` 配置目前只接受 1，加载线程数不受此限。
- 未验证 sysbench 的 prepared statement 是否在执行若干次后从定制计划切换到通用计划。

## 复现

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
mvn package
make -C src/main/native/postgres install extension   # 重建并安装插桩 PG 与触发器库
scripts/native-capture.sh config/ycsb_8m.yaml        # 本次约 48 分钟（加载 12 分钟）
scripts/native-capture.sh config/sysbench_8m.yaml    # 本次约 13 分钟
java -jar build/sibench/sibench.jar access-ledger config/ycsb_8m.yaml results/poc/ycsb-8m/raw REPORT
```

每次运行需要新的数据库和 `results/poc` 子目录；已有的会被拒绝，失败后遗留的数据库需手工删除。行状态审计使用 `-Xmx12g`（`SIBENCH_HEAP` 可覆盖）。`access-ledger` 的 REPORT 必须是新文件，不读取 PG。
