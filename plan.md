# SIBench 实施计划

更新：2026-10-27。

## 目标

以原生 benchmark、真实 Parquet 和可复现索引 Trace，比较不同 KV Index Service 的加载、查询、版本写入及维护成本，不限定具体下游应用场景。

主流程：原生数据生成与 PostgreSQL 加载 → 静止边界导出及验证 Baseline → 原生负载采集与审计 → 规范 Trace / 增量 Parquet 编译 → 索引回放与测量。

**当前阶段**：三来源（含并发的完整 HyBench HTAP）已从原生执行端到端得到 v9 规范 Trace，全部访问投影，参考回放与独立 oracle 通过；通用 Index Service 协议与 RocksDB 参照实现已回放真实 Trace 并逐查询核对原生期望数。远程 32 并发、5% 写入的认证采集端到端约 21 / 10.5 / 36 min，SF10 的导出、编译与空间边界已验收，扩展模型见 [扩展模型](docs/validation/scaling-model.md)。下一步：接入真实服务与测量（第 1 节）待确认；SF100 及以上见第 4 节。

## 固定约定

| 领域 | 约定 |
| --- | --- |
| 来源真实性 | 固定上游提交、PG 环境和一次采集事实；保留原生选择器、比例、业务分支、计时及 SQL。不重跑 SQL 补结果，不模拟业务，不猜测修复 PG 内部实现。派生 Baseline 单独标识，不替代原生执行。 |
| 审计与失败 | 原始事实与规范操作分层保存，转换可追溯；不支持的语义显式拒绝。来源失败按固定源码分类为业务回滚、预期 SQL 失败、预期 API 失败、原生重试与非预期，只有非预期使采集无效：保留诊断，修复后从明确初始状态换新目录重跑，不拼接失败前缀。分类未经真实失败检验前，JDBC 异常仍使采集失败。最终状态正确不能替代访问负载审计。 |
| 核验职责 | 证明方法的检查做一次，证明单次运行的检查每次做。完整核验（`capture.verification: full`）由 `RowStateAudit` 核对源行修改链和最终 PG 状态，编译后 `certify-capture` 写出认证：采集构建（运行 jar 中采集侧代码的类闭包摘要、PG 二进制、触发器库、影响计划的 PG 参数）、来源与索引定义、采集格式，以及本次覆盖的访问路径（来源、索引、扫描种类、投影决定）和提交变化类别（表、操作）。认证采集（`certified`）跳过行审计，仍做会话审计、账本划分、PG 计数器核对与编译检查，采集构建或来源不同、出现认证未覆盖的路径或类别即编译失败；认证采集可以就是短时校准采集。采集侧代码是 prepare / run 阶段入口（采集驱动、各来源 run、HyBench prepare、`CaptureRun`）在 jar 内按常量池名字求得的类闭包，止于每次采集都会自行运行的检查（基线导出与核验、schema 预检、会话审计）和静止库构建；第三方库合为一个摘要。编译器不在其中：行审计证明的是原始采集，编译器的正确性由保留产物的逐字节重编与参考回放保证，所以只改编译器不使认证失效。身份在认证与编译时由产物中运行 jar 的副本（摘要须等于运行清单所记）求出，不依赖运行时代码。同一阶段内不重复上游已核验的结论：运行时的会话审计记入运行清单，之后只核对文件摘要。参考回放与快照 oracle 证明编译器的产物能被正确的索引精确重现，属方法层面，只在认证采集上运行；正式采集的 Trace 在真实索引回放中仍逐查询核对原生期望数，不一致时再以参考回放区分索引与 Trace。编译与维护诊断验证摘要并复用来源结论；转换产物验收只核对源事件到 Parquet / 索引操作的映射，Backend 回放只验证目标结果，不重复核验源行修改链。 |
| 产物与保留 | 最终产物只保留用不同 index 重复实验所必需的内容：规范负载 Trace、Locator 指向的数据文件（基线按静止库共享一份）与 manifest；能由它们推出的内容（Baseline 索引加载操作）不落盘，只为检查服务的内容（原始采集、行审计变化、导出核验用定位表、工作单元来源目录、各类报告）不进入产物，报告单独作为证据保存。原始采集在 Trace 通过全部检查后删除；编译器与 Trace 格式仍在改动的阶段（`baseline.ops` 不落盘、`workload.ops` 压缩之前），认证采集与正式采集的原始数据及其基线都保留，格式升级后直接重编而不必重采（SF10 5% 原始约 8.5 GB）。采集用的克隆库在采集结束后删除，只保留静止库。 |
| 提交与查询 | 默认只发送已提交变更；原始 Trace 保留修改尝试和回滚，不生成反向补偿或回撤开关。转换排除回滚修改及修改后的查询，保留修改前的受支持查询；已提交事务保留写后查询及原始操作位置，不合并修改。 |
| 时间戳与发送 | 同源事务一个 `CommitVersion`，查询携带独立 `ReadTimestamp` 和此前修改位置。`operation` / `transaction` 只决定驱动输入单位，最终均逐项发送；源控制记录不发送给索引，不要求下游事务 API。 |
| client 路由 | 默认 1 client，按源 SQL 请求工作单元确定性轮询，不按索引键或事务绑定；同请求的展开共用 client，同事务允许跨 client。同一 Trace 支持 1/64/128 及更多 client，过滤后必须覆盖全部配置 client，不伪造身份或重复负载填充。 |
| 顺序与可见性 | 一个全局线程保留事务间及事务内本地发送顺序；不保证不同连接的远端接收、执行或可见顺序。Index Service 负责跨 client 语义；发送确认与响应独立，在途上限、超时和排空不作为可见性屏障。并发会话的全局顺序来自 PostgreSQL 事务完成计数器：写事务在持 `ProcArrayLock` 自增时记录的精确位置，即它对其他快照可见的时刻；只读事务不自增，排在同位置写事务之后。查询读取其语句快照计数器所见的最后一个写事务，不按 PID、XID 大小、文件顺序或墙钟推断。 |
| 键与查询 | PK 与索引值为保序编码分量，字节序等于 PG btree 升序 NULLS LAST（文本限 C collation），可拼接为 KV 键。原生执行是确定的，每个访问投影为一组能重现原生结果集的有序查询，每个期望数都是原生事实：一个区间；`= ANY` 的并集组；BitmapAnd 的交集组；被提前停止的主键扫描带 limit，二级扫描为已读完的前段加边界值上被返回行的 (值, PK) 钉住点查。只在事实缺失或超出来源范围时带原因拒绝，账本与编译器共用同一规则。 |
| 索引与布局 | 主键和二级索引使用统一 KV 契约，二级默认全部启用，发送开关不改变源数据、完整 Trace 或 Parquet。索引列值 UPDATE 不是索引定义变更，也不使用业务 timestamp 作为 CommitVersion。Baseline 只 INSERT，增量追加写；Rewrite 不改变键、逻辑版本或 RowID。Locator 更新位置，RowID 不产生搬迁写入，不增加位置映射服务。 |
| 实验与测量 | 长时输入来自足量连续原生事件，`single_pass` 消费，不循环、重排或重映射键。各 Backend 复用相同数据、Trace、开关及确认语义；内部缓存、持久化和 compaction 由 Backend 负责。索引性能与 SQL、文件生成、Rewrite 数据读写、Lookup 后数据读取分别计量。 |

具体字段、失败处理和责任边界以 [Trace 契约](docs/contracts/trace.md)、[存储契约](docs/contracts/storage.md) 为准。

目标来源限定 HyBench、YCSB JDBC、sysbench PG 的固定标准布局：单列主键及来源实际定义的普通单列二级索引。不要求额外 UNIQUE、复合、表达式、partial 或动态 DDL。标准负载不改写已有行的主键值；PK 值 UPDATE 是固定范围外操作，提交及回滚尝试均拒绝，不列为待实现功能。普通列 / 二级索引列 UPDATE 及同主键 DELETE / 重插入仍在范围内，详见 [来源 Schema 契约](docs/contracts/source-schema.md)。

## 当前能力与范围

| 领域 | 当前实现及边界 |
| --- | --- |
| 工程与派生数据 | JDK 17 / Maven、职责目录、平铺配置、固定 benchmark 子模块；HyBench / YCSB / sysbench 的 Java Baseline、源码指纹校验和 dry-run。 |
| 来源 schema 与 PG 预检 | 统一 catalog 覆盖 HyBench 八表完整列及 8 PK / 13 二级、YCSB JDBC 字符串 PK / TEXT 字段、sysbench 配置表数及每表 id PK / k 二级。只读核对实际表 / 列 / 默认值 / 索引定义和有效性、约束及非限定名称解析；三来源采集 / 导出前强制检查并保存证据。 |
| 采集编排 | 三来源同一入口 `scripts/native-capture.sh`：静止库（`postgres.clone_from`）在单独的 PG 集群中只建一次（原生装载、HyBench 上游二级索引、触发器、`VACUUM (ANALYZE)`，之后停机不再写入），每次采集以 `cp --reflink` 复制该集群（不占未改动的块）并把库改名为本次采集库，采集成功后删除副本；预检、导出、原生执行（结束后并行审计全部会话）、行审计（完整核验时）、编译（同遍构建访问账本并核对 PG 计数器，一致才提交 Trace）、认证（完整核验时），每阶段耗时与事件数写入 `run-timing.json`。HyBench 运行原生 `runHTAP`（AP1–13 → TP1–18 → 并发 AT1–6/IQ1–6），事务权重取上游；YCSB / sysbench 以未改动的原生客户端按 workload 顺序运行。并发度取原生参数（HyBench `clients` / `xp_transaction_clients` / `xp_query_clients`，YCSB / sysbench `threads`），每线程一个后端会话；有重叠会话时 `session_order=concurrent`，按事务完成计数器归并。HyBench 静止库由 `base` 阶段按上游 README 建立（原生 gendata、`ddl_pg.sql`、`load_data_pg.sql`，清单记录数据文件 SHA256 与装载行数），装载后删除 CSV、只留运行时读取的两个文件；YCSB / sysbench 静止库以原生客户端装载一次，清单记录装载参数，参数不同的 profile 不能克隆它。统计信息随静止库只取一次，同一静止库的各次采集计划依据相同。`plan-write-ratio` 由校准采集按同一倍数换算各来源原生参数，Trace manifest 报告实际 `row_write_ratio`。限定 PG14.24、read_committed、无 parallel worker、postgres 角色、Zstd 和 `results/poc`。 |
| 原生插桩 | 实现集中在 `sibench_capture.c`，PG 补丁只挂调用点。记录请求序号、执行器深度与 `query_id`、扫描键（含 `= ANY` 数组元素）、bitmap 组合树、行变化、事务边界及其事务完成位置（写事务为持锁自增时的精确值）、语句快照的计数器与 xmin / xmax / xip。默认（`capture.detail: summary`）每个访问在结束前写一条 `scan_summary`（取行、候选、方向、回表与可见行、bitmap 候选与未判定数、提前停止时的末段值与 PK），bitmap 堆判定按产生 TID 的输入集合聚合，结果行记为 64 位哈希，客户端以同一算法核对；`rows` 另写全部逐行事件，账本逐条核对汇总与之相等。默认（`capture.compression: zstd`）后端追踪写成 zstd 流，每事务结束 flush、退出时结束帧，读取方透明解压。缺少这些字段的原始追踪被拒绝。 |
| 采集审计与失败分类 | JDBC / 服务端请求配对、结果行、提交与回滚身份核对；显式记录连接关闭时最后请求无 `executor_end`、事务已以 abort 结束后的无改动 commit。`SourceFailurePolicy` 按驱动状态与客户端语义分类来源失败；sysbench 控制语句数由脚本实际调用加运行时 `--skip-trx` 推出并与服务端残差对账；YCSB 声明比例、请求分布、scan 上限、字段读写与 read-modify-write 非原子性和实测对账；`ChangeKinds` 区分索引列 UPDATE、普通列 UPDATE 与 DELETE 后重插入。 |
| 访问账本 | 账本（v4）由编译同遍构建并写入 Trace 目录，独立的 `access-ledger` 命令结果相同，用于编译失败时诊断全部拒绝。它把原始 PG 索引事件划分为访问单元（一次索引重扫一个单元，BitmapAnd 同一轮实际执行的输入为一组），每个事件恰好归属一处，否则失败。候选、可见行、SQL 返回行分列且不相加。模板身份为 PG `query_id`，输出计划序列及变体数、访问路径与投影决定；投影、拒绝、排除、交集组与查询记录数分别汇总。只读追踪、不连接 PG。`index_insert_attempt` 是物理写尝试，不等于逻辑键维护。 |
| 访问投影 | 单列扫描键求交为区间，IS NULL / IS NOT NULL 为区间，无键为全扫描，`= ANY` 为点的并集组；读到末尾无 limit；主键扫描被提前停止时 limit 为已消费可见行（含降序），二级扫描为前段区间加钉住点查；bitmap 读完堆才投影，BitmapOr 两侧各计，BitmapAnd 为交集组。拒绝：有损页、堆扫描提前停止、全可见 index-only 行无身份、嵌套 bitmap 组合、行比较、复合键、NULL 常量比较、族外键类型、非 C collation 文本区间；排除：规划期探测、未取行的重扫、不可能匹配的键。拒绝是终态而非待实现项：嵌套 bitmap 缺少原生事实（堆只判定最终位图，中间层没有独立可见行数），非 C collation 区间按字节序会给出错误结果；宁可拒绝，不做近似。三来源全部预检 `lc_collate` 为 C，当前产物 `projection.rejected` 为空。查询形态（点 / 区间 / 快照）是来源事实，Trace 不提供按形态过滤的发送开关，分布见 `trace-summary.json` 的 `searched_accesses_by_shape`，由下游按形态聚合观测。 |
| 并发归并 | 并发会话按（事务完成位置、写事务优先、会话顺序）k 路归并事务大小的片段，每个会话一个前瞻读取器只找片段结尾，片段内容流式读出，内存与片段大小无关；片段结果（提交 / 回滚、事务身份）在读其事件之前已知，编译器据此按原顺序写出已结算的前缀，不整事务缓冲；会话内写事务位置严格递增、只读事务不递减，两写事务共用位置即失败。查询读取快照所见的最后一个写事务，早于前一提交时标记 SNAPSHOT_SEARCH；逐单元以快照 xip 交叉验证，冲突即失败。 |
| 文件与规范 Trace | 统一 Parquet writer，256 MiB 目标、1,048,576 行组上限；基线导出按主键顺序每文件恰好 1,048,576 行（末文件为余数），文件与行身份由各表行数预先确定，同一静止库的导出与核验结果与线程数、读取方式无关；v9 为 128 字节前缀加 PK、索引值、下界、上界四个保序编码分量，保留提交版本、读取点、事务内位置、源 SQL 工作单元、连续维护展开及查询的边界、方向与 limit；钉住点查在 PK 分量带行身份，查询组复用展开字段表示组内位置与组宽。编译器由来源 schema 驱动，接受单会话、顺序会话或并发会话，任一访问被拒绝即整次失败；不执行请求的事务不分配版本。目录级预检核对来源清单、摘要、工作单元绑定、完整展开与连续查询组；只支持 v9。 |
| 索引清单与维护 | `IndexCatalog` 由来源 schema 生成，manifest 记录来源与 native 配置并据此重建核对。提交 INSERT / UPDATE / DELETE 统一展开；改键为旧身份 LOGICAL_DELETE + 新身份 UPSERT，未改键也刷新关联版本 / 值；UPDATE 保留 RowID，重插入分配新身份。PK 值 UPDATE 及范围外索引定义拒绝。 |
| 源行状态审计 | `RowStateAudit` 按三来源 schema 统一核对完整列和旧 / 新行；支持字符串及非连续 int64 主键、DELETE / 重插入与插入键交错。同一键的变化按（事务完成计数器、会话、位置）外部排序，与编译器归并同序，旧像链在并发写同一键时也连续。静止边界最终 PG 全行对账，摘要绑定报告和已审计变化；重复调用不查询 PG。Baseline 按来源 schema 验证全值和身份。 |
| 多 client 驱动 | 两种输入模式、全部二级统一过滤（查询组整体发送或整体过滤）、单发送线程、源工作单元路由、异步响应、在途上限、超时、排空和失败清理；过滤后 client 覆盖及单次输入预算预检。工作单元是任务近似，不是实际 serverless DAG；参考 client 共享进程内模型，不模拟网络可见性。 |
| Index Service 协议 | `src/main/proto/index_service.proto`（gRPC）：`Describe` 声明 value 种类、按版本读、同版本有序覆盖、查询组与跨 client 有序；`Execute` 每 client 一条有序流，请求带全局发送序号，查询组整体发送；`Flush` 为阶段边界。`replay-service` 复用回放驱动，每个查询的返回条数必须等于原生期望数，可与参考回放比对结果校验和；多 client 只接受声明按发送序号跨流执行的服务。`index-server` 提供 RocksDB 参照实现（契约 KV 布局，`ordered` 时按序号执行），另有进程内 RocksDB adapter。见 [Index Service 协议契约](docs/contracts/index-service.md)。 |
| 版本参考与查询核验 | Locator / RowID、通用表 / 索引命名空间、按编码键有序的当前及历史点查 / 区间 / limit / 降序扫描、钉住单条目点查、按 PK 求交 / 求并的查询组，null 和字符串 / int64 PK、逻辑删除及全索引精确版本搬迁。Parquet oracle 全改动身份逐版本核对，非唯一等值谓词每索引最多 64 个（半数取修改值、其余按值域均匀含首尾），主键与二级的带 limit 区间探针取值域分位边界（开闭端点、升 / 降序）、无界与修改值锚定区间，逐项核对结果与顺序；`validate-replay-matrix` 要求 1/64/128 client × 两种输入模式 × 二级开 / 关的操作顺序、状态与结果完全等价；裸文件回放不证明源清单完整。 |
| 观测与对照 | `compare-ledgers` 按模板对齐两份账本报告计划 / 访问路径漂移（`query_id` 编入关系 OID、跨库不可比，故回退以归一化 SQL 文本配对；版本不同的账本拒绝比较）。`native-capture.sh` 在原生执行前后各取 `pg_stat_user_indexes` 快照（等其它会话退出、读数稳定），编译（`--counters`，或独立的 `check-capture-completeness`）按 PG14 计数位置核对账本 v4：`idx_tup_read = 候选`、`idx_tup_fetch = 经堆判定可见行`（不含 bitmap 与全可见 index-only）、`idx_scan` 在按扫描轮次与数组元素推出的树下降上下界内，不一致即采集失败；PG14 统计经 UDP 可能丢失，只会表现为低于账本。 |
| 状态与文件生命周期 | 参考模型为排序的不可变基线键序列加有序版本历史，超出预算时分别外部排序为磁盘 run 与落入可落盘有序 map；顺序会话编译与回放驱动按源事务缓冲，上限 1,000,000 个展开操作（并发编译已流式写出，不受此限）；Locator 为（FileID, 行位置）两个 int64，增量 Parquet 达目标大小 rollover。搬迁控制（`check-relocation`）仅选择实际修改表的最新存活行，不保留受保护历史集合、不自然触发、不回收旧文件；不是 SF1000 有界状态方案。 |

## 验证证据与边界

| 验证 | 当前依据 | 不代表 |
| --- | --- | --- |
| 完整 HTAP（并发） | 当前插桩采集，八表 Baseline 全值与身份核对、请求配对与行状态审计通过；全部访问投影（含 BitmapAnd 交集组、数组并集、二级前段加钉住点查），全部单元通过快照交叉验证，同一输入重复编译逐字节一致；1/64/128 client、transaction 模式两种 value 回放与 oracle 通过，见 [HTAP 验证](docs/validation/hybench-sf1-htap.md)。 | 短跑之外的容量或性能；故障注入下各类事件丢失都能被发现；oracle 对访问组只经成员检索覆盖。 |
| YCSB / sysbench | 8,706,000 行基线上当前插桩采集，全部访问投影；YCSB 每个 autocommit 请求恰好一个版本；控制语句、声明比例与语义标识对账通过；同一输入重复编译逐字节一致；1/64/128 client、transaction 模式两种 value 回放与 oracle 通过，见 [原生 Trace 验证](docs/validation/ycsb-sysbench-native-trace.md)。保序编码由 PG 差分测试核对。 | 性能；多会话 YCSB / sysbench；两来源未出现访问组形态（由 HTAP 覆盖）。 |
| 采集完整性与有界状态 | 三来源采集前后的 PG 索引计数器与账本逐项一致（`idx_tup_read`、经堆可见行的 `idx_tup_fetch`、树下降上下界内的 `idx_scan`）；区间 oracle 在三来源新产物上通过；改动后的代码重编同一份原始采集，Trace 数据文件逐字节一致；重写的行审计在真实 sysbench 采集上与旧报告一致；真实 sysbench Trace 强制落盘回放的校验和与全内存回放相同，见 [采集完整性与有界状态验证](docs/validation/capture-completeness.md)。 | 故障注入与插桩开销；超过内存的输入上的容量与耗时；分段续编译、Checkpoint 与恢复。 |
| 汇总采集格式与 Index Service | 三来源各以 rows 采集一次，账本逐条核对汇总与逐行事件相等（sysbench 181,000、YCSB 346,138、HyBench 并发 HTAP 3,412,523 次访问，均 0 拒绝、PG 计数器一致）；删去逐行事件的副本编出的 Trace 与逐行格式除原始行号外逐字段相同（HyBench 含 402,417 次快照读与全部快照交叉验证）；独立 summary 采集 sysbench 原始体积 3.7 GB → 484 MB、原生执行 215 s → 72 s，HyBench 原始体积 38 GB → 3.1 GB，见 [采集完整性验证](docs/validation/capture-completeness.md)。sysbench Trace 经进程内 RocksDB、单 client 与 64 client gRPC（服务端按发送序号执行）回放，181,000 个查询期望数全部吻合、结果校验和与参考回放相同；不排序的服务在 64 client 下出现期望 100 返回 99。 | RocksDB 性能结论（参照实现未调优，按序号执行即串行）；持久化与恢复。 |
| 按设计并发的原生采集 | SF1 规模短跑，三来源均 0 拒绝、PG 计数器一致、128 client 两种 value 的参考回放与 oracle 通过：sysbench 8 线程（48 会话、181,000 次访问、161,070 次快照读）、YCSB 8 线程（48 会话、346,048 次访问、333,492 次快照读）、HyBench TP 8 / AT 8 / IQ 8 client（25 会话、51,202 个提交事务、38,565,046 次访问、37,246,996 次快照读、877,592 次写后查询，核对 10.5 亿个结果项）；sysbench 与 YCSB 经进程内 RocksDB 回放与参考校验和相同；修复后的编译重编 sysbench / YCSB 两份并发采集 Trace 逐字节相同，见 [并发采集验证](docs/validation/concurrent-native-capture.md)。 | 更高并发或更热分布下的来源失败（本轮无死锁、无重试）；SF10 及以上规模。 |
| 实际 PG schema 预检 | PG14.24 上三来源布局、完整表 / 列 / 默认值 / 索引差异、quoted 名称与 search_path 的集成检查通过，差异在导出与采集前拒绝并保留诊断，见 [预检验证](docs/validation/source-schema-preflight.md)。 | 运行期外部 DDL 漂移检测。 |
| SF10 规模与采集 | 远程 32 并发、5% 写入的认证采集三来源端到端约 21 / 10.5 / 36 min；编译与导出优化后重编 Trace 逐字节一致、PG 计数器一致；YCSB 87m 首次导出 + 核验约 41 min。SF1000 外推与磁盘边界见 [扩展模型](docs/validation/scaling-model.md)。 | 真实服务性能；SF1000 实测；共享宿主波动下的单次耗时。 |
| 受控语义测试 | 完整构建单元测试零失败，覆盖访问组投影（前段加钉住点查、BitmapAnd 交集与短路周期、数组并集、嵌套组合拒绝）、v9 格式、计数器归并与快照交叉验证、三来源 schema 与行审计、失败分类与控制语句对账、回滚改键、写后查询、DELETE / 重插入、版本搬迁、展开完整性及异步发送边界；三来源实际 PG 行审计检查见 [统一审计验证](docs/validation/row-state-audit.md)。 | 受控 SQL 是原生客户端执行；原生负载实际覆盖全部受控操作。 |

当前阶段的采集配置为 `config/hybench_sf10.yaml`、`config/ycsb_87m.yaml`、`config/sysbench_87m.yaml`（在远程机运行），`*_8m.yaml` 与 `hybench_sf1.yaml` 用于较小规模。实验数量、摘要、命令和历史记录保留在验证文档及产物中；本计划仅记录当前验收依据与能力边界。

## 执行资源

| 机器 | 资源 | 职责 |
| --- | --- | --- |
| 开发机（Ubuntu 22.04） | 32 线程、62 GB、`/mnt/disk1` 985 GB 云盘（顺序约 270 / 330 MB/s 写 / 读，8 KiB 随机读 0.44 ms） | 协调与发布；编译、空间与回放优化的开发，用保留的四份产物做回归；真实索引回放的开发。不再运行采集（CPU 比远程机慢 2.5–3 倍，PG 构建与远程不同） |
| `sibench_1`（SSH 别名，下同） | 三台相同：TencentOS 3.2（glibc 2.28），Kubernetes pod，cgroup 限 32 CPU、64 GB 内存（含页缓存），宿主 96 核并有其他负载；`/data` 500 GB xfs 云盘（顺序约 80–90 / 130–160 MB/s，8 KiB 随机读 0.8–2 ms）；根分区 50 GB overlay | HyBench SF10 采集；保留 SF10 静止库 `sibench_sf10_base` |
| `sibench_2` | 同上 | YCSB 同量级采集 |
| `sibench_3` | 同上 | sysbench 同量级采集 |

一次采集是一条全局提交顺序，不能拆到多台机器，也不用分布式 PG（全局完成计数器、快照与插桩都以单实例为前提）；多台机器只并行互相独立的流程。远程机 HyBench SF10 吞吐约为开发机的 2.5–3 倍（CPU 受限），见 [采集性能](docs/validation/capture-performance.md)。`/codev` 等已有数据的分区不使用。

远程环境位于 `/data/sibench`，结构与仓库相同，以非 root 用户 `sibench` 运行（PG 拒绝 root）：dnf 安装 `java-17-openjdk-devel libzstd-devel gcc make rsync perl jq`；PG 14.24 由官方源码包（核对 SHA256）在 `sibench_1` 上经 `make -C src/main/native/postgres install extension` 构建，二进制复制到另两台，三台 postgres 与 jar 的 SHA256 相同，认证按此构建区分；集群 `initdb -U postgres --locale=C -E UTF8 --auth=trust`，服务器参数由采集脚本从 `config/postgres_capture.conf` 启动（B1 配置，端口与 socket 目录由脚本追加），实际生效的非默认参数记入 `capture-run.json`，影响计划的参数并入构建身份、随认证比对；`benchmark/` 子模块需连同 `.git` 复制（采集核对上游提交与工作区干净）；运行脚本时设 `SIBENCH_JAVA_HOME` 为 TencentOS 的 JDK 目录。YCSB 为 Java 可直接复制；sysbench 是原生程序，开发机构建的二进制需要 glibc 2.34，不能在 TencentOS（glibc 2.28）上运行，不升级系统 glibc，而是在各机上就地构建：`PATH` 置前采集 PG 的 `runtime/bin`，`LDFLAGS=-Wl,-rpath,<runtime>/lib ./configure --without-mysql --with-pgsql && make`，客户端因此链接采集 PG 自带的 libpq；jar 内嵌的 JNI 库（rocksdbjni、zstd-jni）在 amd64 上最高只要求 glibc 2.12。

## 开发流程

1. 开发机修改代码，`mvn test` 全部通过。
2. 回归：开发机保留 `results/poc/` 下四份完整产物（`hybench-htap-sf10-w5`、`hybench-htap-sf1-c8-20261024`、`sysbench-8m-c8-20261024`、`ycsb-8m-c8b-20261024`）。编译改动用其原始采集重编，Trace 数据文件须逐字节相同（不同须给出原因）；回放或格式改动用其 Trace 回放，结果校验和须与已有参考回放报告相同（SF10：`replay-128.json`，每种 value 191,117,063 个查询、44,507,577,021 个结果项）。
3. 发布：`mvn package` 生成 jar（有采集或回放正在使用 `build/sibench/sibench.jar` 时，在独立副本中打包）；同一个 jar 复制到三台远程机，PG 或插桩改动时在 `sibench_1` 重建并复制。
4. 远程运行：各机按分工运行 `scripts/native-capture.sh`，只为验证代码改动或产出所需的采集而运行，结果稳定即停，不重复空跑；采集侧代码、PG 构建或改变计划的配置变化后，以一次完整核验的正式采集重新认证，只改编译器或导出不必。
5. 回收：只取报告与摘要写入 `docs/validation/` 与本计划；产物按「产物与保留」清理。

## 后续工作

以下只列剩余工作，按主线依赖关系组织。部分实现和固定特例的扩展任务归入对应领域，不以通用类名或 SF1 测试代替完整验收。

### 1. 接入真实服务与测量

- [ ] 其它 KV 存储（rocksdb-cloud 等）按同一协议接入；adapter 只做协议转换，不改变 Trace 语义。
- [ ] 传输侧：批处理（当前每请求一个消息、单 client gRPC 约 6k ops/s）、断连与写入结果未知时的诊断。
- [ ] 跨 client 语义：按发送序号全序执行能重现原生结果但使服务串行；为保留并行度，定义按版本水位与事务内依赖等待的语义（协议需携带依赖信息），并在真实服务上验证其声明；驱动不通过可见性屏障修复结果。
- [ ] 保序限速、预热及测量流程，保持一个发送线程，不以并发调度改变原生顺序。
- [ ] 正确性、持久化、恢复及后台排空验证；记录延迟、吞吐、索引字节、物理 I/O、放大和资源指标，不以进程内立即响应、裸解码速率或 Trace 上传代替服务验收。

### 2. 补齐版本与文件生命周期

- [ ] 固定保留窗口、过期与快照保护规则，实现物理 GC。
- [ ] 按当前版本与受保护历史集合实现自然 Rewrite、确定性装箱、发布屏障及旧文件回收；独立最新版本搬迁控制不替代生命周期实现，并在 v9 产物上重新核验搬迁控制。

### 3. 校准并开展正式实验

- [ ] 来源失败处理：负载按 benchmark 设计并发运行，并发写暴露的失败按原生行为修复处理管线，而不是缩减负载形态；按实际 SQLSTATE 与固定源码行为放行预期失败（`CaptureDriver` 只记录不判废，YCSB `Return=ERROR` 与带 SQLSTATE 的请求错误对账，HyBench catch 不回滚造成的 25P02 连锁只在前一失败已分类时接受；请求审计、行审计与编译按回滚语义接受失败语句）。
- [ ] 三来源分别进行样本容量校准；采集验证已按 8,706,000 行对齐，正式实验的容量仍按各来源校准，不要求行数、索引条目或字节量相等。
- [ ] 完成真实服务上的 1/64/128 client、查询及二级开关对照。
- [ ] 128/256/512 MiB 文件布局对照、真实存储与至少 30 分钟连续回放实验。

### 4. 硬件更新后（SF100 及以上）

SF100 及以上现在不测试；以下随更大的磁盘或多机进行。

- [ ] 规模边界与目标：SF1000 的静止库与基线超出单机 500 GB（见扩展模型）。面向 SF1000 的正式采集以回放预算为目标：`config/long_replay.yaml` 的 6.66 亿有效操作，由 `plan-operations` 推出原生参数、以 `check-replay` 验收（SF10 的正式采集按 5% 写入比例，`plan-write-ratio`）。HyBench 上游工作负载客户端与 DDL 的 `int` ID 在 SF1000 溢出，届时改为 bigint。
- [ ] 有界状态的容量验收：行审计外部排序、编译的落盘行身份与有界提交窗口、参考模型的落盘基线与版本历史已有单测与真实 sysbench Trace 上的内存 / 落盘对比；在远超内存的输入上验证容量与耗时。
- [ ] 分段编译：连续事务边界分段、确定性续编译、Checkpoint 与恢复，原始采集按段编译后即删（SF1000 每次正式采集的原始采集为 16–216 GB，现有磁盘容得下）。
- [ ] 采集编排推广：按目标负载推广当前 PG / 角色 / codec / 输出目录限制。

### 5. 按需再做的采集优化

- [ ] 静止库构建随数据量超线性，主要是大表的 `VACUUM (ANALYZE)`；改动原生装载顺序会改变静止库物理状态，须先确认。
- [ ] 以无插桩运行测出原生执行的插桩开销，缩减其余事件体的格式化。
- [ ] 编译的 `emitAborted` 逐操作复制、账本剩余串行部分、原始文件摘要跨进程沿用。
- [ ] 行审计基线侧归并按文件区间并行，快照 oracle 的耗时只在认证采集上运行。
- [ ] 精简 PG 追踪中的行映像；工作单元目录移出产物；数据 / 产物 / 证据 / 临时目录分开。

## 验收条件

| 维度 | 条件 |
| --- | --- |
| 原生真实性 | 原生访问、提交数据、Parquet 与规范 Trace 对得上；明确区分来源无此操作、已关闭和不支持。 |
| 版本与布局 | 两种 value 的键、版本、结果及真实文件位置正确；DELETE 不使旧版本复活，Rewrite 保留受保护历史。 |
| 可复现与失败 | 相同采集输入重复编译及恢复得到一致事件、身份和布局；损坏、截断、失败或未完成输入在真实发送前拒绝。 |
| 路由与顺序 | 同一规范 Trace 的 1/64/128 client 变体保留相同本地发送序列；逐 client 统计与总量一致，过滤后覆盖不足在真实加载 / 发送前拒绝。128 不是硬上限。 |
| 变体公平性 | 关闭查询或二级发送不改变源数据、写入版本、文件布局及后续依赖；不同变体分别报告。 |
| 真实服务 | 在声明范围内验证正确性、持久化、恢复及后台排空；缺失指标标 N/A，不以进程内模型或文件上传代替存储验证。 |
| 连续供应与性能 | 至少 30 分钟连续输入；`config/long_replay.yaml` 当前为 128 client、666,000,000 个有效运行期操作预算。Index 300,000 ops/s、完整驱动 900,000 ops/s 均为待测目标，预算预检、裸解码及 SF1 短跑不能代替性能验收。 |

## 文档入口

- [README](README.md)：构建、配置及运行命令。
- [能力矩阵](docs/benchmarks/capability-matrix.md)：各来源当前覆盖。
- [Trace 契约](docs/contracts/trace.md)、[存储契约](docs/contracts/storage.md)：详细语义与责任边界。
- [来源 Schema 契约](docs/contracts/source-schema.md)：三来源实际布局、PG 预检及验收边界。
- [按设计并发的原生采集](docs/validation/concurrent-native-capture.md)：三来源 8 线程 / 8 client 短跑、HyBench SF10 校准与 5% 采集、产物清理。
- [采集性能](docs/validation/capture-performance.md)：远程机与开发机、PG 配置对照与插桩开销。
- [编译阶段对照](docs/validation/compile-optimization-20261026.md)：SF10 / 87m 编译器同 raw 对照、new22 回退与清理构建。
- [采集完整性与有界状态验证](docs/validation/capture-completeness.md)：三来源 PG 计数器对照、区间 oracle、重编译与落盘回归。
- [HTAP SF1 验证](docs/validation/hybench-sf1-htap.md)、[YCSB / sysbench 原生 Trace 验证](docs/validation/ycsb-sysbench-native-trace.md)：当前 v9 证据与历史记录。
- [来源 Schema 预检验证](docs/validation/source-schema-preflight.md)、[统一行状态审计](docs/validation/row-state-audit.md)：预检与行审计的测试和集成证据。

实验细节和历史日志保留在对应验证文档及原始产物中；本计划只维护当前约定、进度、后续工作和验收条件。
