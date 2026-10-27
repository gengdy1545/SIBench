# Trace 契约

## 两层产物

原始层保存一次实际执行的请求、参数、结果、索引候选、行变更和提交/回滚。规范层只转换已声明支持的索引操作，保留源事件关联，不重执行 SQL 推测结果。PG 页分裂、HOT、VACUUM 等内部实现不转换为下游 KV 算法。

采集及 Baseline export 前按 [来源 Schema 契约](source-schema.md) 核对实际 PG 表、完整列及全部主键 / 二级索引与固定源码和配置一致。检查只读，不补建或修复环境；原生运行保存预检结果，旧历史产物不补造此证据。三来源 catalog 支持与各自规范 Trace 支持分别验收。

默认原生执行为 HyBench `runHTAP`，保存 AP/TP/并发 AT-IQ 的多会话采集，行变化触发器覆盖八表。v9 **规范投影**编译顺序会话（YCSB / sysbench 各 workload 依次运行）或并发会话（HTAP，按事务完成计数器归并，见下文“身份与版本”）采集，三来源 schema 驱动：每个访问单元（或一个 BitmapAnd 组）按下文“访问投影”成为一组能重现原生结果的有序查询，否则整次编译失败并给出原因；索引维护覆盖固定 schema 的全部主键和二级索引，支持索引列值 UPDATE、DELETE / 重插入、已提交事务写后查询及多 client 路由。

原始层是每个后端一个 zstd 流 `pg-<pid>.jsonl.zst`（JDBC 客户端为 `jdbc-*.jsonl.zst`），完成后由 `capture-run.json` 登记文件清单与 SHA256。每个事务结束时 flush，后端退出时结束帧；帧未结束的文件读取失败（后端未正常退出的采集不可用）。插桩实现在 `src/main/native/postgres/sibench_capture.c`，补丁只在 PG 执行器、索引访问、堆与事务路径上挂调用点。每个文件第一行必须是 `capture_start`（`format` 2，`detail` 为 `summary` 或 `rows`，由配置 `capture.detail` 决定，默认 `summary`）；缺少该行拒绝编译。

一个访问单元的事实在 PG 内按访问累加，在结束它的事件（下一次 `scan_rescan`、`scan_end`、事务 abort 或连接关闭）之前写成一条 `scan_summary`：取行调用次数、找到的候选、最后一次是否找到、扫描方向、recheck、回表次数与可见行、可见性图全可见行、bitmap 候选数与未被判定的候选数、bitmap 是否填完、是否缺行身份。扫描被消费者提前停止时另带最后一个值之前的可见行数、数组键的已完成值段、最后一个值及其上被返回行的主键；BitmapAnd 一轮的第一个成员结束时带“是否有全部输入都产生却从未被判定的候选”。bitmap 堆判定按“产生该 TID 的输入集合”聚合为 `bitmap_verdicts`（可见 / 不可见数和有损页数），在任何输入换单元之前写出，与逐 TID 归属等价。`executor_run_end` 带本次执行返回的行数和每行的 64 位 FNV-1a 哈希（列文本规范化后计算，客户端 `consumed_row` 用同一算法，见 `ResultRowHash`），供客户端 / 服务端结果核对。`detail = rows` 时另写全部逐行事件（`scan_next`、`heap_visibility`、`vm_visible`、`bitmap_candidate`、`bitmap_end`、`bitmap_heap_candidate`、`bitmap_heap_lossy`、`result_row`，客户端行保留列文本）：账本用逐行事件计算并要求每条汇总与之完全相等，用于在同一次原生执行上验收汇总；`summary` 采集中出现逐行事件即失败。

其余事件两种格式相同：`plan_scan` 把索引扫描描述符绑定到计划节点（index / index_only / bitmap）；`plan_bitmap_heap` 把 bitmap 堆扫描节点绑定到为它提供 bitmap 的全部索引扫描，并以 `tree` 给出组合树（扫描标识，或 `{"and":[...]}` / `{"or":[...]}`），扁平输入列表无法区分“并集的交集”与“扫描的并集”；`scan_rescan` 带扫描键（数组键 `= ANY` 带 `array` 元素与 `array_type_oid`，行比较标记 `unsupported_key`）和语句快照；`row_change`、`index_insert_attempt` 是写入事实；`transaction_commit` / `transaction_abort` 带该事务的完成位置 `csn`。`executor_end` 是清理生命周期事件，关闭连接时可能没有该事件。每个事件带请求序号 `req`（第一个不在任何执行器内的 `executor_start` 起算）和执行器嵌套深度 `depth`，使 SPI / 触发器内部查询归属外层请求；`executor_start` 另带 PG 内核 query jumbling 的 `query_id`（采集服务以 `compute_query_id = on` 启动，不改变计划）。行身份是从可见堆元组读出的扫描索引键列值与表主键值，这是 KV 与 PG 共有的唯一行身份（heap TID 在 KV 中没有对应物）。缺少这些字段的追踪被访问账本和编译器拒绝。每个文件保留各会话内部顺序；跨会话顺序只由完成位置决定，见下文“身份与版本”。

## 访问账本

账本（报告 v4）读取原始 PG 追踪，不连接 PG。编译器在编译的同一遍里构建它，写入 Trace 目录的 `access-ledger.json`；独立的 `access-ledger` 命令得到相同报告（输入按文件名排序，模板显示文本取字典序第一个），用于编译因拒绝而失败时列出全部拒绝。一次索引重扫是一个访问单元；嵌套循环内表的每次重扫各算一个。每个原始事件恰好归入访问单元、写维护（`index_insert_attempt`）、执行器生命周期、事务边界、结果行、行变化、索引构建或计划绑定之一，各类之和必须等于输入事件数，否则失败。候选条目、可见行、SQL 返回行是同一访问的三个属性，不相加：bitmap 候选是可见性判断前的 TID，可见行是堆检查可见、可见性图全可见的索引行，或堆判定可见的 bitmap 候选；一个候选 TID 的判定计入产生它的每个索引扫描，所以 BitmapOr 两侧各得自身的可见匹配数；BitmapAnd 的堆只检查交集，同一轮执行的输入组成一个组，组计一次可见数。PG 在交集变空时跳过后续输入，所以一轮只包含实际执行了的输入。堆判定事件（逐 TID 或聚合）单列为 `bitmap_heap_verdicts`，不重复计入访问单元；`scan_summary` 计入它所属的访问单元，`capture_start` 单列为 `capture_header`。每个输入文件报告其 `detail`。

每个访问单元带有编译器使用的同一投影决定（下节）：`projection` 汇总投影形态、拒绝原因和排除原因，存在拒绝时状态为 `has_rejected_units`。模板身份是 PG `query_id`（以字符串输出，避免 JSON 工具丢失 int64 精度），去字面量 SQL 只作显示；同一 query_id 出现多种文本时标注 `text_variants`。每个模板列出计划序列（请求绑定的 plan node、扫描类型、索引）及其请求数，多于一种时 `plan_variants > 1`；再按（来源、plan node、索引、扫描类型、决定）列出访问路径的单元、候选和可见行。零个访问单元是观察结果（例如该请求走了 seq scan），不是计划推断。

规划期的索引端点探测（`IS NOT NULL` 键，执行器之外）、从未取行的重扫和写入路径的唯一性检查不是查询：前两者识别并单列为排除，后者发生在索引 `aminsert` 内部，只计入写维护。`index_insert_attempt` 是物理索引写尝试：每次更新的次数随页内空间变化，不等于逻辑键维护次数，逻辑维护仍由行变化展开得到。账本证明事件被恰好归属一次，不证明没有事件丢失；索引扫描之上的 residual filter 不改变索引结果，不计入。

## 访问投影

`AccessProjection` 对每个访问单元（BitmapAnd 为一组）给出一组查询或一个原因，账本报告与编译器发出共用该规则。原则：**原生执行是确定的，投影出的查询必须重现原生实际的结果集，每个期望数都是原生事实**；一个区间表达不了时拆成多个 KV 可执行的子查询，而不是近似或拒绝。

| 情形 | 决定 |
| --- | --- |
| 单索引列的扫描键（=、<、<=、>=、>）求交为一个编码值区间；IS NULL / IS NOT NULL 是 NULLS LAST 顺序中的区间；无键是全索引扫描 | 一个查询：equality / range / full_scan |
| `= ANY(数组)`：每个不重复的非 NULL 元素（落在其它键的区间内）一个点 | 多于一个点时为并集组 `union`，组期望数为可见行；只有一个点即一个查询；全为 NULL（PG 不检索）排除 `contradictory_scan_keys` |
| 普通或 index-only 扫描读到索引末尾 | 期望数为可见行，无 limit |
| 主键扫描被消费者提前停止（LIMIT、半连接） | 主键无重复值，已消费行是 KV 序前缀：limit=已消费可见行，按扫描方向 |
| 二级索引扫描被消费者提前停止 | 非唯一 btree 同值按 heap TID 排序，KV 按 PK，截断处选中的不是 PK 前缀。拆成 `stopped_*`：最后一个值之前已完整读过的区间（期望数=其中的可见行），加上最后一个值上每个被返回行的钉住点查（(值, PK)，期望数 1）；数组键按元素依次拆。需要每个被返回行的身份，index-only 的全可见行没有身份时拒绝 `stopped_scan_without_row_identity` |
| bitmap（单索引、BitmapOr） | 每个候选 TID 都有堆判定时投影，期望数为判定可见的候选（BitmapOr 两侧各自计数） |
| BitmapAnd（扁平） | 交集组 `intersection`：每个输入一个区间查询，组期望数为堆判定可见的交集行；一轮只有一个输入时即该输入的查询 |
| 有损页、堆扫描提前停止（有候选从未被判定）、嵌套组合、BitmapAnd 输入带数组键 | 拒绝 `bitmap_heap_lossy_page` / `bitmap_heap_stopped_early` / `nested_bitmap_combination` / `bitmap_and_input_with_array_key` |
| 行比较、复合键、与 NULL 常量比较、族外键类型、数组不等式 | 拒绝 `row_comparison` / `composite_key` / `null_comparison` / `unsupported_key_type` / `array_inequality` |
| 文本区间（非点查）且数据库 lc_collate 不是 C/POSIX | 拒绝 `text_interval_requires_c_collation`；等值不受 collation 影响 |
| 规划期端点探测、从未取行的重扫 | 排除，不是负载访问 |

剩下的拒绝都对应**事实缺失**（有损页没有 TID、提前停止的堆扫描没判定部分候选、全可见 index-only 行没有主键）或超出固定来源范围的键形态，不再有"事实存在但模型表达不了"的拒绝。

键值按索引列类型编码：扫描键必须属于列的 btree 族（int2/4/8 对 int，timestamp 与 date 对 timestamp，text/varchar 对 varchar/text，bpchar 对 CHAR）。来源 schema 预检要求文本列与索引使用数据库默认 collation，故 `lc_collate` 决定文本区间是否保序；三来源原生采集库均以 C 建库。投影描述索引访问本身：聚合、排序、DISTINCT 或 LIMIT 使返回行数与可见行不同的模板分别保留两种计数，查询期望数取可见行，不以返回行数代替。

## 核验职责

源行状态只由 `RowStateAudit` 核验：以已验证 Baseline 为初始状态，按 schema 类型比较完整旧 / 新行，提交变化推进状态，回滚变化排除；在采集结束且业务库静止时与 PG 全表最终状态对账。单会话严格保留来源操作顺序；多会话连接保留各会话同键顺序的行因果链。审计只证明行状态，全局提交顺序与查询快照由编译器按完成位置建立。回滚变化检查行形状、类型及主键稳定性，不作为已提交修改链的组成部分。

默认产物为 raw 目录的 `row-state-audit.json` 和 `row-state-audit.json.changes.jsonl`，报告摘要绑定 schema、原始输入、已验证 Baseline 和已审计变化。同一报告路径重复调用只复核摘要，不重新查询 PG 或核验行链；输入改变则拒绝。报告最后原子发布，失败残留不视为成功，使用新的报告路径重试。重新审计必须使用原采集结束时的最终数据库状态或其准确恢复副本。

编译器要求匹配报告或匹配的认证，并检查转换所需的字段、身份及支持范围。

证明方法的检查只做一次。`capture.verification: full` 的采集做行状态审计，编译（带 PG 计数器核对）通过后 `certify-capture` 写出 `certification.json`：构建（运行用 jar、PG 二进制、触发器库的 SHA256，见 `capture-run.json` 的 `build`）、来源提交与 schema 摘要、索引定义、采集格式（`capture_detail`），以及覆盖范围：账本中的全部访问路径（来源、索引、扫描种类、投影决定）和 Trace manifest 中的提交变化类别（表、操作）。`certified` 的采集以 `--certification` 代替行状态审计：编译前要求构建与来源相同，编译后要求格式、访问路径与变化类别全部在认证覆盖范围内，否则编译失败、Trace 保持未完成。认证采集仍做会话审计、账本划分、PG 计数器核对与编译检查；manifest 的 `verification.mode` 为 `row_audit` 或 `certified`。转换产物验收负责源事件到 Parquet / 完整索引展开的映射，独立 Parquet oracle 负责当前 / 历史键、版本和值，Backend 回放负责目标返回结果；这些检查不重新证明源行状态。参考回放按查询生成绑定查询身份、结果顺序和完整 `(PK, version, value)` 的强摘要，服务回放逐查询核对；原生条数仍是独立来源事实，摘要不得由服务响应反向生成。换 client 数、发送开关或 Backend 复用来源审计结论，并复核文件摘要。

三来源 PK 值 UPDATE 固定为范围外负载，提交及回滚尝试均拒绝；同主键普通列 / 二级索引列 UPDATE、DELETE / 重插入仍支持。CHAR 只忽略尾部 ASCII 空格，VARCHAR / TEXT 保留原文，REAL 按源单精度比较，date / timestamp 按类型比较。

## 身份与版本

```text
PrimaryKey   = (Table, IndexID=0, PK, CommitVersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, CommitVersionDescending)
Locator      = (FileID, FileRowPosition)
LogicalValue = RowID
```

- IndexID=0 为主键；TableID 按来源 schema 的表顺序从 1 分配（HyBench 即固定八表顺序，sysbench 为 sbtest1..N，YCSB 为 usertable），二级 IndexID 按每表源 `CREATE INDEX` 顺序分配。`IndexCatalog` 由来源 schema 生成；manifest 保存来源（benchmark、源码目录与 commit、schema 文件 SHA256、YCSB / sysbench 的 native 配置）和完整定义，预检据此重建并逐项核对，不接受自声明的部分清单。transfer 的 1/2/3 分别是 sourceid/targetid/type。二级非唯一谓词是前缀查询，不改为一次 PK Get。
- PK 与 IndexedValue 都是保序编码分量（`IndexKey`）：无符号字节序等于 PG btree 默认升序、NULLS LAST 的顺序（文本限 C collation），且分量自定界，拼接后按元组比较。NULL 为 `0x02`；非空以 `0x01` 开头：int2/int4/int8 为符号位翻转的 int64 大端；timestamp 为 1970 起微秒数同样编码，-infinity / infinity 为两端，BC 与亚微秒拒绝；文本为 UTF-8 加 `0x00` 终止（PG 文本不含 NUL），CHAR 先去掉尾部空格，与 bpchar 比较一致。PG 差分测试 `IndexKeyOrderIT` 核对 int8、timestamp、C collation 的 varchar / char 与 NULL 的排序。
- KV 字节布局：`Table(uint32 大端) ++ IndexID(uint32 大端) ++ [IndexedValue] ++ PK ++ (~CommitVersion 大端)`。点查 v 扫描 `[v, succ(v))`；区间下界含 v 用 `v`、不含用 `succ(v)`，上界含 v 用 `succ(v)`、不含用 `v` 作为排他终点，其中 `succ` 是 `IndexKey.prefixSuccessor`，对其后任意 PK 与版本字节成立。
- CommitVersion 由 SIBench 按原始会话顺序为每个执行过请求的已提交事务递增，包括只读提交，Baseline=0；同事务所有修改一致。连接建立与采集请求标签（`SET application_name`）提交的事务不执行请求，不分配版本，单列 `utility_only_transactions`；因此 YCSB autocommit 每个请求恰好推进一个版本。不使用业务 timestamp、墙钟、XID 大小或下游完成顺序。
- 多会话采集只在 `capture-run.json` 记录 `session_order=sequential` 时编译：原生客户端每个 workload 一个进程，进程退出且没有采集后端残留后才启动下一个，会话按 workload 顺序拼接，版本与工作单元连续；该顺序来自编排事实，不按 PID 或文件名推断。
- `session_order=concurrent` 的会话按 PostgreSQL 事务完成计数器（`ShmemVariableCache->xactCompletionCount`）归并。持有 XID 的事务（写事务，含回滚）在 `ProcArrayEndTransactionInternal` 持 `ProcArrayLock` 排他锁时自增该计数器，这正是它对其他快照可见的时刻；补丁在同一处把自增后的值记到该事务的 `PGPROC` 上（组清理时由组长代为执行，所以记在成员的 `PGPROC` 而不是后端变量），提交 / 回滚记录写出的就是这个精确值，因此"提交对快照可见"等价于"快照计数器 >= 提交位置"，写事务两两位置不同。不持有 XID 的只读事务不改变任何快照、不自增计数器，记录其提交时观察到的计数器，排在同一位置的写事务之后。归并按（位置、写事务优先、会话顺序）对事务大小的片段做 k 路归并，不按 PID、XID 大小、文件顺序或墙钟推断；会话内写事务位置必须严格递增、只读事务不得递减，两个写事务共用位置即失败。片段是事务粒度，所以扫描不会跨片段，访问划分按片段重新开始，避免不同后端复用同一个扫描指针。
- 回滚不推进 CommitVersion，不分配 RowID，不改变已提交索引状态，也不写入增量 Parquet。后续事务从最近的已提交状态继续。
- transactionId 是源提交事件序号。修改的 mutationOrdinal 是事务内行修改序号，二级展开共享序号；组内查询携带同一 transactionId，mutationOrdinal 表示查询前已发生的行修改数。两者只用于来源关联和顺序校验，不作为 KV 版本后缀，不要求索引维护事务状态。
- ReadTimestamp 表示查询的原语句快照（可见提交上界），在 T 选择同键最大版本 `<=T`。没有版本或选中删除版本时不存在，不能跳过删除标记返回旧值。
- **事务内读**：查询保留原语句快照，同时携带本源事务的 CommitVersion（SEARCH.version）和 mutationOrdinal，并标记 TRANSACTION_LOCAL_SEARCH。读取结果 = 快照 `<= ReadTimestamp` 的最新状态，再只叠加本事务在 mutationOrdinal 之前写入的该版本状态；两个位置之间提交的其它事务版本不可见。必须按原始位置发送：此前修改 → 查询 → 后续修改。事务结束后同键同版本只保留最后效果。旧 v9 编译产物以 `version=0` 且 `ReadTimestamp=本源 CommitVersion` 表示事务内读，读取器仍接受该历史形式，但不据此生成新 Trace。
- 顺序会话中，事务首次修改前的查询使用前一次提交的 ReadTimestamp。并发会话中，查询使用"其语句快照的计数器所见的最后一个写事务提交"（只读事务的提交不改变数据，不作读取点）：每个扫描事件记录该快照的计数器、xmin、xmax 和 xip，编译器取位置 <= 快照计数器的最后一个写事务提交作为 ReadTimestamp，因此可以早于其发送位置之前的提交，并标记 SNAPSHOT_SEARCH。编译器对每个扫描交叉验证：计数器判定为可见的提交，不得出现在该快照自己的 xip 里；冲突说明单个 ReadTimestamp 无法表达该可见集合，整次编译失败而不是近似。`suboverflowed` 的快照无法逐个核对子事务，不做交叉验证并单独计数。原始 xmin/xmax/xip 仍是可见性判定本身，不直接当作提交版本。显式历史查询选择可用历史点，未来读点拒绝。
- UPDATE 保留 RowID；DELETE 后重插入分配新 RowID。Rewrite 不改变逻辑版本或 RowID。
- FileID 从 1 分配且不复用；文件内行号从 0 开始。数据集隔离，int64 无损解析。

## 工作单元与路由

`workUnitId` 是请求工作单元身份，与索引键、transactionId、CommitVersion、PID 和物理节点身份独立。编译器把一个经过审计的顶层源 SQL 执行（JDBC/PG 配对或服务端请求序号）作为一个工作单元，在 `executor_start` 处按源请求顺序从 1 编号，先编号再进行回滚或二级过滤。同请求的多个扫描、嵌套查询、行修改及其主键/二级展开共用身份，不把一个请求人为拆成多个工作单元。

`work-units.jsonl.zst` 按编号记录 `work_unit_id`、原始 `application`、`pid`、会话内请求序号 `request` 和请求开始的 `source_event`（跨会话连续编号），其 SHA256 写入 manifest；二进制操作保留 `sourceEvent` 和 `workUnitId`。没有有效索引操作的请求仍在身份清单中，不占用运行期发送量。单个 SQL 工作单元是明确的执行任务近似，不声称恢复了真实 serverless task DAG 或数据节点拓扑。

回放使用固定的 `clientId = (workUnitId - 1) % client_count`，不保存固定 clientId、不按索引键 hash、不绑定源事务、不提供替代路由策略。相同工作单元始终使用一个 client；同一事务的多个工作单元允许跨 client。改变 client 数只改变发送连接，不改变 Trace、版本、Parquet 或操作序列。Baseline 使用连续每 4096 行一个加载工作单元，每行索引展开共用身份；加载身份与运行期 SQL 身份分别统计，Baseline 覆盖不能代替运行期覆盖。

`client_count` 默认 1，64 和 128 为实验配置而非硬上限。预检在二级过滤后检查实际操作覆盖全部配置 client，而不只判断 distinct 工作单元数。覆盖不足在真实加载/发送前拒绝，应延长原生采集，不重复 Trace 或伪造单位填满 client。报告包含 configured/used clients 及各 client 操作、查询、写入和工作单元数；覆盖不等于负载均衡或连接并行吞吐。

## 当前二进制格式

v9 每条记录为 128 字节固定前缀加四个可变键分量，每块最多 4096 条、little-endian 和 CRC32C。前缀依次为 sourceEvent、kind、table、index、flags、version、file、position、rowId、expectedCount、transactionId、mutationOrdinal、readTimestamp、workUnitId、limit、expansionPosition、expansionSize，及 key（PK）、indexedValue、lower、upper 四个 int32 长度（-1 表示不存在，单个最多 32,768 字节），其后依次为分量字节。写入记录携带 key，二级写入另带 indexedValue（NULL 也是一个编码分量），不带 lower/upper；SEARCH 带 lower/upper，缺省一端即无界，二级钉住点查另在 key 中带 PK。解码拒绝损坏长度、畸形分量、非法 UTF-8 和尾部多余字节；table 1–65535、index 0–1024。`workUnitId` 对控制记录为 0，编译生成的运行期/加载操作为正数。

| 操作 | 语义 |
| --- | --- |
| INSERT | Baseline 索引加载 |
| SEARCH | 有序查询：主键比较 PK，二级比较 IndexedValue，结果按 (IndexedValue, PK) 升序（DESCENDING 时降序），limit>0 时取前 limit 个可见项且期望数等于 limit；`readTimestamp>=0` 为原语句快照；普通 / 快照查询 version=0，TRANSACTION_LOCAL_SEARCH 的 version 为本事务 CommitVersion（历史形式为 0）；flags：SNAPSHOT_SEARCH=1、TRANSACTION_LOCAL_SEARCH=4、LOWER_INCLUSIVE=8、UPPER_INCLUSIVE=16、DESCENDING=32、INTERSECT_GROUP=64、UNION_GROUP=128。**钉住点查**：二级点区间且 key 带 PK，恰好命名一个索引条目 (IndexedValue, PK)，期望数 1，无 limit、不入组。**查询组**：带 INTERSECT_GROUP 或 UNION_GROUP 的连续查询是同一次源访问，`expansionPosition` 0..n-1、`expansionSize`=n≥2，成员共享 sourceEvent、table、readTimestamp、version、事务位置、工作单元和组期望数；交集组成员可在同表不同索引，并集组成员是同一索引的点；期望数是成员结果按 PK 求交 / 求并后的行数，成员自身的条数不是源事实，不单独校验 |
| UPSERT | 带提交版本的写入；flags：业务 INSERT=1、UPDATE=2 |
| LOGICAL_DELETE | 显式版本化失效，不使用合法 Locator/RowID 作为哨兵 |
| RELOCATE | 更新完整索引键的确切版本位置；删除或不存在版本拒绝 |
| TRANSACTION_BEGIN / COMMIT | 已提交源事务的操作组边界，TableID=0；仅驱动消费，不发送给目标索引 |

非 SEARCH 的 readTimestamp=-1。每行由 `IndexMutation` 显式展开主键及该表全部二级索引，发送端不得再次隐式维护。写入接口不提供隐式的 transfer-only 展开。

规范 Trace 保留已提交源事务内查询与修改的原始相对顺序，查询占据原生扫描开始的位置，候选数在扫描结束时补齐。每次修改先写主键，再按 IndexID 递增展开；索引列值改变时对旧 `(Table, IndexID, IndexedValue, PK)` 写同提交版本的 LOGICAL_DELETE，再对新身份 UPSERT。未改键的二级条目也 UPSERT，以刷新行版本及 Locator / RowID。这不是修改索引定义，也不是删除业务行或物理删除历史版本。

Baseline 每索引 INSERT；运行期 INSERT 每索引 UPSERT；DELETE 失效主键及全部二级条目。同主键重插入分配新 RowID，UPDATE 保留原 RowID。旧、新条目及主键共享来源事件、版本、事务、mutationOrdinal、workUnitId 和 RowID；除删除条目无物理位置外，值位置一致。`expansionPosition` 从 0 连续编号，`expansionSize` 为完整展开宽度，查询和控制不得插入其中；查询组复用这两个字段表示组内位置与组宽，同样不得被其它记录打断。预检在二级过滤前核对全清单、完整展开和源工作单元绑定，截断或漏项拒绝。

增量文件按实际表惰性创建 `<table>-versions.parquet`，FileID 从基线最大值加 TableID 分配；每次写入保留 `_sib_timestamp`、`_sib_mutation_ordinal` 和 `_sib_row_id`。同键同版本按序覆盖，中间效果服务于原始事务内查询，不产生额外提交版本；后续源事务只能看到最终提交效果。主键值 UPDATE 是范围外操作，明确拒绝；增量 rollover 尚未实现，到达阈值使编译失败而非丢弃后续行。

格式只支持 v9：读取其他版本一律拒绝。

## 发送选择

默认只向下游发送已提交变更；这一约定不限定具体 Index Service 场景。原始 Trace 完整保留修改尝试、提交和回滚，规范转换识别事务结果，只将已提交业务变更展开为下游索引修改。回滚事务的修改不发布，不生成反向补偿操作，也不增加回撤开关。正常业务回滚不等于采集失败；未完成事务不得当作回滚处理后静默消费。

这里的“只发送已提交变更”约束写入，不关闭查询。成功提交的事务中，写后查询按上述时间戳和事务内位置转换；回滚事务中，首次修改前的受支持查询保留，首次修改后的查询不发送，因为其读取环境依赖已排除的修改。当前按“此前是否出现修改”保守筛选，不猜测查询与修改键的依赖关系；排除量独立报告。

编译器在看到源 commit 后才生成该事务的规范操作组及增量行，遇到配对审计通过的 abort 则排除修改及其后的查询。规范 Trace 不生成 ABORT 或反向操作；回滚事实在原始 Trace 中保留。manifest 的 `change_delivery=committed_only`、`raw_row_changes`、`discarded_aborted_row_changes`、`logical_row_version_writes` 和 `index_version_writes` 分别说明发送约定、原始行修改、回滚排除量、已提交行修改和展开后的索引写入；另报告 `transaction_local_searches` 和 `discarded_aborted_transaction_local_searches`。`aborted_transactions` 包括显式回滚和关闭连接产生的 abort。PG `index_insert_attempt` 不等同于下游写入。

```yaml
target:
  secondary_indexes:
    enabled: true
replay:
  strategy: single_pass
  dispatch_mode: operation
  client_count: 1
  max_in_flight: 4096
  request_timeout_seconds: 60
```

二级开关过滤所有 IndexID>0 的加载、查询、写入、逻辑失效和搬迁，不改变主键身份、源顺序、版本、依赖或 Parquet，不替换被过滤查询。查询组整体发送或整体过滤：任一成员在二级索引上且二级关闭时整组过滤，不留下半个交集。

| dispatch_mode | 驱动输入单位 | 发送契约 |
| --- | --- | --- |
| operation（默认） | 一个索引操作 | 完整源组校验后，逐项路由及本地发送 |
| transaction | 一个已提交源事务的有序操作列表，含查询 | 按源组组织输入，组内仍逐项路由及本地发送，可跨 client |

两种模式发送完全相同的操作序列，不合并同键修改，不移动查询，不跨事务重排。`transaction` 不是一组一个 client 或一次组 RPC，不要求原子执行、BEGIN/COMMIT RPC、回滚或索引事务缓存；分组统计属于驱动，不是 adapter 调用数。

一个全局发送线程按 Trace 顺序调用 client，等待当前操作的 `sent` 确认后才发送下一项。`ReplayTarget.SendHandle.sent` 表示完整本地传输完成，不能只是提交到会被重排的客户端异步队列；`response` 表示 adapter 已关联、核对该请求的服务响应，错误或结果不符应异常完成。response 可以乱序完成；不同连接的远端到达、执行及可见顺序不是此本地发送契约。Index Service 自行处理跨 client 语义，benchmark 不增加可见性屏障、反向操作或自动重试。

全局 `max_in_flight` 限制未完成响应数量，默认 4096；达到上限等待响应腾出额度，结束时排空。超时从调用 send 前开始，覆盖本地发送和响应等待；`request_timeout_seconds` 默认 60，允许 1–86400。adapter 的 send 调用必须及时返回句柄，不能无界阻塞；真实 transport 超时与 close 取消资源由 adapter 实现。失败后停止后续发送并关闭已打开 client、取消未完成 future，不自动续接。初始加载完成并排空后才进入运行期，这是加载边界，不是运行期跨 client 可见性屏障。

驱动先缓冲并校验完整源组，确认尾部 COMMIT 后才发送；目标索引收到的是独立版本写入与带 ReadTimestamp 的查询，没有控制记录。每组最多 1,000,000 个展开操作（含被过滤操作），编译器也限制待处理源操作数量；这是输入保护，不是大规模可落盘状态方案。并发会话按 PostgreSQL 事务完成计数器归并后，发送顺序是提交顺序，查询携带其快照所见的 ReadTimestamp，可能早于前一个已发送的提交（SNAPSHOT_SEARCH）。

报告区分落盘记录（含控制）、来源索引操作、二级过滤、实际发送、源事务及各操作类型。`target_calls` 在两种模式下均是逐操作 adapter 调用数，不是网络请求数；`source_dispatch_groups` 仅表示 transaction 模式的非空源组数。报告还包含不受 client 数影响的 `operation_order_checksum`、completed/pending responses、peak in-flight 和背压等待时间；`sending_threads=1`、`service_visibility_owner=index_service`、`downstream_transaction_api_required=false` 明确责任。参考 adapter 立即执行并共享模型状态，只验证路由及操作/结果，不模拟网络并发可见性。RowID 搬迁无索引写入。历史范围、完整并发和 GC 未实现。

## 完成与失败

派生 Baseline 包含 `resolved-config.json`、`files.jsonl`、`manifest.json` 和 Parquet；未完成保留 `INCOMPLETE`。规范编译同样用完成 manifest 和校验值发布，消费者拒绝不完整或被修改产物。

原生运行在 raw 目录写 `capture-run.json`：

- `running`：未完成，不能编译。
- `failed`：保留首个错误、SQLSTATE、请求/事务上下文及堆栈；不能编译或续接。
- `complete`：连接正常关闭，同次请求、结果和提交关联审计通过（各会话并行审计）。清单统一记录 `sessions`（每个 PG 追踪及配对 JDBC 追踪的 SHA256、是否要求扫描键为客户端绑定值、审计结果）、`session_order`（`single`、`sequential` 或 `concurrent`，HTAP 另存 phases）和 `build`。消费前复核全部会话文件、摘要与清单，并要求每个会话记录了通过的审计；审计本身不重复，摘要未变即沿用其结论。缺少会话清单的运行拒绝，需重新采集。

规范编译带 `--counters` 时，在提交 manifest 前按 PG14 计数位置核对同遍构建的账本（`capture-completeness.json`），不一致即失败、保留 `INCOMPLETE`；manifest 记录 `access_ledger_sha256`、`capture_completeness`（`consistent` 或 `not_checked`）与 `business_changes_by_table`。Trace 文件的 SHA256 在写入时计算。

JDBC 异常即使被原生方法捕获也会使当前运行失败；记录或清理异常不覆盖原始原因。审计不等于全局丢事件证明。

非预期错误依据诊断修复，恢复明确初始状态并换新目录重跑，不猜测 PG 内部实现、不补造事件。原始审计核对显式业务回滚和关闭 abort 的请求身份；关闭时仍有事务必须存在对应服务端结束边界，不能因文件已关闭而静默丢弃。八表状态审计和支持范围内的编译均排除回滚修改，不据此拒绝整次有效采集，也不自动重试。编译仍拒绝事务 XID 不匹配、未完成扫描、缺少完成位置或快照冲突的输入；发送仍拒绝无匹配边界、版本/查询位置错误、展开不完整及线程切换。最终状态正确不能替代全局事件完整性证明。

## 长时输入与验证

策略固定为足量连续原生执行、事务边界分段、离线编译和 single_pass；不循环、不重排、不重映射键。分段续编译和 Checkpoint 尚未实现。

预算按二级过滤后的有效运行期操作计算，两种发送模式预算相同，排除 Baseline、控制和派生探针：预热操作 + ceil(速率 × 测量秒数 × (1 + 余量))。`long_replay.yaml` 为 18,000,000 + 300,000 × 1,800 × 1.2 = 666,000,000。不足在加载/发送前拒绝；当前预检只证明数量，不证明持续调度。

`check-replay` 校验 manifest、摘要、固定源索引清单、完整展开、源工作单元绑定、格式、记录数、CRC、过滤后的 client 覆盖和预算，不调用真实 adapter；`replay` 执行两种 value 的状态与查询核对。裸文件 `runFiles/checkFiles` 仅用于受控测试，不具有来源清单完整性证明，生产入口使用目录级预检。

`validate-snapshots` 要求带源码与基线来源的 v9、无 relocation。oracle 独立读取基线和增量 Parquet，不从二级 Trace 值推导预期：核对全部修改主键与完整二级身份在基线、每次该键提交前/提交点和最终点的 key/version/Locator/RowID；每个源二级索引最多 64 个确定性等值谓词（一半取修改过的值，其余按基线值域均匀分布、含首尾）；主键与每个源二级索引的带 limit 区间探针取自值域的 9 个分位边界（相邻边界上下界分别以开 / 闭从各自一侧检验，升序与降序）、无界两向和修改值锚定区间。谓词与区间在基线、首次相关修改前/修改点和最终点逐项核对结果的 key/version/value 及顺序。源文件摘要、增量位置、RowID、版本和事务内修改位置同时核对。查询组只经其成员检索覆盖；该抽样不是全谓词证明；探针独立报告，不写入 Trace 或计入原生负载。
