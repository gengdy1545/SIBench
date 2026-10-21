# SIBench 实施计划

更新：2026-10-21。

## 目标

以原生 benchmark、真实 Parquet 和可复现索引 Trace，比较不同 KV Index Service 的加载、查询、版本写入及维护成本，不限定具体下游应用场景。

主流程：原生数据生成与 PostgreSQL 加载 → 静止边界导出及验证 Baseline → 原生负载采集与审计 → 规范 Trace / 增量 Parquet 编译 → 索引回放与测量。

**当前阶段**：原生 HyBench HTAP 采集与 SF1 状态审计、单会话 v7 全索引转换及参考验证已完成；YCSB / sysbench 已按相同的 8,706,000 行基线完成单会话原生采集，并通过访问账本。下一步补齐完整 HTAP 访问 / 快照投影、YCSB / sysbench 的规范转换并接入真实 Index Service；SF1000 正式实验尚未启动。

## 固定约定

| 领域 | 约定 |
| --- | --- |
| 来源真实性 | 固定上游提交、PG 环境和一次采集事实；保留原生选择器、比例、业务分支、计时及 SQL。不重跑 SQL 补结果，不模拟业务，不猜测修复 PG 内部实现。派生 Baseline 单独标识，不替代原生执行。 |
| 审计与失败 | 原始事实与规范操作分层保存，转换可追溯；不支持的语义显式拒绝。非预期错误使采集无效，保留诊断，修复后从明确初始状态换新目录重跑，不拼接失败前缀。预期业务失败按来源语义处理；最终状态正确不能替代访问负载审计。 |
| 核验职责 | `RowStateAudit` 在采集后统一核对源行修改链和最终 PG 状态，同一份输入只审计一次。编译与维护诊断验证摘要并复用来源结论；转换产物验收只核对源事件到 Parquet / 索引操作的映射，Backend 回放只验证目标结果，不重复核验源行修改链。 |
| 提交与查询 | 默认只发送已提交变更；原始 Trace 保留修改尝试和回滚，不生成反向补偿或回撤开关。当前单会话转换排除回滚修改及修改后的查询，保留修改前的受支持查询；已提交事务保留写后查询及原始操作位置，不合并修改。 |
| 时间戳与发送 | 同源事务一个 `CommitVersion`，查询携带独立 `ReadTimestamp` 和此前修改位置。`operation` / `transaction` 只决定驱动输入单位，最终均逐项发送；源控制记录不发送给索引，不要求下游事务 API。 |
| client 路由 | 默认 1 client，按源 SQL 请求工作单元确定性轮询，不按索引键或事务绑定；同请求的展开共用 client，同事务允许跨 client。同一 Trace 支持 1/64/128 及更多 client，过滤后必须覆盖全部配置 client，不伪造身份或重复负载填充。 |
| 顺序与可见性 | 一个全局线程保留事务间及事务内本地发送顺序；不保证不同连接的远端接收、执行或可见顺序。Index Service 负责跨 client 语义；发送确认与响应独立，在途上限、超时和排空不作为可见性屏障。 |
| 索引与布局 | 主键和二级索引使用统一 KV 契约，二级默认全部启用，发送开关不改变源数据、完整 Trace 或 Parquet。索引列值 UPDATE 不是索引定义变更，也不使用业务 timestamp 作为 CommitVersion。Baseline 只 INSERT，增量追加写；Rewrite 不改变键、逻辑版本或 RowID。Locator 更新位置，RowID 不产生搬迁写入，不增加位置映射服务。 |
| 实验与测量 | 长时输入来自足量连续原生事件，`single_pass` 消费，不循环、重排或重映射键。各 Backend 复用相同数据、Trace、开关及确认语义；内部缓存、持久化和 compaction 由 Backend 负责。索引性能与 SQL、文件生成、Rewrite 数据读写、Lookup 后数据读取分别计量。 |

具体字段、失败处理和责任边界以 [Trace 契约](docs/contracts/trace.md)、[存储契约](docs/contracts/storage.md) 为准。

目标来源限定 HyBench、YCSB JDBC、sysbench PG 的固定标准布局：单列主键及来源实际定义的普通单列二级索引。不要求额外 UNIQUE、复合、表达式、partial 或动态 DDL。标准负载不改写已有行的主键值；PK 值 UPDATE 是固定范围外操作，提交及回滚尝试均拒绝，不列为待实现功能。普通列 / 二级索引列 UPDATE 及同主键 DELETE / 重插入仍在范围内，详见 [来源 Schema 契约](docs/contracts/source-schema.md)。

## 当前能力与范围

| 领域 | 当前实现及边界 |
| --- | --- |
| 工程与派生数据 | JDK 17 / Maven、职责目录、平铺配置、固定 benchmark 子模块；HyBench / YCSB / sysbench 的 Java Baseline、源码指纹校验和 dry-run。 |
| 来源 schema 与 PG 预检 | 统一 catalog 覆盖 HyBench 八表完整列及 8 PK / 13 二级、YCSB JDBC 字符串 PK / TEXT 字段、sysbench 配置表数及每表 id PK / k 二级。只读核对实际表 / 列 / 默认值 / 索引定义和有效性、约束及非限定名称解析；HyBench 导出 / run 前强制检查并保存证据。三来源独立 CLI、实际 SF1 和三来源 PG 布局 / 差异集成核验通过；YCSB / sysbench 原生采集入口接入仍待完成。 |
| HyBench 原生采集 | PG 插桩、JDBC 请求关联、八表行变化、事务和结果采集；异常追踪、完成审计及摘要校验。默认 `runHTAP`：AP1–13 → TP1–18 → 并发 AT1–6/IQ1–6。当前限定 native SF1、PG14.24、read_committed、无 parallel worker、postgres 角色、Zstd 和 `results/poc`，各类源会话配置最多 256；诊断 `tp_subset` 只允许单会话 TP3/5/9。 |
| YCSB / sysbench 原生采集 | `NativeCapture` 与 `scripts/native-capture.sh`：新库、上游客户端建表加载（YCSB 多线程、sysbench 并行 prepare，均不采集）、来源 schema 预检、Baseline 导出与逐值核验、单会话原生执行、`RowStateAudit`、访问账本。参与执行的上游源码须与固定子模块逐字节一致。客户端自报数量与追踪对账：YCSB 逐请求配对 JDBC 与 PG；sysbench 无 JDBC 层，以服务端请求序号和脚本计数核对。限单会话，当前 8,706,000 行；无规范 Trace。 |
| 访问账本 | `AccessLedger` / `access-ledger` 把三来源的原始 PG 索引事件划分为访问单元（一次索引重扫一个单元），每个事件恰好归属一处，否则失败。候选条目、堆可见行、SQL 返回行分列且不相加；规划期端点探测排除并单列；请求序号和嵌套深度使 SPI 查询归属外层请求；`plan_scan` 绑定计划节点。只读追踪、不连接 PG；不接受缺少请求序号的追踪。`index_insert_attempt` 是物理写尝试，不等于逻辑键维护。 |
| 文件与规范 Trace | 统一 Parquet writer，256 MiB 目标、1,048,576 行组上限；v7 保留提交版本、读取点、事务内位置、源 SQL 工作单元及连续维护展开，支持可空 UTF-8 等值索引值。目录级预检核对源码清单、摘要、工作单元绑定和完整展开；只支持 v7，其他版本拒绝读取。完整 HTAP 输入仍被单会话编译器拒绝。 |
| 索引清单与维护 | `IndexCatalog` 复用来源 schema，保持 HyBench 历史索引编号及清单顺序。提交 INSERT / UPDATE / DELETE 统一展开；改键为旧身份 LOGICAL_DELETE + 新身份 UPSERT，未改键也刷新关联版本 / 值；UPDATE 保留 RowID，重插入分配新身份。PK 值 UPDATE 及范围外索引定义拒绝；YCSB / sysbench catalog 支持不代表其 Trace 编译完成。 |
| 源行状态审计 | `RowStateAudit` 按三来源 schema 统一核对完整列和旧 / 新行；支持字符串及非连续 int64 主键、DELETE / 重插入与插入键交错。单会话保留来源顺序，多会话连接保留各会话同键顺序的因果链，不推断全局提交。静止边界最终 PG 全行对账，摘要绑定报告和已审计变化；重复调用不查询 PG。Baseline 按来源 schema 验证全值和身份，要求主键有序、RowID 连续。 |
| 提交变更转换 | 单会话支持范围内的回滚筛选与已提交事务写后查询，不合并同事务修改；回滚不推进版本或分配 RowID。编译前要求匹配的源行审计报告，不再维护普通列完整旧行核验模型；原始、排除、提交及发送量分别报告。 |
| 多 client 驱动 | 两种输入模式、全部二级统一过滤、单发送线程、源工作单元路由、异步响应、在途上限、超时、排空和失败清理；过滤后 client 覆盖及单次输入预算预检。源 SQL / 基线每 4096 行工作单元是任务近似，不是实际 serverless DAG；参考 client 共享进程内模型，不模拟网络可见性。 |
| 版本参考与查询核验 | Locator / RowID、通用表 / 索引命名空间、当前及历史主键 / 二级等值、null 和非连续 int64 PK、逻辑删除及全索引精确版本搬迁。Parquet oracle 全改动身份逐版本核对，非唯一谓词每索引最多 64 个抽样；裸文件回放不证明源清单完整。原生 null 扫描标记拒绝，无范围、复合、表达式或 collation 等价转换。 |
| 状态与文件生命周期 | 参考模型使用分页 PK 存储和稀疏历史，但仍驻内存；源事务缓冲最多 1,000,000 个展开操作。Locator 打包限制 FileID <= `Integer.MAX_VALUE`、行位置 <= `0xffffffff`；增量 rollover 到阈值拒绝。搬迁控制仅选择实际修改表的最新存活行，不保留受保护历史集合、不自然触发、不回收旧文件；不是 SF1000 有界状态方案。 |

HyBench 完整 TP / AT 的索引列值更新来自 `loanapps.timestamp`（AT3）和 `loantrans.timestamp`（TP14/15/16、AT4）。TP3/5/9 子集仅更新 savingaccount 普通列并插入 transfer，不包含已有索引列值 UPDATE；默认 `runHTAP` 不调用 freshness DELETE。实现支持与原生实际覆盖分别验收，具体来源见 [能力矩阵](docs/benchmarks/capability-matrix.md) 和 [HyBench 适配](docs/benchmarks/hybench.md)。

## 验证证据与边界

| 验证 | 已通过证据 | 不代表 |
| --- | --- | --- |
| SF1 Baseline / 原生 HTAP | 八表 8,706,000 基线行的 PG / Parquet 全值和身份核对；四会话 HTAP 请求关联、最终八表状态及原始事件无损打包通过，见 [HTAP 验证](docs/validation/hybench-sf1-htap.md)。 | 大规模容量、全局事件无丢失、完整 HTAP 访问 / 快照投影已验证。 |
| 统一行审计实库重验 | JDK17 / PG14.24 上，原生 HTAP 最终 8,707,767 行、5,408 次提交修改，以及 TP 子集最终 8,731,627 行、76,881 次提交修改全表对账通过。TP 使用审计报告重新编译的 Trace / Parquet / 工作单元与既有通过产物摘要一致，128-client 预检通过，见 [统一审计验证](docs/validation/row-state-audit.md)。 | 原生 SQL 重跑；全局事件无丢失或并发查询快照等价已证明。 |
| SF1 全索引 TP 子集 | 基线 31,212,000 索引条目；重复编译 Trace / Parquet / 工作单元字节一致、manifest 结构化一致；12 组 client / 模式 / 二级变体及最终构建 128 client Parquet oracle 通过，见 [全索引验证](docs/validation/hybench-sf1-secondary-indexes.md)。 | TP 子集发生了索引列值 UPDATE；真实网络、吞吐或持久化已验证。 |
| SF1 原生改键维护 | 真实已提交改键 34 次：loanapps.timestamp 15 次、loantrans.timestamp 19 次，逐键因果维护及两种 value 核验通过。 | 逐键派生版本是全局提交顺序；完整 HTAP 快照或规范 Trace 已实现。 |
| SF1 搬迁控制 | 最新存活修改行的真实 Parquet 内容不变，全部关联索引精确版本 Locator 搬迁与 RowID 无搬迁写核验通过。 | 自然 Rewrite、受保护历史集合搬迁、发布屏障或旧文件回收已完成。 |
| YCSB / sysbench 原生采集 | 与 HyBench SF1 同为 8,706,000 行基线，逐值核验通过；单会话原生客户端：YCSB A/B/C/F/D/E 共 349,959 个 JDBC 请求，sysbench 六个脚本 194,000 次执行 / 58,000 次提交；客户端自报数量与追踪对账、行状态审计、访问账本通过。SF1 HTAP / TP 子集的历史追踪在请求序号加入前经同一账本划分通过（该兼容路径已移除，仅作历史证据），见 [原生采集验证](docs/validation/ycsb-sysbench-native-capture.md)。 | 规范 Trace、多会话、性能、无丢事件，或大表计划稳定性已验证；行数相同不代表索引条目或字节量相同。 |
| 实际 PG schema 预检 | PG14.24 上 51 项检查通过：既有 HyBench SF1、源 DDL 和三来源独立布局、完整表 / 列 / 默认值 / 索引差异、quoted 名称与 search_path；差异发生在导出创建输出及 run 业务采集前，保留失败诊断，见 [预检验证](docs/validation/source-schema-preflight.md)。 | YCSB / sysbench 原生客户端生成 / 执行已验收；运行期外部 DDL 漂移检测已实现。 |
| 受控语义与预检测试 | JDK17 完整构建 113 项测试零失败，覆盖三来源 schema、完整行审计及摘要复用、双会话同键因果链、提交 / 回滚 PK 改写拒绝、列 / 索引差异、JDBC 元数据与搜索路径、采集失败证据、访问账本划分与拒绝规则、服务端审计、autocommit 提交核对、构建副本一致性、采集配置与行数对齐，以及回滚改键、写后查询、DELETE / 重插入、版本搬迁、展开完整性及异步发送边界。另有三来源实际 PG 采集与行审计 9 项检查通过，包括 DELETE / 重插入、写入后回滚及提交 / 回滚 PK 改写拒绝，见 [统一审计验证](docs/validation/row-state-audit.md)。 | 受控 SQL 是原生客户端执行；原生负载实际覆盖全部受控操作；原生 DELETE / 重插入及写入后回滚已有实跑证据。 |

默认采集使用 `config/hybench_sf1.yaml`，诊断 TP 子集使用 `config/hybench_tp_subset_sf1.yaml`。实验数量、摘要、命令和历史记录保留在验证文档及产物中；本计划仅记录当前验收依据与能力边界。

## 后续工作

以下只列剩余工作，按主线依赖关系组织。网络 adapter 可基于现有 TP 子集接入；完整 HTAP 的下游验收依赖其访问与快照投影完成。部分实现和固定特例的扩展任务归入对应领域，不以通用类名或 SF1 测试代替完整验收。

### 1. 补齐原生规范转换

- [ ] **完整访问投影**：(a) 把单列等值访问投影到 YCSB 字符串主键和 sysbench 表的规范 Trace，依赖第 2 节的保序编码；(b) 账本只给出谓词形态的全局分布，应按 SQL 模板输出，使 SF1 HTAP 中 3 个范围、3 个数组 / 行比较、30 个无键扫描单元能定位到具体模板；(c) 采集 bitmap 堆扫描节点的 recheck 与 residual filter 剔除数（现有钩子在索引 AM 层，`recheck_flagged_candidates` 恒为 0）；(d) AP 的 join / 重扫结果与账本交叉核对。验收：每个原生 SQL 模板的每个访问单元要么投影，要么带原因拒绝；聚合、排序或 LIMIT 使返回行数不等于命中数的模板分别报告，不以返回行数代替索引命中数。未支持谓词显式拒绝，范围及变长键使用第 2 节的公共能力。
- [ ] **计划身份与漂移**：`plan_scan` 已记录每个扫描的计划节点，但账本不按模板汇总计划，也不比较同一模板的差异。例：YCSB workload E 的 scan 在一次小规模运行中走 bitmap、另一次不走；大表上 E 有 552 次规划期端点探测，其余 workload 为 0。修复：账本按模板输出计划节点序列，同一模板出现多种序列时标注，并在规模变化前后分别比较。sysbench 的 prepared statement 是否在若干次执行后从定制计划切换到通用计划尚未验证，需先验证。验收：能复现并定位上述差异。不用 `enable_*` 或 hint 固定计划，那会改变来源行为。
- [ ] **并发顺序与快照**：采集可验证的跨会话顺序事实，保留各会话请求 / 修改顺序，并建立源提交到逻辑版本的映射；不能按 PID、XID 大小、文件排序或墙钟推断全局顺序。按实际语句快照映射可见提交及事务内此前修改，验证单一 `ReadTimestamp` 能否表达原生可见集合；不能表达时先补齐契约或拒绝，不复用单会话写后查询假设。
- [ ] **来源预期失败与重试**：按固定源码明确业务回滚、预期 SQL / API 失败、原生重试和非预期错误；保留 SQLSTATE / 状态码、请求与事务身份、每次尝试及最终结果，只发布成功提交的修改。重试仅保留来源实际行为，不由编译器或发送端补造；完成分类和实跑验证前，当前 JDBC 异常使采集失败的规则不放宽。
- [ ] **采集完整性与开销**：跨会话全局丢事件检测及故障注入；核对请求、扫描、行变化、事务边界和原始 / 排除 / 提交 / 展开操作量，分别测量插桩前后开销。请求配对、CRC、最终状态或账本的恰好一次划分正确都不能单独证明无丢事件。增加独立计数器核对：以采集前后 `pg_stat_user_indexes` 的差值核对账本。PG14 的 `idx_scan` 在首次下降索引树时计数（`nbtsearch.c`），对应至少取回一次的访问单元而非全部单元；`idx_tup_read` 对应候选条目，`idx_tup_fetch` 对应 `heap_visibility` 命中。要求期间无其它会话访问，并排除导出与审计的有序扫描。
- [ ] **通用访问审计**：HyBench 的 `PgQueryAudit` 仍是 TP3/5/9 的 transfer dense int 数组特例；以账本和实际 schema / manifest 核对多表、多规模访问并取代它，不将历史 TP3/5/9 特例用于完整 HTAP 验收。行状态核验统一由 `RowStateAudit` 负责，不新增并行修改链检查器。
- [ ] **YCSB / sysbench 规范转换与多会话**：(a) 规范 Trace 编译。YCSB 的 autocommit 连接上，采集用 `SET application_name` 给请求加标签，标签语句本身提交一次，所以每个请求在服务端有 2 次提交、其中一次是空事务；按提交推进 `CommitVersion` 的编译必须排除它，验收为一个 autocommit 请求只推进一个版本。(b) sysbench 对账把控制语句数按每 event 2 条推断，判断脚本是否使用显式事务的方式是匹配源码子串 `prepare_begin()`；应由脚本实际调用 begin / commit 与 `skip_trx` 推出，并以服务端观察核对。(c) 多线程 / 多会话采集，`threads` 目前只接受 1。YCSB 保留 A–F 比例、字段更新、scan 上限及 read-modify-write 非默认原子性，标准 JDBC 仅主键。sysbench 保留 event 内顺序，分别标识 k 索引列 UPDATE、c 普通列 UPDATE 和 DELETE / 重插入，不混算覆盖。
- [ ] **完整转换验收**：完成访问 / 快照投影后，独立核对原生查询可见集合、提交状态、增量 Parquet 和两种 value 的当前 / 历史结果，再验多 client；扩展当前抽样 oracle 的覆盖以满足目标负载。逐键维护审计、受控用例或 TP 子集回放不能替代完整原生验收。

### 2. 补齐公共查询能力

- [ ] 面向真实 KV Backend 的整数 / timestamp / CHAR / VARCHAR 与 YCSB 字符串主键保序编码，当前与历史范围查询及其原生投影；v7 可空 UTF-8 字段只解决 Trace 的等值索引值表示，不代表保序编码完成。不扩展为来源不存在的复合索引。
- [ ] 目标负载需要的原生 null 谓词、collation 及其它查询等价语义；未实现时保持显式拒绝。
- [ ] point / range / snapshot 独立发送开关，默认开启；未出现的原生查询不得凭空补入。

### 3. 接入真实服务与测量

- [ ] KV 网络 adapter：完整本地发送确认、异步响应关联及结果核对、transport 超时和资源清理。
- [ ] 验证服务端声明的版本查找、同版本有序覆盖及跨 client 语义；驱动不通过可见性屏障修复结果。
- [ ] 保序限速、预热及测量流程，保持一个发送线程，不以并发调度改变原生顺序。
- [ ] 正确性、持久化、恢复及后台排空验证；记录延迟、吞吐、索引字节、物理 I/O、放大和资源指标，不以进程内立即响应、裸解码速率或 Trace 上传代替服务验收。

### 4. 补齐版本与文件生命周期

- [ ] 固定保留窗口、过期与快照保护规则，实现物理 GC。
- [ ] 按当前版本与受保护历史集合实现自然 Rewrite、确定性装箱、发布屏障及旧文件回收；独立最新版本搬迁控制不替代生命周期实现。

### 5. 支持大规模连续输入

- [ ] 源行审计的修改集合、编译、源事务缓冲、工作单元和参考历史的有界内存 / 可落盘状态，以及增量文件 rollover；通用 schema / 主键支持不代表大规模审计容量已验收。
- [ ] 通用 int64 FileID / 行位置 Locator，解除参考打包宽度限制并验证全部身份边界。
- [ ] 连续事务边界分段、确定性续编译、Checkpoint 及恢复。
- [ ] 把 HyBench 的 `CapturePoc` 固定 SF1 入口并入 YCSB / sysbench 已用的 schema 驱动编排（`NativeCapture`、`native-capture.sh`）；按目标负载推广当前 PG / 会话 / 角色 / codec / 输出目录限制。失败后遗留的数据库需手工删除，应给出明确的清理步骤，仍不自动删除证据。回放 client 无 128 硬上限不代表采集限制已解除。
- [ ] SF1000 原生 schema 的 int64 ID 适配、资源预算及长时采集入口；容量与输入供应验收前不直接复用 SF1 固定模型或派生生成配置。

### 6. 校准并开展正式实验

- [ ] 三来源分别进行样本容量校准；采集验证已按 8,706,000 行对齐，正式实验的容量仍按各来源校准，不要求行数、索引条目或字节量相等。
- [ ] 完成真实服务上的 1/64/128 client、查询及二级开关对照。
- [ ] 128/256/512 MiB 文件布局对照、真实存储与至少 30 分钟连续回放实验。

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
- [来源 Schema 预检验证](docs/validation/source-schema-preflight.md)：离线差异 / JDBC 测试、实际 SF1 与三来源 PG 集成证据及边界。
- [统一行状态审计](docs/validation/row-state-audit.md)：一次审计的职责、三来源测试与 PK 值改写拒绝边界。
- [YCSB / sysbench 原生采集验证](docs/validation/ycsb-sysbench-native-capture.md)：8,706,000 行基线、客户端对账、访问账本及规模下观察到的现象。
- [HTAP SF1 验证](docs/validation/hybench-sf1-htap.md)：原生 HTAP 采集、八表状态及无损原始 Trace。
- [全索引 SF1 验证](docs/validation/hybench-sf1-secondary-indexes.md)：当前 v7、全部二级维护、真实索引列值更新、12 组回放及 Parquet oracle。
- 历史证据：[TP 子集验证](docs/validation/hybench-sf1-poc.md)、[提交变更及多 client 验证](docs/validation/committed-change-delivery.md)；不作为当前功能范围说明。

实验细节和历史日志保留在对应验证文档及原始产物中；本计划只维护当前约定、进度、后续工作和验收条件。
