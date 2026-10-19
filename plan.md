# SIBench Benchmark 设计与实施计划

## 目标与核心原则

构建独立于具体数据库或 Lakehouse 的 Serverless Index Benchmark，通过统一的真实数据文件与可复现 Trace，评估不同 Index 的加载、查询、更新和维护性能。

- 总原则是还原真实端到端运行中对 Index 产生的操作负载，包括查询、业务修改及文件布局变化引起的索引维护；端到端描述负载来源和依赖关系，计时边界仍限定为 Index Service。
- 真实数据文件、真实索引存储、确定性 Trace。
- Point、Range、Snapshot Lookup 均属于完整实现范围，各自提供开关且默认开启；不将查询能力推迟为后续扩展，也不默认缩减为纯写入与维护评测。
- Benchmark 来源可扩展，首批覆盖 HyBench、YCSB 和 sysbench；HyBench SF100 仅是实验配置示例，不是唯一来源或统一规模标准。
- Baseline 与增量 Replay 分离；Baseline 只执行 INSERT，不执行 Data File Rewrite。
- 数据文件大小与 Rewrite 策略由 SIBench 统一定义，不依赖具体 Lakehouse 的 Commit、Flush 或 Snapshot 策略。
- Rewrite 只改变物理布局，不产生新的逻辑版本，不改变 IndexKey、Timestamp 或 RowID。
- 所有 Index Backend 消费同一份 Logical Trace 和 Physical Trace，按各自 value 模式执行必要的索引操作。
- 只评测 Index Service；数据文件生成、Rewrite 的数据读写、Lookup 后的数据读取及 SQL 执行时间不计入索引性能。
- SIBench 负责数据与负载生成、文件布局、Trace、编排、验证和观测；Index Backend 负责真实索引存储及其内部缓存、持久化与 compaction。
- 暂不引入 RowID 到物理位置的映射服务，也不将此类映射的成本列为实现或验收要求。

## 整体流程

```text
Benchmark Adapter（HyBench / YCSB / sysbench / 后续扩展）
    ↓
Baseline Data Generation（真实 Parquet 文件）
    ↓
Baseline Index Loading（INSERT Only，单独计量）
    ↓
Incremental Workload Generation
    ├── INSERT / UPDATE / DELETE
    ├── Point / Range / Snapshot Lookup
    └── Data File Rewrite
    ↓
Deterministic Logical + Physical Trace
    ↓
Index Benchmark Replay
    ↓
正确性核对、性能统计与存储成本分析
```

以上为逻辑流程。实际执行时，Baseline 文件、增量文件及 Rewrite 文件可以在 Preparation 阶段离线生成，再对多个 Backend 分别加载和回放。各 Backend 复用同一套数据产物与 Trace，不重复执行数据文件写入。

## Benchmark 接入与配置边界

- 通过来源适配器接入初始数据和增量负载，声明数据 schema、表命名空间、键类型、排序规则、行内容生成方式和支持的操作。
- 保留各来源的原生规模参数，例如 HyBench 的 sf、YCSB 的记录数、sysbench 的表数与每表行数；不做未经定义的跨 benchmark 规模换算。
- 每种来源明确标记负载来自原生操作转换还是 SIBench 派生生成器；具体接入路径在第一阶段确定。原生业务语义与派生操作不能混称。
- 来源适配器需说明业务操作如何产生索引请求，保留键与范围分布、读写比例、操作顺序、版本时间点及结果驱动的后续依赖。不能仅凭 SQL 类型将操作标为索引读写，也不能把需要索引查找的 UPDATE 简化成无查找的写请求。
- PostgreSQL 等数据库可作为特定适配器的准备、采集或核对工具，不作为公共生成与回放流程的必需依赖。
- 不实现通用 SQL 执行或解析引擎。对不能准确转换的操作报告覆盖范围和原因，不将部分结果当成完整修改行集合。
- 对不能原生确定性生成的输入，需要固定准备产物或采用明确标识的确定性派生实现；仅设置随机种子不等于获得确定性并发负载。
- 配置按职责划分为 workload、data_layout、versions、trace、target、storage_profile 和 artifacts。数据文件布局与索引内部存储布局分开配置。
- 核心使用 Java，放在 src/main/java/org/sibench/，测试放在 src/test/java/org/sibench/，按负载配置、数据生成、Trace、回放和适配器等职责组织；scripts/ 保留 Python/Shell 编排、服务管理、实验验证与分析报告等辅助入口。
- 上游源码保留在 benchmark/ 子模块；构建产物放在 build/benchmarks/，本地服务状态放在 build/services/。
- 负载配置放在 config/workloads/<benchmark>/，Trace 样例放在 config/trace/，存储配置放在 config/storage/。脚本及文档按仓库约定的领域目录组织，不使用开发阶段名建永久目录。

### 语言与工程实现

- 主语言选择 Java，当前采用 JDK 17 与 Maven，核心构建产物位于 build/sibench/。源码使用标准 src/main/java 和 src/test/java 布局，不在 scripts/ 实现核心算法。
- Java 负责公共配置解析与语义校验、数据生成、Parquet 文件组织、版本状态、有效率、Rewrite/relocation、确定性 Trace、Checkpoint、回放调度、Backend 客户端和延迟采集。Parquet 接入采用 parquet-java；具体依赖版本随生成器实现固定。
- Python/Shell 负责实验进程编排、环境和服务管理、离线分析与报告，通过 Java CLI 及结构化产物交互，不重复实现核心状态演进、配置规则或逐请求回放。
- 选择 Java 的主要考虑是复用 HyBench/YCSB 的 Java 逻辑、直接接入 Parquet 写入，以及统一生成和回放契约；复用原生代码仍需处理随机源、时间戳及结果依赖，不能自动视为确定性实现。
- Rust/C++ 暂不作为第二套核心实现。被测 Index 的语言不限，通过跨进程协议适配；仅在具体 Backend 需要原生库接口且收益明确时引入对应适配器。
- Maven 固定依赖与插件版本；实验 manifest 记录实际 JDK 版本、供应商、JVM 参数、GC 配置、堆与直接内存预算、核心构建版本及依赖指纹。历史原生 benchmark 的独立构建路径继续保留。
- 数十亿行的 ID、计数和逻辑时间使用足够宽的整数，检查溢出。生成、状态管理和回放使用有界队列、批量处理与可落盘状态，避免把全量记录或版本作为 Java 对象驻留堆内。
- 单独定义生成器并行度、请求到达率和 Backend 并发；使用可重建的随机流与依赖调度，操作内容和顺序不能由线程竞争、GC 或实际墙钟决定。
- 性能验收包含空操作接收端与参考服务的驱动容量测试。分别记录计划到达、实际发送和响应完成时间，报告调度延迟、排队、吞吐、CPU、内存、GC 暂停与后台积压；预热独立计量，确认驱动有足够余量后才归因于 Backend。

### Lookup 开关与负载语义

以下为计划采用的配置契约，三项缺省值均为 true：

```yaml
workload:
  lookups:
    point:
      enabled: true
    range:
      enabled: true
    snapshot:
      enabled: true
```

- 为使三个开关独立，point 控制当前时间点的点查，range 控制当前时间点的范围查，snapshot 控制指定历史时间点的查询。Snapshot 请求另带 point/range 访问形态，由 snapshot 开关统一控制，不重复受前两项开关过滤。
- 默认混合回放查询、修改与 Rewrite，保留规范 Trace 中的交错顺序与依赖。开启表示执行该来源或实验配置实际定义的查询，不凭空为原生负载补入不存在的操作；新增查询须明确标为派生负载并记录分布与比例。
- 关闭某类查询得到显式的负载变体，用于隔离成本；不得将被关闭的查询替换为 UPDATE 等写操作，或自动重分配剩余操作比例。
- 开关控制发往被测 Index 的查询请求。Preparation 仍需解析结果驱动的依赖并固定后续操作；关闭查询时保留生成后续操作所需的状态推演。无法固定此类依赖时，该适配器应拒绝相应变体，不能生成语义不明的后续请求。
- 同一基础配方下切换查询开关，不改变写入事件、Timestamp、版本保留、文件布局或 Rewrite 的逻辑位置。过滤保留事件标识和计划时间；固定到达时间的实验保留被过滤请求的时间空隙，全速实验单独报告实际执行的操作组合。
- Snapshot Lookup 开关不控制历史版本保留和清理；关闭历史查询不能自动回收仍受保留策略保护的版本。
- Manifest 与报告保存开关生效值、来源操作数、过滤数量、过滤后操作比例和覆盖率。对比 Backend 时使用相同开关与有效 Trace；缺失已开启且实际需要的查询能力时在回放前报错，不静默关闭。
- 查询关闭时仍在测量窗口之外核对状态与结果。性能报告区分已关闭、来源无此类请求和实际执行，未测得的查询性能不能填为零。

## 索引键、版本与 Value

### 复合键与时间点查询

版本通过通用复合键表达，不要求 Backend 提供原生 MVCC、数据库事务快照或 Lakehouse 快照机制。

```text
IndexKey = (TableNamespace, PrimaryKey, TimestampDescending)
PhysicalValue = (FileID, FileRowPosition)
LogicalValue = RowID
```

当前范围为主键或唯一键索引；本文其他位置的 UserKey 在该范围内指 PrimaryKey，RowPosition 指 FileRowPosition。TableNamespace 可以通过独立命名空间表达；PrimaryKey 可以是原生复合主键。TimestampDescending 表示版本后缀按时间戳降序比较或编码，不表示生成的时间戳递减。

- 同一命名空间内，PrimaryKey 按声明的类型与业务顺序排序，同一 PrimaryKey 的版本按 Timestamp 降序排列。变长字段需有无歧义边界，编码后或适配器比较器提供的顺序必须符合契约，不能直接用普通字符串拼接替代。
- Timestamp 使用确定性的逻辑提交时间，不依赖 Backend 的运行时钟；位宽、单位、取值范围及同键同时间戳的冲突处理在第一阶段固定，不能静默覆盖两个不同逻辑版本。
- 时间点 T 的 Lookup 定义为：在同一个 UserKey 下查找 Timestamp <= T 的最新条目；最新条目为删除标记时返回不存在。当前查询使用已发布的逻辑时间点。
- Snapshot Lookup 表达历史时间点的可见性，不隐含 Backend 原生事务隔离保证。
- UPDATE 产生新的逻辑版本和对应复合键；DELETE 写入带 Timestamp 的逻辑删除标记，使查询在选中该版本时返回不存在，不能直接删除最新条目导致查询回退到更旧版本。逻辑删除标记区别于 Backend 删除某个完整复合键的内部 tombstone，具体编码在第一阶段确定。
- Range Lookup 明确上下界、开闭区间、排序和数量上限；历史范围查询按 UserKey 选择可见版本，数量上限按最终返回的逻辑记录计算。
- 索引条目清理与业务 DELETE 分开统计；清理历史版本和删除标记不能使已删除记录重新可见。
- 业务 UPDATE 是否保留 RowID、删除后重新 INSERT 是否分配新 RowID，仍为待确定的身份规则。无论选择哪种规则，Rewrite 必须保留被搬迁版本已有的 RowID。

非唯一二级索引不属于当前默认实现范围。后续扩展可使用 (TableNamespace, IndexID, IndexedColumns, PrimaryKey, TimestampDescending)，通过 PrimaryKey 区分具有相同索引列值的记录；届时需另外定义索引列更新与删除语义，不默认将这些字段加入现有主键索引。

### 两种 Value 模式

| 模式 | Lookup 返回值 | Rewrite 的索引维护 |
|---|---|---|
| Physical Locator | FileID + FileRowPosition | 更新对应复合键的物理位置 |
| Logical RowID | RowID | 无索引更新，RowID 保持不变 |

- FileID 在一套数据产物的完整生命周期内唯一，由确定性规则分配；不同表、分区、桶及 Rewrite 输出不得碰撞，旧文件回收后不复用其 ID。重复重建同一产物应恢复相同 ID；不同产物由独立命名空间隔离。FileID 与不可变文件的对应关系记录在 manifest，不能用相同 ID 表示内容已改变的新文件。
- FileRowPosition 为从 0 开始的文件内绝对行号。默认 Value 不包含 RowGroupID；文件元数据保留各组的实际行数与边界，可通过行数前缀和推导所属组及组内行号，不能用 row_group_max_rows 直接做整除定位。
- (FileID, RowGroupID, RowInGroup) 可作为未来匹配具体读取器的显式 Locator 扩展，不属于当前默认 Value。能够唯一定位记录不等于无需读取元数据或解码页面；数据文件读取及定位到页面的成本继续排除在 Index Service 计时之外。
- RowID 模式不执行额外的物理位置解析，不建立 RowID → Physical Locator 映射。
- 统一 Physical Trace 可以包含 relocation；RowID 模式消费事件及其顺序边界，但不得因此发送索引 update 或产生新的版本条目。
- 两种模式分别报告其返回语义与成本。返回 RowID 的结果不代表已完成物理数据定位。
- 分别统计逻辑业务操作数、实际索引 API 操作数、条目修改数和物理 I/O，避免把两种模式不同的维护工作量混为一项。
- 真实 Backend 的范围、反向查找或批量能力通过适配器声明。若适配器需要额外索引操作才能实现时间点查询，其成本计入 Index Service 测量；不支持的能力在实验前明确报告。

### 编码契约与依据

- 第一阶段固定 TableNamespace、PrimaryKey 各分量、Timestamp、FileID、FileRowPosition 和 RowID 的类型、位宽或长度编码、字节序、合法范围及溢出处理；不得根据不同 Backend 静默改变规范键值表示。
- 上述 Value 公式描述存活版本的 payload。存活条目与逻辑删除标记需要无歧义的类型表示；标签位置和删除标记 payload 格式单独定义，不用可能合法的 FileID、RowPosition 或 RowID 充当删除哨兵。
- 报告规范键值序列化字节数（含必要类型标签）及 Backend 实际存储、协议与内部版本元数据开销，避免把 payload 大小直接当作索引存储大小。Backend 必需的编码转换显式记录，转换所需的服务工作计入相应测量。
- 设计依据分别记录：Bigtable（OSDI 2006，§2）支持同键版本按时间戳降序组织；Iceberg 1.10.0 的 Position Delete 规范提供“文件与从 0 开始的绝对行号”定位实例，Row Lineage 规范要求纯搬迁保留行身份与更新序号；Parquet 的 FileMetaData、RowGroup 和 OffsetIndex 定义支持从文件行号到组及页面的解析。这些依据不等于证明某种 Locator 编码性能最优。
- 列式格式评测论文 An Empirical Evaluation of Columnar Storage Formats（PVLDB 2023，§5.8.2）区分索引检索与读取底层列式文件。其 row ID 的具体含义不能直接替代本方案中跨 Rewrite 稳定的 RowID 契约。

## Data File Generation

统一采用 Size-Based File Generation：文件按压缩后落盘字节数控制目标大小，row group 按行数上限控制。计划配置如下：

```yaml
data_layout:
  parquet:
    target_file_size_bytes: 268435456  # 256 MiB
    row_group_max_rows: 1048576       # 1,048,576 rows
```

- 文件格式为 Parquet；目标大小默认 256 MiB（268435456 字节），按最终压缩后文件总大小（含元数据）统计，可配置。文件大小是主要实验变量，敏感性实验使用 128 / 256 / 512 MiB。
- 每个 row group 默认最多 1,048,576 行，可配置；row_group_max_rows 表示行数，不是单行大小或字节数。文件封闭时允许最后一个 row group 不足该上限，不要求每组都达到上限。
- 文件目标是近似值，不保证精确等于配置字节数。文件封闭需协调 row group 边界，允许提前结束当前组；大小估算、容差及超大单行的处理规则在第一阶段固定，不允许仅因组未满而无限推迟文件封闭。
- 宽表或大字段可配置额外的确定性字节预算以提前结束 row group；计量口径及阈值写入配置，不根据实时可用内存改变分组。普通行数上限不等同于生成器内存上限。
- 按 Table / Partition / Bucket 分别生成文件；分区与分桶规则、散列算法及桶数固定在配置中，不在不同 Backend 之间改变。
- Baseline 生成初始真实数据文件及其键、版本、RowID、物理位置元数据，索引加载只执行 INSERT，不进行 Rewrite。
- 增量 INSERT 和 UPDATE 产生的新版本采用 Append-Only 写入，按目标文件大小切分，不原地改写已发布文件。
- DELETE 使对应当前版本失效；历史版本是否保留由版本保留规则决定。
- 允许每个生成分组的最后一个文件小于目标大小。文件封闭与发布必须使用确定性规则，不能由 Backend 执行速度决定。
- 压缩算法与适用的压缩级别均可配置，每个实验固定并显式记录生效值及 codec 版本，不隐式依赖 writer 默认值。当前规模配置采用 Zstd level 3，不作为首轮主要敏感性实验变量；writer 与 codec 版本在生成实现中固定。
- Row group 行数及压缩配置先固定用于文件大小实验。按压缩后字节切分时，两者都可能改变文件成员及后续 Rewrite 负载；改变它们属于新的布局配置，需要重新生成文件及 Physical Trace，不能直接复用旧 Locator。
- 对比布局配置时保持初始逻辑数据与逻辑操作序列一致；对比 Index Backend 时则固定全部生成参数，复用相同数据文件及 Logical / Physical Trace。
- 固定 schema、行内容生成规则、writer 版本、压缩参数、page 与 dictionary 配置、行顺序和 FileID 分配规则。保存实际文件大小、文件行数、各 row group 的行数及压缩前编码字节和压缩后字节、产物校验值与完整生效配置。
- RowPosition 定义为文件内从 0 开始的绝对行号，不使用 RowGroupID 与组内行号作为索引 value。保持 FileID、文件成员及行顺序不变时，仅调整 row group 边界不改变 Locator；通过跨 row group 边界读取真实文件验证对应关系。
- 不额外引入 Small-File Compaction、Sort 或 Z-Order。索引 Backend 内部 compaction 不受此项限制，单独观测。

## Data File Rewrite

采用 Validity-Based Rewrite + Deterministic Bin-Packing。

### 有效率与版本保留

```text
RewriteValidity(f) = RetainedRows(f) / TotalRows(f)
触发条件：RewriteValidity(f) <= 70%
```

- CurrentRows 为当前可见版本数；RetainedRows 为当前版本与保留的历史查询时间点仍需要的版本的去重总数。
- ReclaimableRows = TotalRows - RetainedRows。触发阈值表示至少 30% 的行可回收，不将仍需历史查询的版本计为可回收。
- 快照在本方案中是受保留策略保护的逻辑查询时间点，不固定引用旧物理文件。保留版本可以被搬迁；搬迁后仍应返回相同逻辑内容。
- 历史时间点的创建、过期、保留窗口和 Rewrite 检查由确定性规则控制；具体周期与默认值在第一阶段确定。
- 报告当前版本比例、保留版本比例、可回收行数及实际文件字节量，不把行比例直接等同于可回收字节比例。

### Rewrite 流程

1. 按固定的逻辑事件边界检查有效率，筛选达到阈值的文件。
2. 在同一 Table / Partition / Bucket 内，按固定顺序收集所有仍需保留的记录版本。
3. 用确定性装箱规则生成符合当前文件目标大小的新文件（默认约 256 MiB），复用相同的 row group 行数上限和压缩配置；装箱单位、输入顺序及选择规则必须写入契约。
4. 生成 relocation，关联具体 IndexKey、原有 RowID、旧 Locator 和新 Locator，防止旧版本搬迁误更新新版本。
5. Physical Locator 模式完成对应 Value 更新；RowID 模式不产生索引写入。
6. 完成 Rewrite Commit 的发布边界，在旧位置不再被在途操作使用后允许回收旧文件。全部记录均可回收的文件无需生成替代数据文件。

Rewrite 不改变 IndexKey、Timestamp、RowID 或逻辑可见性。第一版建议通过回放屏障完成批量位置更新与发布，不要求额外的对外布局版本接口。并发查询与 relocation 的排序、失败处理及提交完成条件必须在接入契约中确定。

Preparation 中实际生成与保留哪些文件，与 Replay 中何时发布对应事件分开管理；不能因离线准备已结束，就让回放提前看到未来位置。所有 Backend 完成所需实验前，应保留可重建 Baseline 和所用 Trace 区间的产物。

## Deterministic Trace

采用 Deterministic Procedural Trace，不要求永久保存完整的大规模逐操作日志。

- Logical Trace 包含 INSERT、UPDATE、DELETE、Point / Range / Snapshot Lookup、版本时间及保留时间点的生命周期。
- 查询事件记录访问形态、目标时间点、业务来源和依赖；规范事件到实际索引请求的转换可追溯。查询开关只按上述规则生成有效回放视图，不改变基础事件的身份或数据演进。
- Physical Trace 包含 FileID、RowPosition、RowID、文件发布、Rewrite、relocation 和提交边界。
- 两类 Trace 通过稳定事件标识及明确的依赖顺序关联；不同 Backend 使用同一份规范事件序列，不能由各 Backend 自行决定 Rewrite 时机或文件布局。
- 固定 Random Seed、生成器版本、benchmark 子模块版本、数据分布、操作序列、文件布局规则及编码配置。
- 支持按需重建或流式生成。必要时保存 Checkpoint，覆盖生成器位置、随机状态、键与版本状态、文件状态、保留时间点及未完成事件。
- 大规模版本状态、有效率跟踪和统计允许使用磁盘数据结构，明确内存预算。
- 保存配方、输入 manifest、文件元数据、分块事件摘要、产物校验值和必要 Checkpoint。小规模验证及故障定位可导出完整 Trace。
- 恢复生成必须与连续生成得到相同事件、文件布局及标识；事件确定性与文件字节确定性的验证范围分别说明。
- Replay Workload 至少支持 30 分钟持续运行；不能简单循环一段带写入的 Trace，造成重复插入或版本身份冲突。
- 建议分别提供固定事件区间的完成时间实验，以及固定到达速率、持续至少 30 分钟的实验。各 Backend 全速运行固定时长时，报告消费前缀、状态规模和维护事件数，不声称完成同一批工作。
- 并发回放必须保持同键依赖、查询可见时间点与 Rewrite 发布边界；只保留线程内顺序不足以证明结果一致。
- 记录输入生成、预取和回放驱动能力。文件下载、解压及 Trace 供应成本单列，输入不足导致的空闲与 Backend 性能分开报告。

## Index Benchmark 与观测

### 实验流程与指标

每个 Backend 独立执行初始化、Baseline INSERT Loading、持久化就绪检查、缓存设置、预热、增量测量、后台排空和结果核对。各阶段独立计时；预热若包含修改，必须消费共同前缀并从一致状态进入测量。

主要指标：

- Index Loading / Update Throughput，按业务修改、版本条目写入、删除清理与 relocation 分类。
- Point / Range / Snapshot Lookup 的 QPS、延迟分位数、返回数量、错误率与覆盖率。
- 按当前点查、当前范围查和历史查询分别统计；历史查询再按访问形态细分，总请求数避免重复计数。默认报告混合负载及 Rewrite 期间的查询表现，关闭开关的实验明确标为负载变体。
- Index Storage Size，分别记录前台结束与后台排空后的状态。
- Index Write / Read Amplification，声明逻辑字节量、物理字节量及统计窗口。
- Data Rewrite 引起的 Index Maintenance Cost，包括位置更新操作数、耗时、字节量及对前台延迟的影响。
- 索引内部 compaction、后台积压与排空时间。
- 并发扩展性、CPU、内存、缓存、本地磁盘与 EFS/S3 I/O。
- 在提供价格配置时估算资源成本，记录价格日期、部署方式与计价假设。

RowID 模式因 relocation 产生的索引写入数应为零；Trace 调度和屏障开销仍按实际发生情况记录，不能等同于全部运行成本为零。Data Rewrite 自身的数据读写开销作为 Preparation 指标单列，不计入索引读写放大或维护耗时。

### 存储与 Serverless 实验边界

- 本地 SSD、EFS、S3 API 兼容对象存储及混合部署作为独立存储维度，按真实 Backend 已支持的能力开展实验。
- 分别声明索引、WAL、缓存和实验数据文件的位置；仅将 Trace 或 Parquet 上传对象存储不能作为索引对象存储支持的证据。
- 对比时固定资源预算、并发、缓存条件、写入确认语义及持久化目标。区分已接收、本地持久化和远端持久化。
- 覆盖冷缓存、热缓存和超缓存工作集；报告实际清理的缓存层，不可验证的冷缓存条件明确标记。
- 按能力提供持久化屏障、指标快照、后台排空和重启恢复接口。连接中断或写入结果未知时保存诊断，不自动重试非幂等操作。
- 观测对象请求数、读写字节、重试、限流、本地及 EFS I/O、后台维护与资源使用；每项标记统计范围和来源，不可获得的指标为 N/A。
- Serverless 的首轮验收建立固定资源预算下的基线；冷启动、闲置恢复、弹性扩缩容和成本实验在 Backend 支持及场景定义完成后扩展，不以固定并发测试替代弹性验证。
- 使用实验独立命名空间和存储 prefix；凭据通过引用传递，配置快照脱敏，清理只作用于本次实验资源。

## 实施阶段与验收

### 初始规模配置

- 已建立 HyBench [SF1000 配置](config/workloads/hybench/sf1000.yaml)，对应原生 1000x，共 8706000000 行。SF 不直接换算成 Parquet 字节量。
- [YCSB 大规模候选配置](config/workloads/ycsb/large.yaml) 使用 recordcount=1000000000、10 个字段各 100 字节；[sysbench 大规模候选配置](config/workloads/sysbench/large.yaml) 使用 100 表 × 每表 10000000 行，保留原生行结构。规模选择按各来源原生参数与整洁档位展开，不要求跨 benchmark 精确匹配记录数或字节量，也不将这些档位称为上游标准规模。
- 三套配置均标注 calibration_status=pending。目标是建立量级合理的大规模实验；代表性样本用于核实逻辑内容字节、Parquet 字节和资源预算，必要时调整规模档位，不改变行形态凑齐总量。主要对比同一来源、同一负载下的不同 Index，跨来源分别报告记录数、数据与索引字节量及分布差异。
- 三套配置显式记录源提交与定义文件摘要、规模参数、请求种子、256 MiB 文件目标、1048576 行 row group 上限，使用可配置的 Zstd level 3 作为当前生成基线。配置存在不代表随机源和生成器已实现确定性。
- 在仓库根目录运行 `java -jar build/sibench/sibench.jar inspect-scale` 核验配置与环境，完整生成前测量代表性样本的压缩大小、吞吐与峰值空间。HyBench 原生 PostgreSQL transfer.id 的 int32 容量不足以承载 SF1000；需在数据适配器明确使用 int64，不能直接复用该 DDL。
- 当前完成规模配置与检查入口，确定性 Parquet 适配器、代表性样本校准及全量产物仍待实现；本次未启动全量生成。增量操作比例与数据规模分别配置。

### 已有基础与迁移状态

旧计划第一阶段的原生运行验证已于 2026-10-18 完成，记录见 [原生 benchmark 验证报告](docs/validation/native-smoke.md)。三个 PostgreSQL 原生最小案例及其证据保留；HyBench 完整 Maven 依赖构建的阻断仍以原报告为准。

这些结果是来源适配的已有基础，不代表本计划的新语义契约、真实文件生成或确定性回放已经完成。旧版“原生采集 → 请求投影 → PK 与模拟 value 回放”不再是公共实施主线。现有契约与配置需按下列阶段迁移，历史日志和 manifest 不改写。

### 第一阶段：语义契约与小规模参考模型

**状态：待实施。目标：先建立可核对的数据、版本、文件与索引操作关系。**

- 梳理 HyBench、YCSB、sysbench 的初始数据与增量负载入口，确定原生转换或派生生成路径，并定义后续 benchmark 的扩展接口。
- 固定主键升序与版本时间戳降序的复合键契约、各字段编码及宽度、FileID 唯一且不复用的分配规则、逻辑删除表示、时间点与范围查询、RowID 身份、历史保留与清理规则；在契约文档中补充论文与格式规范的对应依据。
- 定义三类 Lookup 默认开启的独立开关、历史查询分类、过滤规则及结果依赖处理，建立业务操作到实际索引操作的可追溯契约。
- 落实文件按压缩后字节、row group 按行数上限及 RowPosition 为文件内绝对行号的契约；确定文件大小容差、组边界与尾文件封闭规则、可选字节预算、writer 与 codec 版本、Rewrite 检查周期和装箱算法。
- 定义 Logical / Physical Trace、Checkpoint、manifest、relocation 及发布屏障契约。
- 建立有序参考模型和小规模真实 Parquet 样例，支持两种 value 模式、版本查询与位置验证。
- 更新 docs/contracts/ 下的 Trace 与存储契约，以及相关配置样例，明确旧格式兼容或迁移方式。

**验收：** 人工可核对的小案例覆盖初始加载、修改、删除、历史时间点、保留过期与 Rewrite。证明 Rewrite 不改变逻辑结果；Physical Locator 正确更新，RowID 不变且无 relocation 索引写入。未决契约在进入第二阶段前收敛。

### 第二阶段：真实文件与确定性生成器

**状态：待实施。目标：完成从多种来源到可重建数据产物与 Trace 的准备流程。**

- 使用 Java 实现配置语义校验、生成器与 Trace 核心；Python/Shell 编排实验和分析结果，来源适配按契约复用 Java/Lua 等现有实现。
- 实现 prepare 与 generate，完成 Baseline、增量 Append-Only、有效率跟踪、历史保留、Rewrite 和 relocation 生成。
- 实现文件字节目标与 row group 行数上限的独立配置、固定压缩参数及完整元数据记录；Baseline、增量和 Rewrite 使用同一套布局规则。
- 首批接入 HyBench、YCSB、sysbench；公共层不包含只能服务某一种 benchmark 的规模或 schema 假设。
- 实现默认开启的 Point、Range、Snapshot 查询生成与独立开关；保留来源的操作分布与依赖，并验证开关变体不改变基础写入和文件演进。
- 实现流式事件、Checkpoint 恢复、有限内存状态管理与产物校验。
- 提供 analyze，输出操作及键分布、版本保留、文件大小、有效率、Rewrite、relocation 和覆盖率报告。
- 为 128 / 256 / 512 MiB 建立配置样例，固定 row group 行数上限及压缩配置，生成可支持至少 30 分钟回放的增量工作负载；大规模运行由显式实验配置启动。

**验收：** 三种来源均完成小规模端到端准备；相同配置重复生成以及中断恢复后，规范事件序列、文件布局和标识一致。真实 Parquet 内容与 Locator 相符，Baseline 无 Rewrite，Rewrite 只保留所需版本且不跨规定分组。提供长时回放的输入供应能力证据。

### 第三阶段：真实索引接入与性能实验

**状态：待实施。目标：同一套产物与 Trace 可以驱动多个真实 Index Backend。**

- 定义加载、读写、扫描、版本查找、能力声明、relocation 应用、屏障、指标及关闭等适配接口。
- 先用参考模型及独立测试进程完成跨进程契约验证，再接入真实 server；实际网络协议由适配器封装。
- 提供 replay，支持单线程正确性基线及保持依赖的并发性能实验。
- 完整实现 Point、Range、Snapshot Lookup 的回放、正确性验证及指标采集，默认在混合负载中开启；为缺少某类原生查询的来源另设有明确标识的派生案例以验证公共能力。
- 完成两种 value 模式的操作转换与成本分类，验证 RowID 模式不会因 Rewrite 发送写请求。
- 实现统一阶段计时、后台排空、存储观测和结果核对，报告驱动瓶颈、能力缺失及不可比配置。
- 以空操作接收端和参考服务验证 Java 驱动容量，独立采集 JVM/GC、调度与排队指标，检查驱动端是否限制可观测的 Backend 吞吐。
- 在支持的本地、EFS/S3 或混合部署上运行固定负载；落实 30 分钟持续运行与文件大小敏感性实验。

**验收：** 三种来源的 Trace 在参考模型与接入的真实 Backend 上通过正确性核对；至少一套真实 Backend 完成两种 value 模式、三类 Lookup 默认开启的混合负载及长时实验，并验证独立关闭查询的变体。实际存储支持须有索引 I/O、持久化与声明的恢复能力证据。只有内存模型、测试进程或产物上传时，不视为真实索引存储验收完成；外部协议或存储能力缺失应列为未满足依赖。

## 验证重点与待确定事项

验证重点：

- 复合键类型与排序、原生复合主键及变长字段边界、同键版本时间戳降序、同键同时间戳冲突、编码溢出、删除及重新插入、空范围、数量上限和表命名空间隔离。
- 验证存活 payload 与逻辑删除标记无歧义、最新删除版本阻止旧值重新可见，以及序列化字节统计包含必要标签。
- 验证跨表、分区、桶和 Rewrite 的 FileID 唯一性、回收后不复用及确定性重建；使用不等长 row group 的前缀和核对文件行号与组内坐标，不依赖组行数上限整除。
- 三类 Lookup 缺省均开启，覆盖三开关全部八种组合、历史点查与历史范围查的分类、能力缺失预检及报告口径；核对过滤后写入、版本保留、文件布局与 Rewrite 序列不变。
- 使用包含查询结果驱动后续修改的案例核对依赖保留，并验证测量窗口之外的正确性查询不会混入性能指标。
- 基于 RetainedRows 的 70% 阈值边界、历史保留保护、全部失效文件、尾文件及分组约束。
- 文件目标以压缩后总字节数计量，row group 不超过配置行数上限；覆盖不足一组、恰好组上限、跨组、提前封闭、宽行及超大单行，核对大小容差和确定性边界规则。
- 文件成员与行顺序固定时，验证仅改变 row group 边界不会改变文件内绝对 RowPosition；布局配置改变后重新核对 Locator 与真实文件，避免将旧 Physical Trace 用于新布局。
- Rewrite 前后版本不变、Locator 与真实文件一致、RowID 不变，以及重复或失败 relocation 的处理。
- 连续生成与 Checkpoint 恢复一致；并发回放的同键依赖、范围查询可见性和发布屏障正确。
- Baseline 只 INSERT；所有 Backend 起始状态与 Trace 一致；文件数据 I/O 排除，索引后台工作不漏计。
- 缺失能力、断连、结果未知及受控存储故障的诊断；恢复保证按 Backend 声明验证。
- 指标缺失为 N/A；逻辑操作、索引维护和物理请求分开；不同返回语义、持久化目标及消费区间明确标识。

第一阶段需确定的实施细节，不视为已经讨论通过的默认值：

- 各 benchmark 的初始数据与增量负载接入路径。
- 业务 UPDATE 及删除后重新 INSERT 的 RowID 分配规则。
- 主键及版本的具体保序编码、各键值字段位宽或长度编码、FileID 确定性分配实现、Timestamp 冲突处理、删除标记、历史保留窗口及版本清理的具体参数；已确定的版本降序、FileID 不复用及文件内绝对行号规则不再作为未决选项。
- 文件目标大小容差、row group 提前封闭与可选字节预算、未满文件发布边界、超大单行处理、writer 与 codec 版本、Rewrite 检查周期和装箱算法。
- 长时实验的到达率、事件区间、并发依赖调度及真实 Backend 接入协议。

以上未决细节不改变已确定的核心约束：**Rewrite 不产生逻辑版本；Physical Locator 模式更新位置；RowID 模式不修改 RowID、不执行索引 update，也不引入额外映射。**
