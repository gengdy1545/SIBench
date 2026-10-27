# Index Service 协议契约

驱动把规范 Trace 回放到 KV Index Service 时使用的通用协议。定义在 [index_service.proto](../../src/main/proto/index_service.proto)（gRPC，`sibench.index.v1`）。协议只承载逻辑索引操作；服务端如何编码键、附加版本、分区或落盘由它自己决定。操作语义继承 [Trace 契约](trace.md)，本页只写协议层的约定与责任。

## 调用

| RPC | 用途 |
| --- | --- |
| `Describe` | 服务名、协议版本（1）、value 种类（locator / row_id）、是否支持按版本读、同版本有序覆盖、事务内读（`transaction_local_reads`）、查询组、跨 client 有序（`cross_client_ordering`）、完整提交发布（`complete_commit_publication`），以及机器可校验的确认层级（`write_ack`、`baseline_durable_only_at_flush`、`flush_makes_durable`）。驱动在发送前核对：value 种类与回放一致；按版本读与同版本有序覆盖必须支持；Trace 含查询组 / 事务内查询而服务不支持对应能力时拒绝；`client_count > 1` 而服务既不声明跨 client 全序、也不声明完整提交发布时拒绝；确认层级未声明或 Flush 不保证声明的持久化时拒绝 |
| `Execute` | 每个 benchmark client 一条双向流。服务按接收顺序执行一条流上的请求，一个请求能看到同一条流上更早收到的全部写入；响应带请求 id，可乱序返回。每个请求带驱动的全局发送序号 `sequence`（整次回放、全部流连续编号，一个查询组占一个序号），非基线请求还带来源 `transaction_id` 与 1-based `transaction_operation`。声明 `cross_client_ordering` 的服务按 `sequence` 执行；声明 `complete_commit_publication` 的服务按事务内 `transaction_operation` 执行，并等待完整提交标记发布读取版本 |
| `Flush` | 在此之前任意流上收到的请求都达到服务声明的持久化层级后返回，并返回当前完整提交发布水位 `published_version`。用于加载、测量、排空之间的阶段边界，不是读屏障 |

新增字段是向后兼容的能力声明，协议版本仍为 1；旧服务不声明这些能力时，驱动按上面规则拒绝，而不是假定支持。

## 请求

索引条目为 `(table, index, indexed_value, primary_key)`，主键索引（index 0）没有 indexed_value；分量是 Trace 的保序编码，字节序即源顺序且自定界。

| 请求 | 语义 |
| --- | --- |
| `Write` INSERT | Baseline 加载，版本 0 |
| `Write` UPSERT | 条目的新版本，带 value |
| `Write` LOGICAL_DELETE | 从该版本起条目不存在，不带 value；不得物理删除使旧版本复活 |
| `Write` RELOCATE | 替换一个已存在且未删除的确切版本的 value；不存在则 INVALID；row_id 模式下不得改变 row id |
| `Search` | 对首分量（主键索引为 PK，二级为 indexed_value）的区间；缺省一端为无界；结果按 (indexed_value, PK) 升序（descending 时降序），每个条目取 `<= read_version` 的最新版本且未删除，limit>0 时取前 limit 个 |
| `Search` 事务内读 | `local_version > 0`：先取 `read_version` 快照，再只叠加该精确版本中在此请求之前已执行的写入（全序模式按 `sequence`；完整提交模式按同一 `transaction_id` 的 `transaction_operation`）；两者之间的其它事务版本不可见。只适用于 `transaction_local_reads` 为 true 的服务 |
| `Search` 钉住点查 | 二级索引，lower = upper = indexed_value，`pinned_primary_key` 指定唯一条目，结果 0 或 1 条 |
| `SearchGroup` | 一次源访问的多个查询，同表、同 read_version 与 local_version；INTERSECTION 返回第一个成员中所有其他成员也找到的 PK，UNION 返回第一个成员加上后续成员新增的 PK |
| `Commit` | 一个已提交源事务的完整标记：`version`、实际发送的写请求数 `writes` 和全部请求数 `operations`。完整提交服务收到标记且该版本全部写入已应用后才发布；它不是“最高已见版本”的替代 |

响应 `status`：OK；INVALID（请求违反协议）；UNAVAILABLE（服务未能执行，写入结果未知，驱动不重试）。查询返回结果条目（PK、选中的版本、value）。

## 驱动侧

`replay-service` 复用回放驱动的路由、单发送线程、在途上限、超时和排空，配置 `target.service` 选择 adapter：

```yaml
target:
  secondary_indexes:
    enabled: true
  service:
    adapter: grpc            # 或 rocksdb：进程内 RocksDB，不经协议与网络
    endpoint: 127.0.0.1:50551
    value: locator           # 或 row_id
```

- 每个 client 一个连接和一条流；流接受请求即为本地发送完成，不代表送达或执行。
- 查询组成员在 Trace 中连续且同属一个工作单元，驱动在同一 client 上攒齐后作为一个 `SearchGroup` 发出，组响应完成全部成员。
- 每个查询（或查询组）的返回条数必须等于 Trace 的原生期望数，否则回放失败；同时逐查询比较绑定查询身份、结果顺序和完整 `(PK, version, value)` 的 SHA-256 摘要。摘要由参考回放从规范 Trace 生成一次，`--reference` 直接复用其 sidecar，否则在回放前生成；缺失、错序、同条数错集合、错版本、错 Locator / RowID、重复结果和跨查询交换都会失败。旧全局 value XOR 仅保留为历史定位字段，不再作为完整正确性证据。
- **跨 client 顺序 / 完整发布**：查询结果依赖源顺序上更早的写入。多条流并发执行时，服务必须声明 `cross_client_ordering` 并按 `sequence` 全序执行，或声明 `complete_commit_publication` 并按事务内 `transaction_operation` + 提交水位执行；不同连接上的到达顺序不等于发送顺序。驱动不做等待或屏障去修补结果，也不重试。`client_count > 1` 且两种能力都未声明时，`replay-service` 直接失败。
- **完整提交模式**：服务声明后，驱动在每个源事务的操作之后发送一个 `Commit`，携带版本和实际发送的写 / 全部请求数；被二级过滤掉的操作不计数。驱动只为已发送的操作发标记，不猜测缺失请求；提交标记响应失败或超时即停止回放，未知写入不重试。查询等待服务发布其 `read_version`，事务内查询等待同一事务前缀，而不是等待全局最大版本。
- 阶段为 Baseline 加载、Flush、负载、Flush，分别计时；报告带各 client 的操作、查询、写入与工作单元数。

`index-server` 以 `purpose: index_service` 配置（[示例](../../config/index_service_rocksdb.yaml)）在给定端口提供一个新建的 RocksDB 索引；目录已存在即拒绝，一个索引只由一份 Trace 加载一次。

## 最小比较契约与并发边界

首轮比较只要求下列能力；未声明或不支持的能力在发送前拒绝，不通过驱动等待、重试或额外屏障补齐。

| 能力 | Trace / 比较要求 | 协议字段 | 当前 RocksDB 参照 |
| --- | --- | --- | --- |
| 点 / 范围 / limit 查询、数组并集、BitmapAnd 交集、钉住点查 | 首轮全部来源访问形态 | `Search` / `SearchGroup` | 支持 |
| 快照 / 历史读 | `read_version` 选择 `<=` 版本；历史深度由服务保留策略决定 | `versioned_reads` | 支持 |
| 写后读 | 原语句快照 + 本事务截至查询位置的写入；另一个事务在两者之间的提交不可见 | `transaction_local_reads`、`Search.local_version` | 支持 |
| 同键同版本覆盖 | 同事务内按发送顺序覆盖，中间状态可用于其后的查询 | `ordered_same_version_overwrite` | 支持 |
| 确认 / 持久化 | 写入确认层级机器可读；加载与 Flush 边界分别声明 | `write_ack`、`baseline_durable_only_at_flush`、`flush_makes_durable` | WAL（非 fsync）；Baseline 到 Flush 才落盘 |
| 跨 client 顺序 / 依赖读 | 多 client 时按驱动 `sequence` 全序执行，或按事务内操作顺序与完整提交发布并行 | `cross_client_ordering` / `complete_commit_publication` | 两种模式均支持 |
| 批处理 | 查询组按一个请求执行；写入不要求事务 API | `search_groups` | 支持查询组，无写入批 API |
| 维护 / 恢复 | 不在首轮最小契约内；单独报告能力与证据 | 文档字段，不在协议声明 | 未实现 |
| 提交完整发布水位 | 不以“最大见过版本”推进；需要提交标记和完整写入计数 | `Commit`；`FlushResponse.published_version` | 支持 |

当前参照实现提供三种模式：单 client 顺序；声明 `cross_client_ordering` 后按 `sequence` 全序执行的诊断模式；以及声明 `complete_commit_publication` 后的并行发布模式。诊断模式保证一条查找之前的全部 `< read_version` 写入已完成，但以串行执行换取。发布模式只在一个事务的请求内保持顺序；不同事务可并行。版本 `V` 仅在收到 `Commit{V, writes, operations}` 且 `V` 的全部写入已应用、`V-1` 已发布时推进；`read_version=V` 的查询等待该水位，事务内查询还等待同一事务更早的 `transaction_operation`。`Flush` 返回该水位，最大已见版本从不单独推进它。

## RocksDB 实现

`RocksDbBackend` 按 Trace 契约的 KV 布局存储：`table(u32) ++ index(u32) ++ [indexed_value] ++ PK ++ ~version(u64)`，大端，同一条目的各版本从新到旧相邻；value 为存在标记、indexed_value 长度及 file / position / row_id。按版本读定位到 `~read_version` 处取第一个同条目版本；`local_version` 高于快照时先取该精确版本，不存在才回退到快照，因此另一个事务在两者之间的版本不可见。逻辑删除是一个不存在的版本。Baseline INSERT 不写 WAL，`Flush` 时刷盘；其余写入走 WAL 不 fsync。

服务端配置 `service.ordered: true` 时，`IndexServer` 按 `sequence` 执行：先到的请求暂存，直到全部更小的序号执行完，因此声明跨 client 有序（历史 sysbench Trace 的 64 client 回放曾与参考回放结果校验和相同；新回放使用逐查询摘要）；暂存量受驱动在途上限约束，重复或已执行过的序号返回 INVALID。代价是整个服务按发送顺序串行执行，多 client 只增加连接与流，不增加服务端并行度。

配置 `service.publication: true` 时（[示例](../../config/index_service_rocksdb_publication.yaml)），`IndexServer` 使用 `PublishedIndex`：每个 `transaction_id` 一个 lane，按 `transaction_operation` 执行，不同事务并行；写入按版本累计，收到 `Commit` 且计数吻合后推进 `published_version`；查询在 `read_version` 未发布时等待。lane 的监控器不在后端写入或发布等待期间持有，因此较早操作可从另一连接到达。当前并发实现为每等待查询占用一个服务线程，尚未做非阻塞调度；它是正确性参照，不是调优后的吞吐结果。

进程内 adapter 在发送线程上逐个执行；声明完整提交时也使用同一 `PublishedIndex`。这是协议的第一个参照实现，用于验证协议与驱动，不代表 RocksDB 调优后的性能。Rockset 类服务（rocksdb-cloud 等）、其它 KV 存储按同一协议接入。

## 边界

- 尚未覆盖：持久化与崩溃恢复验证、写入结果未知时的诊断、限速与预热流程、物理 GC（`versions.gc.owner` 仍为 none）。
- 完整提交模式已实现并验证，但仍是正确性参照：查询等待占用服务线程，不同事务可并行，事务内严格有序；未证明高并发扩展性或恢复后的发布水位。缺失 / 未知提交会阻塞相应版本的读取并最终超时，不会用最大已见版本绕过。已发布版本的迟到写入、另一事务复用同一版本，以及 pending / 正在执行的重复事务位置，在接触后端前拒绝；不会被拒绝响应掩盖已发生的状态修改。为拒绝迟到请求，参照实现保留已完成事务的 lane 身份，内存随事务数线性增长，尚无有界回收。
- 逐查询摘要需要与 Trace 同序的期望结果 sidecar；服务回放按发送顺序消费它，响应可乱序完成。摘要证明服务返回与参考模型一致，原生期望条数仍是独立事实；两者都不能单独替代 Parquet oracle 对参考模型历史语义的验证。
- 协议发送的是逻辑操作，Locator 指向的 Parquet 由驱动侧的文件生命周期负责；服务不读取数据文件。
