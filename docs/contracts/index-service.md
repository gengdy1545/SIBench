# Index Service 协议契约

驱动把规范 Trace 回放到 KV Index Service 时使用的通用协议。定义在 [index_service.proto](../../src/main/proto/index_service.proto)（gRPC，`sibench.index.v1`）。协议只承载逻辑索引操作；服务端如何编码键、附加版本、分区或落盘由它自己决定。操作语义继承 [Trace 契约](trace.md)，本页只写协议层的约定与责任。

## 调用

| RPC | 用途 |
| --- | --- |
| `Describe` | 服务名、协议版本（1）、value 种类（locator / row_id）、是否支持按版本读、同版本有序覆盖、查询组、跨 client 有序（`cross_client_ordering`），以及确认写入的持久化层级说明。驱动在发送前核对：value 种类与回放一致，按版本读与同版本有序覆盖必须支持；Trace 含查询组而服务不支持时拒绝；`client_count > 1` 而服务不声明跨 client 有序时拒绝 |
| `Execute` | 每个 benchmark client 一条双向流。服务按接收顺序执行一条流上的请求，一个请求能看到同一条流上更早收到的全部写入；响应带请求 id，可乱序返回。每个请求带驱动的全局发送序号 `sequence`（整次回放、全部流连续编号，一个查询组占一个序号）。跨流的顺序由 `cross_client_ordering` 声明：只有声明为 true 时，服务按 `sequence` 执行而不是按网络到达顺序，先到的请求要等全部更小的序号执行完 |
| `Flush` | 在此之前任意流上收到的请求都达到服务声明的持久化层级后返回。用于加载、测量、排空之间的阶段边界，不是读屏障 |

## 请求

索引条目为 `(table, index, indexed_value, primary_key)`，主键索引（index 0）没有 indexed_value；分量是 Trace 的保序编码，字节序即源顺序且自定界。

| 请求 | 语义 |
| --- | --- |
| `Write` INSERT | Baseline 加载，版本 0 |
| `Write` UPSERT | 条目的新版本，带 value |
| `Write` LOGICAL_DELETE | 从该版本起条目不存在，不带 value；不得物理删除使旧版本复活 |
| `Write` RELOCATE | 替换一个已存在且未删除的确切版本的 value；不存在则 INVALID；row_id 模式下不得改变 row id |
| `Search` | 对首分量（主键索引为 PK，二级为 indexed_value）的区间；缺省一端为无界；结果按 (indexed_value, PK) 升序（descending 时降序），每个条目取 `<= read_version` 的最新版本且未删除，limit>0 时取前 limit 个 |
| `Search` 钉住点查 | 二级索引，lower = upper = indexed_value，`pinned_primary_key` 指定唯一条目，结果 0 或 1 条 |
| `SearchGroup` | 一次源访问的多个查询，同表、同 read_version；INTERSECTION 返回第一个成员中所有其他成员也找到的 PK，UNION 返回第一个成员加上后续成员新增的 PK |

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
- 每个查询（或查询组）的返回条数必须等于 Trace 的原生期望数，否则回放失败；结果折入与参考回放相同的校验和，`--reference` 给出参考回放报告时要求查询数、结果项数与校验和完全相同。
- **跨 client 有序**：查询的结果依赖源顺序上更早的全部写入。多条流并发执行时，只有服务按 `sequence` 跨流执行才能重现；不同连接上的到达顺序不等于发送顺序（实测：不排序的服务在 64 client 下第 7,370 个请求即出现期望 100 条、返回 99 条），所以服务端单纯加锁串行执行也不够。驱动不做等待或屏障去修补结果，也不重试。`client_count > 1` 且服务未声明该能力时，`replay-service` 直接失败。这意味着多 client 实验的可行性与代价本身就是被测服务的一项结果，不是驱动的默认前提。
- 阶段为 Baseline 加载、Flush、负载、Flush，分别计时；报告带各 client 的操作、查询、写入与工作单元数。

`index-server` 以 `purpose: index_service` 配置（[示例](../../config/index_service_rocksdb.yaml)）在给定端口提供一个新建的 RocksDB 索引；目录已存在即拒绝，一个索引只由一份 Trace 加载一次。

## RocksDB 实现

`RocksDbBackend` 按 Trace 契约的 KV 布局存储：`table(u32) ++ index(u32) ++ [indexed_value] ++ PK ++ ~version(u64)`，大端，同一条目的各版本从新到旧相邻；value 为存在标记、indexed_value 长度及 file / position / row_id。按版本读定位到 `~read_version` 处取第一个同条目版本；逻辑删除是一个不存在的版本。Baseline INSERT 不写 WAL，`Flush` 时刷盘；其余写入走 WAL 不 fsync。

服务端配置 `service.ordered: true` 时，`IndexServer` 按 `sequence` 执行：先到的请求暂存，直到全部更小的序号执行完，因此声明跨 client 有序（sysbench Trace 的 64 client 回放与参考回放结果校验和相同）；暂存量受驱动在途上限约束，重复或已执行过的序号返回 INVALID。代价是整个服务按发送顺序串行执行，多 client 只增加连接与流，不增加服务端并行度。`false` 时各流按到达立即并发执行，只能单 client 回放。进程内 adapter 在发送线程上逐个执行，天然按发送顺序。这是协议的第一个参照实现，用于验证协议与驱动，不代表 RocksDB 调优后的性能。Rockset 类服务（rocksdb-cloud 等）、其它 KV 存储按同一协议接入。

## 边界

- 尚未覆盖：持久化与崩溃恢复验证、写入结果未知时的诊断、限速与预热流程、物理 GC（`versions.gc.owner` 仍为 none）。
- 按 `sequence` 全序执行是能重现原生结果的最严格语义，不是唯一的可行语义。查询实际只依赖 `read_version` 以内的写入与同事务内更早的写入，服务可以按版本水位等待以保留并行度；这类语义需要协议另外携带依赖信息，待真实服务接入时再定，在此之前多 client 只接受按序号执行的服务。
- 协议发送的是逻辑操作，Locator 指向的 Parquet 由驱动侧的文件生命周期负责；服务不读取数据文件。
