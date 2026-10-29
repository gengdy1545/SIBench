# Index Service 契约

更新：2026-10-29。当前协议为 `sibench.index.v2`，定义在 `src/main/proto/index_service.proto`。服务统一使用完整提交发布；没有全局发送序号调度、调度模式开关或旧协议读取分支。Trace 仍为当前 v9，索引键、读取语义和逐查询摘要格式不变。

## 接口与确认边界

| 接口 | 语义 |
| --- | --- |
| `Describe` | `protocol_version=2`，声明 value 种类、按版本读取、同版本覆盖、事务内读、查询组、完整提交发布、Baseline 批量写及确认 / Flush 持久化层级。驱动拒绝其它协议版本、缺少完整提交发布或不支持输入所需能力的服务。 |
| `Execute` | 每个 client 一条双向流；请求 id 在流内唯一，响应按 id 配对，可以乱序完成。服务按 `transaction_id` / `transaction_operation` 执行同一事务，不同事务可并行；连接到达顺序不能决定版本可见性。 |
| `Flush` | 等待已接受请求到达声明的持久化层级，记录完整发布水位与 Trace binding，返回索引位置。用作加载、workload 和恢复边界，不作为驱动补救查询顺序的逐请求屏障。 |

写入结果未知时不自动重试。`INVALID` 表示请求违反契约；`UNAVAILABLE` 表示执行失败，写入可能已经生效。首次错误、之后的排空与进程清理分别记录。

## 数据、事务与读取

索引条目为 `(table, index, indexed_value, PK)`；主键索引 `index=0` 没有 indexed value。键组件使用 `IndexKey` 的保序编码。value 为 RowID，或两个完整 int64 的 Locator `(file, position)`；两种模式的能力分别核验。

| 操作 | 约束 |
| --- | --- |
| Baseline `INSERT` | version 0；不写 WAL，到 Flush 才保证声明的持久化。允许 `WriteBatch`，整批先验证，非法成员使整批失败；在途额度和逻辑调用数仍按成员计。 |
| `UPSERT` / `LOGICAL_DELETE` | 按新版本写入；同键同版本按事务内位置覆盖。删除保留不可见版本，不能使旧值复活。 |
| `RELOCATE` | 必须命中已有精确版本，改变物理位置而不创造逻辑版本；不表示完整 Rewrite / GC 协议已实现。 |
| `Search` | 在 read_version 选择不高于读点的最新未删除版本；支持主键 / 二级范围、开闭界、方向、limit 及钉住 PK 的二级查找。结果按索引值、PK 排序，倒序时反转。 |
| 事务内 `Search` | read_version 保留原语句快照，local_version 指定本事务的精确版本，只叠加更早 transaction_operation 的本事务写入；其它中间提交不可见。 |
| `SearchGroup` | 同一源访问的成员在服务端按 PK 求交或并；交集沿首成员顺序，并集保留首出现顺序。成员必须有相同 table、读取点和组身份。 |
| `Commit` | 声明事务版本、实际已发送写入数与操作数；操作数包含查询、组和写入，过滤后的空事务也有提交标记。 |

发布版本 V 必须同时满足：提交标记已收到、该事务所有操作完成、写入数一致、V−1 已发布。最大已见版本不能替代连续完整水位。快照读等待 read_version 发布；事务内读还等待同一事务更早的位置。

已发布版本的迟到写入、跨事务复用版本、pending / executing 重复位置在接触后端前拒绝，不允许返回失败后仍改变状态。已完成事务 lane 释放，用位图保留拒绝迟到请求所需的身份；位图仍随最大 transaction_id 增长，未实现前缀回收。

## 结果与校验

普通查询每 65,536 条结果分块返回，`more=true` 表示后续仍有同 id 的块；仅最后一块携带最终状态。服务遵守 gRPC 流控，驱动逐块累积摘要，不保留整个普通查询结果。查询组仍整体物化，其大规模容量未证明。

每个查询校验原生期望条数及有序 SHA-256，摘要包含源查询身份、范围 / 钉住键、读取点、事务位置和完整 `(PK, version, value)`；Locator 的两个 int64 都参与。错键、错版本、错 value、顺序变化、跨查询交换、丢块和未核对响应都必须失败。

参考 sidecar 的头部绑定 Baseline / workload Trace、v9、value 和二级过滤配置，文件 SHA-256 与参考报告一致。显式提供的参考报告必须包含当前逐查询摘要；缺失时拒绝，不退回全局 XOR。未提供参考报告时，可调用同一 Java 参考回放生成期望。组合矩阵比较 `query_digest_sha256`，另保留最终状态、选中版本和操作顺序的独立校验职责。

`request_bytes` 累计 `Request.getSerializedSize()`（含批请求）；`result_bytes` 累计 `Result.getSerializedSize()`，在收到最终响应并核对条数后计入。不包含响应外层、重复字段标签 / 长度、空响应、gRPC / HTTP2 / TCP 开销或重传，不作为网络字节。

## 发布执行与恢复

`IndexServer` 和进程内 adapter 均使用 `PublishedIndex`。未就绪请求挂在事务 lane 或按版本索引的等待表上，不占工作线程；就绪后写入 / commit 交给固定 worker 池，查询使用独立线程池，后端调用与响应回调不持有排序锁。Baseline 无需事务排序，可并行加载。

`Flush` 将发布水位与 Trace binding 写入索引，与数据一起持久化。相同索引拒绝另一份 Trace 或 value / 二级配置的 binding。最小 index manifest 与报告同目录，包含输入身份、版本覆盖、键 / value 编码、后端位置、构建与摘要依据；source snapshot 未接入时为 N/A。

`reopen: true` 打开已有索引并恢复最后一次 Flush 的水位；该水位以上的版本不可见。`verify-service` 按原读点核对全部可重放快照查询，事务内查询单列不核对；它们依赖运行中的事务前缀，不能在事务结束后伪造重放。

## RocksDB 实现

`RocksDbBackend` 的键布局为 `table(u32) ++ index(u32) ++ [indexed_value] ++ PK ++ ~version(u64)`，同一 entry 的版本从新到旧相邻。按版本定位到不高于读点的最新版本；事务内读先取该精确 local_version，不存在才回退快照。Baseline 不写 WAL，其余写入走本地 WAL、不 fsync。

维护配置为 [index_service_rocksdb.yaml](../../config/index_service_rocksdb.yaml)、[service_replay_grpc.yaml](../../config/service_replay_grpc.yaml) 和 [service_replay_rocksdb.yaml](../../config/service_replay_rocksdb.yaml)，统一默认 row_id。Locator 是当前支持的另一种 value，双方配置必须一致。

## rocksdb-cloud 后端

薄 JNI 只转发字节，复用同一 Java 键布局、版本选择与发布逻辑。版本固定在 `index/versions.yaml`，构建入口为 `scripts/build-rocksdb-cloud.sh`；配置使用 `storage`、`profile` 和 profile prefix 下的独立 `object_path`，凭据只从环境读取。

| 项目 | 语义 |
| --- | --- |
| 写确认 | memtable + 本地 WAL，不 fsync；Baseline 不写 WAL。 |
| Flush | SST / MANIFEST 上传并确认后返回，保证最后一次 Flush 前远端持久；之后的写入可能只在本地。 |
| 冷启动 | `keep_local_sst_files=true`；丢失本地目录后从 bucket 下载所需 SST，未上传的 WAL 不可恢复。 |
| 删除 | 不自动建桶；`run_purger=false`，普通过期 / 不可见文件删除默认延迟 1 h；桶字节可暂时高于本地。不是逻辑版本 GC。 |
| 计量 | 每次 Flush 写入 `<index>.engine-stats.jsonl`；本地后端对象请求为 N/A。 |

对象请求按类型记录次数、失败数、字节、累计微秒及固定 32 桶直方图，上界为 1、2、4…2^30 μs，最后一桶溢出；`overflow_after=2^30`、溢出上界 `Long.MAX_VALUE`。p50 / p95 / p99 为最近秩所在桶的上界；成功、失败都计入，samples=0 不解释分位数。并发快照不是事务性的，分位数按直方图自身 samples 计算。回调不单列 SDK 内部 HTTP 重试，累计并行时间不能加到 wall。

Java / JNI 的统计格式必须配套，缺少直方图的 JNI 在打开时拒绝；不保留旧格式分支。`summarize_index_run.py` 从同一次运行的 `EVENT_LOG_v1` 汇总实际 flush / compaction，绑定报告、日志与分析脚本身份；job 时间不等于进程 CPU，sequence number 不等于用户键范围，缺少日志为 N/A。

## 验证边界

读取、发布与结果校验的反例见 [正确性验证依据](../validation/correctness.md)；三来源完整输入见 [完整验收](../validation/acceptance.md)，对象存储见 [存储与恢复证据](../validation/rocksdb-cloud.md)。

尚未实现数据文件 Rewrite、物理版本 GC、未知写入的恢复重试和完整 source snapshot 生命周期；目标规模崩溃恢复、巨大查询组和正式后端性能排名未验收。已保留完整输入的证据按未改变的 Trace、查询与发布语义复用；协议 2 与单一服务入口由当前测试另行验证，不能把原构建计时当作当前构建性能。
