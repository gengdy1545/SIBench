# Trace 契约

## 两层产物

原始层保存一次实际执行的请求、参数、结果、索引候选、行变更和提交/回滚。规范层只转换已声明支持的索引操作，保留源事件关联，不重执行 SQL 推测结果。PG 页分裂、HOT、VACUUM 等内部实现不转换为下游 KV 算法。

默认原生执行为 HyBench `runHTAP`，保存 AP/TP/并发 AT-IQ 的多会话采集，行变化触发器覆盖八表。当前 v5 **规范投影**仍限于显式单会话 TP 子集：8 表主键加载、savingaccount 主键查询/修改、transfer 两个 sourceid/targetid 二级索引；支持已提交事务内的写后查询。HTAP 不套用单会话提交顺序，未支持的事件或可见性拒绝转换。覆盖见 [HTAP 验证](../validation/hybench-sf1-htap.md)、[TP 子集验证](../validation/hybench-sf1-poc.md) 和 [保序发送验证](../validation/committed-change-delivery.md)。

`CapturePoc pack` 将 HTAP PG 原始事件写入 64 字节 access 记录及无损 `payload.jsonl`；块使用 CRC32C，输入及输出保存 SHA256。事件码 1–15 不变，16 为 `executor_run_end`，记录执行器返回的事实；`executor_end` 是清理生命周期事件，关闭连接时可能没有该事件。按文件名排序保留各会话内部顺序，不声明全局提交顺序或 KV 回放语义。

## 身份与版本

```text
PrimaryKey   = (Table, IndexID=0, PK, CommitVersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, CommitVersionDescending)
Locator      = (FileID, FileRowPosition)
LogicalValue = RowID
```

- IndexID=0 为主键，transfer 的 1/2 为 sourceid/targetid。二级非唯一谓词是前缀查询，不改为一次 PK Get。
- CommitVersion 由 SIBench 按原始单会话每次提交顺序递增，包括只读提交，Baseline=0；同事务所有修改一致。不使用业务 timestamp、墙钟、XID 大小或下游完成顺序。
- 回滚不推进 CommitVersion，不分配 RowID，不改变已提交索引状态，也不写入增量 Parquet。后续事务从最近的已提交状态继续。
- transactionId 是源提交事件序号。修改的 mutationOrdinal 是事务内行修改序号，二级展开共享序号；组内查询携带同一 transactionId，mutationOrdinal 表示查询前已发生的行修改数。两者只用于来源关联和顺序校验，不作为 KV 版本后缀，不要求索引维护事务状态。
- ReadTimestamp 表示查询可见的提交上界，在 T 选择同键最大版本 `<=T`。没有版本或选中删除版本时不存在，不能跳过删除标记返回旧值。
- 单会话事务首次修改前的查询使用前一次提交的 ReadTimestamp；修改后的查询使用本源事务的 CommitVersion，并标记 TRANSACTION_LOCAL_SEARCH。必须按原始位置发送：先完成前面的修改，再查询，最后执行后续修改。因此查询能看到此前写入，不能看到同事务尚未发送的修改。事务结束后同键同版本只保留最后效果。
- 上述读己之写转换仅对单会话保序输入成立；不得推广为并发事务都读取自身 CommitVersion。并发快照尚无完整映射，不能把原始 xmin/xmax/xip 当作通用提交版本。显式历史查询选择可用历史点，未来读点拒绝。
- UPDATE 保留 RowID；DELETE 后重插入分配新 RowID。Rewrite 不改变逻辑版本或 RowID。
- FileID 从 1 分配且不复用；文件内行号从 0 开始。数据集隔离，int64 无损解析。

## 当前二进制格式

v5 使用 112 字节记录、每块 4096 条、little-endian 和 CRC32C。字段由 `IndexOperationTrace` 编解码，组边界、查询位置和展开顺序由 `ReplayProjection` 校验。

| 操作 | 语义 |
| --- | --- |
| INSERT | Baseline 索引加载 |
| SEARCH | 等值谓词；version=0、readTimestamp>=0；flags：SNAPSHOT_SEARCH=1、DERIVED_SEARCH=2、TRANSACTION_LOCAL_SEARCH=4 |
| UPSERT | 带提交版本的写入；flags：业务 INSERT=1、UPDATE=2 |
| LOGICAL_DELETE | 显式版本化失效，不使用合法 Locator/RowID 作为哨兵 |
| RELOCATE | 更新完整索引键的确切版本位置；删除或不存在版本拒绝 |
| TRANSACTION_BEGIN / COMMIT | 已提交源事务的操作组边界，TableID=0；仅驱动消费，不发送给目标索引 |

非 SEARCH 的 readTimestamp=-1。transfer 每行展开主键和两个二级条目，发送端不得再次隐式维护。当前编译器仅支持 savingaccount 的受控 DELETE，拒绝原生重插入及索引列变更。

规范 Trace 保留已提交源事务内查询与修改的原始相对顺序，查询占据原生扫描开始的位置，候选数在扫描结束时补齐。transfer 的一次行修改连续展开为主键、sourceid、targetid 三项，查询不得插在展开中间。增量 Parquet 保留每次修改，行含 `_sib_timestamp` 和 `_sib_mutation_ordinal`。同键同版本按序覆盖，中间效果服务于原始事务内查询，不产生额外提交版本；后续源事务只能看到最终提交效果。

读兼容：v4 同为 112 字节但没有组内写后查询契约；v3 为 104 字节，旧 SEARCH.version 映射为 ReadTimestamp；v1/v2 为 88 字节，仅支持最新状态兼容，不具备可信事务版本。v1 的 transfer 行写入在读取时展开，其他格式已显式展开。旧文件不改写，新编译输出 v5；只保留旧格式读取，不保留旧发送策略。

## 发送选择

默认只向下游发送已提交变更；这一约定不限定具体 Index Service 场景。原始 Trace 完整保留修改尝试、提交和回滚，规范转换识别事务结果，只将已提交业务变更展开为下游索引修改。回滚事务的修改不发布，不生成反向补偿操作，也不增加回撤开关。正常业务回滚不等于采集失败；未完成事务不得当作回滚处理后静默消费。

这里的“只发送已提交变更”约束写入，不关闭查询。成功提交的事务中，写后查询按上述时间戳和事务内位置转换；回滚事务中，首次修改前的受支持查询保留，首次修改后的查询不发送，因为其读取环境依赖已排除的修改。当前按“此前是否出现修改”保守筛选，不猜测查询与修改键的依赖关系；排除量独立报告。

当前单会话编译器在看到源 commit 后才生成该事务的规范操作组及增量行，遇到配对审计通过的 abort 则排除修改及其后的查询。规范 Trace 不生成 ABORT 或反向操作；回滚事实在原始 Trace 中保留。manifest 的 `change_delivery=committed_only`、`raw_row_changes`、`discarded_aborted_row_changes`、`logical_row_version_writes` 和 `index_version_writes` 分别说明发送约定、原始行修改、回滚排除量、已提交行修改和展开后的索引写入；另报告 `transaction_local_searches` 和 `discarded_aborted_transaction_local_searches`。`aborted_transactions` 包括显式回滚和关闭连接产生的 abort。PG `index_insert_attempt` 不等同于下游写入。

```yaml
target:
  secondary_indexes:
    enabled: true
replay:
  strategy: single_pass
  dispatch_mode: operation
```

二级开关过滤所有 IndexID>0 的加载、查询、写入、逻辑失效和搬迁，不改变主键身份、源顺序、版本、依赖或 Parquet，不替换被过滤查询。

| dispatch_mode | 发送单位 | 顺序与完成契约 |
| --- | --- | --- |
| operation（默认） | 每次一个索引操作 | 同一线程逐项发送，当前操作执行完成后才发送下一项 |
| transaction | 一个已提交源事务的有序操作列表，含查询 | 同一线程调用一次分组接口，组内逐项完成，整组完成后才发送下一组 |

两种模式执行相同操作，不合并同键修改，不移动查询，不跨事务重排。`transaction` 仅决定分组调用，不要求原子执行、BEGIN/COMMIT RPC、回滚或事务缓存。`ReplayTarget` 的返回表示操作或整组已经执行完成；异步 adapter 必须等待完成，不能仅入队后返回。组失败后驱动停止，不自动重试或补偿。

驱动先缓冲并校验完整源组，确认尾部 COMMIT 后才发送；目标索引收到的是独立版本写入与带 ReadTimestamp 的查询，没有控制记录。每组最多 1,000,000 个展开操作（含被过滤操作），编译器也限制待处理源操作数量；这是输入保护，不是大规模可落盘状态方案。发送端只用一个线程，保持规范输入的事务顺序和事务内顺序；多会话原生 benchmark 的全局顺序尚未捕获，不能据此声称完整 HTAP 保序回放已完成。

报告区分落盘记录（含控制）、来源索引操作、二级过滤、实际发送、源事务及各操作类型。`target_calls` 是驱动对 adapter 的调用数，不是网络请求数；`sending_threads=1`、`downstream_transaction_api_required=false` 明确发送契约。RowID 搬迁无索引写入。历史主键和 transfer 等值查询已实现；历史范围、完整并发和 GC 未实现。

## 完成与失败

派生 Baseline 包含 `resolved-config.json`、`files.jsonl`、`manifest.json` 和 Parquet；未完成保留 `INCOMPLETE`。规范编译同样用完成 manifest 和校验值发布，消费者拒绝不完整或被修改产物。

原生运行在 raw 目录写 `capture-run.json`：

- `running`：未完成，不能编译。
- `failed`：保留首个错误、SQLSTATE、请求/事务上下文及堆栈；不能编译或续接。
- `complete`：连接正常关闭，同次 JDBC/PG 请求、结果和提交关联审计通过。HTAP 保存 phases 和所有 sessions 的配对原始文件 SHA256，消费前复核全部配对摘要、清单及审计；旧子集保留单会话格式。

JDBC 异常即使被原生方法捕获也会使当前运行失败；记录或清理异常不覆盖原始原因。旧无运行 manifest 的采集必须找到唯一配对 JDBC 日志并通过同样审计。审计不等于全局丢事件证明。

非预期错误依据诊断修复，恢复明确初始状态并换新目录重跑，不猜测 PG 内部实现、不补造事件。单会话及 HTAP 原始审计核对显式业务回滚和关闭 abort 的请求身份；关闭时仍有事务必须存在对应服务端结束边界，不能因文件已关闭而静默丢弃。八表状态审计和支持范围内的编译均排除回滚修改，不据此拒绝整次有效采集，也不自动重试。编译仍拒绝事务 XID 不匹配、未完成扫描和完整 HTAP 输入；发送仍拒绝无匹配边界、版本/查询位置错误、展开不完整及线程切换。最终状态正确不能替代并发访问可见性或全局事件完整性证明。

## 长时输入与验证

策略固定为足量连续原生执行、事务边界分段、离线编译和 single_pass；不循环、不重排、不重映射键。分段续编译和 Checkpoint 尚未实现。

预算按二级过滤后的有效运行期操作计算，两种发送模式预算相同，排除 Baseline、控制和派生探针：预热操作 + ceil(速率 × 测量秒数 × (1 + 余量))。`long_replay.yaml` 为 18,000,000 + 300,000 × 1,800 × 1.2 = 666,000,000。不足在加载/发送前拒绝；当前预检只证明数量，不证明持续调度。

`check-replay` 校验 manifest、摘要、格式、记录数、CRC 和预算；`replay` 执行两种 value 的状态与查询核对。`validate-snapshots` 要求完整 v3–v5 且无 relocation，以独立行历史 oracle 比较 key/version/value：全部修改主键的基线、每个提交前/提交点和最终点；每个二级索引最多 64 个修改谓词的基线、首次修改前/修改点和最终点。探针独立报告，不写入 Trace 或计入原生负载。
