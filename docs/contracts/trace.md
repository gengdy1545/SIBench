# Trace 契约

## 两层产物

原始层保存一次实际执行的请求、参数、结果、索引候选、行变更和提交/回滚。规范层只转换已声明支持的索引操作，保留源事件关联，不重执行 SQL 推测结果。PG 页分裂、HOT、VACUUM 等内部实现不转换为下游 KV 算法。

当前原生范围：8 表主键加载、savingaccount 主键查询/修改、transfer 两个 sourceid/targetid 二级索引。未支持的事件或可见性拒绝转换。详细覆盖见 [SF1 验证](../validation/hybench-sf1-poc.md)。

## 身份与版本

```text
PrimaryKey   = (Table, IndexID=0, PK, CommitVersionDescending)
SecondaryKey = (Table, IndexID, IndexedValue, PK, CommitVersionDescending)
Locator      = (FileID, FileRowPosition)
LogicalValue = RowID
```

- IndexID=0 为主键，transfer 的 1/2 为 sourceid/targetid。二级非唯一谓词是前缀查询，不改为一次 PK Get。
- CommitVersion 由 SIBench 按原始单会话每次提交顺序递增，包括只读提交，Baseline=0；同事务所有修改一致。不使用业务 timestamp、墙钟、XID 大小或下游完成顺序。
- transactionId 是源提交事件序号，mutationOrdinal 是事务内行修改序号；二级展开共享序号。两者不作为版本后缀。
- ReadTimestamp 表示查询可见的提交上界，在 T 选择同键最大版本 `<=T`。没有版本或选中删除版本时不存在，不能跳过删除标记返回旧值。
- 原生当前查询在扫描开始保存最新已发布点。历史查询须显式标记，未来读点拒绝。并发快照、读己之写尚无完整映射，不能把原始 xmin/xmax/xip 当作通用提交版本。
- UPDATE 保留 RowID；DELETE 后重插入分配新 RowID。Rewrite 不改变逻辑版本或 RowID。
- FileID 从 1 分配且不复用；文件内行号从 0 开始。数据集隔离，int64 无损解析。

## 当前二进制格式

v4 使用 112 字节记录、每块 4096 条、little-endian 和 CRC32C。字段和合法组合由 `IndexOperationTrace` 校验。

| 操作 | 语义 |
| --- | --- |
| INSERT | Baseline 索引加载 |
| SEARCH | 等值谓词；version=0、readTimestamp>=0；flags：SNAPSHOT_SEARCH=1、DERIVED_SEARCH=2 |
| UPSERT | 带提交版本的写入；flags：业务 INSERT=1、UPDATE=2 |
| LOGICAL_DELETE | 显式版本化失效，不使用合法 Locator/RowID 作为哨兵 |
| RELOCATE | 更新完整索引键的确切版本位置；删除或不存在版本拒绝 |
| TRANSACTION_BEGIN / COMMIT | 已提交写入投影的发布边界，TableID=0 |

非 SEARCH 的 readTimestamp=-1。transfer 每行展开主键和两个二级条目，发送端不得再次隐式维护。当前编译器仅支持 savingaccount 的受控 DELETE，拒绝原生重插入及索引列变更。

规范 Trace/增量 Parquet 保留每次已提交事务中的原始修改，行含 `_sib_timestamp` 和 `_sib_mutation_ordinal`。同键同版本的中间效果用于审计，最终只形成一个提交版本，不让后续查询看到中间状态。

读兼容：v3 为 104 字节，旧 SEARCH.version 映射为 ReadTimestamp；v1/v2 为 88 字节，仅支持最新状态兼容，不具备可信事务版本。v1 的 transfer 行写入在读取时展开，其他格式已显式展开。旧文件不改写，新编译输出 v4。

## 发送选择

```yaml
target:
  secondary_indexes:
    enabled: true
  transaction_writes:
    coalesce_same_key: true
```

二级开关过滤所有 IndexID>0 的加载、查询、写入、逻辑失效和搬迁，不改变主键身份、源顺序、版本、依赖或 Parquet，不替换被过滤查询。

事务合并默认开启：在提交边界先过滤，再按完整索引键只保留最后效果，按最后出现顺序发送；最后为 DELETE 时必须保留。关闭时按原始顺序发送全部覆盖，但同键同版本仍只发布最后状态。不按 value 合并不同键，不跨事务合并历史版本。

当前参考模型串行暂存后发布，每事务最多 1,000,000 个展开操作；真实 adapter 尚未实现，必须保证同键顺序和发布屏障，能力不足应拒绝。历史主键和 transfer 等值查询已实现；历史范围、完整并发和 GC 未实现。

报告区分落盘记录（含控制）、来源索引操作、二级过滤、合并、省略后发送、事务及各操作类型。参考接收事件数不等于网络 API 请求数；RowID 搬迁无索引写入。

## 完成与失败

派生 Baseline 包含 `resolved-config.json`、`files.jsonl`、`manifest.json` 和 Parquet；未完成保留 `INCOMPLETE`。规范编译同样用完成 manifest 和校验值发布，消费者拒绝不完整或被修改产物。

原生运行在 raw 目录写 `capture-run.json`：

- `running`：未完成，不能编译。
- `failed`：保留首个错误、SQLSTATE、请求/事务上下文及堆栈；不能编译或续接。
- `complete`：连接正常关闭，同次 JDBC/PG 请求、结果和提交关联审计通过，保存两份原始文件 SHA256。

JDBC 异常即使被原生方法捕获也会使当前运行失败；记录或清理异常不覆盖原始原因。旧无运行 manifest 的采集必须找到唯一配对 JDBC 日志并通过同样审计。审计不等于全局丢事件证明。

非预期错误依据诊断修复，恢复明确初始状态并换新目录重跑，不猜测 PG 内部实现、不补造事件。来源预期失败保留实际查询/回滚且不发布失败修改；当前成功事务投影尚不支持其完整转换，遇到回滚明确拒绝，不自动忽略或重试。

## 长时输入与验证

策略固定为足量连续原生执行、事务边界分段、离线编译和 single_pass；不循环、不重排、不重映射键。分段续编译和 Checkpoint 尚未实现。

预算按过滤、合并后的有效运行期操作计算，排除 Baseline、控制和派生探针：预热操作 + ceil(速率 × 测量秒数 × (1 + 余量))。`long_replay.yaml` 为 18,000,000 + 300,000 × 1,800 × 1.2 = 666,000,000。不足在加载/发送前拒绝；当前预检只证明数量，不证明持续调度。

`check-replay` 校验 manifest、摘要、格式、记录数、CRC 和预算；`replay` 执行两种 value 的状态与查询核对。`validate-snapshots` 要求完整 v3/v4 且无 relocation，以独立行历史 oracle 比较 key/version/value：全部修改主键的基线、每个提交前/提交点和最终点；每个二级索引最多 64 个修改谓词的基线、首次修改前/修改点和最终点。探针独立报告，不写入 Trace 或计入原生负载。
