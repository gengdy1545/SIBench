# 最小负载记录契约

状态为采集接口设计 `trace_version=1`，供采集器实现。当前 smoke 仅输出运行日志和数据库行数，不产生本契约的真实逐请求记录。[records.example.jsonl](../../configs/trace/records.example.jsonl) 是人工构造的说明样例。

## 文件和标识

每个实验包含 manifest、初始键集合和按线程分片的压缩 JSONL 请求记录；分片以 manifest 列出的校验值校验，未完成的分片不得标为完成。初始键导出发生在加载结束、运行之前，记录数据库快照/停写条件。

| 字段 | 类型与语义 |
| --- | --- |
| trace_version | 整数1 |
| run_id / event_id | 实验内唯一标识；event_id稳定关联开始与完成 |
| benchmark | ycsb / sysbench / hybench |
| namespace | 原始表名，跨表相同PK不相撞 |
| phase | load / warmup / measure |
| thread_id / thread_seq | 字符串线程ID及线程内递增序号；不是数据库提交顺序 |
| operation_group_id | YCSB RMW、sysbench event 或 HyBench transaction 的逻辑分组 |
| transaction_id / attempt | 可空事务ID及从1开始的尝试编号；重试不覆盖原尝试 |
| record_type | request / transaction_end |
| operation | get / insert / update / delete / scan / other；事务结局记录为null |
| key / range | 带类型点键或范围二选一；未知时允许均为null，必须提供原因 |
| original | 原生操作名、SQL模板ID、类型化绑定参数；不记录连接凭据 |
| observation | submitted/success/error/unknown、原生状态、affected_rows、returned_rows、生成键等；未知计数为null |
| transaction_outcome | committed / rolled_back / unknown / not_applicable |
| projection | exact_key / access_range_only / unsupported / unknown，加原因 |
| timing | 同进程单调时钟开始/结束；记录单位ns和时钟域，不比较跨主机绝对顺序 |

请求记录可以在调用完成时写入，但仍必须保留提交数与未完成数的汇总。进程崩溃后不能从“缺少完成记录”推断操作未执行；manifest标记运行不完整，严格回放拒绝不完整输入。

事务最终状态由 `transaction_end` 记录决定；请求时尚不知道提交状态则记 unknown。自动提交调用成功时按实际 binding 语义标记 committed；无事务概念才标 not_applicable。连接断开而提交结果未知不归为回滚。

## 键与范围

整数键表示为 `{"type":"int64","value":"123"}`：十进制字符串避免JSON消费者丢失64位精度。字符串键为 `{"type":"utf8","value":"user00000123"}`。manifest声明比较器，当前支持数值顺序或UTF-8字节序；与参考库排序不一致的输入禁止默默按另一顺序回放。

闭区间例：`{"kind":"bounds","lower":{"type":"int64","value":"10"},"upper":{"type":"int64","value":"19"},"lower_inclusive":true,"upper_inclusive":true}`。

YCSB扫描例：`{"kind":"start_limit","start":{"type":"utf8","value":"user00000123"},"inclusive":true,"limit":10}`。它不蕴含结束键，也不保证返回10行。

数据库生成主键可在结果观测中补充为实际 key，同时标记 `key_origin=database_generated`。非PK谓词保持 original 和 unsupported，不能用返回键替换请求范围。

## 统计及投影规则

- 请求统计覆盖所有发起尝试，按操作、表、阶段、状态与尝试分组；事务失败不删除请求记录。
- 点键 distinct、频次和 min/max 仅对已知点键计算；范围端点单独统计。范围请求不自动展开成每个整数键，也不把端点频次当成点查热点。
- 已知键覆盖率以全部相应请求为分母，同时报告 unknown/unsupported 的数量。数字区间宽度单列，不等于访问行数或命中数。
- 首版严格回放选择成功且确认提交的可投影记录；受影响0行的写入记录为 no_effect 并从成功状态变更流中排除，计数单独显示。写入受影响行数未知则严格状态回放拒绝，不能补造成功。
- 请求级压力回放若保留回滚、未命中或失败请求，应另用显式实验模式，不与严格状态回放混称。
- 聚合查询投影为范围读取时标 access_range_only，报告不包含原聚合运算成本。
- 模拟 value 固定算法、种子、长度与版本号，保存在manifest。记录仅提供键访问语义，不能推断真实行值或二级索引旧值。

## manifest 最小内容

固定子模块提交、构建方式、原生配置及实际生效参数、时间和时区、分片校验值、原生数据集及初始键快照、键比较器、value生成规则、采集完整性、过滤计数、storage_profile标识和观测限制。数据库连接配置脱敏。后续修改必需字段或语义时提升版本；未知版本拒绝自动解释。

## 验收样例

采集器实现后必须覆盖 int64 大键、不同表相同键、开闭端点、YCSB start_limit、数据库生成键、回滚、重试、结果未知、0行写入、非PK查询和半截分片。当前仅验证说明样例JSON语法和契约自洽，不声称完成采集器测试。
