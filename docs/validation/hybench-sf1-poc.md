# HyBench SF1 TP 子集验证

此报告记录历史 TP 子集及 v4 回放证据。默认入口已切换到原生 `runHTAP`，对应 [HTAP SF1 报告](hybench-sf1-htap.md)；本报告不作为 HTAP 访问负载的验证结论。

更新：2026-10-20。结论：原生单会话子集、数据核对、v4 编译及历史版本参考验证通过。真实 Backend、完整 HyBench 和 SF1000 正式 Trace 尚未验收。

## 环境与参数

| 项目 | 生效值 |
| --- | --- |
| HyBench | 提交 eaad91b52956fddd3d48ce49f792928056b7b360，子模块未修改 |
| PG | 14.24，隔离构建副本应用主项目采集插桩 |
| 原生负载 | sf=1x，1 客户端，60 秒，TP3/TP5/TP9=35/30/35 |
| 隔离与服务 | Read Committed；本机 55433；禁用并行查询、并行建索引、autovacuum |
| 文件目标 | 268435456 字节（256 MiB） |
| Row group | 最多 1048576 行；编码缓冲 67108864 字节 |
| 压缩 | Zstd level 3，单线程；page/dictionary 1048576 字节 |

使用原生生成器、DDL、加载 SQL、索引与事务方法；只是显式选择 TP 子集，不修改业务逻辑。SIBench 调用原生随机选择器，后续版本/发送设置不改变采集事实。`hybench_sf1000.yaml` 属于派生 Baseline，不能作为正式原生采集入口。

## 验证流程

1. **原生 Load**：生成 SF1 输入，使用原生 PG DDL 与加载 SQL。从 Load 起保存索引维护和后续建索引事件；原生构建成本不等于规范 INSERT Loading。
2. **Baseline 导出**：在无增量写入边界按 PK 读取 PG 实际值，保留数据库类型转换后的内容；分配 FileID、文件内行号、RowID，版本统一为 0。独立 PG 重载核对全部业务列和身份，文件摘要写入 `verified-manifest.json`。
3. **Baseline 索引抽取**：读取已验证 Parquet 的 PK、版本、RowID 和文件位置，生成 8 表主键 INSERT。transfer 另按 sourceid/targetid 展开两个二级条目，共 20706000 条。
4. **原生事务采集**：JDBC 记录请求、参数和消费结果；PG 记录实际索引身份、搜索键、扫描候选、结果、行变更与提交。同次执行关联，不补跑查询。
5. **Replay 索引抽取**：实际扫描转换为主键点查或二级等值查询；已提交行变化写入增量 Parquet，展开相关索引版本写入。按原始提交顺序映射 CommitVersion，扫描开始记录 ReadTimestamp；同事务原始重复修改保留。
6. **参考回放**：消费同一 Trace，核对二级开/关和 Locator/RowID 组合。历史探针用独立行历史 oracle 比较每条 key/version/value，不混入原生负载。当前两种保序发送模式另见 [发送验证](committed-change-delivery.md)。

TP3 是主键查询；TP5 按 transfer 两个二级索引访问候选，保留 ORDER BY/LIMIT 的实际执行，不改成返回 PK 的一次 Get；TP9 读取余额后更新两个 savingaccount 行并 INSERT transfer。当前没有 AP、DELETE 或其他表修改，不能宣称完整 CRUD。

## 文件与结果

基线行数：customer=300000、company=2000、savingaccount/checkingaccount 各 302000、transfer=6000000、checking/loanapps/loantrans 各 600000，总计 8706000。

| 项目 | 结果 |
| --- | ---: |
| Baseline Parquet | 8 文件，160132003 字节 |
| transfer 文件 | 111217512 字节，5 × 1048576 行 + 757120 行 |
| 增量 savingaccount | 51254 行，1 组，1096168 字节 |
| 增量 transfer | 25627 行，1 组，502050 字节 |
| TP3 / TP5 / TP9 | 25846 / 22109 / 25627 |
| JDBC 请求 | 150463，未关联 PG 事件 0 |
| 原生提交 / 行修改 | 73582 / 76881 |
| 原生查询 | 146945：主键 102727，二级 44218 |
| TP5 bitmap 候选 / 消费结果核对 | 889309 / 25627 行 |
| v4 运行期记录 | 422244：索引 275080，事务控制 147164 |

文件未达到 256 MiB 是正常尾文件，不能据此调整正式参数；SF1 未覆盖大文件 rollover。两次独立 JVM 编译的 Trace 和增量 Parquet 均字节一致。

| 二级 | 保留全部操作时的有效运行期操作 | 保留索引版本 |
| --- | ---: | ---: |
| 开 | 275080 | 20834134 |
| 关 | 179608 | 8782880 |

两种 value 均通过。采集中有一次事务内同账户重复修改，当前发送完整保留这两次修改。全部 72870 个修改主键产生每种 value 299500 个历史探针；二级开启另有 512 个集合探针。原生指定历史点 SQL 数为 0。历史日志与报告保留原记录，不作为当前发送配置说明。

版本由 SIBench 保序映射，不随机重排。同事务所有写入相同版本，查询选择最大版本 `<=ReadTimestamp`。例如首次 INSERT 在 100，随后 110、120 修改：读 90 不存在，读 105 返回 100，读 130 返回 120；实际基线键从版本 0 可见。逻辑删除阻止旧值复活；DELETE/重插入及历史版本搬迁另用受控测试。

强制最新行搬迁验证 47243 个账户：Locator 写入 47243，RowID 写入 0。不是自然 70% 阈值 Rewrite、全历史搬迁或 GC。短跑/裸解码数字不作为真实服务或持续吞吐结果。长时预算为 666000000 个有效操作，现有输入预检失败，不循环扩充。

## 错误处理

非预期 PG/JDBC/采集错误使本次运行无效。`capture-run.json` 保存首个原因、SQLSTATE、请求/事务上下文和堆栈；即使原生方法捕获异常，也不能发布完成状态。正常关闭并通过同次 JDBC/PG 审计后才标 complete；编译复核摘要并拒绝 failed/running。旧采集必须通过配对日志审计。

依据真实报错修复配置、适配或采集问题，恢复明确初始状态，用新运行目录重跑；不维护或猜测 PG 内部算法、不拼接失败前缀。来源预期失败不改变其业务语义。当前单会话编译排除配对审计通过的回滚修改及修改后的查询，见 [提交变更发送验证](committed-change-delivery.md)。来源预期 SQL 错误与重试的完整转换仍待实现。

错误路径通过 JDBC 代理测试覆盖请求执行、准备、提交、连接设置、结果读取、会话标记、关闭及初始化；还验证首个错误保留、缺失服务端日志、失败前缀、旧日志回滚/错误/截断拒绝和文件被改动。JDK 17 离线构建与 48 项测试通过。此次未新运行 PG 原生事务。

## 使用入口

```bash
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc check "$PWD" config/hybench_tp_subset_sf1.yaml
java -Xmx2g -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc compile "$PWD" config/hybench_tp_subset_sf1.yaml results/poc/hybench-sf1/raw/pg-1475807.jsonl results/poc/hybench-sf1/normalized-check
```

`check` 不连接 PG。`export`/`run` 使用预先准备的专用 PG，不隐式启动服务；新运行使用新 artifacts/raw 目录和同一初始逻辑数据。`run` 在原生 `Data_1x` 所在工作目录执行，classpath 包含固定 HyBench classes/lib 和不再覆盖的 SIBench JAR。编译要求独立验证的 Baseline，输出目录必须不存在。回放命令见 [README](../../README.md#原生采集与回放)。

PG 构建：固定 14.24 源码解压到 `build/benchmarks/postgresql-14.24/`，执行 `make -C src/main/native/postgres install extension`，只作用于隔离副本。启动设置 `SIBENCH_CAPTURE_DIR` 为与 JDBC 一致的 raw 目录，使用独立数据目录、端口和 UTC。application_name 前缀为 `sibench:`；触发器安装时由 `capture_library` 指定 row_capture.so。历史运行只安装 savingaccount/transfer 触发器，当前安装脚本扩展到八表。自动编排尚未完成。

## 证据与剩余工作

原始证据：`results/poc/hybench-sf1/` 下的 native 输入、加载/建索引日志、`baseline/verified-manifest.json`、`raw/pg-1475807.jsonl`、`raw/jdbc-1475783-1.jsonl` 和各 audit 报告。

历史 v4 证据：`results/validation/hybench-sf1-snapshot-lookup-20261020/` 下的 normalized/、normalized-repeat/、当次配置及 replay/snapshots 报告、兼容检查和 relocation-control/。原始日志、manifest 和既有产物未改写；历史配置路径保留原记录，现用配置统一位于 `config/`。

本次错误策略离线回归位于 `results/validation/hybench-sf1-capture-policy-20261020/`：原始配对日志审计通过，重新编译的两份 Trace 和两个增量 Parquet 摘要与历史 v4 证据一致，当次默认发送配置下 Locator/RowID 回放通过。

后续“默认只发送已提交变更”的代码、受控回滚测试和既有 SF1 离线回归单独记录于 [提交变更筛选验证](committed-change-delivery.md)，不改写本报告历史日志与计数。

剩余：完整多表 CRUD/索引列变更、来源预期 SQL 错误/重试转换、并发快照/跨会话顺序/历史范围、全局完整性与采集开销、真实 Backend、自然 Rewrite/GC、历史 payload 回读、大规模状态/rollover/Checkpoint 和长时调度。单会话已提交事务内写后查询已受控验证。当前状态与验收统一见 [计划](../../plan.md)。
