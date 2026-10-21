# HyBench SF1 全索引验证

验证日期：2026-10-21。固定 HyBench 提交 `eaad91b52956fddd3d48ce49f792928056b7b360`，JDK 17，根 Maven 项目。复用已经完成审计的原生 SF1 输入，不重执行 SQL、不启动 PostgreSQL、不修改上游源码或随机选择器。

产物目录：`results/validation/hybench-sf1-secondary-indexes-20261021/`。

## 实现与范围

`IndexCatalog` 从固定 PostgreSQL DDL 读取八表主键和全部 13 二级定义；TableID 保持表契约，IndexID 按每表源索引顺序分配。默认二级开启表示全部二级，不再只展开 transfer 的 sourceid / targetid。

`IndexMutation` 显式展开一行的全部索引维护。索引列值 UPDATE 在同一 CommitVersion 下失效旧二级身份，再 UPSERT 新身份；未改键的二级条目也更新关联行版本及值。UPDATE 保留 RowID，DELETE / 重插入分配新 RowID。同事务多次修改及其间查询保持原始位置，不合并、不生成反向回滚操作。v7 保存可空 UTF-8 索引值和连续展开位置 / 宽度；预检在过滤前核对固定源清单和全部维护条目。

完整 HTAP 的访问 / 快照投影尚未实现。下面两项证据独立：

- TP3/5/9 子集：真实规范 Trace、增量 Parquet、全部基线索引和多 client 回放；原生运行只更新 savingaccount 普通列并插入 transfer，不包含已有索引列值 UPDATE。
- 完整 HTAP：真实已提交 `old/new` 的逐键因果链维护审计，验证实际索引列值 UPDATE；逐键派生版本不是全局提交顺序，不生成完整 HTAP 规范 Trace，也不验证原生并发查询快照。

## TP 子集编译

来源为 `results/poc/hybench-sf1/baseline/` 和 `raw/pg-1475807.jsonl`，消费前重新核对配对 JDBC / PG 审计及来源摘要。两次独立 JVM 编译产物在 `normalized-jdk17/` 和 `normalized-repeat/`。

| 项目 | 结果 |
| --- | ---: |
| 基线行 / 主键条目 | 8,706,000 |
| 基线二级条目 | 22,506,000 |
| 基线全部索引条目 | 31,212,000 |
| 来源 SQL 工作单元 | 150,463 |
| 提交 / 回滚 | 73,582 / 0 |
| 已提交行修改 | 76,881 |
| UPDATE / INSERT / DELETE | 51,254 / 25,627 / 0 |
| 规范运行期记录，含控制 | 499,125 |
| 源控制记录 | 147,164 |
| 原生 SEARCH / 事务内写后 SEARCH | 146,945 / 25,627 |
| 全索引版本写入 | 205,016 |
| 二级开启运行期发送 | 351,961 |
| 二级关闭运行期发送 / 二级过滤 | 179,608 / 172,353 |

`compilation-comparison.json` 比较两次输出：Trace、Parquet 和工作单元清单字节完全一致；manifest 的所有字段和值结构化一致。JSON 对象属性顺序不是跨 JVM 字节契约。

增量文件为 `savingaccount-versions.parquet`（FileID 11，51,254 行）和 `transfer-versions.parquet`（FileID 13，25,627 行），不使用旧的固定文件名 / 两表 FileID 特例。两个 Parquet 的行内容摘要与历史相同，但 v7 Trace 增加完整二级条目及新格式，不能复用旧 Trace 摘要或操作数量。

## 回放与独立 Oracle

`replay-matrix/` 的 1/64/128 client × operation/transaction × 二级开启/关闭，共 12 组配置，每组 Locator / RowID 均通过。相同二级设置、相同 value 的发送顺序、查询结果、选中版本和最终主键状态摘要在全部变体间一致；响应排空，无未完成响应。client 是进程内共享状态的参考 client，不代表真实网络连接或跨连接可见性。

| 每种 value 的结果 | 二级开启 | 二级关闭 |
| --- | ---: | ---: |
| 基线发送 | 31,212,000 | 8,706,000 |
| 运行期发送 | 351,961 | 179,608 |
| 查询结果项核对 | 992,036 | 102,727 |
| 保留索引版本 | 31,417,014 | 8,782,880 |
| 有效运行期工作单元 | 150,463 | 128,354 |
| 128 client 的单 client 操作数范围 | 2,642–2,866 | 1,347–1,458 |

`parquet-oracle-secondary-false.json` / `parquet-oracle-secondary-true.json` 从独立基线及增量 Parquet 提取预期，不从二级 Trace 值计算答案：

- 全部 72,870 个修改主键在基线、每次该键提交前 / 提交点和最终点核对，每种 value 共 299,500 次主键探针。
- 二级开启时对全部修改行的完整二级身份逐版本核对，同时覆盖全部 13 个源二级索引的 775 个确定性非唯一等值谓词；每索引最多 64 个。每种 value 共 506,464 次二级身份 / 谓词探针、24,657,557 个派生结果项。
- 原生查询量和派生探针量分别统计；探针不写入规范 Trace，不计入原生供应或性能。
- 源 Parquet 摘要、增量文件位置、PK、RowID、提交版本和事务内修改序号同时核对。抽样非唯一结果集不是全部谓词或全部 SQL 的证明。

最终构建的 128 client oracle 单独保存为 `parquet-oracle-final-128.json`，包含同事务短暂索引值的提交后失效检查及最终预检边界。

## 真实 HTAP 索引值更新

来源为 `results/poc/hybench-htap-sf1/baseline/` 和 `raw/capture-run.json` 中四个配对会话。`native-maintenance-verified.json` 先重新验证完成审计与摘要，然后只收集提交行变化；不按 PID、XID、文件名或墙钟推断全局提交顺序。

| 项目 | 结果 |
| --- | ---: |
| 全部基线行 / 索引条目解码 | 8,706,000 / 31,212,000 |
| 已提交行变化 / 修改主键 | 5,408 / 3,706 |
| 回滚排除行变化 | 0 |
| 实际索引列值更新 | 34 |
| `loanapps.timestamp` / `idx_loanapps_2` | 15 |
| `loantrans.timestamp` / `idx_loantrans_2` | 19 |
| 两种 value 的旧 / 新键可见性探针 | 36,020 |

每键从独立基线全行出发，以真实 before-image 连接后续已提交变化，调用共享完整维护展开和参考模型，验证改键前旧身份可见、改键后旧身份不可见 / 新身份可见，以及关联版本 / 值。实际主键和索引值来自原生行，RowID / Locator 与版本为该维护审计的派生控制值，不是完整 HTAP 物理布局证明。

该轮 13 次原生业务回滚发生在写入前，因此排除行变化为 0；不能声称原生回滚改键已发生。回滚改键由受控测试单独核验。

## 受控边界与搬迁

JDK 17 离线 `mvn -o -q package`：88 项测试，失败 / 错误为 0。当前测试覆盖八表全索引基线、timestamp 往返改键及其间查询、回滚改键、回滚不占 RowID / 版本、DELETE / 重插入、未改键的行版本刷新、null / 空串 / CHAR / VARCHAR、超过 int32 的 PK、三个二级索引，以及源码漏项、未支持 DDL、维护截断、事务 / 扫描未完成和非法 UTF-8 拒绝。

`RelocationCheck` 不再固定 savingaccount 或 FileID，按 manifest 选择所有修改表的最新存活行，读写真实 Parquet 并验证主键及全部二级条目的精确版本搬迁。SF1 控制产物位于 `relocation/`，检查 Locator 更新和 RowID 不产生索引搬迁写。它不搬迁受保护历史集合、不自然触发、不回收旧文件，不代表 Rewrite / GC 生命周期完成。

本轮控制保留 72,870 行及其 196,994 个索引条目：savingaccount 47,243 行（新 FileID 14）、transfer 25,627 行（新 FileID 15）。Locator 精确版本搬迁 196,994 次，RowID 为 0；重新读取两个真实 Parquet 文件，全行内容一致。

历史 v1–v6 只读兼容保留，部分 transfer 展开写入接口从生产代码移除；历史诊断 fixture 留在测试域。旧验证文档和日志保留并标为历史，不把旧范围或数量说成当前全索引能力。

## 复验命令

从仓库根目录运行，使用 JDK 17；输出必须是新目录 / 新报告，不能覆盖或拼接失败前缀。编译和维护诊断复用 [统一行状态审计](row-state-audit.md) 报告；先对各自采集执行一次 `audit-rows`，下面的 `TP_ROW_AUDIT.json` 和 `HTAP_ROW_AUDIT.json` 分别指向对应成功报告。

```bash
java -Xmx16g -cp build/sibench/sibench.jar org.sibench.capture.IndexTraceCompiler /mnt/disk1/SIBench config/hybench_tp_subset_sf1.yaml results/poc/hybench-sf1/raw/pg-1475807.jsonl NEW_NORMALIZED TP_ROW_AUDIT.json
java -Xmx16g -cp build/sibench/sibench.jar org.sibench.replay.ReplayMatrixValidation NEW_NORMALIZED NEW_REPORT_DIRECTORY
java -Xmx12g -cp build/sibench/sibench.jar org.sibench.capture.IndexMaintenanceValidation /mnt/disk1/SIBench config/hybench_sf1.yaml results/poc/hybench-htap-sf1/baseline results/poc/hybench-htap-sf1/raw NEW_REPORT.json HTAP_ROW_AUDIT.json
java -Xmx16g -cp build/sibench/sibench.jar org.sibench.capture.RelocationCheck /mnt/disk1/SIBench config/hybench_tp_subset_sf1.yaml NEW_NORMALIZED NEW_RELOCATION_DIRECTORY
```

长任务使用固定构建副本，运行中不替换 jar。首轮默认非 JDK17 导致 Parquet 读取失败的 `normalized/INCOMPLETE` 保留；一次使用可变 jar 的审计因重新打包后延迟加载失败，未产生通过报告。只有上述完成报告计入证据。

此报告记录当次全索引实验，不作为后续功能状态清单；当前三来源 schema / PG 预检范围见 [来源 Schema 契约](../contracts/source-schema.md)，剩余访问 / 快照、范围 / 保序编码、生命周期、大规模及真实服务工作统一维护在 [计划](../../plan.md)。
