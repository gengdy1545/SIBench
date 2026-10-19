# SIBench

SIBench 旨在通过真实 Parquet 文件和确定性 Trace，还原 HyBench、sysbench、YCSB 等端到端负载对索引产生的查询、更新与维护操作。当前已实现 Java Baseline 生成入口、配置检查与 dry-run；Baseline 尚未执行数据写入验证，增量 Trace、Rewrite 和索引回放仍待实现。

## 目录结构

| 目录或文件 | 用途 |
| --- | --- |
| `benchmark/` | 原生 benchmark Git 子模块 |
| `src/main/java/org/sibench/` | Java 核心：负载配置、数据生成、Trace、回放与适配器 |
| `src/test/java/org/sibench/` | Java 测试 |
| `pom.xml`、`build/sibench/` | Maven 构建定义与核心构建产物 |
| `config/workloads/` | 按 benchmark 分类的负载配置 |
| `docs/benchmarks/` | 使用说明和能力对照 |
| `docs/contracts/` | trace 与存储实验契约 |
| `build/benchmarks/`、`build/services/` | 本地构建副本和服务数据，Git 忽略 |
| `results/datasets/` | Baseline 数据集输出，Git 忽略 |
| `plan.md` | 里程碑与验收标准 |

目录按职责和领域组织，文件名说明用途；开发阶段不作为永久目录边界。

核心采用 Java 17，CLI 位于 `org.sibench.cli.Sibench`，数据生成实现位于 `org.sibench.generation`；Trace 和回放核心后续在 Java 中实现。

## 初始化与使用

在仓库根目录构建核心并检查配置（需要 JDK 17 或更高版本、Maven、Git；实验应固定实际 JDK）：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
git submodule update --init --recursive
mvn package
"$JAVA_HOME/bin/java" -jar build/sibench/sibench.jar inspect-scale
```

`mvn package` 包含测试并生成带依赖的可执行 JAR。可追加具体配置路径；从其他目录执行时使用 `--root /path/to/SIBench`。配置错误返回非零状态，成功输出 JSON。`inspect-scale` 和 `prepare --dry-run` 都不生成数据，不创建输出目录。

子模块路径为 `benchmark/HyBench-2024`、`benchmark/sysbench`、`benchmark/YCSB`，用于锁定原始数据规则和参数来源。

当前实现边界与后续工作见 [实施计划](plan.md)。

大规模初始配置位于 `config/workloads/`：HyBench 使用 `sf1000.yaml`，YCSB 和 sysbench 使用 `large.yaml`。各来源保留自身数据形态，采用独立规模档位，不要求记录数相等；当前仍待样本容量校准，尚未生成全量数据。运行 `java -jar build/sibench/sibench.jar inspect-scale` 可核验配置和源码指纹。

项目配置统一位于 `config/`，不写入 `benchmark/` 子模块。

## Baseline 生成配置

三份 YAML 都是 `prepare` 实际消费的配置，不含示例 endpoint 或未实现 adapter 占位。

| 来源 | 配置 | 初始规模 |
| --- | --- | --- |
| HyBench | `config/workloads/hybench/sf1000.yaml` | 1000x，8 表，共 8,706,000,000 行 |
| YCSB | `config/workloads/ycsb/large.yaml` | 1,000,000,000 行，10 个字段 × 100 字节 |
| sysbench | `config/workloads/sysbench/large.yaml` | 100 表 × 每表 10,000,000 行 |

只检查配置、上游版本和 schema：

```bash
java -jar build/sibench/sibench.jar prepare --dry-run config/workloads/hybench/sf1000.yaml
java -jar build/sibench/sibench.jar prepare --dry-run config/workloads/ycsb/large.yaml
java -jar build/sibench/sibench.jar prepare --dry-run config/workloads/sysbench/large.yaml
```

将来正式生成时使用下面的命令；本次未执行：

```bash
java -Xmx4g -jar build/sibench/sibench.jar prepare config/workloads/hybench/sf1000.yaml
```

可将配置路径换成 YCSB/sysbench。生成器要求输出目录不存在，不覆盖已有产物；读取子模块时检查固定提交、定义文件摘要和干净工作区。检查只能证明与仓库固定版本一致，不声称这些 fork 与各项目最新官方分支相同。构建和运行不修改 benchmark 源码。

### 参数与文件组织

- 文件目标默认 256 MiB，作为 128/256/512 MiB 文件大小实验的中间档位，不声称是通用最优值。
- Row group 最多 1,048,576 行；编码缓冲达到 64 MiB 也提前封闭，避免宽表必须积累满一百万行。此预算是编码器缓冲判断值，不是整个 JVM 的硬内存上限。
- 使用 parquet-java 1.15.2、Zstd level 3、单线程压缩、1 MiB page 和 dictionary 上限、启用 dictionary。可选 codec 为 zstd/snappy/none；后两者的 compression_level 必须为 0。压缩固定用于布局可复现，不作为本轮主要实验变量。
- 每个完整 row group 写出后，以实际已写字节判断文件是否达到目标。最终大小包含 footer，因此允许越过目标一个组及元数据的大小；不承诺固定百分比容差。尾文件可小于目标；若一个文件组数达到 32767，提前封闭以满足 writer 的组序号范围。
- 默认不分区、单桶，配置为 `partitioning: none`、`buckets: 1`、`bucket_function: ordinal_mod`。多桶按源记录序号取模，表内不按业务键额外排序；按表、桶逐个流式写入，仅打开一个 writer。其他分区函数尚未实现，配置时直接拒绝。
- FileID 从 1 确定性递增；同一数据集不复用，数据集身份由配置摘要隔离。FileRowPosition 是文件内从 0 开始的行号；RowID 从 1 起按固定表顺序和源记录序号分配，跨表不重复。Baseline Timestamp 默认 0，全部条目为 INSERT。
- `generation.seed` 控制按记录和字段域寻址的 SplitMix64 随机流。Baseline 顺序固定，不受墙钟或线程调度影响。没有随机选择未来业务查询比例；Lookup 开关属于后续增量负载配置。
- `min_free_disk_bytes` 默认保留 20 GiB；每 4096 行及新建文件前检查。它是停止阈值，不是全量容量证明，缓存与当前文件仍可能消耗空间。实际 Parquet 大小、速度及资源预算尚未校准。

### 来源适配与差异

所有生成配置显式声明 `mode: deterministic_derived`。适配器不执行原生数据库加载流程，不改子模块，也不声称输出与原生生成器逐字节相同。

- **HyBench：** 读取固定源码中的 TOML、姓名、地区和枚举词表，保留 8 表结构、规模、账户关系与贷款申请/交易配对。使用 UTC 和配置中的日期范围替换墙钟时间；ID 统一 int64、金额 double，避免原生 PostgreSQL int32 上限及 real 精度差异。blocked 状态改为每账户固定种子的 Bernoulli 判定，不复现原生有放回抽取 blocked ID 的过程；ID 抽样覆盖完整有效区间，修正原生部分上界排除。保留原生 checking 类型选择的实际枚举结果。贷款配对共享同一随机域，贷款通过时合同日期为次日。字段内容保留生成器信息，不执行 SQL CHAR 截断/补齐；不输出原生风控辅助抽样文件。
- **YCSB：** 保留原生 FNV64 hashed key 与补零规则，以及默认字段字节掩码分布；使用固定随机流代替 ThreadLocalRandom，字段作为 binary 写入。哈希键生成规则继承原生行为，未增加全量唯一性去重扫描。
- **sysbench：** 保留 id/k/c/pad 结构、119/59 字符的数字分组文本和 uniform 的 k 分布。使用确定性随机流，不执行 SQL CHAR 补齐或创建数据库二级索引。表数和每表行数由配置决定。

这些差异属于可复现数据生成契约，不应将生成结果报告为原生事务执行或完整 HyBench 业务等价测试。

### 产物与完成状态

输出目录含 `resolved-config.json`、`files.jsonl`、`manifest.json` 及按表/桶组织的 Parquet。文件清单记录文件大小、校验值、组行数与压缩前后字节、源序号起点/步长、主键列和 RowID 起点；文件内存储 `_sib_row_id` 与 `_sib_timestamp`。读取主键列与元数据即可重建 Baseline INSERT，不额外保存十亿行 JSON 操作日志。

每个文件先写 `.partial`，完成后发布；最后写总 manifest 并移除 `INCOMPLETE`。消费者必须同时检查总 manifest 存在、`INCOMPLETE` 不存在及校验值。中断产物保持不完整，当前不支持 Checkpoint 续写；应换新输出目录从头重建，不静默覆盖。总 manifest 记录配置和文件清单摘要、源码提交、Parquet 版本、JDK/JVM 信息及打包 JAR 摘要。

本轮只完成编译、无数据生成的契约测试和 dry-run。尚未验证实际 Parquet 写入/读取、字节级重建一致性或大规模吞吐，增量文件、Rewrite、Checkpoint 和索引回放不包含在当前 Baseline 实现中。
