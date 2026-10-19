# SIBench

SIBench 旨在通过真实 Parquet 文件和确定性 Trace，还原 HyBench、sysbench、YCSB 等端到端负载对索引产生的查询、更新与维护操作。当前已实现 Java 规模配置检查及原生最小运行验证，数据生成和索引回放仍待实现。

## 目录结构

| 目录或文件 | 用途 |
| --- | --- |
| `benchmark/` | 原生 benchmark Git 子模块 |
| `src/main/java/org/sibench/` | Java 核心：负载配置、数据生成、Trace、回放与适配器 |
| `src/test/java/org/sibench/` | Java 测试 |
| `pom.xml`、`build/sibench/` | Maven 构建定义与核心构建产物 |
| `config/workloads/` | 按 benchmark 分类的负载配置 |
| `config/trace/`、`config/storage/` | 记录格式样例与存储配置 |
| `scripts/benchmarks/` | benchmark 构建工具 |
| `scripts/services/` | 本地依赖服务管理 |
| `scripts/validation/` | 原生运行验证与证据归档 |
| `docs/benchmarks/` | 使用说明和能力对照 |
| `docs/contracts/` | trace 与存储实验契约 |
| `docs/validation/` | 验证报告与按实验归档的证据 |
| `build/benchmarks/`、`build/services/` | 本地构建副本和服务数据，Git 忽略 |
| `results/native-validation/` | 本地原生验证产物，Git 忽略 |
| `plan.md` | 里程碑与验收标准 |

目录按职责和领域组织，文件名说明用途；开发阶段不作为永久目录边界。验证证据使用实验标识归档，日期表示实验发生时间，不表示开发阶段。

核心采用 Java 17，规模校验位于 `src/main/java/org/sibench/workloads/ScaleInspector.java`，CLI 位于 `org.sibench.cli.Sibench`。Python/Shell 用于实验编排、服务管理、验证及离线分析；数据生成、Trace 和回放核心后续在 Java 中实现。

## 初始化与使用

在仓库根目录构建核心并检查配置（需要 JDK 17 或更高版本、Maven、Git；实验应固定实际 JDK）：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
git submodule update --init --recursive
mvn package
"$JAVA_HOME/bin/java" -jar build/sibench/sibench.jar inspect-scale
```

`mvn package` 包含测试并生成带依赖的可执行 JAR。可追加具体配置路径；从其他目录执行时使用 `inspect-scale --root /path/to/SIBench`。配置错误返回非零状态，成功输出 JSON。目前该命令仅校验初始规模，不生成数据或宣称容量校准完成。

子模块路径为 `benchmark/HyBench-2024`、`benchmark/sysbench`、`benchmark/YCSB`。构建脚本从这些路径导出固定提交，生成文件仍放在 `build/benchmarks/`。

当前已完成原生最小运行验证。环境准备、运行命令及限制见 [原生 benchmark 验证报告](docs/validation/native-smoke.md)，后续工作见 [实施计划](plan.md)。

大规模初始配置位于 `config/workloads/`：HyBench 使用 `sf1000.yaml`，YCSB 和 sysbench 使用 `large.yaml`。各来源保留自身数据形态，采用独立规模档位，不要求记录数相等；当前仍待样本容量校准，尚未生成全量数据。运行 `java -jar build/sibench/sibench.jar inspect-scale` 可核验配置和源码指纹。

配置目录已由 `configs/` 迁移为 `config/`，项目配置不写入 `benchmark/` 子模块。历史实验日志与 manifest 保留当时路径；迁移说明见 [验证报告](docs/validation/native-smoke.md#配置目录迁移)。
