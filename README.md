# SIBench

SIBench 用于研究 HyBench、sysbench 和 YCSB 的负载，统计键访问分布，并为下游 index server 提供统一的测试接入。存储实验覆盖本地 SSD、对象存储和混合部署，具体能力依赖下游实现。

## 目录结构

| 目录或文件 | 用途 |
| --- | --- |
| `benchmark/` | 原生 benchmark Git 子模块 |
| `configs/workloads/` | 按 benchmark 分类的负载配置 |
| `configs/trace/`、`configs/storage/` | 记录格式样例与存储配置 |
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

## 初始化与使用

```bash
git submodule update --init --recursive
git submodule status
```

子模块路径为 `benchmark/HyBench-2024`、`benchmark/sysbench`、`benchmark/YCSB`。构建脚本从这些路径导出固定提交，生成文件仍放在 `build/benchmarks/`。

当前已完成原生最小运行验证。环境准备、运行命令及限制见 [原生 benchmark 验证报告](docs/validation/native-smoke.md)，后续工作见 [实施计划](plan.md)。
