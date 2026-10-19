# 原生 benchmark 验证报告

原生使用验证已完成：三个固定版本的 benchmark 均完成 PostgreSQL 原生最小案例，形成使用说明、参数与能力矩阵、配置样例，以及后续采集和存储实验契约。验证日期为 **2026年10月18日，Asia/Shanghai**。

当前成果可用于实现负载采集与统计。这里的“完成”限定为原生使用路径和契约建立；逐请求采集、统计引擎、index server 回放及对象存储性能对比尚未实现。

## 交付导航

| 交付物 | 内容 |
| --- | --- |
| [YCSB 使用说明](../benchmarks/ycsb.md) | 加载/运行、规模、字段与键编码、扫描和binding |
| [sysbench 使用说明](../benchmarks/sysbench.md) | prepare/run/cleanup、事务组合、参数覆盖问题 |
| [HyBench 使用说明](../benchmarks/hybench.md) | 生成/加载/运行、派生SF、反馈依赖及构建限制 |
| [参数和能力矩阵](../benchmarks/capability-matrix.md) | 跨工具参数含义、PK映射及实测边界 |
| [最小记录契约](../contracts/trace.md) | 类型化键、范围、事务、未知状态及统计口径 |
| [存储后端实验契约](../contracts/storage.md) | SSD/对象/混合部署、持久化、缓存、指标与验收 |
| [配置样例](../../config) | 三种原生配置、HyBench参数、记录与存储设计样例 |
| [验证脚本](../../scripts) | 固定源码构建、专属PostgreSQL、原生smoke运行 |
| [验证证据](evidence/20261018-native-smoke) | 机器可读摘要、命令记录、构建与运行日志副本及校验值 |

## 环境与版本

- Ubuntu 22.04.5，x86_64；运行使用 Java 17，Maven 3.9.8。机器默认 Java 23，验证显式选择 Java 17。
- PostgreSQL 14.24，Ubuntu 软件包解压到 `build/services/postgres/runtime/`；专属集群位于 `build/services/postgres/data/`，仅监听 `127.0.0.1:55432`，locale=C、UTF8。
- 三个子模块源码统一位于 `benchmark/`；源码内容保持不变，通过 `git archive HEAD` 导出到 `build/benchmarks/` 后构建。

| 子模块 | 固定提交 |
| --- | --- |
| YCSB | `66302f301b13f60d4bcb2f29f478586bb1d6f2e0` |
| sysbench | `3ceba0b1e115f8c50d1d045a4574d8ed643bd497` |
| HyBench-2024 | `eaad91b52956fddd3d48ce49f792928056b7b360` |

sysbench 在导出源码目录构建时，其 configure 可能读取外层SIBench的Git hash。交付脚本设置 `GIT_CEILING_DIRECTORIES` 阻止该行为，以manifest中的源提交为准，避免把错误banner当作版本证据。

## 原生案例验收

| 工具 | 数据与负载 | 验收条件 |
| --- | --- | --- |
| YCSB | 1000行，200次操作，1线程；50%读/50%更新，uniform、ordered补零键 | 加载1000次INSERT OK；读/更新合计200且均OK；前后行数1000；删除测试库 |
| sysbench | 1表×1000行，20事件，1线程；原生oltp_read_write | 280读＋80写＋40其他=400 SQL；20事务、0错误；前后1000行；cleanup后0表；删除测试库 |
| HyBench | 8表共16030初始行；TP1/TP9各50%，1线程、1分钟 | 逐表初始行数匹配；运行日志无SQL异常；transfer行数增长；删除测试库 |

历史HyBench案例使用相同配置，但随机请求并不相同。实际计数、运行时间、完整argv及最终复跑结果保存在 [summary.json](evidence/20261018-native-smoke/summary.json)。日志中的吞吐仅用于确认执行，不用于性能排名：数据规模、操作单位、运行时长和系统状态并不一致。

## 发现的问题及处置

| 问题 | 实际影响 | 处置与后续要求 |
| --- | --- | --- |
| HyBench Maven旧HTTP传递依赖 | 标准Maven构建失败 | 保留失败证据，使用原始源码javac＋声明依赖完成最小路径；完整依赖修复仍待处理 |
| HyBench classpath资源配置 | `gendata -f`不能调整生成规模 | 资源目录置于classpath最前；生成与运行共享同一份smoke参数 |
| HyBench账户CSV追加 | 重跑可能重复PK | 每次运行使用新目录 |
| HyBench Log4j无效属性 | 启动产生配置ERROR | 只在运行目录副本移除该属性；子模块不变 |
| sysbench配置覆盖 | 设1000行但首次实际加载10000行 | 配置转原生argv并加入1000行断言；首次误判不计入最终验收 |
| YCSB直接使用binding依赖目录 | 缺HTrace而无法启动 | 使用构建分发包的完整lib |
| HyBench明文配置日志 | 真实连接信息可能被打印 | 本次隔离实例无密码；接真实库前须脱敏 |
| 存储对象与PK无直接等价关系 | 不能以trace上传宣称完成对象后端测试 | 定义独立storage_profile、真实I/O与持久化验收 |

子模块目录迁移后，历史证据中的命令、源码路径和脚本校验值仍保留采集时的内容；旧路径不作为当前执行入口，历史脚本校验值也不用于校验已更新的脚本。当前构建入口为 `scripts/benchmarks/build.sh`，服务入口为 `scripts/services/postgres.sh`，验证入口为 `scripts/validation/native_smoke.py`。历史路径映射见 [证据归档说明](evidence/README.md)。

失败尝试的完整原始文件保留于 `results/native-validation/`；验收证据包保留关键失败摘要和最终成功日志。没有通过伪造行数、跳过异常或修改事务代码完成验收。

## 复现步骤

在仓库根目录执行。依赖安装适用于本次Ubuntu环境；无需Docker。PostgreSQL下载脚本使用系统APT候选版本，复现时应核对并记录包版本；本次具体版本见证据包。

```bash
sudo apt-get install -y --no-install-recommends autoconf automake libtool libaio-dev libpq-dev libllvm14
bash scripts/services/postgres.sh fetch
bash scripts/benchmarks/build.sh
bash scripts/services/postgres.sh start
python3 scripts/validation/native_smoke.py --benchmark ycsb
python3 scripts/validation/native_smoke.py --benchmark sysbench
python3 scripts/validation/native_smoke.py --benchmark hybench
bash scripts/services/postgres.sh stop
```

另需已有 git、rg、C/C++构建工具、Python≥3.9、Maven和JDK17。本次这些基础工具已存在。SSJ不存在本地Maven缓存时build脚本只下载该声明jar，不解析其旧传递依赖。可用 `SIBENCH_JAVA_HOME`、`SIBENCH_SSJ_JAR` 覆盖对应路径；smoke脚本的 `--java-home` 需同步。

`build.sh` 拒绝覆盖已有源码构建目录。如复用本次完成的构建，直接从启动PostgreSQL开始；如重建，先移动旧的三个 `build/benchmarks/<benchmark>` 目录留存。每次运行输出在 `results/native-validation/<benchmark>-<时间>/`。`result.json` 有最终status、执行命令和退出码；进程退出0之外还检查原生状态和数据库行数。

PostgreSQL使用loopback与本地trust认证，仅用于隔离验证；不应将该配置用于共享或生产服务。测试库名固定为 `sibench_ycsb`、`sibench_sysbench`、`sibench_hybench`；已有同名库时脚本失败，不删除它。失败运行可能保留测试库以便检查，确认属于本次实验后单独清理再重跑。

本机无需保留服务常驻，验收后已停止专属PostgreSQL；编译依赖、构建副本和证据留存。运行文件在 `.gitignore` 已忽略的build/results目录中，文档、样例与脚本可纳入版本控制。

## 后续实施入口

采集器按YCSB、sysbench、HyBench顺序实现采集，将本次配置和行数断言作为集成基线。优先处理生成主键、事务最终状态、受影响0行、未知范围和日志脱敏。存储能力矩阵中的未知项须等真实index server接口和后端实现提供后验证，不是使用验证报告可补造的结论。

参考PostgreSQL初始化和生命周期的官方说明，实际验收以本机日志为准：

```text
https://www.postgresql.org/docs/14/app-initdb.html
https://www.postgresql.org/docs/14/app-pg-ctl.html
```

## 目录职责调整后的复验

三个 benchmark 已在 `build/benchmarks/` 重新构建，并通过 `scripts/validation/native_smoke.py` 完成 PostgreSQL 最小案例。服务程序及数据使用 `build/services/postgres/`，负载配置按 benchmark 从 `configs/workloads/` 加载。脚本语法、文档链接、配置样例以及新证据快照校验值均检查通过。PostgreSQL 已停止。

## 配置目录迁移

2026-10-19，项目配置统一从 `configs/` 迁移到根目录 `config/`。当前运行脚本及文档链接已使用新路径；上述历史验证描述、`evidence/` 内的日志和 SHA256SUMS 保留原样。核对旧 manifest 时，应将历史 `configs/` 路径映射到 `config/`；内容已演进的文件需使用对应历史版本核对，不重新计算或覆盖历史摘要。新的证据采集使用 `config/`。

大规模配置使用 `config/workloads/hybench/sf1000.yaml`、`config/workloads/ycsb/large.yaml` 和 `config/workloads/sysbench/large.yaml`。这些配置尚未执行全量生成，不属于原生 smoke 的已验证产物。

本次复验的命令、运行结果和校验值见 [复验证据](evidence/20261018-234423-122683-native-smoke/summary.json)。历史证据保留原样。
