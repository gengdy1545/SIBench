# 采集环境与阶段计量

更新：2026-10-29。当前单次采集为 `acceptance-20261029T053122Z`，完整结果与输入身份见 [三来源验收](acceptance.md)。本页报告来源生成管线，不把采集吞吐当作索引服务性能。

## 执行环境

开发机为 32 线程、约 62 GB 内存，工作区 `/mnt/disk1/SIBench`，用于构建、离线测试和证据整理。远程工作区 `/data/sibench`，TencentOS 3.2 / glibc 2.28、AMD EPYC 9K65；各 pod 配额 32 CPU，内存上限 64,000,000,000 B（59.60 GiB，含页缓存），`/data` 为 500 GiB xfs。宿主可见 96 个逻辑 CPU，JVM 按配额识别 32；不能把可见数量当作实验配额。原始环境快照见本轮 `environment/`。共享宿主可能存在其他负载，单样本不能排除宿主波动。

本轮 `sibench_1 / 2 / 3` 分别运行 HyBench / YCSB / sysbench；`sibench_4` 构建 JNI 并完成小输入恢复、存储和 PG 集成验证。完整本地 / COS 后端按来源依次运行，每一对使用同机同输入和相同资源预算，不并行竞争 COS。参考与 oracle 在各来源主机上先完成。

使用 JDK 17.0.20 / 远程 17.0.20.1、PG 14.24，业务进程以 UID 1000 的 `sibench` 运行。PG 采用 [postgres_capture.conf](../../config/postgres_capture.conf)、read_committed、C collation、禁用 parallel worker，追踪格式为 summary + zstd；实际有效配置随运行保存。

PG 二进制 SHA-256 `c5e02bffee01d18b6ba5a7b6bec19dd52c3a82e4887014ff47b982bc9638bbe1`，row_capture 库 SHA-256 `0c802b68cd168458c554023220cdc1164101397592f9f96ba57ce4b7503ddaa7`。应用、JNI 和来源提交以同轮身份文件为准。

## 本轮生成条件

三组均从已验证、停机的静止 PG 基库 reflink 克隆，复用摘要匹配的 Baseline Parquet；重新执行原生 workload、全部会话审计、完整行状态审计、计数器核对、Trace 编译和新认证。**本轮没有重新测量初始原生 gendata / load / 建索引 / Baseline 全量导出成本**。prepare / export 的亚秒耗时是复用开销，不能写成从零建库或导出速率。

| 来源 | 基线行数 | 原生并发与负载 |
| --- | ---: | --- |
| HyBench SF10 | 87,060,000 | AP 1；TP 32；XP AT 32 / IQ 32；TP / XP 各 5 分钟 |
| YCSB | 87,060,000 | 32 线程，JDBC A / B / C / F / D / E，10 × 100 字节字段 |
| sysbench | 87,060,000 | 8 线程，8 表 × 10,882,500 行，六个固定 oltp 脚本 |

负载量由当前 Java `plan-write-ratio` 根据同来源同规模校准输入推导；有效配置和校准 manifest 随运行保存。按时间停止的 HyBench 会有比例偏差，实际修改数以最终 Trace 为准；不删除查询或截断事务来凑 5%。

## 分阶段墙钟

单位为秒，取同轮 `run-timing.json` 的阶段记录，保留两位小数。

| 来源 | 原生执行 | 行审计 | 编译初始化 | Baseline 派生 | workload 编译 |
| --- | ---: | ---: | ---: | ---: | ---: |
| HyBench SF10 | 727.85 | 483.55 | 2.14 | 58.34 | 315.21 |
| YCSB 87m | 1,584.33 | 1,042.27 | 11.98 | 28.84 | 574.66 |
| sysbench 87m | 411.64 | 202.80 | 2.56 | 36.21 | 245.79 |

阶段边界见原始 timing；workload 编译包含访问账本与计数器核对。上述子阶段不包含全部 JVM 启停与收尾，不能简单相加冒充端到端。认证后已审计的逐行变化文件按现有管线规则移除；保留原生事实、行审计报告、新认证和编译输入绑定，以自身认证重编。

## 资源与保护边界

采集 JVM 堆上限 32 GiB，参考 / 独立 oracle 40 GiB，局部性分析 24 GiB，服务与驱动各 16 GiB。大阶段独立上限 6 小时；后端 setup 600 秒、单请求 600 秒、replay 6 小时、teardown 30 秒分别计时，第一处错误与后续清理分开保存。600 秒是实验保护，不是服务性能验收目标。磁盘余量低于 20 GiB 时停止受影响采集。

每个命令保留 `/usr/bin/time -v` 与应用 timing。采集命令的 CPU 不含独立 PG daemon；其 max RSS 是 GNU time 记录的最大子进程值，不能把它当作 PG、客户端和 JVM 的内存总和。各 JVM 的进程 CPU / RSS 可单列。30 秒主机采样包含页缓存与其他进程；cgroup 历史 HWM 不能归因到本轮。共享 `/data` 的前后差值也不能当作单阶段临时空间。

阶段 CPU / RSS、最终产物和服务端指标统一见 [测量口径与本轮结果](measurement.md)。原始采集目录为各来源主机 `results/poc/acceptance-20261029T053122Z/<source>/`；命令、配置和资源记录在同机 `results/validation/acceptance-20261029T053122Z/<source>/`。

本轮是一次代表输入验证，不是采集优化前后对照；不沿用旧构建的加速倍数，也未重新测量插桩开销、不同 PG 参数或不同线程数的因果影响。
