# 采集性能：机器与 PG 配置

2026-10-25。回答两个问题：采集 PG 的配置能把原生执行提速多少，插桩本身占多少；以及采集应在哪类机器上运行。

## 方法

在 `sibench_1`（远程机，见 [计划的执行资源](../../plan.md#执行资源)：Kubernetes pod，32 CPU / 64 GB，EPYC 9K65，`/data` 云盘）上，以 TencentOS 构建的同一份插桩 PG 14.24 与同一个 jar，从同一个 SF10 静止库（`base` 阶段原生 gendata 161 s、装载约 150 s，87,060,000 行）每次克隆新库，按 `config/hybench_sf10.yaml` 的并发（TP 8、AT 8、IQ 8 client）运行 AP 一轮、TP 2 分钟、XP 2 分钟。每种配置跑两遍，顺序交错，以摊平宿主机上其他负载的波动。原版 PG 由同一源码不打补丁构建，用同一数据目录运行（补丁只改共享内存结构，不改盘上格式）；克隆库由插桩 PG 准备后删去行捕获触发器再切换，HyBench 由相同方式（不经采集驱动）运行。

| 配置 | 内容 |
| --- | --- |
| A | 采集集群当时的配置：`shared_buffers` 256 MB，其余默认（`max_wal_size` 1 GB、`checkpoint_timeout` 5 min、`synchronous_commit` on） |
| B1 | A 加不改变执行计划的参数：`shared_buffers` 16 GB、`max_wal_size` 32 GB、`min_wal_size` 4 GB、`checkpoint_timeout` 30 min、`checkpoint_completion_target` 0.9、`wal_buffers` 64 MB、`synchronous_commit` off、`maintenance_work_mem` 2 GB |
| B2 | B1 加改变执行计划的 `effective_cache_size` 40 GB、`work_mem` 64 MB |
| C | B1，原版 PG，无采集 |

## 结果

2 分钟内完成数（两遍）：

| 配置 | TP 事务 | AT 事务 | IQ 查询 |
| --- | --- | --- | --- |
| A | 10,921 / 10,262 | 4,914 / 4,639 | 4,292 / 4,263 |
| B1 | 14,521 / 14,253 | 6,214 / 6,166 | 4,707 / 4,765 |
| B2 | 14,256 / 14,316 | 6,112 / 6,307 | 4,564 / 4,407 |
| C | 12,330 / 14,640 | 5,864 / 6,124 | 5,716 / 5,716 |

- B1 相对 A：TP 约 +36%、AT 约 +30%、IQ 约 +10%。B2 相对 B1 无收益，改变计划的参数不采用。
- C 相对 B1：TP、AT 在两遍之间的波动范围内（C 的两遍 TP 相差 2,300），IQ 约快 17%。插桩开销集中在每个查询含数千次索引访问的 IQ。
- 同为配置 A，`sibench_1` 的 TP 91 事务 / 秒、AT 41、IQ 36，开发机 SF10 校准为 36.5、17.8、14.7，远程机约快 2.5–3 倍；远程机磁盘随机读更慢（8 KiB 0.8–2 ms，开发机 0.44 ms），说明此规模下原生执行主要受 CPU 限制。
- 开发机上 SF1 → SF10 同为 8 client，TP 从 332 降到 38.6 事务 / 秒；远程机上的对照表明这一下降中配置可解释的部分有限，主要是更大的数据下每个事务更贵与机器本身。

## 结论

- 采集在远程机上运行，使用 B1 配置；开发机用于开发、回归与回放。
- 写入比例固定时，原生执行时长 = 目标写入 ÷ 写入吞吐。按 B1 在远程机，HyBench SF10 达到 5% 写入约需 TP、XP 各 17.5 分钟（开发机为各 53 分钟），以远程实测为准。
- 配置已接近上限（原版 PG 也只快 0–17%）；进一步缩短原生执行只能靠 benchmark 自身的并发参数，需同时验证正确性与来源失败处理。

## 产物

`sibench_1:/data/sibench/results/poc/ab/<A1|A2|B1a|B1b|B2a|B2b|Ca|Cb>/`：每次运行的 PG 配置（`postgres.conf`）、`run-timing.json`、`raw/capture-run.json`（插桩运行）或 `run.log`（原版运行）；原始追踪已删除，克隆库已删除，SF10 静止库 `sibench_sf10_base` 保留。驱动脚本与配置在 `sibench_1:/data/sibench/build/ab/`。

## 不代表

- 只覆盖 HyBench SF10、8 client、2 分钟阶段；YCSB / sysbench 与更长阶段未测。
- 远程机是共享宿主上的 pod，两遍之间的波动可达 15%；结论只取超出波动的差异。
- 原版运行不经采集驱动，C 与 B1 的差异包含 JDBC 包装层与服务端插桩两部分，未再拆分。
