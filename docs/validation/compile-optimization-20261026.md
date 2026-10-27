# 编译阶段对照（2026-10-26）

目的：在 remote SF10 / 87m 的同一份原始采集上，对照旧 jar 与编译优化后的 jar；不重跑原生负载。
所有对照使用同一 raw、同一 baseline、同一认证、同一 PG 计数器文件，只替换 `-cp` 的 jar。
`time.txt` 为 `/usr/bin/time -v` 原文，`timing.json` 为 `compile-trace --timing` 的阶段计时。

## Jars

| 名称 | SHA256 | 说明 |
| --- | --- | --- |
| old | `bec20bf5c4c4a47d4bbbee4cde7bd34dafd17765402a653231ab947f266a32c1` | 部署中的旧 jar（内联跟踪） |
| new22 | `a1b1c420b6792a3fe1df45b3342e79aea3fb2071ed39359f9b037d11ea9f27a9` | 按会话跟踪，实测更慢，已回退 |
| new24 | `8c54705a1a5242a9164ab9a54bbb84f12d200b0b6e253915e43074d6296564a1`（HyBench / sysbench） / `cbe03ed169f82565c5d5738e84cc36d6d5ddf6ce260d9867fb8873cdaabb1c09`（YCSB） | 内联跟踪 + 账本无装箱计数 + Unit 集合按需分配；两次打包的编译器类字节相同 |
| strict | `f41dcb01d6758e86cefaa77b137c58bc681ee105a7ce1f17040f21ab28f65711` | 最终 v9 / zstd 严格版本：删除旧明文、`baseline.ops`、`locators.tsv`、`Detail.LEGACY` 与未接入的 per-session stage；273 个测试通过；当前 zstd raw 重编逐字节相同 |

## 结果

| 来源 | 旧 jar | new24 | 变化 |
| --- | --- | --- | --- |
| HyBench SF10（`hl9`） | 390.99 s，负载 322.88 s，baseline 64.27 s，RSS 24.24 GB | 384.85 s，负载 321.49 s，baseline 59.52 s，RSS 24.13 GB | wall −1.6%，负载 −0.4%，RSS −0.4% |
| sysbench 87m（`sl16`） | 243.93 s，负载 205.81 s，baseline 34.73 s，CPU 671.5+33.0 s，RSS 15.57 GB | 243.07 s，负载 205.87 s，baseline 33.67 s，CPU 645.4+33.3 s，RSS 15.30 GB | wall −0.4%，CPU −3.7%，RSS −1.7% |
| YCSB 87m（`ycsb-87m-n22`） | 561.02 s，负载 520.62 s，baseline 36.23 s，CPU 1592.0+141.4 s，RSS 13.40 GB | 553.71 s，负载 524.29 s，baseline 25.31 s，CPU 1631.9+147.3 s，RSS 14.87 GB | wall −1.3%，负载 +0.7%，RSS +11% |

- 三份 Trace 与旧 jar 逐字节相同（HyBench 13 个文件、sysbench 13 个、YCSB 16 个；含 `workload.ops`、`manifest.json`、`access-ledger.json`、全部增量 Parquet、`work-units.jsonl.zst`）。
- 三份 `capture-completeness` 均为 `consistent`，PG 计数器一致。
- YCSB 的 raw 是本轮重采集（32 线程、5% 写入，原生 1,572.1 s、原始 9.3 GB）；它替代了已被清理的旧 raw，Trace 仍由真实原生负载编译。
- 旧 jar 的 `baseline` 阶段在 YCSB 上比 new24 慢约 11 s，是 page cache 差异；负载阶段 new24 反而慢 3.7 s，CPU 高约 2.5%，RSS 高 1.5 GB。把解析线程降到 8 的对照 wall 559.47 s、RSS 15.01 GB，也不优于默认 32，故默认保持 32。

## 按会话跟踪（new22）的结论

在 sysbench 87m 上，new22 跑到 79M / 106M 事件时已用约 1,002 CPU 秒；旧 jar 跑完 106M 只用约 705 CPU 秒。主线程始终 100%，tracker 线程多数空闲，调用/队列开销大于并行收益，因此停止该轮并回退，未以其计时作为结论。

## 解析线程对照

同一 new24，仅加 `-Dsibench.compile.parse_threads=8`：

| 来源 | 默认（32） | 8 | 说明 |
| --- | --- | --- | --- |
| HyBench SF10 | 384.85 s，负载 321.49 s，RSS 24.13 GB | 386.65 s，负载 323.65 s，RSS 23.58 GB | wall +0.5%，RSS −2.3% |
| sysbench 87m | 243.07 s，CPU 645.4+33.3 s，RSS 15.30 GB | 241.76 s，CPU 665.2+33.7 s，RSS 14.76 GB | wall −0.5%，RSS −3.5%，CPU +3% |
| YCSB 87m | 553.71 s，负载 524.29 s，RSS 14.87 GB | 559.47 s，负载 529.21 s，RSS 15.01 GB | wall +1.0%，RSS +0.9% |

解析线程数只影响吞吐/内存折中，不改变 Trace 字节。

## 正确性

- 全部对照都通过 `--certification` 与 `--counters`；认证覆盖、账本和 PG 计数器一致。
- 本地 SF1 c8 在本轮清理前用 new24 重编保留 raw，13 个 Trace 文件与 `regress-20261025/sf1-c8-v22` 逐字节相同；负载阶段 272.8 s（此前 294.7 s）。该旧 raw/trace 已随旧格式数据清理，报告保留。
- 最终严格构建（`f41d…`）删除旧明文、`baseline.ops`、`locators.tsv`、`Detail.LEGACY` 与未接入的 stage；当前 zstd raw（HyBench / sysbench / YCSB）同 raw 重编仍逐字节相同。SF1 旧明文 raw 已清理，只保留 v22 zstd Trace、baseline 与报告。
- 未运行新的行审计或 oracle；本轮只验证编译器输出与既有认证 Trace 等价，不重复方法层面的审计。

## 保留

- 远端保留 YCSB `results/poc/ycsb-87m-n22/{raw,trace}`（原始 9.3 GB、Trace 4.1 GB）及 `run-timing.json`；HyBench / sysbench 只保留本轮计时与校验结论，临时 Trace 可删除。
- 计时原文、Trace 对照与摘要保存在 `results/poc/regress-20261026/`；本文不保存大文件。