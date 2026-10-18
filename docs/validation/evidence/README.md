# 验证证据归档

每个子目录对应一次证据快照，名称由实验时间与验证用途组成。
`scripts/validation/collect_evidence.py` 创建独立快照，不覆盖已有记录。

`20261018-native-smoke/` 是原生 benchmark 首次验收的历史证据。
其中的命令、manifest 和 SHA256SUMS 保留当时路径及内容；不要把它们当成
当前目录的执行脚本。目录整理后的对应关系如下：

| 历史位置 | 当前职责位置 |
| --- | --- |
| `docs/phase1/evidence/` | 本目录下的 `20261018-native-smoke/` |
| `scripts/phase1/build.sh` | `scripts/benchmarks/build.sh` |
| `scripts/phase1/postgres.sh` | `scripts/services/postgres.sh` |
| `scripts/phase1/smoke.py` | `scripts/validation/native_smoke.py` |
| `scripts/phase1/collect_evidence.py` | `scripts/validation/collect_evidence.py` |
| `configs/phase1/` | `configs/workloads/`、`configs/trace/`、`configs/storage/` |
| `build/phase1/` | `build/benchmarks/`、`build/services/postgres/` |
| `results/phase1/` | `results/native-validation/` |

旧构建副本在本机 `build/archive/20261018-native-builds/` 留存，新的构建在
职责目录重新生成。历史证据文件可按迁移后的路径校验原始内容；发生修改的脚本
不应与旧脚本散列比较。新快照的 SHA256SUMS 使用当前仓库相对路径。
