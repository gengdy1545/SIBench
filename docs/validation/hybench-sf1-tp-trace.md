# HyBench SF1 TP 子集 v8 Trace 验证

更新：2026-10-22。HyBench TP3/TP5/TP9（35/30/35）单会话诊断负载按当前插桩重新采集，从原生执行端到端编译为 v8 规范 Trace：全部 130,092 个访问单元投影，包括 TP5 `sourceid = $1 OR targetid = $2` 的 BitmapOr。

## 采集

| 项目 | 内容 |
| --- | --- |
| 数据库 | 从静止原生 SF1 Load `sibench_sf1_baseline_check` 克隆的新库 `sibench_sf1_tp_unified`；执行未修改的上游 `create_index_pg.sql`（13 个二级索引、4 个序列）与 `install-row-capture.sql`（八表触发器）。源库不变 |
| 服务 | PG14.24 插桩，`compute_query_id = on`，C collation，UTC，无并行 worker，autovacuum 关闭 |
| Baseline | 运行前导出并对该库逐值核验：8,706,000 行，31,212,000 个索引条目 |
| 负载 | 原生 1 分钟：TP3 22,702、TP5 19,456、TP9 22,826 个事务，单 client，`session_order=single` |
| 行状态 | `RowStateAudit` 通过：68,478 个已提交行修改，0 回滚 |
| 配置 | [hybench_tp_subset_sf1.yaml](../../config/hybench_tp_subset_sf1.yaml)，仅产物目录与数据库名改为本轮 |

证据在 `results/poc/native-trace-20261022/hybench-tp/`（含 profile 副本）。

## 访问投影

| 模板 | 请求 | 访问单元 | 候选 | 可见 | 返回 | 决定 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `select * from savingaccount where accountid=$1` | 22,702 | 22,702 | 22,952 | 22,702 | 22,702 | 主键点查 |
| `select balance from savingaccount where accountid = $1` | 22,826 | 22,826 | 23,107 | 22,826 | 22,826 | 主键点查 |
| `update savingaccount set balance = balance ± $1 where accountid = $2`（两个模板） | 45,652 | 45,652 | 45,928 | 45,652 | 0 | 主键点查 |
| `select * from transfer where sourceid = $1 or targetid = $2 order by timestamp desc limit ?` | 19,456 | 38,912 | 764,370 | 764,370 | 19,456 | 两个二级点查（BitmapOr 两侧） |

- 账本 `projection.status = complete`：130,092 个点查，0 拒绝、0 排除；每个模板只有一种计划序列。
- TP5 的每个 bitmap 候选 TID 都得到堆判定，按产生它的索引分别计数；transfer 只插入，候选与可见相等，这是观察结果。返回行 19,456 是 LIMIT 之后的 SQL 结果，不作为索引命中数。
- savingaccount 的候选多于可见行是余额更新留下的死版本。

## 转换与验证

| 指标 | 数值 |
| --- | ---: |
| 提交事务 / 版本 | 64,984 |
| 行版本写入（INSERT / UPDATE） | 68,478（22,826 / 45,652） |
| 展开后的索引写入 | 182,608 |
| 事务内写后查询 | 22,826 |
| 运行期记录（含控制） | 442,668 |

| 检查 | 结果 |
| --- | --- |
| `replay`，1 / 64 / 128 client 及 transaction 输入模式，Locator 与 RowID | 通过；130,092 个查询、855,550 个结果项；四种配置的发送顺序校验和相同 |
| `validate-snapshots`（独立读取 Parquet） | 通过；65,124 个修改主键、267,204 个历史主键探针、775 个二级谓词共 451,760 个二级探针 |
| `RelocationCheck`（强制搬迁最新存活修改行） | 通过；65,124 行、175,900 个索引条目，Locator 模式全部精确版本搬迁，RowID 模式无搬迁写 |
| `IndexMaintenanceValidation`（逐键维护） | 通过；31,212,000 个基线条目、456,520 次可见性探针；本负载无索引列值改写 |

## 不代表

- 不是完整 HTAP：并发会话仍被编译器拒绝；TP 子集不含已有索引列值 UPDATE。
- 不是性能结论；参考模型在进程内。
- 账本证明事件被恰好归属一次，不证明没有事件丢失。

## 复现

```bash
make -C src/main/native/postgres install extension && mvn package
# PG 以 SIBENCH_CAPTURE_DIR=<raw> 启动；克隆库并安装索引与触发器：
psql -c "CREATE DATABASE sibench_sf1_tp_heap TEMPLATE sibench_sf1_baseline_check" postgres
psql -v ON_ERROR_STOP=1 -f benchmark/HyBench-2024/conf/create_index_pg.sql sibench_sf1_tp_heap
psql -v ON_ERROR_STOP=1 -v capture_library="$PWD/build/services/postgresql-capture/row_capture.so" \
  -f src/main/native/postgres/install-row-capture.sql sibench_sf1_tp_heap
P=results/poc/native-trace-20261022/hybench_tp_subset_sf1.yaml
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc export "$PWD" $P
java -cp build/sibench/sibench.jar org.sibench.capture.PgBaselineVerify --config "$PWD" $P \
  jdbc:postgresql://127.0.0.1:55433/sibench_sf1_tp_heap postgres <baseline>
# 在含 Data_1x 的目录，classpath 加 HyBench classes/lib：
java -cp ... org.sibench.capture.CapturePoc run "$ROOT" $P
java -cp build/sibench/sibench.jar org.sibench.capture.CapturePoc audit "$PWD" $P
java -jar build/sibench/sibench.jar access-ledger $P <raw>
java -jar build/sibench/sibench.jar compile-trace $P <trace>
```

导出后、运行前必须完成 Baseline 核验；该流程尚未并入 `native-capture.sh`。
