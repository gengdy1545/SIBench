# 参数和索引操作能力矩阵

本矩阵区分源码行为和当前验证范围。各结论的源码入口见 [YCSB](ycsb.md)、[sysbench](sysbench.md)、[HyBench](hybench.md)。这里的“可投影”指采集与投影的设计可行性，不表示采集器已经实现。

## 规模与执行参数

| 维度 | YCSB | sysbench OLTP | HyBench |
| --- | --- | --- | --- |
| 初始数据量 | recordcount | tables × table-size | sf 查表获得各表行数 |
| 数据生成 | load 调 binding 在线插入 | prepare 创建并填充表 | gendata 先产生 CSV，再 COPY |
| 请求生成 | CoreWorkload 在线产生 DB 调用 | Lua event 在线产生 SQL | TP/AP/AT 客户端在线产生 SQL，部分依赖结果 |
| 长度 | operationcount / maxexecutiontime | events / time | tpRunMins / apRunMins / xpRunMins |
| 长度单位 | 操作 / 秒 | event / 秒 | 分钟 |
| 并发 | threadcount | threads | 按模式指定 client 数 |
| 原生限速 | target | rate | 未确认通用限速参数 |
| 操作混合 | 随机权重 | 每 event 固定次数 | TP/AT 事务随机百分比 |
| 随机种子 | 未验证统一入口 | rand-seed | 未验证统一入口 |
| 数据大小 | 字段数和字段长度 | 默认固定字段布局 | 固定金融业务 schema |
| 模拟 value 大小 | 三者均由 SIBench 投影配置决定，默认100字节 | 同左 | 同左 |
| 最小实测 | 1000行＋200操作 | 1000行＋20事件 | 16030初始行＋1分钟TP1/TP9 |

不可把三者都记成 `scale_factor=1` 后直接比较。实验 manifest 要保留原生规模参数、实际行数、PK min/max、数据字节量的计量层级；范围宽度不是行数，数据库大小也不是 SSD 或对象存储实际占用。

## 操作映射

| 原始行为 | 拟投影 | 条件与限制 |
| --- | --- | --- |
| YCSB read/update/insert | get/update/insert | 保留实际字符串 PK；字段更新转换为模拟值替换 |
| YCSB scan | scan(start, limit) | 保留字符串比较器，结束键未知 |
| YCSB read-modify-write | get＋update | 记录同一逻辑操作组，不赋予额外原子性 |
| YCSB delete | 无标准A–F来源 | 需扩展工作负载 |
| sysbench point select | get | 显式 id |
| sysbench range select | scan(lower, upper) | BETWEEN 两端包含；聚合和排序仅投影访问区间 |
| sysbench index/non-index update | update | 第一版两者均替换模拟值，保留原标签 |
| sysbench delete-insert | delete＋insert | 同键且有顺序，保留事务归属 |
| sysbench 自增 insert | insert | 需捕获数据库生成 id，不能用预估计数器代替 |
| HyBench 显式 PK 点操作 | get/update/delete | 以已绑定实际参数为准；delete不在当前TP smoke覆盖内 |
| HyBench INSERT DEFAULT | insert | 捕获实际生成键才能回放 |
| HyBench 非PK筛选、JOIN、AP计算 | 不直接投影 | 保留模板/参数及不可投影原因，返回键不等于扫描过的全部键 |
| HyBench 结果驱动事务 | 分条投影支持的操作 | 查询实际执行后再采集后续参数，记录事务结局 |

## 原生能力验证状态

| 能力 | YCSB | sysbench | HyBench |
| --- | --- | --- | --- |
| 当前源码构建 | Maven通过 | autotools通过 | javac通过；Maven依赖阻断 |
| PostgreSQL 加载 | 通过 | 通过 | 8表通过 |
| PostgreSQL 原生运行 | 读/更新通过 | 标准混合OLTP通过 | 派生TP1/TP9通过 |
| 自动清理测试库 | 通过 | 通过，另验证cleanup删表 | 通过 |
| 完整标准模式测试 | 未执行 | 未覆盖其他脚本 | 未覆盖其余TP/AP/AT |
| 每请求 trace 采集 | 待实现采集器 | 待实现采集器 | 待实现采集器 |
| index server 回放 | 待实现适配器 | 待实现适配器 | 待实现适配器 |
| 对象存储后端验证 | 待提供server | 待提供server | 待提供server |

## 采集器实施注意事项

1. 先实现 YCSB DB 边界采集，再接 sysbench 绑定参数和 HyBench JDBC 模板；不要以客户端打印日志代替结构化记录。
2. 用实际行数核对生效规模，sysbench 配置转 argv；HyBench 生成和运行共享同一参数资源并使用独立目录。
3. 获取自增键、事务结局和受影响行数；受影响0行与键不存在、操作失败分别记录。
4. HyBench 配置与错误日志脱敏；修复或隔离旧 Maven 依赖问题，当前源码编译路径可继续复用。
5. 对来自并发数据库运行的 trace，不保证采集完成顺序就是可串行化顺序；第一条正确性基线使用单线程采集与回放，多线程只做明确标注的负载投影。
