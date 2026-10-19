# 存储后端实验契约

本文确定配置与验收口径。仓库尚无真实 index server、协议或后端实现；本次 PostgreSQL 是参考执行环境，既没有执行 SSD 性能对比，也没有验证对象存储后端。下表的待确认项由真实适配器接入时补齐。

## 配置与职责

[profiles.example.yaml](../../config/storage/profiles.example.yaml) 是后续配置设计样例，当前 native_smoke.py 脚本不消费它。`workload` 指逻辑负载，`target` 指server接入，`storage_profile` 指被测数据存储，`artifacts` 指实验产物位置。目标协议、endpoint及server配置路径未提供时必须显式未配置，不能默认连到某个服务。

SIBench保存配置、驱动逻辑键操作和汇总指标；server决定一个对象包含多少索引页或键、何时上传、如何缓存、合并和回收。对象大小不能由 `value_bytes` 推导，逻辑scan也不等同于S3 LIST或字节Range GET。对象存储实验产物上传是独立功能，不计作后端验收。

## 能力矩阵

| 能力 | 本地SSD目标 | S3兼容对象目标 | 混合目标 | 当前实测 |
| --- | --- | --- | --- | --- |
| server协议与进程生命周期 | 待提供 | 待提供 | 待提供 | 无 |
| 索引/value落点 | 本地配置待核验 | 远端对象落点待核验 | 分层策略待核验 | 无 |
| WAL/缓存落点与预算 | 待核验 | 本地或远端均须说明 | 各层分别说明 | 无 |
| PK比较器及点操作 | server能力声明 | 同左 | 同左 | 无 |
| bounds/start_limit扫描 | server能力声明 | 同左 | 同左 | 无 |
| 写入确认语义 | accepted/local_durable等 | accepted/local/remote_durable等 | 按路径声明 | 无 |
| flush/wait_durable屏障 | 目标层级待定义 | 远端完成条件待定义 | 各层分别定义 | 无 |
| 缓存控制 | server/OS层 | 本地缓存；远端服务缓存可能不可控 | 全部可控层 | 无 |
| 重启恢复 | 依声明验证 | 依声明验证 | 依声明验证 | 无 |
| 请求/字节/后台任务指标 | I/O指标接口待提供 | SDK/server/provider接口待提供 | 分层汇总 | 无 |

unknown 与 unsupported 分开：前者尚未调查或无法观测，后者server明确没有能力。缺少可选监控显示N/A；缺少实验必需能力则该实验不能通过前置检查。

## 实验流程与公平比较

1. 固定初始键快照、请求记录、模拟值算法、并发、限速和错误策略；每次使用独立 namespace／对象prefix。
2. 保存生效配置：server版本、provider及API、bucket/region、存储类别、部署位置、WAL/缓存位置、缓存字节预算、后台合并策略。凭据仅保留环境变量引用。
3. 加载后等待声明的持久化屏障。按场景设置冷缓存或执行热缓存预热，并记录实际清理层；超缓存场景说明工作集与哪一层缓存比较。
4. 前台测量记录索引调用吞吐、延迟、状态；结束后另行计时等待上传、flush、compaction排空。前台确认耗时与远端持久化耗时分别报告。
5. 按声明验证重启后的键值与扫描结果。限定资源清理；逻辑delete生效与远端对象被回收分开核验。

只有持久化目标、缓存条件、负载和测量阶段可比时才合并结果。不同网络位置、存储类别、前台确认策略另分组。无法控制对象服务内部缓存时将该层标为未知，不声称“全链路冷缓存”。

## 指标定义

| 层级 | 指标 | 来源与解释 |
| --- | --- | --- |
| 逻辑索引 | ops/s、P50/P95/P99、错误/未命中 | 回放客户端，同一计时边界 |
| server缓存 | hit/miss、容量、逐出 | server接口；未提供为N/A |
| 对象请求 | GET/PUT/LIST/DELETE/HEAD、重试、限流 | server/SDK或provider；标明来源，不能相加造成重复计数 |
| 传输 | 上传/下载字节 | 实际传输层计数，不以value字节替代 |
| 后台 | 上传/合并积压、排空时间 | server指标或屏障 |
| 空间 | 对象数量/大小、本地占用、版本保留 | 测量时间点与服务配置固定 |
| 放大 | 物理字节/已定义逻辑字节 | 分母与统计窗口明确且数据完整才计算 |

Amazon S3 官方将总请求时延与首字节时延分开，并说明部分删除指标按对象数量计量；provider指标不是索引操作计数。S3一致性保证也不能替代index server自身的写入确认与持久化契约。具体服务不得仅凭“S3兼容”继承所有AWS行为。核查参考为AWS官方文档：

```text
https://docs.aws.amazon.com/AmazonS3/latest/userguide/metrics-dimensions.html
https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
```

## 存储后端验收门槛

同一固定负载至少在真实server的本地与一种对象后端完成加载、点读、写入、更新、删除、扫描及声明的持久化恢复验证。必须有真实后端I/O证据。混合部署按实际组件落点标记。超时、限流、暂时不可达和提交结果未知通过专属测试环境验证；未经server幂等契约保证的写入不自动重试。
