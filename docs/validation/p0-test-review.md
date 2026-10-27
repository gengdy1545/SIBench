# P0 测试复核

日期：2026-10-27。范围：当前工作区的 P0 发布、事务内读取、逐查询校验与驱动提交标记测试。复核阶段仅修改测试并记录验证；随后按下方“复核处理”修复了两项生产缺陷。本记录不代替原生 PG 及三来源完整验收。

## 复核处理

复核后已修复两项发布模式缺陷，未降低断言、未使用 Ignore：

- `PublishedIndex` 不再在提交后删除事务 lane，而是标记 `complete`。迟到请求在 `Lane.prepare` 阶段被拒绝，不会触碰后端。`Publication` 在写入前用 `reserve(version, transaction)` 校验发布水位、事务归属和提交声明的写入数，并在写进行期间阻止该版本发布；写入失败时释放预留。即使迟到请求使用另一事务 ID，也会在 `reserve` 阶段拒绝。
- `Lane.prepare` 现在同时拒绝 pending 中已存在的位置和正在执行的 `next` 位置，重复请求不会覆盖首个请求。
- 新增 `lateWriteUnderAnotherTransactionCannotReuseAPublishedVersion`，并保留复核文档中的迟到 / 重复位置回归。
- 当前 `mvn -q -Dtest=PublishedIndexTest,TraceRequestsTest,ReplayProjectionTest test` 与根目录 `mvn -q test`：**296 tests，0 failures / 0 errors / 0 skipped**。

复核时的两个失败仍保留在下面的原始记录中，作为修复前证据；不能把这些失败当作当前结果。

## 结论与复现

复核时根目录 `rtk mvn -q test`：**294 tests，2 failures，0 errors，0 skipped**；292 项通过。两项失败均来自新增的发布模式边界测试，针对性运行和完整运行都复现。失败断言保持启用，不改为“期望当前错误”的测试，也未使用 Ignore。修复后的当前结果为 **296 tests，0 failures / 0 errors / 0 skipped**，见“复核处理”。

```bash
rtk mvn -q -Dtest=PublishedIndexTest,TraceRequestsTest,ReplayProjectionTest test
rtk mvn -q test
```

原始结果：`build/sibench/surefire-reports/TEST-org.sibench.adapters.PublishedIndexTest.xml` 及同目录其他测试报告（重跑会覆盖）。本次不改用户已有暂存内容；下方 SHA256 标识受查实现及本次测试版本，不能只用未提交工作区的 HEAD 代表测试身份。

## 复核时已确认的缺陷（已修复）

### 已发布版本被返回失败的迟到写入改变

测试：`PublishedIndexTest.rejectedLateWriteCannotChangeAnAlreadyPublishedSnapshot`。

1. 事务 800 写入 PK=1、version=1、RowID=11，提交并发布。
2. 同一事务及操作位置再次写 RowID=99。
3. 请求返回非 OK，但读取 version=1 得到 99；预期仍为 11。

[PublishedIndex](../../src/main/java/org/sibench/adapters/PublishedIndex.java) 在提交完成后删除 lane；迟到请求重新创建 lane。`Lane.execute` 先调用 `backend.write`，随后 `Publication.applied` 才拒绝已发布版本，因此错误响应不能阻止状态损坏。需要在写入前验证版本 / 事务是否合法，并保证检查与并发发布之间不存在竞争；不能仅修改返回状态或吞掉异常。

### 重复的待执行事务位置覆盖原请求

测试：`PublishedIndexTest.duplicatePendingPositionCannotReplaceTheOriginalWrite`。

先送事务 900 的 operation=2（RowID=22），在缺少 operation=1 时等待；再送同一位置的不同写入（RowID=99），最后补 operation=1。当前两个请求均返回 OK，新增断言在“duplicate position accepted”处失败。

`Lane.prepare` 只检查 `operation < next`，随后 `pending.put` 覆盖已有相同位置；没有拒绝待执行的重复操作。需要覆盖 pending 和 executing 两个状态的唯一性，并保留首个合法请求。测试后续还要求最终结果保持 22。

这两个用例属于迟到 / 重复请求的防御与状态完整性验证；**未据此声称正常无重试 Trace 必然得到错误结果**。但发布模式不能满足当前错误处理与验证要求，不能据原有绿灯宣称 P0 全部验收完成。

## 测试整理与补强

| 文件 | 调整 | 理由与保留的验证职责 |
| --- | --- | --- |
| [PublishedIndexTest](../../src/test/java/org/sibench/adapters/PublishedIndexTest.java) | 新增提交标记先到、版本缺口、迟到写入与待执行重复位置四项测试；事务内前缀测试改为后续写先到；替换固定等待 100–150 ms 的猜测为有界观察工作线程等待 / 完成状态，退出时检查线程池终止。 | 覆盖完整性、连续发布和事务内顺序；保留原有关闭与独立事务并行测试。 |
| [TraceRequestsTest](../../src/test/java/org/sibench/adapters/TraceRequestsTest.java) | 修正 `query(sourceEvent,...)` 忽略参数、固定写 5 的夹具；增加同结果不同来源身份及 workUnit 绑定检查；删除与错键重叠的 set 分支，把 XOR 负例改为仅改变 RowID。 | 防止夹具掩盖身份差异；错值测试不再被错主键顺带抓住。 |
| 同上 | 用逐字段、顺序、条数、64 位高位变化测试替换 `digest(x) == digest(x)` 自我比较；保留 RowID 模式不受物理 position 影响的语义检查。 | 原测试名声称验证 identity，实际仅比较自身和 version；新用例覆盖真实语义，而非重复实现。 |
| [RocksDbBackendTest](../../src/test/java/org/sibench/adapters/RocksDbBackendTest.java) | 事务内读预期改为手算的具体主键 / 版本 / RowID，补删除、同 PK 重插入、新身份和旧 snapshot。 | 避免在关键语义上仅让 RocksDB 与参考模型互证；原有随机差分测试继续保留，提供更广覆盖。 |
| [ReplayProjectionTest](../../src/test/java/org/sibench/replay/ReplayProjectionTest.java) | 替换只统计一次 commit 的旧用例，使用 `readTimestamp=0/version=1` 新语义，跨两种输入模式及二级开关核对发送顺序、实际写 / 操作计数、事务位置、下一事务计数清零和过滤后的空事务标记。 | 原用例仍采用旧 local-read 表达，并未验证方法名所称的操作顺序；新测试沿真实 TraceRequests 转换路径检查协议结果。 |

没有整批删除兼容 / 旧格式相关测试：仍声明支持的 v9 读取、拒绝缺字段输入，以及 sequence 全序诊断模式都有当前职责。只按“旧”或“兼容”关键词删除会丢失有效验证。本轮精简集中于上述 P0 用例，没有宣称审计并清理整个仓库的每项测试。

## 交接与边界

- 先修复两项发布缺陷并跑 `PublishedIndexTest`；相关小测试通过后按 AGENTS.md 运行完整 `mvn test`。测试预期无需随错误实现改变。
- 复核时 `plan.md` 仍有任务 3 的 P0 测量口径未完成；1.3 的矩阵依据是短 sysbench，新语义三来源完整验收不能由本次单测替代。复核处理后已在 `plan.md` 1.3 记录迟到 / 重复请求修复；完整三来源、网络重传和正式性能仍不能由本次单测替代。
- 未核查远程原生证据身份、未运行大 Trace 回放、未进行性能评估或故障恢复全量审计。两个失败通过直接调用真实 RocksDB 发布执行器复现，未声称已覆盖网络重传全过程。

## 受查版本

复核处理后的当前版本：

| 文件 | SHA256 |
| --- | --- |
| `src/main/java/org/sibench/adapters/PublishedIndex.java` | `9763f5d4f90fba20f14ecb904e1dfbfc5766d3a71d5e4b705424ea7139587bd6` |
| `src/test/java/org/sibench/adapters/PublishedIndexTest.java` | `206344d034bb00935feacf96a1ac8ee76be63d0b3cff7ba2acdd7ac9d83cc60a` |
| `src/test/java/org/sibench/adapters/TraceRequestsTest.java` | `03969de1d81a4d1c0177952e3e7c2d384285ad629e3267643bf680b405df40eb` |
| `src/test/java/org/sibench/adapters/RocksDbBackendTest.java` | `a0315e747b0d7fef7e3f488012d0a33074791e1a6cac4f1b3a4a5765effe9220` |
| `src/test/java/org/sibench/replay/ReplayProjectionTest.java` | `9c926f7c07f56cf3fb860e850e53849b2a91729eaaa84a5f221517ec21de5ab8` |

复核时（修复前）`PublishedIndex.java` / `PublishedIndexTest.java` 的 SHA256 分别为 `27ef8abb...e08b` / `32afb447...145e`，保留为问题复现身份。
