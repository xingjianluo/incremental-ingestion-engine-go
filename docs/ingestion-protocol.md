# 增量摄入协议、位点提交与结构兼容规则

本项目用**本地文件**同时模拟变更数据源（事件日志）与目标端（物化表），实现一套可重复运行、
可中断恢复、可解释最终状态的增量摄入流程。无外部依赖（仅 Go 标准库）。

## 1. 目录结构

```
internal/
  protocol/   变更事件模型、Schema 定义、校验与结构兼容性规则
  storage/    本地文件实现：来源事件日志、目标端表、拒绝记录、变更流水、结构变更历史
  ingest/     位点存储、并发 worker、处理器、摄入引擎
cmd/demo/     端到端可运行演示
```

运行后所有状态都落盘到 `./data/`：

| 文件 | 作用 |
| --- | --- |
| `data/sources/<source>.jsonl` | 来源变更事件日志（每行一个事件，`seq` 从 1 连续） |
| `data/targets/<source>.json` | 目标端物化表（行数据 + 已应用 `applied_seq`） |
| `data/offsets/<source>.offset.json` | 已提交位点（watermark），原子写 + fsync |
| `data/rejects/<source>.rejects.jsonl` | 被拒绝记录及**可区分原因**（按 seq 幂等） |
| `data/changelog/<source>.changelog.jsonl` | 已落地变更流水（按 seq 幂等） |
| `data/schemas/<source>.schema-history.jsonl` | 结构变更历史（兼容/不兼容都留痕，兼容版本附完整 Schema） |

## 2. 摄入协议（事件、操作、拒绝原因）

每条事件 `ChangeEvent` 包含：`seq`（来源内从 1 连续递增）、`source_name`、`op`、
`schema_ver`、`payload`、`emitted_at`。

支持三种操作：

- `insert`：仅插入。同主键已存在一条**不同**记录时，按 `PRIMARY_KEY_CONFLICT` 拒绝。
  同一批尚未提交的事件里若有多个 `insert` 命中同一主键，引擎在并发处理**之前**先按 seq
  顺序对整批做一次锁内预解析（`TargetStore.PrepareBatch`）：最小 seq 插入成功，
  其余稳定判为冲突。因此冲突结果与 worker 调度顺序无关，可重复复现。
- `upsert`：按主键插入或覆盖（最后写入生效），不产生主键冲突。
- `delete`：按主键删除，幂等（删除不存在的主键视为成功）。

坏记录**不会被静默丢弃**，而是写入拒绝日志，原因严格区分：

| RejectReason | 触发条件 |
| --- | --- |
| `MISSING_PRIMARY_KEY` | 主键字段缺失或为 null |
| `MISSING_REQUIRED_FIELD` | Schema 声明的必需字段缺失或为 null |
| `TYPE_MISMATCH` | 字段值类型与声明不符（如 int 字段收到字符串/小数） |
| `PRIMARY_KEY_CONFLICT` | `insert` 命中已存在的不同主键记录 |
| `INCOMPATIBLE_SCHEMA` | 载荷出现未声明字段、操作类型未知、引用未注册 Schema 版本等 |

校验顺序固定为：主键 → 必需字段 → 类型 → 未声明字段，因此一条坏记录只对应一个确定原因。

### 幂等（重复消费）

- 目标端记录每个成功应用的 `seq`（`applied_seq`）。同一 `seq` 重复投递时直接返回
  `duplicate-skip`，不重复写行；冲突的 `insert` **不登记** applied 标记，
  因此重放时仍稳定报 `PRIMARY_KEY_CONFLICT`，原因不会被"重复"掩盖。
- 拒绝日志与变更流水都按 `seq` 去重，重复运行不产生重复条目。
- 因此同一批数据无论运行多少遍（含消息重投、进程重启），目标端行数、拒绝记录、
  变更流水均与首轮完全一致：**无重复、无丢失**。

## 3. 位点提交协议（断点与并发协同）

- 位点 watermark 表示"≤ 该 seq 的事件均已处置完成（成功落地或被显式拒绝留痕）"。
- 唯一合法的提交是 `commit(committed + 1)`：
  - `seq <= committed` → `ErrOffsetRollback`（回退，拒绝）；
  - `seq > committed + 1` → `ErrOffsetGap`（跳段，拒绝），位点保持不变。
  - 引擎遇到违例立即中止并打印 FATAL 日志，**绝不"从中间继续"**。
- **并发分工**：每批事件在分发前先做一次按 seq 顺序的锁内预解析，确定批内重投与主键冲突；
  worker 池随后只负责无顺序依赖的"校验 + 落地/拒绝"（存储层加锁且按 seq 幂等）；
  **位点提交由引擎按 seq 升序串行执行**。因此即使处理完成顺序是乱的，
  提交顺序与 watermark 也严格为 1,2,3,…，单调推进。
- 坏记录同样推进位点（它已被显式拒绝并留痕），保证 watermark 连续、管道不会卡死。
- 每次提交成功立即原子写盘并 fsync；进程重启后从位点文件恢复到最近一次已提交位点，
  从 `watermark+1` 继续。已应用但位点尚未提交的事件重放时由幂等机制吸收（at-least-once）。

## 4. 来源结构变化的兼容规则

Schema 版本号必须严格 +1。规则如下：

### 允许（兼容，自动处理）

- **新增字段**，且具备确定、稳定的缺省语义之一：
  - 声明 `has_default=true` 及与类型一致的 `default`：历史行与新记录缺失时一律取该默认值；
  - 非必需字段（nullable）：缺失一律补 `nil`。
- 注册兼容新版本时，引擎立即用该缺省语义**回填全部历史行**，回填行数记入结构变更历史；
  新记录缺字段时由 `CoerceRecord` 按同一规则补全。历史回填与新写入共用同一缺省语义，结果稳定。
- 必需性放宽（required → optional）允许。

### 禁止（显式失败，绝不静默错列）

- 删除字段；
- 字段改名（表现为旧名消失 + 新名出现）；
- 修改字段类型；
- 修改主键；
- 修改既有字段的缺省值/缺省语义；
- 必需性收紧（optional → required）；
- 新增**必需**字段却不提供缺省值；
- 版本号跳号。

不兼容变化：不登记新版本（注册表停留在旧版本），在结构变更历史写一条
`compatible=false` 的留痕（含可读原因），并返回包装了 `ErrIncompatibleSchema` 的错误，
由调用方显式处理。载荷中出现 Schema 未声明的字段同样按 `INCOMPATIBLE_SCHEMA` 拒绝，
防止来源多出的列在目标端静默丢失或错列。

兼容版本的完整 Schema 会随结构历史持久化，进程重启后通过
`SchemaHistory.RestoreRegistry` 重建内存注册表，缺省语义在重启后保持不变。

## 5. 本地验证方法

前置：Go 1.25+（开发环境为 Go 1.26）。

```bash
# 端到端演示（自动清理并重建 ./data，打印位点、提交顺序、拒绝记录、变更流水、最终状态）
go run ./cmd/demo

# 全量单元测试（-v 可看到位点、提交顺序与变更流水日志）
go test -v ./...

# 带竞态检测（覆盖并发消费）
go test -race ./...
```

### 测试覆盖的三类场景

1. **重复消费幂等**（`TestRepeatedConsumptionIdempotent`）：
   首轮摄入含主键冲突/缺必需字段/类型错/缺主键的坏记录；再普通重跑、模拟进程重启重跑、
   以及强制消息重投（清空位点重放同一日志）。断言目标端无重复无丢失、四类拒绝原因各 1 条、
   拒绝记录与流水条数始终不变。
2. **并发推进与中断恢复**（`TestOffsetConcurrentMonotonic`、
   `TestConcurrentProcessingOrderedCommitAndRecovery`、`TestOffsetCommitProtocol`、
   `TestOffsetRecoverFromDisk`、`TestEngineRejectsGapAndRollback`）：
   16 worker 竞争提交 1..100、8 worker 处理 30 条事件，断言提交顺序严格连续、watermark 单调；
   人为回退/跳段提交必须被明确拒绝；模拟位点停在 17 的中断后重启，从 18 续到 30 且流水不重复。
3. **结构变更**（`TestSchemaEvolution`，以及 protocol 包的兼容性表驱动测试）：
   新增带默认值字段 → 历史行回填、新记录补默认；新增必需无缺省字段、删字段/改名、改类型、
   改主键、必需性收紧、版本跳号 → 全部显式失败并留痕，注册表与目标端保持原状；
   事件引用未注册版本被按原因拒绝；重启后缺省语义依旧成立。

单测通过 `t.Logf` 打印：当前 watermark、成功提交顺序（commit_order）、
按原因分组的拒绝计数，以及完整变更流水（seq/op/action）。
