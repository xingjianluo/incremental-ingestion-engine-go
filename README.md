# incremental-ingestion-engine-go

一套本地可运行的增量数据摄入与落地流程：用本地文件模拟数据源（JSONL 变更流水）与目标端（JSON 快照 + 死信文件），保证在**重复消费、位点回退、来源结构变化**下，最终数据状态可预期、可解释；并额外提供**被拒数据修复重投**与**可重复执行的结果对账**能力。

## 目录结构

```
internal/ingest/
  types.go           事件、操作类型、拒绝原因等核心类型
  source.go          本地 JSONL 数据源（行号即位点）
  sink.go            本地目标端：幂等落地、修复落地（冲突裁决）、死信记录
  offset.go          位点存储：单调推进、CAS 提交
  schema.go          结构注册表：版本管理、兼容校验、投影
  schema_history.go  结构变更流水：所有注册尝试（含不兼容）落盘留痕、重启可查
  repair.go          修复重投：被拒记录修好后重新走校验/投影/落地，不推进位点
  reconcile.go       结果对账：来源流水 × 落地状态 × 处置结果，确定性对齐
  pipeline.go        摄入流水线：采集 -> 校验 -> 投影 -> 落地 -> 提交位点
cmd/ingestdemo/      端到端本地演示
```

## 摄入协议

变更事件（`Event`）逐行追加到来源文件，每行一个 JSON：

```json
{"seq":1,"id":"e1","op":"insert","pk":"a","schemaVersion":1,"fields":{"name":"alice","age":30}}
```

- `seq`：来源端单调递增位点；`id`：事件唯一标识（幂等去重）；`op`：`insert` / `update` / `delete`；
- `pk`：主键，必填；`schemaVersion`：事件遵循的结构版本；`fields`：字段载荷（`delete` 可空）。

**校验与拒绝**：坏记录写入死信文件 `deadletter.jsonl`，原因可区分，绝不静默丢弃：

| 原因 | 含义 |
|---|---|
| `missing_pk` | 主键为空 |
| `missing_field` | 必需字段缺失 |
| `type_mismatch` | 字段类型与结构声明不匹配 |
| `unknown_schema` | 事件声明的结构版本未注册 |
| `incompatible_schema` | 载荷含最新结构中不存在的字段（防静默错列）；或事件所遵循的结构演进不兼容 |
| `unknown_op` | 未知操作类型 |
| `pk_conflict` | 修复重投撞上目标端已有主键且无法凭来源顺序安全裁决先后（见下） |

**幂等落地**：目标端按事件 `id` 去重，且同一主键上 `seq` 不大于已应用值的正常消费事件被跳过；死信按 `(id, reason)` 去重。因此同一批数据重复运行后，目标端不重复、不丢失。

## 位点提交规则

- 位点 = 已消费的事件**行数**（不是 seq 值），持久化在 `offset` 文件中（写临时文件 + rename，崩溃安全）。
- 提交为 CAS：`Commit(prev, next)` 要求 `prev` 等于当前已提交位点且 `next > prev`。
  - `next <= committed` → `ErrOffsetRegression`（回退，拒绝）；
  - `prev != committed` → `ErrOffsetGap`（跳段/并发竞争失败，拒绝）。
- 并发消费时多个 worker 共享同一 `OffsetStore`：只有赢得 CAS 的提交生效，失败者以最新已提交位点重试，**已提交位点只单调推进**。
- 进程中断后重新 `Load()` 即从最近一次已提交位点恢复。

## 结构兼容规则

结构以版本注册（首版本必须为 1，之后每次 +1），演进规则：

| 变化 | 结果 |
|---|---|
| 新增字段且显式声明缺省值（`HasDefault`） | 兼容；老事件与存量行一律回填该缺省值，语义确定且稳定 |
| 新增字段但未声明缺省值 | 拒绝（`ErrIncompatibleSchema`） |
| 删除字段 | 拒绝，错误详情含字段名与版本 |
| 字段改名（删除 + 新增同类型字段） | 拒绝，详情中提示 `possible rename to "..."` |
| 已有字段类型变更、可选改必需 | 拒绝 |

事件落地前会被**投影**到最新结构：缺失的新增字段填缺省值；载荷中出现最新结构之外的字段时以 `incompatible_schema` 拒绝并进入死信，目标端绝不静默错列。

### 结构变更流水（可追溯）

使用 `NewSchemaRegistryWithHistory(path)` 创建带留痕的注册表后，**每一次**版本注册尝试——包括被判定为不能安全兼容、没有生效的那些——都会追加到 `schema-history.jsonl`，保存版本号、是否接受、错误详情与该版本的字段快照：

- 进程重启后用 `History()` / `HistoryForVersion(v)` 依然能查到，可据此解释“某条记录为什么被判成结构不兼容”；
- 同内容尝试（版本 + 是否接受 + 详情 + 字段集相同）只留痕一次，重复注册不产生重复流水。

## 被拒数据修复重投

被拒记录在上游改好后，可通过**修复队列**（`RepairQueue`，另一个 JSONL 文件）重新提交：

```go
rq := ingest.NewRepairQueue(dir + "/repairs.jsonl")
rq.Append(ingest.Event{Seq: 2, ID: "e2", /* 沿用原事件 ID，字段已修好 */})
res, _ := p.ProcessRepairs(rq)
```

- 修复记录就是普通 `Event`，走**和正常消费完全相同**的校验、投影、落地流程；
- **不推进来源位点**：位点只反映来源流水的消费进度，修复重投对它没有任何影响；
- 沿用原事件 `id`，因此**重复处理同一批修复是幂等的**：已生效的跳过、死信按 `(id, reason)` 去重，不产生重复数据、不重复堆积拒绝记录；
- 修复成功后，该记录在对账中从 `rejected` 翻转为 `applied`（标记 `repaired=true`），历史拒绝原因仍保留以便解释来龙去脉。

### 主键冲突边界（`pk_conflict`）

修正后的记录如果撞上目标端已存在的主键，按来源顺序能否安全裁决分三类，全程在 Sink 锁内原子完成：

| 情形 | 处置 |
|---|---|
| 修复 seq **新于**该主键已生效 seq | 可凭来源顺序安全判定为更新 → **生效** |
| 修复 seq **等于**已生效 seq，但属于另一条事件 | 无法安全判定先后 → **`pk_conflict` 拒绝**，绝不改写已有数据，也不静默略过 |
| 修复 seq **旧于**已生效 seq（含主键已被删除） | 确定性过期（`stale`），不回滚更新、不复活已删行 |

**同一批修复里同一主键出现多次**时，只有一条能生效，裁决结果与文件顺序、并发无关、可复现：

1. `seq` 最大者胜出；
2. `seq` 相同则事件 `id` 字典序最小者胜出；
3. 其余同主键记录一律以 `pk_conflict` 拒绝，详情里直接写明胜出者是谁。

## 结果对账（可重复执行）

`Reconciler` 把**来源流水、目标端落地状态、死信处置结果**三方对齐，为每条记录给出确定结论：

```go
rep, _ := ingest.NewReconciler(src, sink, offsets, reg).
    WithRepairQueue(rq).   // 可选：同时对齐修复队列
    Reconcile()
```

| 状态 | 含义 |
|---|---|
| `applied` | 最终在目标端生效（insert/update 成行为一行，或 delete 已生效） |
| `rejected` | 最终仍被拒，`Reasons` 给出全部可区分原因 |
| `skipped_stale` | 已消费但来源顺序上确定过期（同主键已有更新 seq 生效） |
| `pending` | 位点（行数）尚未推进到该记录，尚未处理 |

报告还包含：

- `Counts` / `ReasonCounts`：分类计数。`ReasonCounts` 只统计**最终仍被拒**的记录——修复翻转后不再计数，重复投递不会重复计数；
- `Records[].Reasons`：该事件尝试过的全部拒绝原因（即使修好生效，历史原因也保留）；
- `Records[].Repaired`：是否曾通过修复队列重投；
- `OrphanRepairs`：出现在修复队列、但来源流水里没有同 ID 事件的记录（含是否已生效）；
- `SchemaChanges`：全部结构变更尝试（含不兼容），用于解释结构相关结论。

**确定性保证**：报告只依赖本地文件状态、不含时间或随机因素，记录按来源出现顺序排列。因此重复运行、并发消费、进程重启后，对同样的输入产出一致且可复现的结论（测试中以归一化字符串逐字节比较）。

## 本地验证

```bash
# 运行单元测试（覆盖重复消费、并发推进、结构变更、修复重投、主键冲突、对账确定性；
# -v 日志可看到位点、提交顺序、修复裁决与结构变更流水）
go test ./internal/ingest/ -v

# 运行端到端演示
go run ./cmd/ingestdemo
```

演示会在临时目录（`$TMPDIR/ingestdemo`）生成：`source.jsonl`（来源）、`repairs.jsonl`（修复队列）、`sink.json`（目标端）、`deadletter.jsonl`（死信）、`offset`（位点）、`schema-history.jsonl`（结构变更流水），可直接查看最终状态与对账输出。

## 兼容范围

- 本次为**增量能力**：`NewPipeline` / `Run` / `RunOnce` / `UpgradeSchema` 等既有 API 与文件格式（`sink.json`、`deadletter.jsonl`、`offset`）保持不变，原有用例无需改动。
- 结构变更留痕为可选项：继续使用 `NewSchemaRegistry()` 行为与以前一致（仅内存记录 `History`）；需要重启可查时改用 `NewSchemaRegistryWithHistory(path)`。
- 修复重投与对账都是只读/幂等流程：修复不改来源文件、不推进位点；对账不修改任何状态，可安全地反复、并发执行。
