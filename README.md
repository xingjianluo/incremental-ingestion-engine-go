# incremental-ingestion-engine-go

一套本地可运行的增量数据摄入与落地流程：用本地文件模拟数据源（JSONL 变更流水）与目标端（JSON 快照 + 死信文件），保证在**重复消费、位点回退、来源结构变化**下，最终数据状态可预期、可解释。

## 目录结构

```
internal/ingest/
  types.go     事件、操作类型、拒绝原因等核心类型
  source.go    本地 JSONL 数据源（行号即位点）
  sink.go      本地目标端：幂等落地 + 死信记录
  offset.go    位点存储：单调推进、CAS 提交
  schema.go    结构注册表：版本管理、兼容校验、投影
  pipeline.go  摄入流水线：采集 -> 校验 -> 投影 -> 落地 -> 提交位点
cmd/ingestdemo/ 端到端本地演示
```

## 摄入协议

变更事件（`Event`）逐行追加到来源文件，每行一个 JSON：

```json
{"seq":1,"id":"e1","op":"insert","pk":"a","schemaVersion":1,"fields":{"name":"alice","age":30}}
```

- `seq`：来源端单调递增位点；`id`：事件唯一标识（幂等去重）；`op`：`insert` / `update` / `delete`；
- `pk`：主键，必填；`schemaVersion`：事件遵循的结构版本；`fields`：字段载荷（`delete` 可空）。

**校验与拒绝**：坏记录不会被写入死信文件 `deadletter.jsonl`，原因可区分，绝不静默丢弃：

| 原因 | 含义 |
|---|---|
| `missing_pk` | 主键为空 |
| `missing_field` | 必需字段缺失 |
| `type_mismatch` | 字段类型与结构声明不匹配 |
| `unknown_schema` | 事件声明的结构版本未注册 |
| `incompatible_schema` | 载荷含最新结构中不存在的字段（防静默错列） |
| `unknown_op` | 未知操作类型 |

**幂等落地**：目标端按事件 `id` 去重，且同一主键上 `seq` 不大于已应用值的事件被跳过；死信按 `(id, reason)` 去重。因此同一批数据重复运行后，目标端不重复、不丢失。

## 位点提交规则

- 位点 = 已消费的事件行数，持久化在 `offset` 文件中（写临时文件 + rename，崩溃安全）。
- 提交为 CAS：`Commit(prev, next)` 要求 `prev` 等于当前已提交位点且 `next > prev`。
  - `next <= committed` → `ErrOffsetRegression`（回退，拒绝）；
  - `prev != committed` → `ErrOffsetGap`（跳段/并发竞争失败，拒绝）。
- 并发消费时多个 worker 共享同一 `OffsetStore`：只有赢得 CAS 的提交生效，失败者以最新已提交位点重试，因此**已提交位点只单调推进**。
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

事件落地前会被**投影**到最新结构：缺失的新增字段填缺省值；载荷中出现最新结构之外的字段（来源于已删除/改名字段）时以 `incompatible_schema` 拒绝并进入死信，目标端绝不静默错列。不兼容的演进失败时，错误信息保留字段名、版本等可追溯信息。

## 本地验证

```bash
# 运行单元测试（覆盖重复消费、并发推进、结构变更；日志打印位点、提交顺序与变更流水）
go test ./internal/ingest/ -v

# 运行端到端演示
go run ./cmd/ingestdemo
```

演示会在临时目录生成 `source.jsonl`（来源）、`sink.json`（目标端）、`deadletter.jsonl`（死信）、`offset`（位点），可直接查看最终状态。
