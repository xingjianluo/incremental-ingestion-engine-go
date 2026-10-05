package ingest

// Op 表示变更事件的操作类型。
type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Event 是一条变更事件。
type Event struct {
	Seq           uint64         `json:"seq"`           // 来源端单调递增位点
	ID            string         `json:"id"`            // 事件唯一标识（幂等去重用）
	Op            Op             `json:"op"`            // 操作类型
	PK            string         `json:"pk"`            // 主键
	SchemaVersion int            `json:"schemaVersion"` // 事件所遵循的来源结构版本
	Fields        map[string]any `json:"fields"`        // 字段载荷
}

// RejectReason 区分记录被拒绝的原因。
type RejectReason string

const (
	RejectMissingPK      RejectReason = "missing_pk"
	RejectMissingField   RejectReason = "missing_field"
	RejectTypeMismatch   RejectReason = "type_mismatch"
	RejectUnknownSchema  RejectReason = "unknown_schema"
	RejectIncompatSchema RejectReason = "incompatible_schema"
	RejectUnknownOp      RejectReason = "unknown_op"
	// RejectPKConflict 表示修正重投的记录撞上目标端已存在的主键，
	// 且无法凭来源顺序安全判定先后；绝不改写已有数据，也不静默略过。
	RejectPKConflict RejectReason = "pk_conflict"
)

// Rejection 记录一条被拒绝的事件及其可区分原因。
type Rejection struct {
	EventID string       `json:"eventId"`
	Seq     uint64       `json:"seq"`
	Reason  RejectReason `json:"reason"`
	Detail  string       `json:"detail"`
}

// ResubmitOutcome 汇总一次修复重投的结果。
type ResubmitOutcome struct {
	Applied  []string    `json:"applied"`  // 本次实际生效的事件 ID
	Skipped  []string    `json:"skipped"`  // 幂等跳过的事件 ID（此前已应用过）
	Rejected []Rejection `json:"rejected"` // 本次被拒记录（含可区分原因）
}

// Disposition 是对账后一条记录的最终处置结论。
type Disposition string

const (
	DispApplied  Disposition = "applied"  // 已生效（目标端最终状态包含它）
	DispRejected Disposition = "rejected" // 被拒（死信中有记录，且之后未生效）
	DispSkipped  Disposition = "skipped"  // 已消费但被跳过（同主键上有更新的事件）
	DispPending  Disposition = "pending"  // 尚未消费（位点未推进到）
)

// ReconcileEntry 是一条记录的对账结论。
type ReconcileEntry struct {
	EventID     string       `json:"eventId"`
	Seq         uint64       `json:"seq"`
	PK          string       `json:"pk"`
	Disposition Disposition  `json:"disposition"`
	Reason      RejectReason `json:"reason,omitempty"` // 被拒时的可区分原因
	Detail      string       `json:"detail,omitempty"`
}

// ReconcileReport 对齐来源流水、目标端最终状态与处置结果。
// 结论按 (Seq, EventID) 排序，同样输入必然得到同样报告。
type ReconcileReport struct {
	Entries  []ReconcileEntry    `json:"entries"`  // 来源流水中的记录（按事件 ID 去重）
	External []ReconcileEntry    `json:"external"` // 不在来源流水中的处置记录（如修复重投）
	Counts   map[Disposition]int `json:"counts"`
}

// SchemaAttempt 记录一次结构版本注册尝试（含被判定不兼容的），
// 持久化在本地变更流水里，进程重启后依然可查。
type SchemaAttempt struct {
	Version  int     `json:"version"`
	Fields   []Field `json:"fields"`
	Accepted bool    `json:"accepted"`
	Reason   string  `json:"reason,omitempty"` // 拒绝时的可追溯详情
}
