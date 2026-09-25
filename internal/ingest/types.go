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
)

// Rejection 记录一条被拒绝的事件及其可区分原因。
type Rejection struct {
	EventID string       `json:"eventId"`
	Seq     uint64       `json:"seq"`
	Reason  RejectReason `json:"reason"`
	Detail  string       `json:"detail"`
}
