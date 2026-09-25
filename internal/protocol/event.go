// Package protocol 定义增量摄入协议：变更事件、拒绝原因、Schema 与兼容性规则。
package protocol

import "time"

// OpType 表示一条变更事件的操作类型。
type OpType string

const (
	OpInsert OpType = "insert" // 仅插入：同主键已存在即主键冲突
	OpUpsert OpType = "upsert" // 插入或按主键更新（最后写入覆盖）
	OpDelete OpType = "delete" // 按主键删除，幂等
)

// RejectReason 描述记录被拒绝的可区分原因。
type RejectReason string

const (
	// RejectPrimaryKeyConflict 目标端已存在不同内容的同主键记录（仅插入语义下冲突）。
	RejectPrimaryKeyConflict RejectReason = "PRIMARY_KEY_CONFLICT"
	// RejectMissingRequiredField 记录缺失 Schema 声明的必需字段。
	RejectMissingRequiredField RejectReason = "MISSING_REQUIRED_FIELD"
	// RejectTypeMismatch 字段值与 Schema 声明类型不匹配。
	RejectTypeMismatch RejectReason = "TYPE_MISMATCH"
	// RejectMissingPrimaryKey 记录缺少主键字段。
	RejectMissingPrimaryKey RejectReason = "MISSING_PRIMARY_KEY"
	// RejectIncompatibleSchema 来源结构发生无法安全兼容的变化。
	RejectIncompatibleSchema RejectReason = "INCOMPATIBLE_SCHEMA"
)

// ChangeEvent 是来源端产生的一条变更事件。
// Seq 在单个 SourceName 内严格单调递增、连续且从 1 开始。
type ChangeEvent struct {
	Seq        int64                  `json:"seq"`
	SourceName string                 `json:"source_name"`
	Op         OpType                 `json:"op"`
	SchemaVer  int                    `json:"schema_ver"`
	Payload    map[string]interface{} `json:"payload"`
	EmittedAt  time.Time              `json:"emitted_at"`
}

// RejectedRecord 保留一条被拒绝记录的可追溯信息。
type RejectedRecord struct {
	Event      ChangeEvent  `json:"event"`
	Reason     RejectReason `json:"reason"`
	Detail     string       `json:"detail"`
	RejectedAt time.Time    `json:"rejected_at"`
}
