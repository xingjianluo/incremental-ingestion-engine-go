package ingest

import (
	"fmt"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/storage"
)

// RecordProcessor 是 Processor 的默认实现：
//  1. 按事件 SchemaVer 取 Schema（未注册版本 -> 显式拒绝）；
//  2. 校验记录（主键缺失/必需字段缺失/类型不匹配/未声明字段）；
//  3. 通过校验的记录规范化（类型规整、缺省补全）后幂等落地目标端；
//  4. insert 主键冲突按可区分原因拒绝（批内冲突由引擎预解析，结果确定）；
//  5. 拒绝记录写入拒绝日志、成功落地写入变更流水，二者均按 Seq 幂等。
type RecordProcessor struct {
	registry  *protocol.SchemaRegistry
	target    storage.TargetStore
	rejects   storage.RejectLog
	changelog storage.ChangeLogger
	now       func() time.Time
}

// NewRecordProcessor 组装默认处理器。
func NewRecordProcessor(
	reg *protocol.SchemaRegistry,
	tgt storage.TargetStore,
	rejects storage.RejectLog,
	cl storage.ChangeLogger,
) *RecordProcessor {
	return &RecordProcessor{
		registry:  reg,
		target:    tgt,
		rejects:   rejects,
		changelog: cl,
		now:       time.Now,
	}
}

func (p *RecordProcessor) reject(ev protocol.ChangeEvent, reason protocol.RejectReason, detail string) ProcessResult {
	rec := protocol.RejectedRecord{
		Event:      ev,
		Reason:     reason,
		Detail:     detail,
		RejectedAt: p.now(),
	}
	// 拒绝日志按 Seq 幂等：重复运行不会产生重复拒绝记录。
	if _, err := p.rejects.Append(rec); err != nil {
		return ProcessResult{Event: ev, OK: false, Reason: reason, Detail: fmt.Sprintf("%s (reject persist error: %v)", detail, err)}
	}
	return ProcessResult{Event: ev, OK: false, Reason: reason, Detail: detail}
}

// Process 实现 Processor，并发安全由底层存储的互斥锁与批次预解析保证。
func (p *RecordProcessor) Process(ev protocol.ChangeEvent, plan storage.BatchPlan) ProcessResult {
	schema, err := p.registry.Get(ev.SourceName, ev.SchemaVer)
	if err != nil {
		return p.reject(ev, protocol.RejectIncompatibleSchema,
			fmt.Sprintf("schema v%d is not registered: %v", ev.SchemaVer, err))
	}
	d := plan.BySeq[ev.Seq] // 零值表示不在批内（独立调用），交由 target 自身判定

	pk, pkReason, pkDetail := extractPK(schema, ev)
	if pkReason != "" {
		return p.reject(ev, pkReason, pkDetail)
	}

	if ev.Op == protocol.OpDelete {
		return p.applyDelete(ev, schema, pk, d)
	}
	if ev.Op != protocol.OpInsert && ev.Op != protocol.OpUpsert {
		return p.reject(ev, protocol.RejectIncompatibleSchema,
			fmt.Sprintf("unknown op type %q", ev.Op))
	}

	if reason, detail := schema.ValidateRecord(ev.Payload); reason != "" {
		return p.reject(ev, reason, detail)
	}
	data := schema.CoerceRecord(ev.Payload)

	// 重投（该 seq 已应用过）：幂等跳过，连冲突都不再报。
	if d.Dup {
		dup, _, applyErr := p.target.Apply(ev, pk, data)
		if applyErr != nil {
			return p.reject(ev, protocol.RejectIncompatibleSchema, applyErr.Error())
		}
		if !dup {
			// 理论不可达：计划判定为 dup，底层也应一致；防御性记录。
			return p.reject(ev, protocol.RejectIncompatibleSchema,
				fmt.Sprintf("batch plan/state disagree on dup for seq %d", ev.Seq))
		}
		return p.writeChangelog(ev, pk, "duplicate-skip")
	}

	// 主键冲突（历史行占用，或同批更早的同主键 insert/upsert 占用）：
	// 预解析已按 seq 顺序确定，不依赖 worker 调度。
	if d.Conflict {
		existing, _, _ := p.target.Get(pk)
		return p.reject(ev, protocol.RejectPrimaryKeyConflict,
			fmt.Sprintf("insert conflicts with existing row pk=%s existing=%v", pk, existing))
	}

	// 落地前探测行是否已存在，仅用于流水标注 inserted/updated。
	_, getOK, _ := p.target.Get(pk)
	dup, conflict, applyErr := p.target.Apply(ev, pk, data)
	if applyErr != nil {
		return p.reject(ev, protocol.RejectIncompatibleSchema, applyErr.Error())
	}
	if conflict {
		// 批内未预解析到（独立调用）时，仍以底层原子结果兜底。
		existing, _, _ := p.target.Get(pk)
		return p.reject(ev, protocol.RejectPrimaryKeyConflict,
			fmt.Sprintf("insert conflicts with existing row pk=%s existing=%v", pk, existing))
	}
	if dup {
		return p.writeChangelog(ev, pk, "duplicate-skip")
	}
	action := "inserted"
	if ev.Op == protocol.OpUpsert && getOK {
		action = "updated"
	}
	return p.writeChangelog(ev, pk, action)
}

// extractPK 校验并取出主键键值；delete 与写入共用。
func extractPK(schema *protocol.Schema, ev protocol.ChangeEvent) (string, protocol.RejectReason, string) {
	for _, pkField := range schema.PrimaryKey {
		v, ok := ev.Payload[pkField]
		if !ok || v == nil {
			return "", protocol.RejectMissingPrimaryKey,
				fmt.Sprintf("%s event missing primary key field %q", ev.Op, pkField)
		}
	}
	return schema.PrimaryKeyValue(ev.Payload), "", ""
}

func (p *RecordProcessor) applyDelete(ev protocol.ChangeEvent, schema *protocol.Schema,
	pk string, d storage.SeqDisposition) ProcessResult {
	dup, _, applyErr := p.target.Apply(ev, pk, nil)
	if applyErr != nil {
		return p.reject(ev, protocol.RejectIncompatibleSchema, applyErr.Error())
	}
	action := "deleted"
	if dup || d.Dup {
		action = "duplicate-skip"
	}
	return p.writeChangelog(ev, pk, action)
}

func (p *RecordProcessor) writeChangelog(ev protocol.ChangeEvent, pk, action string) ProcessResult {
	_, _ = p.changelog.Append(storage.ChangelogEntry{
		Seq:       ev.Seq,
		Source:    ev.SourceName,
		Op:        string(ev.Op),
		PK:        pk,
		Action:    action,
		SchemaVer: ev.SchemaVer,
		AppliedAt: p.now(),
	})
	return ProcessResult{Event: ev, OK: true, Action: action}
}
