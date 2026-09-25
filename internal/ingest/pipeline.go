package ingest

import (
	"errors"
	"fmt"
)

// Pipeline 串联 来源 -> 校验 -> 投影 -> 落地 -> 位点提交。
type Pipeline struct {
	source   *Source
	sink     *Sink
	offsets  *OffsetStore
	registry *SchemaRegistry

	// Logf 可选日志钩子，测试用它打印位点、提交顺序与变更流水。
	Logf func(format string, args ...any)
}

// NewPipeline 创建摄入流水线。
func NewPipeline(src *Source, sink *Sink, offsets *OffsetStore, reg *SchemaRegistry) *Pipeline {
	return &Pipeline{source: src, sink: sink, offsets: offsets, registry: reg}
}

func (p *Pipeline) logf(format string, args ...any) {
	if p.Logf != nil {
		p.Logf(format, args...)
	}
}

// RunOnce 消费一批数据并提交位点。
// 返回本批消费的事件数（含被拒绝进入死信的记录）。
// 并发调用时，未赢得 CAS 的调用返回 ErrOffsetGap，调用方应重试；
// 已提交位点只会单调推进。
func (p *Pipeline) RunOnce(batchSize int) (int, error) {
	base := p.offsets.Committed()
	batch, err := p.source.ReadFrom(base, batchSize)
	if err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}
	p.logf("read batch: offset=%d size=%d", base, len(batch))

	var valid []Event
	var rejections []Rejection
	for _, e := range batch {
		if rej := p.validate(e); rej != nil {
			rejections = append(rejections, *rej)
			p.logf("reject: seq=%d id=%s reason=%s detail=%s", e.Seq, e.ID, rej.Reason, rej.Detail)
			continue
		}
		projected, err := p.registry.Project(e)
		if err != nil {
			rejections = append(rejections, Rejection{
				EventID: e.ID, Seq: e.Seq,
				Reason: RejectIncompatSchema,
				Detail: err.Error(),
			})
			p.logf("reject: seq=%d id=%s reason=%s detail=%s", e.Seq, e.ID, RejectIncompatSchema, err)
			continue
		}
		e.Fields = projected
		valid = append(valid, e)
	}

	applied, err := p.sink.Apply(valid)
	if err != nil {
		return 0, err
	}
	if err := p.sink.AddRejections(rejections); err != nil {
		return 0, err
	}
	p.logf("apply: offset=%d applied=%d rejected=%d", base, applied, len(rejections))

	next := base + uint64(len(batch))
	if err := p.offsets.Commit(base, next); err != nil {
		return 0, fmt.Errorf("commit offset %d -> %d: %w", base, next, err)
	}
	p.logf("commit: offset %d -> %d", base, next)
	return len(batch), nil
}

// validate 返回 nil 表示通过，否则返回可区分原因的拒绝记录。
func (p *Pipeline) validate(e Event) *Rejection {
	if e.PK == "" {
		return &Rejection{EventID: e.ID, Seq: e.Seq, Reason: RejectMissingPK, Detail: "primary key is empty"}
	}
	switch e.Op {
	case OpInsert, OpUpdate:
		return p.registry.Validate(e)
	case OpDelete:
		// 删除只需主键，不校验字段载荷。
		return nil
	default:
		return &Rejection{EventID: e.ID, Seq: e.Seq, Reason: RejectUnknownOp, Detail: fmt.Sprintf("unknown op %q", e.Op)}
	}
}

// Run 循环消费直到来源读尽；并发安全，多个消费者可同时调用。
// 位点竞争失败（ErrOffsetGap）时自动以最新已提交位点重试。
func (p *Pipeline) Run(batchSize int) error {
	for {
		n, err := p.RunOnce(batchSize)
		if err != nil {
			if errors.Is(err, ErrOffsetGap) {
				p.logf("commit race lost at offset=%d, retrying", p.offsets.Committed())
				continue
			}
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// UpgradeSchema 注册新版本结构；若演进兼容，则把新增字段的缺省值
// 回填到目标端存量行，保证缺省语义对历史数据同样确定且稳定。
func (p *Pipeline) UpgradeSchema(s Schema) error {
	prev, _ := p.registry.Latest()
	if err := p.registry.Register(s); err != nil {
		return err
	}
	var added []Field
	for _, f := range s.Fields {
		if _, ok := prev.field(f.Name); !ok && f.HasDefault {
			added = append(added, f)
		}
	}
	if err := p.sink.BackfillDefaults(added); err != nil {
		return err
	}
	p.logf("schema upgraded to v%d, backfilled %d field(s)", s.Version, len(added))
	return nil
}
