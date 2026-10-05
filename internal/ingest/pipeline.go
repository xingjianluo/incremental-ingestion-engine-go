package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Pipeline 串联 来源 -> 校验 -> 投影 -> 落地 -> 位点提交。
type Pipeline struct {
	source   *Source
	sink     *Sink
	offsets  *OffsetStore
	registry *SchemaRegistry

	// resubmitMu 串行化修复重投，保证并发重投的结果确定且可复现。
	resubmitMu sync.Mutex

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

// Resubmit 把上游修复后的记录重新投递处理：走与正常消费完全相同的
// 校验、投影与落地流程，但不读取来源流水、也不推进已提交位点。
//
// 保证：
//   - 同一批修正数据重复提交：幂等，不产生重复数据、不重复堆拒绝记录、位点不动；
//   - 修正记录撞上目标端已存在主键且无法凭来源顺序安全判定先后时，
//     以 RejectPKConflict 拒绝，绝不改写已有数据，也不静默略过；
//   - 同一批内同一主键出现多次时，按 (Seq, EventID) 取最大者生效，
//     其余以 RejectPKConflict 拒绝——与输入顺序、并发调用无关，结果确定可复现。
func (p *Pipeline) Resubmit(events []Event) (*ResubmitOutcome, error) {
	p.resubmitMu.Lock()
	defer p.resubmitMu.Unlock()

	outcome := &ResubmitOutcome{}
	var valid []Event
	for _, e := range events {
		// 归一化为 JSON 线格式语义，与正常消费路径（来源文件 JSONL 解码）保持一致。
		ne, err := normalizeEvent(e)
		if err != nil {
			outcome.Rejected = append(outcome.Rejected, Rejection{
				EventID: e.ID, Seq: e.Seq, Reason: RejectTypeMismatch,
				Detail: fmt.Sprintf("event not JSON-normalizable: %v", err),
			})
			continue
		}
		e = ne
		if rej := p.validate(e); rej != nil {
			outcome.Rejected = append(outcome.Rejected, *rej)
			p.logf("resubmit reject: id=%s seq=%d reason=%s detail=%s", e.ID, e.Seq, rej.Reason, rej.Detail)
			continue
		}
		projected, err := p.registry.Project(e)
		if err != nil {
			outcome.Rejected = append(outcome.Rejected, Rejection{
				EventID: e.ID, Seq: e.Seq, Reason: RejectIncompatSchema, Detail: err.Error(),
			})
			p.logf("resubmit reject: id=%s seq=%d reason=%s detail=%s", e.ID, e.Seq, RejectIncompatSchema, err)
			continue
		}
		e.Fields = projected
		valid = append(valid, e)
	}

	// 批内裁决：同一主键按 (Seq, EventID) 降序取唯一候选，其余确定性地拒绝。
	candidates := make([]Event, len(valid))
	copy(candidates, valid)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].PK != candidates[j].PK {
			return candidates[i].PK < candidates[j].PK
		}
		if candidates[i].Seq != candidates[j].Seq {
			return candidates[i].Seq > candidates[j].Seq
		}
		return candidates[i].ID > candidates[j].ID
	})
	var winners []Event
	for i, c := range candidates {
		if i > 0 && c.PK == candidates[i-1].PK {
			w := candidates[i-1]
			outcome.Rejected = append(outcome.Rejected, Rejection{
				EventID: c.ID, Seq: c.Seq,
				Reason: RejectPKConflict,
				Detail: fmt.Sprintf("pk %q appears multiple times in resubmit batch; superseded by event %s (seq %d)",
					c.PK, w.ID, w.Seq),
			})
			p.logf("resubmit reject: id=%s seq=%d reason=%s superseded by id=%s in same batch",
				c.ID, c.Seq, RejectPKConflict, w.ID)
			continue
		}
		winners = append(winners, c)
	}

	applied, skipped, conflicts, err := p.sink.ApplyResubmit(winners)
	if err != nil {
		return nil, err
	}
	outcome.Applied = applied
	outcome.Skipped = skipped
	outcome.Rejected = append(outcome.Rejected, conflicts...)
	for _, id := range applied {
		p.logf("resubmit applied: id=%s", id)
	}
	for _, id := range skipped {
		p.logf("resubmit skipped (already applied): id=%s", id)
	}
	for _, c := range conflicts {
		p.logf("resubmit reject: id=%s seq=%d reason=%s detail=%s", c.EventID, c.Seq, c.Reason, c.Detail)
	}
	if err := p.sink.AddRejections(outcome.Rejected); err != nil {
		return nil, err
	}
	p.logf("resubmit done: applied=%d skipped=%d rejected=%d committed offset unchanged=%d",
		len(applied), len(skipped), len(outcome.Rejected), p.offsets.Committed())
	return outcome, nil
}

// normalizeEvent 把事件经 JSON 往返归一化，使直接构造的事件与
// 从来源文件解码的事件具有相同的类型语义（如数字一律为 float64）。
func normalizeEvent(e Event) (Event, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return Event{}, err
	}
	var out Event
	if err := json.Unmarshal(b, &out); err != nil {
		return Event{}, err
	}
	return out, nil
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
