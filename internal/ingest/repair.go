package ingest

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
)

// RepairQueue 以本地 JSONL 文件承接“被上游修好后重新投递”的记录。
// 修复记录就是普通 Event（沿用同一套校验、投影、落地与拒绝原因），
// 但不经过来源位点推进——位点只反映来源流水的消费进度，修复重投不影响它。
type RepairQueue struct {
	path string
}

// NewRepairQueue 创建修复重投队列（延迟建文件）。
func NewRepairQueue(path string) *RepairQueue { return &RepairQueue{path: path} }

// Path 返回队列文件路径（对账时用来识别哪些记录经过修复）。
func (q *RepairQueue) Path() string { return q.path }

// Read 读取队列中的全部修复记录（重复处理时每次都读全量，靠幂等去重）。
func (q *RepairQueue) Read() ([]Event, error) {
	f, err := os.Open(q.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// Append 追加一条或多条修复记录（模拟上游修好后重新提交）。
func (q *RepairQueue) Append(events ...Event) error {
	if err := os.MkdirAll(filepath.Dir(q.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(q.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.Flush()
}

// RepairResult 汇总一次修复重投处理的结果（重复执行时各项确定、不重复计数）。
type RepairResult struct {
	Read     int // 队列中读到的修复记录条数（去重前）
	Applied  int // 本次实际生效条数
	Skipped  int // 事件 ID 已生效（重复重投）
	Stale    int // seq 旧于该主键已生效 seq（确定性过期）
	Rejected int // 被拒绝条数（校验不过 / 投影不兼容 / 主键冲突）
}

// ProcessRepairs 处理修复队列中的全部记录：
// 走与正常消费完全相同的校验、投影与落地流程，但绝不推进来源位点；
// 同一批修正数据重复提交不会产生重复数据，也不会重复堆积拒绝记录；
// 主键冲突以 pk_conflict 单独成类拒绝，不改写已有数据、不静默略过。
func (p *Pipeline) ProcessRepairs(q *RepairQueue) (RepairResult, error) {
	batch, err := q.Read()
	if err != nil {
		return RepairResult{}, err
	}
	res := RepairResult{Read: len(batch)}
	if len(batch) == 0 {
		return res, nil
	}
	p.logf("repairs: read size=%d", len(batch))

	// 与 RunOnce 相同的校验与投影。
	var valid []Event
	var rejections []Rejection
	for _, e := range batch {
		if rej := p.validate(e); rej != nil {
			rejections = append(rejections, *rej)
			p.logf("repair reject: seq=%d id=%s reason=%s detail=%s", e.Seq, e.ID, rej.Reason, rej.Detail)
			continue
		}
		projected, err := p.registry.Project(e)
		if err != nil {
			rejections = append(rejections, Rejection{
				EventID: e.ID, Seq: e.Seq,
				Reason: RejectIncompatSchema,
				Detail: err.Error(),
			})
			p.logf("repair reject: seq=%d id=%s reason=%s detail=%s", e.Seq, e.ID, RejectIncompatSchema, err)
			continue
		}
		e.Fields = projected
		valid = append(valid, e)
	}
	res.Rejected += len(rejections)

	// 冲突感知的确定性落地（主键冲突在这里产生 pk_conflict）。
	br, err := p.sink.ApplyRepairs(valid)
	if err != nil {
		return RepairResult{}, err
	}
	res.Applied += len(br.Applied)
	res.Skipped += len(br.Skipped)
	res.Stale += len(br.Stale)
	for _, id := range br.Applied {
		p.logf("repair applied: id=%s", id)
	}
	for _, id := range br.Stale {
		p.logf("repair stale: id=%s", id)
	}

	// 主键冲突同样进入死信：AddRejections 按 (id, reason) 去重，
	// 重复处理不会重复堆积。
	rejections = append(rejections, br.Conflicts...)
	res.Rejected += len(br.Conflicts)
	for _, r := range br.Conflicts {
		p.logf("repair reject: seq=%d id=%s reason=%s detail=%s", r.Seq, r.EventID, r.Reason, r.Detail)
	}
	if err := p.sink.AddRejections(rejections); err != nil {
		return RepairResult{}, err
	}
	p.logf("repairs done: applied=%d skipped=%d stale=%d rejected=%d (offset untouched=%d)",
		res.Applied, res.Skipped, res.Stale, res.Rejected, p.offsets.Committed())
	return res, nil
}
