package ingest

import "sort"

// Disposition 是对账结论中每条记录的最终处置状态。
type Disposition string

const (
	// DispApplied：最终在目标端生效（insert/update 成行为一行，或 delete 已生效）。
	DispApplied Disposition = "applied"
	// DispRejected：被拒绝（原因见 Reasons；含主键冲突 pk_conflict）。
	DispRejected Disposition = "rejected"
	// DispSkippedStale：来源顺序上确定过期，被幂等跳过（同主键已有更新 seq 生效）。
	DispSkippedStale Disposition = "skipped_stale"
	// DispPending：来源位点尚未推进到该记录，尚未处理。
	DispPending Disposition = "pending"
)

// RecordDisposition 描述一条来源记录的最终对账结论。
type RecordDisposition struct {
	Seq     uint64      `json:"seq"`
	EventID string      `json:"eventId"`
	PK      string      `json:"pk"`
	Op      Op          `json:"op"`
	Status  Disposition `json:"status"`
	// Reasons 是该事件尝试过的全部拒绝原因流水（按死信文件顺序，去重由 Sink 保证）。
	// 即使最终通过修复生效，历史原因也保留以便解释“它是怎么到这一步的”。
	Reasons []string `json:"reasons,omitempty"`
	// Repaired 表示该记录曾通过修复队列重新投递。
	Repaired bool `json:"repaired,omitempty"`
}

// OrphanRepair 表示出现在修复队列、但在来源流水里找不到同 ID 事件的记录。
type OrphanRepair struct {
	EventID string `json:"eventId"`
	Seq     uint64 `json:"seq"`
	PK      string `json:"pk"`
	Applied bool   `json:"applied"`
}

// ReconcileReport 是一次对账的完整结论。
// 只依赖磁盘与本地状态，不含时间/随机因素：重复运行、并发消费、进程重启后
// 对同样输入产出逐字节一致的结论。
type ReconcileReport struct {
	CommittedOffset uint64              `json:"committedOffset"`
	Total           int                 `json:"total"` // 来源流水总条数
	Counts          map[Disposition]int `json:"counts"`
	ReasonCounts    map[string]int      `json:"reasonCounts"` // 仅统计“最终仍被拒”记录的原因，重复修复不会重复计数
	Records         []RecordDisposition `json:"records"`      // 按来源出现顺序
	OrphanRepairs   []OrphanRepair      `json:"orphanRepairs,omitempty"`
	SchemaChanges   []SchemaChange      `json:"schemaChanges,omitempty"` // 全部结构变更尝试（含不兼容）
}

// Reconciler 做“来源流水 × 落地状态 × 处置结果”三方对齐。
type Reconciler struct {
	source   *Source
	sink     *Sink
	offsets  *OffsetStore
	registry *SchemaRegistry
	repair   *RepairQueue
}

// NewReconciler 创建对账器。
func NewReconciler(src *Source, sink *Sink, offsets *OffsetStore, reg *SchemaRegistry) *Reconciler {
	return &Reconciler{source: src, sink: sink, offsets: offsets, registry: reg}
}

// WithRepairQueue 让对账同时对齐修复队列（标记修复记录、识别来源外的孤儿修复）。
func (r *Reconciler) WithRepairQueue(q *RepairQueue) *Reconciler {
	r.repair = q
	return r
}

// Reconcile 生成一次可重复执行的确定性对账报告。
func (r *Reconciler) Reconcile() (ReconcileReport, error) {
	// 全量读取来源（limit<=0），而非只读到位点：要为每一条流水给出结论。
	events, err := r.source.ReadFrom(0, 0)
	if err != nil {
		return ReconcileReport{}, err
	}
	committed := r.offsets.Committed()

	// 处置结果：死信按事件 ID 汇总（同一 (id,reason) 在 Sink 层已去重）。
	reasonByID := map[string][]string{}
	for _, rej := range r.sink.Rejections() {
		reasonByID[rej.EventID] = append(reasonByID[rej.EventID], string(rej.Reason))
	}

	// 修复队列：识别哪些记录是修好后重投的。
	repairedIDs := map[string]struct{}{}
	var repairEvents []Event
	if r.repair != nil {
		repairEvents, err = r.repair.Read()
		if err != nil {
			return ReconcileReport{}, err
		}
		for _, e := range repairEvents {
			repairedIDs[e.ID] = struct{}{}
		}
	}

	rep := ReconcileReport{
		CommittedOffset: committed,
		Total:           len(events),
		Counts:          map[Disposition]int{},
		ReasonCounts:    map[string]int{},
		Records:         make([]RecordDisposition, 0, len(events)),
	}

	// 位点是“已消费的来源行数”而非 seq 值：来源按 seq 升序追加，
	// 因此前 committed 条事件已处理，其余尚未处理（pending）。
	for idx, e := range events {
		rd := RecordDisposition{
			Seq: e.Seq, EventID: e.ID, PK: e.PK, Op: e.Op,
			Reasons: append([]string(nil), reasonByID[e.ID]...),
		}
		if _, ok := repairedIDs[e.ID]; ok {
			rd.Repaired = true
		}

		switch {
		case uint64(idx) >= committed:
			// 位点之后：尚未消费。
			rd.Status = DispPending
		case r.sink.Applied(e.ID):
			// 最终生效（正常消费或修复重投后生效）。
			rd.Status = DispApplied
		case len(rd.Reasons) > 0:
			// 有拒绝记录且最终未生效：被拒。
			rd.Status = DispRejected
			for _, why := range rd.Reasons {
				rep.ReasonCounts[why]++
			}
		default:
			// 已消费、未生效、也无拒绝：来源顺序上过期被幂等跳过。
			if last, ok := r.sink.LastSeq(e.PK); ok && e.Seq <= last {
				rd.Status = DispSkippedStale
				rd.Reasons = []string{"superseded by newer seq for pk " + e.PK}
			} else {
				// 理论上不可达（已消费记录必然三者居其一），保守归为 pending。
				rd.Status = DispPending
			}
		}
		rep.Counts[rd.Status]++
		rep.Records = append(rep.Records, rd)
	}

	// 孤儿修复：修复队列里有、来源流水里没有同 ID 事件。
	if r.repair != nil {
		sourceIDs := map[string]struct{}{}
		for _, e := range events {
			sourceIDs[e.ID] = struct{}{}
		}
		for _, e := range repairEvents {
			if _, ok := sourceIDs[e.ID]; ok {
				continue
			}
			rep.OrphanRepairs = append(rep.OrphanRepairs, OrphanRepair{
				EventID: e.ID, Seq: e.Seq, PK: e.PK, Applied: r.sink.Applied(e.ID),
			})
		}
		sort.Slice(rep.OrphanRepairs, func(i, j int) bool {
			return rep.OrphanRepairs[i].EventID < rep.OrphanRepairs[j].EventID
		})
	}

	// 结构变更流水（含被判定不兼容的尝试），用于解释结构不兼容结论。
	if r.registry != nil {
		rep.SchemaChanges = r.registry.History()
	}
	return rep, nil
}
