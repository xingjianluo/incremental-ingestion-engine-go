package ingest

import (
	"fmt"
	"sort"
)

// Reconcile 对齐来源流水、目标端最终状态与各类处置结果，
// 给出来源中每条记录（按事件 ID 去重）的最终结论：
// 生效 / 被跳过 / 被拒（含可区分原因）/ 尚未消费。
//
// 本方法是持久化状态上的纯计算：不修改任何状态，重复运行、并发消费、
// 进程重启之后，同样的输入必然得到一致且可复现的报告；
// 同一事件 ID 在来源或重投中出现多次只计一次，重复投递不会重复计数。
func (p *Pipeline) Reconcile() (*ReconcileReport, error) {
	events, err := p.source.ReadFrom(0, 0)
	if err != nil {
		return nil, err
	}
	committed := p.offsets.Committed()
	applied := p.sink.AppliedIDs()
	lastSeqByPK := p.sink.LastSeqByPK()
	rejections := p.sink.Rejections()

	// 同一事件的拒绝记录按发生顺序取最后一条（最终结论）。
	rejectedByID := map[string]Rejection{}
	for _, r := range rejections {
		rejectedByID[r.EventID] = r
	}

	rep := &ReconcileReport{Counts: map[Disposition]int{}}
	seen := map[string]struct{}{}
	for _, e := range events {
		if _, dup := seen[e.ID]; dup {
			continue // 来源中重复投递的同一事件只计一次
		}
		seen[e.ID] = struct{}{}
		rep.Entries = append(rep.Entries, reconcileOne(e, committed, applied, lastSeqByPK, rejectedByID))
	}

	// 不在来源流水中的处置记录（例如修复重投的事件）。
	for id := range applied {
		if _, ok := seen[id]; !ok {
			rep.External = append(rep.External, ReconcileEntry{
				EventID: id, Disposition: DispApplied, Detail: "not in source stream (resubmitted)",
			})
		}
	}
	for id, r := range rejectedByID {
		if _, ok := seen[id]; !ok {
			if _, ok := applied[id]; ok {
				continue // 已生效，最终结论以 applied 为准（上面已记）
			}
			rep.External = append(rep.External, ReconcileEntry{
				EventID: id, Seq: r.Seq, Disposition: DispRejected,
				Reason: r.Reason, Detail: r.Detail + " (not in source stream)",
			})
		}
	}

	sort.Slice(rep.Entries, func(i, j int) bool {
		if rep.Entries[i].Seq != rep.Entries[j].Seq {
			return rep.Entries[i].Seq < rep.Entries[j].Seq
		}
		return rep.Entries[i].EventID < rep.Entries[j].EventID
	})
	sort.Slice(rep.External, func(i, j int) bool { return rep.External[i].EventID < rep.External[j].EventID })
	for _, e := range rep.Entries {
		rep.Counts[e.Disposition]++
	}
	for _, e := range rep.External {
		rep.Counts[e.Disposition]++
	}

	p.logf("reconcile: committed=%d applied=%d skipped=%d rejected=%d pending=%d external=%d",
		committed, rep.Counts[DispApplied], rep.Counts[DispSkipped],
		rep.Counts[DispRejected], rep.Counts[DispPending], len(rep.External))
	return rep, nil
}

// reconcileOne 判定来源中一条记录的最终处置结论；最终状态优先：
// 曾被拒但之后修复重投并生效的记录，结论为 applied。
func reconcileOne(e Event, committed uint64, applied map[string]struct{},
	lastSeqByPK map[string]uint64, rejectedByID map[string]Rejection) ReconcileEntry {
	entry := ReconcileEntry{EventID: e.ID, Seq: e.Seq, PK: e.PK}
	if _, ok := applied[e.ID]; ok {
		entry.Disposition = DispApplied
		return entry
	}
	if r, ok := rejectedByID[e.ID]; ok {
		entry.Disposition = DispRejected
		entry.Reason = r.Reason
		entry.Detail = r.Detail
		return entry
	}
	if e.Seq <= committed {
		entry.Disposition = DispSkipped
		if last, ok := lastSeqByPK[e.PK]; ok && last >= e.Seq {
			entry.Detail = fmt.Sprintf("superseded: pk %q already applied at seq %d", e.PK, last)
		} else {
			entry.Detail = "consumed but not applied"
		}
		return entry
	}
	entry.Disposition = DispPending
	entry.Detail = fmt.Sprintf("not yet consumed (committed offset %d)", committed)
	return entry
}
