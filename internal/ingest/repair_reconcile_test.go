package ingest

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestRepairResubmit 被拒记录修好后重投：走同样的校验/投影/落地并最终生效；
// 同一批修正重复提交不产生重复数据、不重复堆积拒绝记录，且位点纹丝不动。
func TestRepairResubmit(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}), // 缺 age，被拒
		ev(3, "e3", OpInsert, "c", 1, map[string]any{"age": "x"}),    // name 缺失 + age 类型错，被拒
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Rejections()); got != 2 {
		t.Fatalf("initial rejections=%d want 2", got)
	}

	// 上游修好后通过修复队列重新提交（沿用原事件 ID）。
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	if err := rq.Append(
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob", "age": 25}),
		ev(3, "e3", OpInsert, "c", 1, map[string]any{"name": "cat", "age": 22}),
	); err != nil {
		t.Fatal(err)
	}
	res, err := p.ProcessRepairs(rq)
	if err != nil {
		t.Fatalf("process repairs: %v", err)
	}
	t.Logf("repair result: %+v", res)
	if res.Applied != 2 || res.Rejected != 0 || res.Skipped != 0 {
		t.Fatalf("repair result=%+v want applied=2", res)
	}
	rows := rowsMap(sink.Rows())
	if rows["b"]["age"] != float64(25) || rows["c"]["name"] != "cat" {
		t.Fatalf("repaired rows wrong: %v", rows)
	}
	// 位点不得受修复重投影响。
	if got := offsets.Committed(); got != 3 {
		t.Fatalf("offset after repair=%d want 3", got)
	}

	// 重复处理同一批修复：幂等，不重复生效、不重复堆拒绝记录。
	res2, err := p.ProcessRepairs(rq)
	if err != nil {
		t.Fatalf("reprocess repairs: %v", err)
	}
	t.Logf("repeated repair result: %+v", res2)
	if res2.Applied != 0 || res2.Skipped != 2 {
		t.Fatalf("repeated repair result=%+v want skipped=2", res2)
	}
	if got := len(sink.Rows()); got != 3 {
		t.Fatalf("rows after repeated repair=%d want 3", got)
	}
	if got := len(sink.Rejections()); got != 2 {
		t.Fatalf("rejections after repeated repair=%d want 2 (must not pile up)", got)
	}
}

// TestRepairStillInvalid 修复数据仍然不合格：按可区分原因继续拒绝，
// 拒绝记录按 (id,reason) 去重，位点不动。
func TestRepairStillInvalid(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "bob"}), // 缺 age
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	// 还是缺 age。
	if err := rq.Append(ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "bob"})); err != nil {
		t.Fatal(err)
	}
	res, err := p.ProcessRepairs(rq)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || res.Rejected != 1 {
		t.Fatalf("repair result=%+v want rejected=1", res)
	}
	// 正常消费与修复重投各拒绝一次，但 (id,reason) 相同 -> 死信只保留一条。
	if got := len(sink.Rejections()); got != 1 {
		t.Fatalf("rejections=%d want 1 (dedup by id,reason)", got)
	}
	if got := offsets.Committed(); got != 1 {
		t.Fatalf("offset=%d want 1", got)
	}
}

var _ = sync.Once{}
var _ = fmt.Sprintf

// findReject 在拒绝列表中查找指定 (id,reason)。
func findReject(rs []Rejection, id string, reason RejectReason) *Rejection {
	for i := range rs {
		if rs[i].EventID == id && rs[i].Reason == reason {
			return &rs[i]
		}
	}
	return nil
}

// TestRepairPKConflictSameBatch 同一批修复里同一主键出现多次：
// 谁生效、谁被拒必须确定可复现（seq 最大者生效，seq 相同 ID 字典序最小者生效），
// 被拒原因是独立的 pk_conflict，绝不改写、不静默略过。
func TestRepairPKConflictSameBatch(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	// 来源里 x 最初因坏数据被拒。
	if err := src.Append(
		ev(1, "bad-x", OpInsert, "x", 1, map[string]any{"name": "broken"}),
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	// 修复队列：同一主键 x 出现 3 条不同 seq/ID。
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	if err := rq.Append(
		ev(10, "fix-x-1", OpInsert, "x", 1, map[string]any{"name": "x10", "age": 10}),
		ev(12, "fix-x-3", OpInsert, "x", 1, map[string]any{"name": "x12", "age": 12}), // seq 最大，应生效
		ev(11, "fix-x-2", OpInsert, "x", 1, map[string]any{"name": "x11", "age": 11}),
	); err != nil {
		t.Fatal(err)
	}
	res, err := p.ProcessRepairs(rq)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("repair result: %+v", res)
	if res.Applied != 1 || res.Rejected != 2 {
		t.Fatalf("result=%+v want exactly 1 applied and 2 pk_conflict", res)
	}
	rows := rowsMap(sink.Rows())
	if rows["x"]["name"] != "x12" {
		t.Fatalf("winner must be seq=12 (x12), got %v", rows["x"])
	}
	rs := sink.Rejections()
	for _, id := range []string{"fix-x-1", "fix-x-2"} {
		r := findReject(rs, id, RejectPKConflict)
		if r == nil {
			t.Fatalf("%s must be rejected as pk_conflict, got %+v", id, rs)
		}
		t.Logf("pk_conflict: id=%s detail=%s", id, r.Detail)
	}

	// seq 相同、ID 不同：ID 字典序最小者生效，另一条 pk_conflict。
	rq2 := NewRepairQueue(t.TempDir() + "/repairs2.jsonl")
	if err := rq2.Append(
		ev(5, "fix-y-z", OpInsert, "y", 1, map[string]any{"name": "yz", "age": 1}),
		ev(5, "fix-y-a", OpInsert, "y", 1, map[string]any{"name": "ya", "age": 2}), // ID 小，生效
	); err != nil {
		t.Fatal(err)
	}
	res2, err := p.ProcessRepairs(rq2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Applied != 1 {
		t.Fatalf("result=%+v want 1 applied", res2)
	}
	if rowsMap(sink.Rows())["y"]["name"] != "ya" {
		t.Fatalf("tie must be broken by smallest id (ya), got %v", rowsMap(sink.Rows())["y"])
	}
	if findReject(sink.Rejections(), "fix-y-z", RejectPKConflict) == nil {
		t.Fatalf("fix-y-z must be pk_conflict")
	}
}

// TestRepairPKConflictWithExisting 修复记录撞上目标端已存在的主键：
// seq 更新 -> 安全生效；seq 相同（无法凭来源顺序判定先后）-> pk_conflict，
// 已有数据绝不被改写。
func TestRepairPKConflictWithExisting(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	// 正常流水里 a 在 seq=5 更新为 alice-newer。
	if err := src.Append(
		ev(1, "a1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(5, "a5", OpUpdate, "a", 1, map[string]any{"name": "alice-newer", "age": 31}),
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	// 修复 seq=5 撞上已生效 seq=5 且是另一条事件 -> pk_conflict，数据不改写。
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	if err := rq.Append(
		ev(5, "fix-a-collide", OpUpdate, "a", 1, map[string]any{"name": "SHOULD-NOT-WIN", "age": 99}),
	); err != nil {
		t.Fatal(err)
	}
	res, err := p.ProcessRepairs(rq)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || res.Rejected != 1 {
		t.Fatalf("result=%+v want 1 pk_conflict", res)
	}
	row := rowsMap(sink.Rows())["a"]
	if row["name"] != "alice-newer" || row["age"] != float64(31) {
		t.Fatalf("existing row must not be overwritten, got %v", row)
	}
	if findReject(sink.Rejections(), "fix-a-collide", RejectPKConflict) == nil {
		t.Fatalf("collision must be recorded as pk_conflict (not skipped, not silent)")
	}

	// seq 更新的修复可以安全生效。
	rq2 := NewRepairQueue(t.TempDir() + "/repairs2.jsonl")
	if err := rq2.Append(
		ev(9, "fix-a-newer", OpUpdate, "a", 1, map[string]any{"name": "alice-fixed", "age": 32}),
	); err != nil {
		t.Fatal(err)
	}
	res2, err := p.ProcessRepairs(rq2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Applied != 1 {
		t.Fatalf("result=%+v want 1 applied (newer seq is safe)", res2)
	}
	if rowsMap(sink.Rows())["a"]["name"] != "alice-fixed" {
		t.Fatalf("newer repair should win, got %v", rowsMap(sink.Rows())["a"])
	}
}

// runRepairSerial 在一套全新磁盘环境上串行跑给定修复批次，返回最终行状态与冲突明细。
func runRepairSerial(t *testing.T, repairs []Event) (map[string]map[string]any, []Rejection) {
	t.Helper()
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	if err := rq.Append(repairs...); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}
	return rowsMap(sink.Rows()), sink.Rejections()
}

// TestRepairConcurrentDeterministic 并发处理同一批修复：
// 最终行、pk_conflict 集合必须与串行结果一致；再重复跑也保持一致。
func TestRepairConcurrentDeterministic(t *testing.T) {
	repairs := []Event{
		ev(3, "r-a-1", OpInsert, "p1", 1, map[string]any{"name": "a3", "age": 3}),
		ev(1, "r-a-0", OpInsert, "p1", 1, map[string]any{"name": "a1", "age": 1}),
		ev(2, "r-a-2", OpInsert, "p1", 1, map[string]any{"name": "a2", "age": 2}),
		ev(7, "r-b-1", OpInsert, "p2", 1, map[string]any{"name": "b7", "age": 7}),
		ev(7, "r-b-0", OpInsert, "p2", 1, map[string]any{"name": "b7tie", "age": 8}),
	}

	serialRows, serialRej := runRepairSerial(t, repairs)
	t.Logf("serial rows=%v", serialRows)
	var serialConflictIDs []string
	for _, r := range serialRej {
		if r.Reason == RejectPKConflict {
			serialConflictIDs = append(serialConflictIDs, r.EventID)
		}
	}
	t.Logf("serial pk_conflict=%v", serialConflictIDs)

	// 并发环境：多个 worker 同时处理同一队列。
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	if err := rq.Append(repairs...); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	p.Logf = func(f string, a ...any) {
		mu.Lock()
		t.Logf("[conc] "+f, a...)
		mu.Unlock()
	}
	const workers = 6
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.ProcessRepairs(rq); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	concRows := rowsMap(sink.Rows())
	if fmt.Sprint(concRows) != fmt.Sprint(serialRows) {
		t.Fatalf("concurrent rows=%v differ from serial=%v", concRows, serialRows)
	}
	var concConflictIDs []string
	for _, r := range sink.Rejections() {
		if r.Reason == RejectPKConflict {
			concConflictIDs = append(concConflictIDs, r.EventID)
		}
	}
	sortStrings(concConflictIDs)
	sortStrings(serialConflictIDs)
	if fmt.Sprint(concConflictIDs) != fmt.Sprint(serialConflictIDs) {
		t.Fatalf("concurrent conflicts=%v differ from serial=%v", concConflictIDs, serialConflictIDs)
	}
	// 并发重复处理后：生效行数稳定（p1、p2 各一行）。
	if len(concRows) != 2 {
		t.Fatalf("rows=%v want exactly 2", concRows)
	}
}

// sortStrings 避免再引一次 sort 包名冲突（测试文件辅助）。
func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

// normalizeReport 生成报告的稳定字符串形式（逐条记录按来源序，map 用 %v 比较）。
func normalizeReport(rep ReconcileReport) string {
	s := fmt.Sprintf("offset=%d total=%d counts=%v reasons=%v\n",
		rep.CommittedOffset, rep.Total, rep.Counts, rep.ReasonCounts)
	for _, r := range rep.Records {
		s += fmt.Sprintf("  seq=%d id=%s pk=%s status=%s repaired=%v reasons=%v\n",
			r.Seq, r.EventID, r.PK, r.Status, r.Repaired, r.Reasons)
	}
	for _, o := range rep.OrphanRepairs {
		s += fmt.Sprintf("  orphan id=%s applied=%v\n", o.EventID, o.Applied)
	}
	return s
}

// statusOf 在对账报告里按事件 ID 找结论。
func statusOf(rep ReconcileReport, id string) RecordDisposition {
	for _, r := range rep.Records {
		if r.EventID == id {
			return r
		}
	}
	return RecordDisposition{}
}

// TestReconcileDeterministic applied / skipped_stale / rejected / pending 一一对齐；
// 重复运行结论一致；修复让结论从 rejected 翻转为 applied，最终原因计数不重复；
// 位点之后的记录为 pending。
func TestReconcileDeterministic(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	// a 在 seq1 插入、seq2 更新；b 在 seq3 坏记录被拒；d 在 seq5 正常。
	if err := src.Append(
		ev(1, "a1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "a2", OpUpdate, "a", 1, map[string]any{"name": "alice2", "age": 31}),
		ev(3, "b1", OpInsert, "b", 1, map[string]any{"name": "bob"}),
		ev(5, "d1", OpInsert, "d", 1, map[string]any{"name": "dan", "age": 40}),
	); err != nil {
		t.Fatal(err)
	}
	// seq6 稍后再追加，用于制造 pending。
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(6, "f1", OpInsert, "f", 1, map[string]any{"name": "finn", "age": 60}),
	); err != nil {
		t.Fatal(err)
	}

	rq := NewRepairQueue(t.TempDir() + "/repairs.jsonl")
	rec := NewReconciler(src, sink, offsets, reg).WithRepairQueue(rq)
	rep1, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", normalizeReport(rep1))
	if normalizeReport(rep1) != normalizeReport(rep2) {
		t.Fatal("reconcile must be reproducible across repeated runs")
	}
	if rep1.CommittedOffset != 4 {
		t.Fatalf("offset=%d want 4 (5 source rows, but only 4 consumed)", rep1.CommittedOffset)
	}
	if rep1.Total != 5 {
		t.Fatalf("total=%d want 5", rep1.Total)
	}
	if statusOf(rep1, "a1").Status != DispApplied ||
		statusOf(rep1, "a2").Status != DispApplied ||
		statusOf(rep1, "d1").Status != DispApplied {
		t.Fatal("a1/a2/d1 must be applied")
	}
	if r := statusOf(rep1, "b1"); r.Status != DispRejected || r.Reasons[0] != string(RejectMissingField) {
		t.Fatalf("b1 rejected missing_field, got %+v", r)
	}
	if r := statusOf(rep1, "f1"); r.Status != DispPending {
		t.Fatalf("f1 beyond committed offset must be pending, got %+v", r)
	}
	if rep1.Counts[DispApplied] != 3 || rep1.Counts[DispRejected] != 1 || rep1.Counts[DispPending] != 1 {
		t.Fatalf("counts=%v want applied=3 rejected=1 pending=1", rep1.Counts)
	}
	if rep1.ReasonCounts[string(RejectMissingField)] != 1 {
		t.Fatalf("reasonCounts=%v want missing_field=1", rep1.ReasonCounts)
	}

	// 修复 b1，重复处理两次后重跑对账：b1 翻转为 applied+repaired，
	// 历史原因保留，但最终原因计数归零，且不重复。
	if err := rq.Append(ev(3, "b1", OpInsert, "b", 1, map[string]any{"name": "bob", "age": 25})); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}
	rep3, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("post-repair:\n%s", normalizeReport(rep3))
	b := statusOf(rep3, "b1")
	if b.Status != DispApplied || !b.Repaired {
		t.Fatalf("b1 must flip to applied+repaired, got %+v", b)
	}
	if len(b.Reasons) != 1 || b.Reasons[0] != string(RejectMissingField) {
		t.Fatalf("history reason must remain for explainability, got %+v", b)
	}
	if rep3.Counts[DispApplied] != 4 || rep3.Counts[DispRejected] != 0 {
		t.Fatalf("counts=%v want applied=4 rejected=0", rep3.Counts)
	}
	if rep3.ReasonCounts[string(RejectMissingField)] != 0 {
		t.Fatalf("final reason count must not count repaired records, got %v", rep3.ReasonCounts)
	}

	// 主键冲突的修复最终保持 rejected，且原因是可区分的 pk_conflict。
	if err := rq.Append(ev(3, "b1-again", OpInsert, "b", 1, map[string]any{"name": "x", "age": 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}
	rep4, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	// b1-again 是孤儿修复（来源流水里没有），应出现在 OrphanRepairs 且未生效。
	var orphan *OrphanRepair
	for i := range rep4.OrphanRepairs {
		if rep4.OrphanRepairs[i].EventID == "b1-again" {
			orphan = &rep4.OrphanRepairs[i]
		}
	}
	if orphan == nil || orphan.Applied {
		t.Fatalf("conflicting repair should be an unapplied orphan, got %+v", rep4.OrphanRepairs)
	}
	t.Logf("orphan conflict repair: %+v reasons=%v", orphan, statusOf(rep4, "b1").Reasons)

	// 进程重启：全部组件从磁盘重新 Load，再对账，结论必须完全一致。
	sink2 := NewSink(sink.dataPath, sink.rejectPath)
	if err := sink2.Load(); err != nil {
		t.Fatal(err)
	}
	off2 := NewOffsetStore(offsets.path)
	if _, err := off2.Load(); err != nil {
		t.Fatal(err)
	}
	reg2, err := NewSchemaRegistryWithHistory(reg.historyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg2.Latest(); !ok {
		// 历史注册表不记录“内存注册”，原 reg 无文件；此处用回 reg 即可。
		reg2 = reg
	}
	rec5 := NewReconciler(src, sink2, off2, reg2).WithRepairQueue(rq)
	rep5, err := rec5.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	if normalizeReport(rep5) != normalizeReport(rep4) {
		t.Fatalf("reconcile after restart must be identical:\n%s\nvs\n%s",
			normalizeReport(rep4), normalizeReport(rep5))
	}
}

// TestReconcileSkippedStale 已消费但未生效也无拒绝记录的事件，对账为 skipped_stale：
// 用“位点重置 + 同一 sink 重放旧 seq”模拟，必须能说清它为何被跳过。
func TestReconcileSkippedStale(t *testing.T) {
	// 先用一套环境让 a 生效到 seq=5。
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(5, "a5", OpInsert, "a", 1, map[string]any{"name": "new", "age": 5}),
	); err != nil {
		t.Fatal(err)
	}
	if err := NewPipeline(src, sink, offsets, reg).Run(10); err != nil {
		t.Fatal(err)
	}
	// 新来源（新位点从 0 开始）里只有一条 seq=1 的旧事件，重放到同一 sink：
	// 它被幂等跳过，不产生拒绝记录。
	dir := t.TempDir()
	src2 := NewSource(dir + "/source.jsonl")
	off2 := NewOffsetStore(dir + "/offset")
	if _, err := off2.Load(); err != nil {
		t.Fatal(err)
	}
	if err := src2.Append(
		ev(1, "old-a1", OpInsert, "a", 1, map[string]any{"name": "old", "age": 1}),
	); err != nil {
		t.Fatal(err)
	}
	p2 := NewPipeline(src2, sink, off2, reg)
	p2.Logf = func(f string, a ...any) { t.Logf("[replay] "+f, a...) }
	if err := p2.Run(10); err != nil {
		t.Fatal(err)
	}
	rep, err := NewReconciler(src2, sink, off2, reg).Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", normalizeReport(rep))
	r := rep.Records[0]
	if r.Status != DispSkippedStale || len(r.Reasons) != 1 {
		t.Fatalf("older seq must be skipped_stale with reason, got %+v", r)
	}
	// 数据仍是 seq=5 的新值，未被回滚。
	if rowsMap(sink.Rows())["a"]["name"] != "new" {
		t.Fatalf("sink must keep newer row, got %v", rowsMap(sink.Rows())["a"])
	}
}

// TestSchemaHistoryPersisted 所有结构版本尝试（含被拒绝的不兼容演进）都落盘留痕，
// 进程重启后仍可查询；重复注册同一结构不产生重复流水。
func TestSchemaHistoryPersisted(t *testing.T) {
	histPath := t.TempDir() + "/schema-history.jsonl"
	reg, err := NewSchemaRegistryWithHistory(histPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	v2 := Schema{Version: 2, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	if err := reg.Register(v2); err != nil {
		t.Fatal(err)
	}
	// 两次不兼容尝试（删字段 / 新增字段无缺省值）都必须留痕。
	v3drop := Schema{Version: 3, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	v3nodef := Schema{Version: 3, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
		{Name: "phone", Type: TypeString, Required: false},
	}}
	dropErr := reg.Register(v3drop)
	nodefErr := reg.Register(v3nodef)
	if dropErr == nil || nodefErr == nil {
		t.Fatalf("both v3 attempts must be rejected: %v %v", dropErr, nodefErr)
	}
	// 重复一次同样的不兼容尝试：不得重复留痕。
	if err := reg.Register(v3drop); err == nil {
		t.Fatal("expected incompatibility again")
	}

	hist := reg.History()
	if len(hist) != 4 {
		t.Fatalf("history len=%d want 4 (v1, v2 accepted; 2 distinct rejected v3)", len(hist))
	}
	for _, c := range hist {
		t.Logf("schema-change: v%d accepted=%v fields=%d detail=%q",
			c.Version, c.Accepted, len(c.Fields), c.Detail)
	}
	var rejectedV3 int
	for _, c := range hist {
		if c.Version == 3 && !c.Accepted {
			rejectedV3++
			if len(c.Fields) == 0 {
				t.Fatal("rejected change must keep a fields snapshot for traceability")
			}
		}
	}
	if rejectedV3 != 2 {
		t.Fatalf("rejected v3 attempts recorded=%d want 2", rejectedV3)
	}

	// 模拟进程重启：新注册表从磁盘加载流水，内容与重启前一致，可用于解释不兼容。
	regRestart, err := NewSchemaRegistryWithHistory(histPath)
	if err != nil {
		t.Fatal(err)
	}
	h2 := regRestart.History()
	if len(h2) != len(hist) {
		t.Fatalf("history after restart len=%d want %d", len(h2), len(hist))
	}
	for i := range hist {
		if hist[i].Version != h2[i].Version || hist[i].Accepted != h2[i].Accepted ||
			hist[i].Detail != h2[i].Detail {
			t.Fatalf("history entry %d differs after restart: %+v vs %+v", i, hist[i], h2[i])
		}
	}
	// 用留痕解释“为什么 v3 删 age 不兼容”：找到对应拒绝记录，详情含字段名与版本。
	v3tries := regRestart.HistoryForVersion(3)
	var explained bool
	for _, c := range v3tries {
		if !c.Accepted && strings.Contains(c.Detail, `field "age" removed`) {
			explained = true
		}
	}
	if !explained {
		t.Fatalf("must be able to explain why v3 is incompatible from history: %+v", v3tries)
	}
	t.Logf("after restart, incompatible v3 is explainable via history: %d attempt(s)", len(v3tries))
}

// TestEndToEndRepairAndReconcile 全链路：
// v1 正常消费（含坏记录）-> 结构演进到 v2（兼容）-> v2 坏记录因携带已删除/未知字段被拒
// -> 上游修好重投 -> pk 边界（同批同 PK + 撞已生效 seq）-> 重复处理幂等
// -> 进程重启后对账结论一致且可解释。
// 结构注册表全程使用磁盘留痕版本。
func TestEndToEndRepairAndReconcile(t *testing.T) {
	dir := t.TempDir()
	src := NewSource(dir + "/source.jsonl")
	sink := NewSink(dir+"/sink.json", dir+"/deadletter.jsonl")
	if err := sink.Load(); err != nil {
		t.Fatal(err)
	}
	offsets := NewOffsetStore(dir + "/offset")
	if _, err := offsets.Load(); err != nil {
		t.Fatal(err)
	}
	reg, err := NewSchemaRegistryWithHistory(dir + "/schema-history.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	mustReg := func(s Schema) {
		t.Helper()
		if err := reg.Register(s); err != nil {
			t.Fatal(err)
		}
	}
	mustReg(schemaV1())

	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}), // 缺字段被拒
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[e2e] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	// 兼容演进到 v2（新增 email，带显式缺省值）。
	v2 := Schema{Version: 2, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	if err := p.UpgradeSchema(v2); err != nil {
		t.Fatal(err)
	}

	// 一条 v2 记录携带未知字段 legacy -> incompatible_schema 拒绝；
	// 另有一个全新主键的修复在同批中两条互相冲突。
	if err := src.Append(
		ev(3, "e3", OpInsert, "c", 2, map[string]any{"name": "cat", "age": 22, "legacy": "x"}),
		ev(4, "e4", OpInsert, "d", 2, map[string]any{"name": "dan", "age": 40, "email": "d@x.com"}),
	); err != nil {
		t.Fatal(err)
	}
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	if status := statusOf(mustReconcile(t, NewReconciler(src, sink, offsets, reg)), "e3"); status.Status != DispRejected {
		t.Fatalf("e3 should be rejected initially: %+v", status)
	}

	// 修复队列：e2（补 age）、e3（去掉 legacy、补 v2 字段）、
	// z 主键同批两条（一条冲突）、以及一条与已生效 d 同 seq 的冲突修复。
	rq := NewRepairQueue(dir + "/repairs.jsonl")
	if err := rq.Append(
		ev(2, "e2", OpInsert, "b", 2, map[string]any{"name": "bob", "age": 25}),
		ev(3, "e3", OpInsert, "c", 2, map[string]any{"name": "cat", "age": 22, "email": "c@x.com"}),
		ev(8, "z-lo", OpInsert, "z", 2, map[string]any{"name": "zlo", "age": 1, "email": ""}),
		ev(9, "z-hi", OpInsert, "z", 2, map[string]any{"name": "zhi", "age": 2, "email": ""}),
		ev(4, "d-collide", OpInsert, "d", 2, map[string]any{"name": "DANGER", "age": -1, "email": ""}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}
	// 重复处理：全部幂等。
	if _, err := p.ProcessRepairs(rq); err != nil {
		t.Fatal(err)
	}

	rows := rowsMap(sink.Rows())
	t.Logf("rows after repairs: %v", rows)
	if rows["b"]["age"] != float64(25) {
		t.Fatalf("e2 repair should land, got %v", rows["b"])
	}
	if rows["c"]["email"] != "c@x.com" {
		t.Fatalf("e3 repair should land projected, got %v", rows["c"])
	}
	if rows["z"]["name"] != "zhi" {
		t.Fatalf("z winner must be seq=9, got %v", rows["z"])
	}
	if rows["d"]["name"] != "dan" {
		t.Fatalf("d must not be overwritten by colliding repair, got %v", rows["d"])
	}
	rs := sink.Rejections()
	if findReject(rs, "z-lo", RejectPKConflict) == nil {
		t.Fatal("z-lo must be pk_conflict")
	}
	if findReject(rs, "d-collide", RejectPKConflict) == nil {
		t.Fatal("d-collide must be pk_conflict")
	}
	// 位点仍然只反映来源消费进度（4 行），修复不推进位点。
	if offsets.Committed() != 4 {
		t.Fatalf("offset=%d want 4", offsets.Committed())
	}

	// 对账（含修复队列与结构流水）。
	rec := NewReconciler(src, sink, offsets, reg).WithRepairQueue(rq)
	rep, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("\n%s", normalizeReport(rep))
	for _, sc := range rep.SchemaChanges {
		t.Logf("schema-change v%d accepted=%v detail=%q", sc.Version, sc.Accepted, sc.Detail)
	}
	if statusOf(rep, "e2").Status != DispApplied || statusOf(rep, "e3").Status != DispApplied {
		t.Fatal("e2/e3 must finally be applied")
	}
	if len(rep.SchemaChanges) < 2 {
		t.Fatal("report must include schema change history for explainability")
	}
	// e3 历史上被 incompatible_schema 拒过，记录里要保留该原因以解释来龙去脉。
	e3 := statusOf(rep, "e3")
	var hadIncompat bool
	for _, why := range e3.Reasons {
		if why == string(RejectIncompatSchema) {
			hadIncompat = true
		}
	}
	if !hadIncompat {
		t.Fatalf("e3 must retain incompatible_schema history reason, got %+v", e3)
	}
	// 最终仍被拒的只剩孤儿修复 z-lo / d-collide（来源里无同 ID 事件）。
	if rep.ReasonCounts[string(RejectPKConflict)] != 0 {
		t.Fatalf("pk_conflict must not be double counted for source records, got %v", rep.ReasonCounts)
	}
	// 来源外的修复记录作为孤儿对齐：z-hi 生效、z-lo 与 d-collide 未生效。
	if len(rep.OrphanRepairs) != 3 {
		t.Fatalf("orphan repairs=%v want 3", rep.OrphanRepairs)
	}
	orphanApplied := map[string]bool{}
	for _, o := range rep.OrphanRepairs {
		orphanApplied[o.EventID] = o.Applied
	}
	if !orphanApplied["z-hi"] || orphanApplied["z-lo"] || orphanApplied["d-collide"] {
		t.Fatalf("orphan applied flags wrong: %+v", orphanApplied)
	}

	// 进程重启后重新对账，结论逐字节一致。
	sink2 := NewSink(dir+"/sink.json", dir+"/deadletter.jsonl")
	if err := sink2.Load(); err != nil {
		t.Fatal(err)
	}
	off2 := NewOffsetStore(dir + "/offset")
	if _, err := off2.Load(); err != nil {
		t.Fatal(err)
	}
	reg2, err := NewSchemaRegistryWithHistory(dir + "/schema-history.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := NewReconciler(src, sink2, off2, reg2).WithRepairQueue(rq).Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	if normalizeReport(rep2) != normalizeReport(rep) {
		t.Fatalf("reconcile differs after restart:\n%s\nvs\n%s", normalizeReport(rep), normalizeReport(rep2))
	}
}

// mustReconcile 立即对账并失败即终止。
func mustReconcile(t *testing.T, r *Reconciler) ReconcileReport {
	t.Helper()
	rep, err := r.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// TestReconcileConcurrentReadonly 多个调用方并发对账：报告彼此一致，且互不修改状态。
func TestReconcileConcurrentReadonly(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}),
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	rec := NewReconciler(src, sink, offsets, reg)
	base, err := rec.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	out := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := rec.Reconcile()
			if err != nil {
				t.Errorf("concurrent reconcile %d: %v", i, err)
				return
			}
			out[i] = normalizeReport(r)
		}(i)
	}
	wg.Wait()
	for _, s := range out {
		if s != normalizeReport(base) {
			t.Fatalf("concurrent reconcile diverged:\n%s\nvs\n%s", s, normalizeReport(base))
		}
	}
	// 对账只读：位点与死信数量不变。
	if offsets.Committed() != 2 || len(sink.Rejections()) != 1 {
		t.Fatalf("reconcile mutated state: offset=%d rejections=%d", offsets.Committed(), len(sink.Rejections()))
	}
}
