package ingest

import (
	"fmt"
	"sync"
	"testing"
)

// TestResubmitRejected 修复重投：被拒记录修好后可重新投递，走同样的
// 校验/投影/落地流程；同一批修正数据重复提交幂等，且不影响已推进位点。
func TestResubmitRejected(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}),                // 缺必需字段 age
		ev(3, "e3", OpInsert, "c", 1, map[string]any{"name": "c", "age": "thirty"}), // 类型不匹配
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	committedBefore := offsets.Committed()
	if got := len(sink.Rejections()); got != 2 {
		t.Fatalf("rejections=%d want 2", got)
	}

	// 上游修好后重新投递（沿用原事件 ID 与位点）。
	fixed := []Event{
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob", "age": 25}),
		ev(3, "e3", OpInsert, "c", 1, map[string]any{"name": "cat", "age": 33}),
	}
	out, err := p.Resubmit(fixed)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resubmit outcome: applied=%v skipped=%v rejected=%v", out.Applied, out.Skipped, out.Rejected)
	if len(out.Applied) != 2 || len(out.Rejected) != 0 {
		t.Fatalf("outcome=%+v want 2 applied 0 rejected", out)
	}
	rows := rowsMap(sink.Rows())
	if got := rows["b"]["age"]; got != float64(25) {
		t.Fatalf("row b age=%v want 25", got)
	}
	if got := rows["c"]["name"]; got != "cat" {
		t.Fatalf("row c name=%v want cat", got)
	}
	// 重投不得推进位点。
	if got := offsets.Committed(); got != committedBefore {
		t.Fatalf("committed=%d want unchanged %d", got, committedBefore)
	}

	// 同一批修正数据重复提交：幂等跳过，不产生重复数据、不重复堆拒绝记录。
	out2, err := p.Resubmit(fixed)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resubmit dup outcome: applied=%v skipped=%v rejected=%v", out2.Applied, out2.Skipped, out2.Rejected)
	if len(out2.Applied) != 0 || len(out2.Skipped) != 2 || len(out2.Rejected) != 0 {
		t.Fatalf("dup outcome=%+v want 0 applied 2 skipped 0 rejected", out2)
	}
	if got := len(sink.Rows()); got != 3 {
		t.Fatalf("rows=%d want 3 after duplicate resubmit", got)
	}
	if got := len(sink.Rejections()); got != 2 {
		t.Fatalf("rejections=%d want still 2 (no piling)", got)
	}
	if got := offsets.Committed(); got != committedBefore {
		t.Fatalf("committed=%d want unchanged %d", got, committedBefore)
	}

	// 仍然修不好的记录：再次拒绝，但死信按 (id, reason) 去重不重复堆。
	bad := []Event{ev(4, "e4", OpInsert, "d", 1, map[string]any{"name": "dan"})}
	out3, err := p.Resubmit(bad)
	if err != nil {
		t.Fatal(err)
	}
	if len(out3.Rejected) != 1 || out3.Rejected[0].Reason != RejectMissingField {
		t.Fatalf("outcome=%+v want 1 missing_field rejection", out3)
	}
	if _, err := p.Resubmit(bad); err != nil {
		t.Fatal(err)
	}
	if got := len(sink.Rejections()); got != 3 {
		t.Fatalf("rejections=%d want 3 (dedup by id+reason)", got)
	}
	t.Logf("final committed offset: %d (unchanged by resubmits)", offsets.Committed())
}

// TestResubmitPKConflict 主键冲突边界：修正记录撞上目标端已存在主键且无法
// 凭来源顺序安全判定时，以 pk_conflict 拒绝，绝不改写已有数据；
// 同批同主键多次出现时，谁生效谁被拒确定且可复现（与输入顺序、并发无关）。
func TestResubmitPKConflict(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(5, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	// 修正记录位点(3)不晚于目标端该主键已应用位点(5)：无法安全判定先后 -> pk_conflict。
	stale := []Event{ev(3, "fix1", OpInsert, "a", 1, map[string]any{"name": "alice-fixed", "age": 31})}
	out, err := p.Resubmit(stale)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Rejected) != 1 || out.Rejected[0].Reason != RejectPKConflict {
		t.Fatalf("outcome=%+v want 1 pk_conflict rejection", out)
	}
	if got := rowsMap(sink.Rows())["a"]["name"]; got != "alice" {
		t.Fatalf("existing row rewritten! name=%v want alice", got)
	}
	t.Logf("pk_conflict rejection: %+v", out.Rejected[0])

	// 位点更晚(6)的修正记录可以安全判定先后 -> 正常生效。
	out2, err := p.Resubmit([]Event{ev(6, "fix2", OpUpdate, "a", 1, map[string]any{"name": "alice-fixed", "age": 31})})
	if err != nil {
		t.Fatal(err)
	}
	if len(out2.Applied) != 1 {
		t.Fatalf("outcome=%+v want 1 applied", out2)
	}
	if got := rowsMap(sink.Rows())["a"]["name"]; got != "alice-fixed" {
		t.Fatalf("row a name=%v want alice-fixed", got)
	}

	// 同批同主键出现两次：高 Seq 生效、低 Seq 以 pk_conflict 拒绝；
	// 颠倒输入顺序结果必须一致。
	batch := []Event{
		ev(8, "dup-lo", OpInsert, "z", 1, map[string]any{"name": "lo", "age": 1}),
		ev(9, "dup-hi", OpInsert, "z", 1, map[string]any{"name": "hi", "age": 2}),
	}
	out3, err := p.Resubmit(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(out3.Applied) != 1 || out3.Applied[0] != "dup-hi" {
		t.Fatalf("outcome=%+v want dup-hi applied", out3)
	}
	if len(out3.Rejected) != 1 || out3.Rejected[0].EventID != "dup-lo" || out3.Rejected[0].Reason != RejectPKConflict {
		t.Fatalf("outcome=%+v want dup-lo pk_conflict", out3)
	}
	if got := rowsMap(sink.Rows())["z"]["name"]; got != "hi" {
		t.Fatalf("row z name=%v want hi", got)
	}

	// 并发重复投递同一修正批：结果确定且可复现。
	const workers = 8
	var wg sync.WaitGroup
	outcomes := make([]*ResubmitOutcome, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, err := p.Resubmit([]Event{
				ev(10, "cc1", OpInsert, "cc", 1, map[string]any{"name": "n", "age": 1}),
			})
			if err != nil {
				t.Error(err)
				return
			}
			outcomes[i] = o
		}(w)
	}
	wg.Wait()
	appliedTotal := 0
	for i, o := range outcomes {
		t.Logf("concurrent outcome[%d]: applied=%v skipped=%v rejected=%v", i, o.Applied, o.Skipped, o.Rejected)
		appliedTotal += len(o.Applied)
		if len(o.Rejected) != 0 {
			t.Fatalf("outcome[%d]=%+v want no rejection", i, o)
		}
	}
	if appliedTotal != 1 {
		t.Fatalf("applied total=%d want exactly 1 across concurrent resubmits", appliedTotal)
	}
	if got := len(sink.Rows()); got != 3 {
		t.Fatalf("rows=%d want 3", got)
	}
	// 冲突拒绝记录去重：fix1 与 dup-lo 各一条，不随并发重复堆。
	conflicts := 0
	for _, r := range sink.Rejections() {
		if r.Reason == RejectPKConflict {
			conflicts++
			t.Logf("deadletter conflict: id=%s detail=%s", r.EventID, r.Detail)
		}
	}
	if conflicts != 2 {
		t.Fatalf("pk_conflict rejections=%d want 2", conflicts)
	}
}

// TestReconcile 结果对账：对齐来源流水、目标端状态与处置结果；
// 重复运行、进程重启后结论一致且可复现，重复投递不重复计数。
func TestReconcile(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),  // applied
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}),               // rejected: missing_field
		ev(3, "e3", OpInsert, "a", 1, map[string]any{"name": "a2", "age": 31}),     // applied (seq 3 > 1)
		ev(5, "e5", OpInsert, "c", 1, map[string]any{"name": "cat", "age": 20}),    // applied
		ev(4, "e4", OpInsert, "c", 1, map[string]any{"name": "cat-old", "age": 9}), // skipped: stale seq on pk c
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),  // 来源内重复投递，只计一次
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	// 再追加一条尚未消费的记录 -> pending。
	if err := src.Append(ev(7, "e7", OpInsert, "d", 1, map[string]any{"name": "dan", "age": 40})); err != nil {
		t.Fatal(err)
	}

	rep1, err := p.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rep1.Entries {
		t.Logf("reconcile: id=%s seq=%d pk=%s -> %s %s %s", e.EventID, e.Seq, e.PK, e.Disposition, e.Reason, e.Detail)
	}
	t.Logf("counts: %v", rep1.Counts)
	if rep1.Counts[DispApplied] != 3 || rep1.Counts[DispRejected] != 1 ||
		rep1.Counts[DispSkipped] != 1 || rep1.Counts[DispPending] != 1 {
		t.Fatalf("counts=%v want applied=3 rejected=1 skipped=1 pending=1", rep1.Counts)
	}
	if got := len(rep1.Entries); got != 6 {
		t.Fatalf("entries=%d want 6 (duplicate delivery counted once)", got)
	}

	// 重复运行：结论必须逐条一致。
	rep2, err := p.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rep1.Entries) != fmt.Sprint(rep2.Entries) || fmt.Sprint(rep1.Counts) != fmt.Sprint(rep2.Counts) {
		t.Fatalf("reconcile not reproducible:\n%v\n%v", rep1.Entries, rep2.Entries)
	}

	// 修复被拒记录并重投后：最终结论变为 applied，且作为来源流水中的记录呈现。
	if _, err := p.Resubmit([]Event{ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob", "age": 25})}); err != nil {
		t.Fatal(err)
	}
	// 纯重投事件（不在来源流水中）应出现在 external。
	if _, err := p.Resubmit([]Event{ev(9, "fix-only", OpInsert, "x", 1, map[string]any{"name": "x", "age": 1})}); err != nil {
		t.Fatal(err)
	}
	rep3, err := p.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rep3.Entries {
		t.Logf("reconcile after fix: id=%s -> %s %s", e.EventID, e.Disposition, e.Reason)
	}
	for _, e := range rep3.External {
		t.Logf("external: id=%s -> %s %s", e.EventID, e.Disposition, e.Detail)
	}
	if rep3.Counts[DispApplied] != 5 || rep3.Counts[DispRejected] != 0 {
		t.Fatalf("counts after fix=%v want applied=5 rejected=0", rep3.Counts)
	}
	if len(rep3.External) != 1 || rep3.External[0].EventID != "fix-only" || rep3.External[0].Disposition != DispApplied {
		t.Fatalf("external=%+v want fix-only applied", rep3.External)
	}

	// 进程重启后：从磁盘恢复全部状态，对账结论不变。
	sink2 := NewSink(sink.dataPath, sink.rejectPath)
	if err := sink2.Load(); err != nil {
		t.Fatal(err)
	}
	offsets2 := NewOffsetStore(offsets.path)
	if _, err := offsets2.Load(); err != nil {
		t.Fatal(err)
	}
	p2 := NewPipeline(src, sink2, offsets2, reg)
	p2.Logf = func(f string, a ...any) { t.Logf("[restart] "+f, a...) }
	rep4, err := p2.Reconcile()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rep3.Entries) != fmt.Sprint(rep4.Entries) ||
		fmt.Sprint(rep3.External) != fmt.Sprint(rep4.External) ||
		fmt.Sprint(rep3.Counts) != fmt.Sprint(rep4.Counts) {
		t.Fatalf("reconcile after restart diverged:\n%+v\n%+v", rep3, rep4)
	}
	t.Logf("reconcile reproducible across restart: counts=%v", rep4.Counts)
}

// TestSchemaLedger 结构变更流水：所有注册尝试（含被判定不兼容的）都持久化，
// 进程重启后依然可查，可用于解释记录为何被判结构不兼容。
func TestSchemaLedger(t *testing.T) {
	dir := t.TempDir()
	ledger := dir + "/schema-ledger.jsonl"
	reg := NewSchemaRegistry(ledger)
	if err := reg.Load(); err != nil {
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
	// 不兼容尝试：新增字段 phone 但未声明缺省值。必须被拒绝且留痕。
	v3nodefault := Schema{Version: 3, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
		{Name: "phone", Type: TypeString, Required: false},
	}}
	if err := reg.Register(v3nodefault); err == nil {
		t.Fatal("add field without default should be rejected")
	}

	attempts := reg.Attempts()
	for _, a := range attempts {
		t.Logf("ledger attempt: v%d accepted=%v reason=%s", a.Version, a.Accepted, a.Reason)
	}
	if len(attempts) != 3 || !attempts[0].Accepted || !attempts[1].Accepted || attempts[2].Accepted {
		t.Fatalf("attempts=%+v want [accept accept reject]", attempts)
	}
	if attempts[2].Reason == "" {
		t.Fatal("rejected attempt must carry a traceable reason")
	}

	// 进程重启：新注册表从流水恢复，已接受版本与全部尝试记录都在。
	reg2 := NewSchemaRegistry(ledger)
	if err := reg2.Load(); err != nil {
		t.Fatal(err)
	}
	latest, ok := reg2.Latest()
	if !ok || latest.Version != 2 {
		t.Fatalf("latest after reload=%v want v2", latest.Version)
	}
	if got := len(reg2.Attempts()); got != 3 {
		t.Fatalf("attempts after reload=%d want 3", got)
	}

	// 用流水解释一条记录为何被判结构不兼容：
	// 携带 phone 的 v2 事件，phone 不在任何已注册版本里（v3 尝试被拒）-> incompatible_schema。
	src, sink, offsets, _ := newEnv(t)
	p := NewPipeline(src, sink, offsets, reg2)
	p.Logf = func(f string, a ...any) { t.Logf("[run] "+f, a...) }
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 2, map[string]any{"name": "alice", "age": 30, "phone": "123"}),
	); err != nil {
		t.Fatal(err)
	}
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}
	rejs := sink.Rejections()
	if len(rejs) != 1 || rejs[0].Reason != RejectIncompatSchema {
		t.Fatalf("rejections=%+v want 1 incompatible_schema", rejs)
	}
	t.Logf("rejection explained by ledger: id=%s reason=%s detail=%s", rejs[0].EventID, rejs[0].Reason, rejs[0].Detail)
	explained := false
	for _, a := range reg2.Attempts() {
		if !a.Accepted && a.Version == 3 {
			explained = true
			t.Logf("root cause in ledger: v3 attempt rejected: %s", a.Reason)
		}
	}
	if !explained {
		t.Fatal("ledger should contain the rejected v3 attempt explaining the rejection")
	}
}
