package ingest

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// newEnv 在临时目录搭建一套 来源/目标/位点/结构注册表。
func newEnv(t *testing.T) (*Source, *Sink, *OffsetStore, *SchemaRegistry) {
	t.Helper()
	dir := t.TempDir()
	src := NewSource(dir + "/source.jsonl")
	sink := NewSink(dir+"/sink.json", dir+"/deadletter.jsonl")
	if err := sink.Load(); err != nil {
		t.Fatalf("sink load: %v", err)
	}
	offsets := NewOffsetStore(dir + "/offset")
	if _, err := offsets.Load(); err != nil {
		t.Fatalf("offset load: %v", err)
	}
	reg := NewSchemaRegistry()
	return src, sink, offsets, reg
}

func schemaV1() Schema {
	return Schema{Version: 1, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
	}}
}

func ev(seq uint64, id string, op Op, pk string, ver int, fields map[string]any) Event {
	return Event{Seq: seq, ID: id, Op: op, PK: pk, SchemaVersion: ver, Fields: fields}
}

func rowsMap(rows []Row) map[string]map[string]any {
	m := map[string]map[string]any{}
	for _, r := range rows {
		m[r.PK] = r.Fields
	}
	return m
}

// TestDuplicateConsumption 重复消费：同一批数据重复运行后目标端不得重复或丢失，
// 坏记录按可区分原因拒绝。
func TestDuplicateConsumption(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	events := []Event{
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
		ev(2, "e2", OpInsert, "b", 1, map[string]any{"name": "bob"}),                // 缺必需字段 age
		ev(3, "e3", OpInsert, "", 1, map[string]any{"name": "x", "age": 1}),         // 缺主键
		ev(4, "e4", OpInsert, "c", 1, map[string]any{"name": "c", "age": "thirty"}), // 类型不匹配
		ev(5, "e5", OpUpdate, "a", 1, map[string]any{"name": "alice2", "age": 31}),
		ev(6, "e6", OpDelete, "a", 1, nil),
		ev(7, "e7", OpInsert, "d", 1, map[string]any{"name": "dan", "age": 40}),
	}
	if err := src.Append(events...); err != nil {
		t.Fatal(err)
	}

	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[run1] "+f, a...) }
	if err := p.Run(3); err != nil {
		t.Fatalf("run1: %v", err)
	}
	if got := offsets.Committed(); got != 7 {
		t.Fatalf("committed=%d want 7", got)
	}
	wantRows := rowsMap(sink.Rows())
	t.Logf("run1 final rows: %v", wantRows)
	if len(wantRows) != 1 {
		t.Fatalf("rows=%v want only pk=d", wantRows)
	}
	if got := wantRows["d"]["name"]; got != "dan" {
		t.Fatalf("row d name=%v", got)
	}

	// 拒绝原因必须可区分。
	reasons := map[RejectReason]int{}
	for _, r := range sink.Rejections() {
		reasons[r.Reason]++
		t.Logf("deadletter: id=%s seq=%d reason=%s detail=%s", r.EventID, r.Seq, r.Reason, r.Detail)
	}
	for _, want := range []RejectReason{RejectMissingField, RejectMissingPK, RejectTypeMismatch} {
		if reasons[want] != 1 {
			t.Fatalf("reason %s count=%d want 1 (all=%v)", want, reasons[want], reasons)
		}
	}

	// 第二次运行：位点已在末尾，不应再消费。
	if err := p.Run(3); err != nil {
		t.Fatalf("run2: %v", err)
	}
	if got := len(sink.Rows()); got != 1 {
		t.Fatalf("after run2 rows=%d want 1", got)
	}

	// 模拟进程重启 + 位点被重置的重复消费：新位点从 0 开始，同一 Sink 重放全部事件。
	dir2 := t.TempDir()
	offsets2 := NewOffsetStore(dir2 + "/offset")
	if _, err := offsets2.Load(); err != nil {
		t.Fatal(err)
	}
	p2 := NewPipeline(src, sink, offsets2, reg)
	p2.Logf = func(f string, a ...any) { t.Logf("[replay] "+f, a...) }
	if err := p2.Run(2); err != nil {
		t.Fatalf("replay: %v", err)
	}
	gotRows := rowsMap(sink.Rows())
	if len(gotRows) != len(wantRows) {
		t.Fatalf("after replay rows=%v want %v", gotRows, wantRows)
	}
	for pk, f := range wantRows {
		if fmt.Sprint(gotRows[pk]) != fmt.Sprint(f) {
			t.Fatalf("row %s changed after replay: %v -> %v", pk, f, gotRows[pk])
		}
	}
	if got := len(sink.Rejections()); got != 3 {
		t.Fatalf("rejections after replay=%d want 3 (dedup)", got)
	}
	t.Logf("replay kept rows stable: %v, rejections=%d", gotRows, len(sink.Rejections()))
}

// TestConcurrentCommit 并发消费：已提交位点单调推进，回退与跳段被明确拒绝，
// 进程中断后从最近已提交位点恢复。
func TestConcurrentCommit(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	const n = 50
	var events []Event
	for i := 0; i < n; i++ {
		events = append(events, ev(uint64(i+1), fmt.Sprintf("e%02d", i), OpInsert,
			fmt.Sprintf("pk-%02d", i), 1, map[string]any{"name": fmt.Sprintf("u%d", i), "age": 20 + i}))
	}
	if err := src.Append(events...); err != nil {
		t.Fatal(err)
	}

	p := NewPipeline(src, sink, offsets, reg)
	var mu sync.Mutex
	var commitOrder []string
	p.Logf = func(f string, a ...any) {
		msg := fmt.Sprintf(f, a...)
		mu.Lock()
		commitOrder = append(commitOrder, msg)
		mu.Unlock()
	}

	const workers = 4
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := p.Run(3); err != nil {
				errs <- fmt.Errorf("worker %d: %w", id, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if got := offsets.Committed(); got != n {
		t.Fatalf("committed=%d want %d", got, n)
	}
	if got := len(sink.Rows()); got != n {
		t.Fatalf("rows=%d want %d (no dup/loss under concurrency)", got, n)
	}
	mu.Lock()
	for _, c := range commitOrder {
		t.Logf("flow: %s", c)
	}
	mu.Unlock()

	// 位点回退与跳段必须被明确拒绝。
	if err := offsets.Commit(n, n-1); !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("regression commit err=%v want ErrOffsetRegression", err)
	}
	t.Logf("regression commit rejected: %v", offsets.Commit(n, n-1))
	if err := offsets.Commit(0, n+10); !errors.Is(err, ErrOffsetGap) {
		t.Fatalf("gap commit err=%v want ErrOffsetGap", err)
	}
	t.Logf("gap commit rejected: %v", offsets.Commit(0, n+10))
	if err := offsets.Commit(n, n+1); err != nil {
		t.Fatalf("valid commit: %v", err)
	}
	t.Logf("valid commit accepted: %d -> %d", n, n+1)

	// 模拟进程中断恢复：新 OffsetStore 从磁盘加载最近已提交位点。
	offsets2 := NewOffsetStore(offsets.path)
	v, err := offsets2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if v != n+1 {
		t.Fatalf("recovered offset=%d want %d", v, n+1)
	}
	t.Logf("recovered committed offset after restart: %d", v)
}

// TestSchemaEvolution 结构变更：新增字段缺省语义确定；删字段/改名显式失败；
// 携带已删除字段的事件被拒绝且可追溯，不得静默错列。
func TestSchemaEvolution(t *testing.T) {
	src, sink, offsets, reg := newEnv(t)
	if err := reg.Register(schemaV1()); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(
		ev(1, "e1", OpInsert, "a", 1, map[string]any{"name": "alice", "age": 30}),
	); err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { t.Logf("[v1] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	// 兼容演进：新增字段 email，显式缺省值 ""。
	v2 := Schema{Version: 2, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	if err := p.UpgradeSchema(v2); err != nil {
		t.Fatalf("upgrade to v2 (add field with default) should be compatible: %v", err)
	}
	t.Logf("schema v2 registered: add field email with default \"\"")

	// 不兼容演进：删字段。
	v3drop := Schema{Version: 3, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	if err := reg.Register(v3drop); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("drop field err=%v want ErrIncompatibleSchema", err)
	}
	t.Logf("drop field rejected: %v", reg.Register(v3drop))

	// 不兼容演进：改名（删除+新增）。
	v3rename := Schema{Version: 3, Fields: []Field{
		{Name: "fullName", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	err := reg.Register(v3rename)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("rename err=%v want ErrIncompatibleSchema", err)
	}
	t.Logf("rename rejected: %v", err)

	// 不兼容演进：新增字段但未声明缺省值。
	v3nodefault := Schema{Version: 3, Fields: []Field{
		{Name: "name", Type: TypeString, Required: true},
		{Name: "age", Type: TypeInt, Required: true},
		{Name: "email", Type: TypeString, Required: false, HasDefault: true, Default: ""},
		{Name: "phone", Type: TypeString, Required: false},
	}}
	if err := reg.Register(v3nodefault); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("add field without default err=%v want ErrIncompatibleSchema", err)
	}
	t.Logf("add field without default rejected: %v", reg.Register(v3nodefault))

	// v2 事件：一条带 email，一条不带（应填缺省值）；一条携带已不存在的字段（应拒绝）。
	if err := src.Append(
		ev(2, "e2", OpInsert, "b", 2, map[string]any{"name": "bob", "age": 25, "email": "b@x.com"}),
		ev(3, "e3", OpInsert, "c", 2, map[string]any{"name": "cat", "age": 22}),
		ev(4, "e4", OpInsert, "d", 2, map[string]any{"name": "dan", "age": 40, "legacy": "x"}),
	); err != nil {
		t.Fatal(err)
	}
	p.Logf = func(f string, a ...any) { t.Logf("[v2] "+f, a...) }
	if err := p.Run(10); err != nil {
		t.Fatal(err)
	}

	rows := rowsMap(sink.Rows())
	t.Logf("final rows: %v", rows)
	// 老数据投影到 v2：email 填缺省值，语义确定且稳定。
	if got := rows["a"]["email"]; got != "" {
		t.Fatalf("row a email=%v want default \"\"", got)
	}
	if got := rows["b"]["email"]; got != "b@x.com" {
		t.Fatalf("row b email=%v", got)
	}
	if got := rows["c"]["email"]; got != "" {
		t.Fatalf("row c email=%v want default \"\"", got)
	}
	// 携带未知字段的事件必须被拒绝，不得静默错列。
	if _, ok := rows["d"]; ok {
		t.Fatalf("row d must be rejected, got %v", rows["d"])
	}
	var foundIncompat bool
	for _, r := range sink.Rejections() {
		t.Logf("deadletter: id=%s reason=%s detail=%s", r.EventID, r.Reason, r.Detail)
		if r.EventID == "e4" && r.Reason == RejectIncompatSchema {
			foundIncompat = true
		}
	}
	if !foundIncompat {
		t.Fatalf("e4 should be rejected with reason %s", RejectIncompatSchema)
	}
	// 目标端字段不得出现已知结构之外的键。
	for pk, f := range rows {
		for k := range f {
			if k != "name" && k != "age" && k != "email" {
				t.Fatalf("row %s has unexpected field %q (silent misalignment)", pk, k)
			}
		}
	}
	if got := offsets.Committed(); got != 4 {
		t.Fatalf("committed=%d want 4", got)
	}
}
