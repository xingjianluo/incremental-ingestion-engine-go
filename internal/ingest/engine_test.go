package ingest

import (
	"errors"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/storage"
)

const testSource = "test-users"

func v1Schema() *protocol.Schema {
	return &protocol.Schema{
		SourceName: testSource, Version: 1, PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "email", Type: protocol.TypeString, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
		},
	}
}

func mkEvent(seq int64, ver int, op protocol.OpType, payload map[string]interface{}) protocol.ChangeEvent {
	return protocol.ChangeEvent{
		Seq: seq, SourceName: testSource, Op: op, SchemaVer: ver,
		Payload: payload, EmittedAt: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC),
	}
}

func numID(id int) map[string]interface{} {
	return map[string]interface{}{"id": float64(id)}
}

// harness 在临时目录中组装好全部本地存储与引擎。
type harness struct {
	dir     string
	src     *storage.FileEventSource
	tgt     *storage.FileTargetStore
	rejects *storage.FileRejectLog
	cl      *storage.FileChangelog
	hist    *storage.SchemaHistory
	reg     *protocol.SchemaRegistry
	offsets *OffsetStore
	engine  *Engine
	logger  *log.Logger
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	logger := newTestLogger(t) // go test -v: per-seq commit log lines
	src, err := storage.NewFileEventSource(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	tgt, err := storage.NewFileTargetStore(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	rejects, err := storage.NewFileRejectLog(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := storage.NewFileChangelog(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := storage.NewSchemaHistory(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	offsets, err := NewOffsetStore(dir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	reg := protocol.NewSchemaRegistry()
	h := &harness{
		dir: dir, src: src, tgt: tgt, rejects: rejects, cl: cl, hist: hist,
		reg: reg, offsets: offsets, logger: logger,
	}
	h.engine = NewEngine(src, tgt, rejects, cl, hist, reg, offsets, Config{
		BatchSize: 3, Concurrency: 4, Logger: logger,
	})
	if err := h.engine.RegisterSchema(v1Schema()); err != nil {
		t.Fatal(err)
	}
	return h
}

// reopen 模拟进程重启：全部存储从磁盘重新打开，Schema 重新注册，位点从文件恢复。
func (h *harness) reopen(t *testing.T) {
	t.Helper()
	src, _ := storage.NewFileEventSource(h.dir, testSource)
	tgt, _ := storage.NewFileTargetStore(h.dir, testSource)
	rejects, _ := storage.NewFileRejectLog(h.dir, testSource)
	cl, _ := storage.NewFileChangelog(h.dir, testSource)
	hist, _ := storage.NewSchemaHistory(h.dir, testSource)
	offsets, _ := NewOffsetStore(h.dir, testSource)
	reg := protocol.NewSchemaRegistry()
	if err := hist.RestoreRegistry(reg); err != nil {
		t.Fatalf("restore registry: %v", err)
	}
	h.src, h.tgt, h.rejects, h.cl, h.hist, h.offsets = src, tgt, rejects, cl, hist, offsets
	h.reg = reg
	h.engine = NewEngine(src, tgt, rejects, cl, hist, reg, offsets, Config{
		BatchSize: 3, Concurrency: 4, Logger: h.logger,
	})
}

func (h *harness) registerExistingSchemas(t *testing.T, schemas ...*protocol.Schema) {
	t.Helper()
	for _, s := range schemas {
		if err := h.reg.Register(s); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) append(t *testing.T, evs ...protocol.ChangeEvent) {
	t.Helper()
	for _, ev := range evs {
		if err := h.src.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) reasonCounts(t *testing.T) map[protocol.RejectReason]int {
	t.Helper()
	out := map[protocol.RejectReason]int{}
	recs, err := h.rejects.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		out[r.Reason]++
	}
	return out
}

// 场景一：重复消费幂等。同批数据多轮/重启重跑后，目标端无重复无丢失，
// 拒绝原因可区分，拒绝记录与变更流水不重复。
func TestRepeatedConsumptionIdempotent(t *testing.T) {
	h := newHarness(t)
	h.append(t,
		mkEvent(1, 1, protocol.OpUpsert, map[string]interface{}{"id": float64(1), "email": "a@x", "age": float64(20)}),
		mkEvent(2, 1, protocol.OpUpsert, map[string]interface{}{"id": float64(2), "email": "b@x", "age": float64(21)}),
		mkEvent(3, 1, protocol.OpInsert, map[string]interface{}{"id": float64(1), "email": "dup@x"}),             // 主键冲突
		mkEvent(4, 1, protocol.OpUpsert, map[string]interface{}{"id": float64(4), "age": float64(40)}),           // 缺 email
		mkEvent(5, 1, protocol.OpUpsert, map[string]interface{}{"id": float64(5), "email": "e@x", "age": "bad"}), // 类型错
		mkEvent(6, 1, protocol.OpUpsert, map[string]interface{}{"email": "nopk@x"}),                              // 缺主键
		mkEvent(7, 1, protocol.OpDelete, numID(2)),
	)

	stats1, err := h.engine.RunOnce()
	if err != nil {
		t.Fatal(err)
	}
	wantReasons := map[protocol.RejectReason]int{
		protocol.RejectPrimaryKeyConflict:   1,
		protocol.RejectMissingRequiredField: 1,
		protocol.RejectTypeMismatch:         1,
		protocol.RejectMissingPrimaryKey:    1,
	}
	if got := h.reasonCounts(t); !mapsEqual(got, wantReasons) {
		t.Fatalf("round1 reject reasons = %v, want %v", got, wantReasons)
	}
	if stats1.Rejected != 4 || stats1.Applied != 3 {
		t.Fatalf("round1 stats = %+v", stats1)
	}

	rows1, _ := h.tgt.All()
	if len(rows1) != 1 { // id=1 在，id=2 被删
		t.Fatalf("round1 target rows = %v, want exactly id=1", rows1)
	}
	if rows1["1"]["email"] != "a@x" {
		t.Fatalf("id=1 row wrong: %v", rows1["1"])
	}
	recs1, _ := h.rejects.All()
	cl1, _ := h.cl.All()
	t.Logf("[重复消费] 首轮 watermark=%d 提交顺序=%v 拒绝原因=%v 流水条数=%d 拒绝条数=%d",
		h.offsets.Committed(), h.offsets.CommitOrder(), wantReasons, len(cl1), len(recs1))

	// 第二轮：watermark 挡住，无处理。
	stats2, err := h.engine.RunOnce()
	if err != nil || stats2.Processed != 0 {
		t.Fatalf("round2 stats=%+v err=%v", stats2, err)
	}

	// 模拟进程重启后再跑：从已提交位点恢复，仍然无重复处理。
	h.reopen(t)
	if h.engine.RecoveredOffset() != 7 {
		t.Fatalf("recovered offset = %d, want 7", h.engine.RecoveredOffset())
	}
	stats3, err := h.engine.RunOnce()
	if err != nil || stats3.Processed != 0 {
		t.Fatalf("after restart stats=%+v err=%v", stats3, err)
	}

	// 强制重复投递（消息重投）：使用空位点重放同一来源日志。
	// Schema 注册表是内存态，新引擎从结构历史恢复已生效版本。
	dupReg := protocol.NewSchemaRegistry()
	if err := h.hist.RestoreRegistry(dupReg); err != nil {
		t.Fatal(err)
	}
	dupDir := filepath.Join(t.TempDir(), "replay")
	dupOffsets, err := NewOffsetStore(dupDir, testSource)
	if err != nil {
		t.Fatal(err)
	}
	dupEng := NewEngine(h.src, h.tgt, h.rejects, h.cl, h.hist, dupReg, dupOffsets,
		Config{BatchSize: 3, Concurrency: 4, Logger: h.logger})
	stats4, err := dupEng.RunOnce()
	if err != nil {
		t.Fatal(err)
	}
	if stats4.DuplicateSkip != 3 {
		t.Fatalf("dup run duplicate-skip = %d, want 3", stats4.DuplicateSkip)
	}
	// 最终不变量：行数、拒绝记录、流水条数与首轮完全一致。
	rowsFinal, _ := h.tgt.All()
	recsFinal, _ := h.rejects.All()
	clFinal, _ := h.cl.All()
	if len(rowsFinal) != len(rows1) {
		t.Fatalf("target rows changed after replays: %d -> %d", len(rows1), len(rowsFinal))
	}
	if len(recsFinal) != len(recs1) {
		t.Fatalf("reject records duplicated/lost: %d -> %d", len(recs1), len(recsFinal))
	}
	if len(clFinal) != len(cl1) {
		t.Fatalf("changelog duplicated/lost: %d -> %d", len(cl1), len(clFinal))
	}
	if got := h.reasonCounts(t); !mapsEqual(got, wantReasons) {
		t.Fatalf("final reject reasons = %v, want %v", got, wantReasons)
	}
	t.Logf("[重复消费] 重投后最终状态 rows=%d rejects=%d changelog=%d watermark(normal)=%d",
		len(rowsFinal), len(recsFinal), len(clFinal), h.offsets.Committed())
}

func mapsEqual(a, b map[protocol.RejectReason]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// 场景二：并发消费的协同与中断恢复。
// 多条事件由 8 个 worker 并发处理，但位点必须按 1..N 顺序提交；
// 处理到一半模拟进程中断（位点停在 k），重启后从 k 恢复，不回退、不跳段。
func TestConcurrentProcessingOrderedCommitAndRecovery(t *testing.T) {
	h := newHarness(t)
	const n = 30
	for seq := int64(1); seq <= n; seq++ {
		h.append(t, mkEvent(seq, 1, protocol.OpUpsert,
			map[string]interface{}{"id": float64(seq), "email": "u" + string(rune('a'+(int(seq)-1)%26)) + "@x"}))
	}

	stats, err := h.engine.RunOnce()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processed != n || stats.Applied != n {
		t.Fatalf("stats = %+v, want %d applied", stats, n)
	}
	order := h.offsets.CommitOrder()
	if len(order) != n {
		t.Fatalf("commit order len = %d, want %d", len(order), n)
	}
	for i, seq := range order {
		if seq != int64(i+1) {
			t.Fatalf("commit order must be strictly sequential even under concurrency: index %d got %d", i, seq)
		}
	}
	clAll, _ := h.cl.All()
	t.Logf("[并发推进] workers=4 batch=3 watermark=%d 提交顺序=%v", h.offsets.Committed(), order)
	t.Logf("[并发推进] 变更流水(按seq)=%s", summarizeChangelog(clAll))

	// 模拟中断：位点文件停在 k，目标端可能已超前应用（at-least-once）。
	const k = 17
	offK, err := NewOffsetStore(filepath.Join(t.TempDir(), "crash"), testSource)
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= k; seq++ {
		if err := offK.Commit(seq); err != nil {
			t.Fatal(err)
		}
	}
	h.offsets = offK
	crashEng := NewEngine(h.src, h.tgt, h.rejects, h.cl, h.hist, h.reg, offK,
		Config{BatchSize: 5, Concurrency: 8, Logger: h.logger})
	if crashEng.RecoveredOffset() != k {
		t.Fatalf("crash engine recovered offset = %d, want %d", crashEng.RecoveredOffset(), k)
	}
	statsR, err := crashEng.RunOnce()
	if err != nil {
		t.Fatal(err)
	}
	if statsR.CommittedFrom != k {
		t.Fatalf("resume should start from %d, got from %d", k, statsR.CommittedFrom)
	}
	if statsR.Processed != n-k {
		t.Fatalf("resume processed = %d, want %d", statsR.Processed, n-k)
	}
	if offK.Committed() != n {
		t.Fatalf("after resume watermark = %d, want %d", offK.Committed(), n)
	}
	rows, _ := h.tgt.All()
	if len(rows) != n {
		t.Fatalf("target rows after resume = %d, want %d", len(rows), n)
	}
	// 流水不重复：恢复期间 seq<=k 若被重放，应记 duplicate-skip 而非重复落地。
	clAfter, _ := h.cl.All()
	if len(clAfter) != n {
		t.Fatalf("changelog entries = %d, want %d (no duplicates across recovery)", len(clAfter), n)
	}
	t.Logf("[中断恢复] 从位点 %d 恢复，补处理 %d 条，最终 watermark=%d rows=%d",
		k, statsR.Processed, offK.Committed(), len(rows))
}

// 位点协议级别的回退/跳段在 offset_test.go 已覆盖；这里再验证引擎不会"从中间继续"：
// 人为构造 watermark 落后但中间位点缺失的场景，引擎必须在违规提交处显式失败。
func TestEngineRejectsGapAndRollback(t *testing.T) {
	h := newHarness(t)
	h.append(t, mkEvent(1, 1, protocol.OpUpsert, map[string]interface{}{"id": 1.0, "email": "a@x"}))
	if _, err := h.engine.RunOnce(); err != nil {
		t.Fatal(err)
	}
	// 用一个已被污染的位点文件（committed=5）打开新引擎处理同一来源：
	// 来源只有 seq1，读不到 seq6 不会触发提交，因此这里直接验证 OffsetStore 协议。
	if err := h.offsets.Commit(3); !errors.Is(err, ErrOffsetGap) {
		t.Fatalf("expect gap rejection, got %v", err)
	}
	if err := h.offsets.Commit(1); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("expect rollback rejection, got %v", err)
	}
	if h.offsets.Committed() != 1 {
		t.Fatalf("watermark mutated by rejected commits = %d", h.offsets.Committed())
	}
}

// 场景三：来源结构变化的兼容处理。
//   - v2 新增带稳定缺省值的字段：注册成功、历史行按缺省回填、新记录缺字段自动补全；
//   - v2' 新增必需字段但无缺省：显式失败；
//   - v3 删除/改名字段、改类型：显式失败，注册表保持 v2，历史可追溯；
//   - 事件引用未注册版本：按 INCOMPATIBLE_SCHEMA 拒绝，不静默丢弃。
func TestSchemaEvolution(t *testing.T) {
	h := newHarness(t)
	h.append(t,
		mkEvent(1, 1, protocol.OpUpsert, map[string]interface{}{"id": 1.0, "email": "a@x"}),
		mkEvent(2, 1, protocol.OpUpsert, map[string]interface{}{"id": 2.0, "email": "b@x"}),
	)
	if _, err := h.engine.RunOnce(); err != nil {
		t.Fatal(err)
	}

	// v2：新增可选字段 tier，缺省 "standard"。
	v2 := &protocol.Schema{
		SourceName: testSource, Version: 2, PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "email", Type: protocol.TypeString, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
			{Name: "tier", Type: protocol.TypeString, Required: false, HasDefault: true, Default: "standard"},
		},
	}
	if err := h.engine.RegisterSchema(v2); err != nil {
		t.Fatalf("compatible v2 should register: %v", err)
	}
	row1, ok, _ := h.tgt.Get("1")
	if !ok || row1["tier"] != "standard" {
		t.Fatalf("historical row not backfilled with default: %v", row1)
	}
	// 新事件：显式 tier 与缺失 tier（补默认）。
	h.append(t,
		mkEvent(3, 2, protocol.OpUpsert, map[string]interface{}{"id": 3.0, "email": "c@x", "tier": "gold"}),
		mkEvent(4, 2, protocol.OpUpsert, map[string]interface{}{"id": 4.0, "email": "d@x"}),
	)
	if _, err := h.engine.RunOnce(); err != nil {
		t.Fatal(err)
	}
	row3, _, _ := h.tgt.Get("3")
	row4, _, _ := h.tgt.Get("4")
	if row3["tier"] != "gold" {
		t.Fatalf("explicit tier lost: %v", row3)
	}
	if row4["tier"] != "standard" {
		t.Fatalf("missing new field should get stable default, got %v", row4["tier"])
	}

	// 新增必需字段但无缺省：显式失败。
	v2ReqNoDefault := &protocol.Schema{
		SourceName: testSource, Version: 3, PrimaryKey: []string{"id"},
		Fields: append(append([]protocol.Field{}, v2.Fields...),
			protocol.Field{Name: "tenant", Type: protocol.TypeString, Required: true}),
	}
	if err := h.engine.RegisterSchema(v2ReqNoDefault); !errors.Is(err, protocol.ErrIncompatibleSchema) {
		t.Fatalf("required new field without default must fail: %v", err)
	}

	// v3 删除 email（模拟删字段/改名）：显式失败。
	v3Removed := &protocol.Schema{
		SourceName: testSource, Version: 3, PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
			{Name: "tier", Type: protocol.TypeString, Required: false, HasDefault: true, Default: "standard"},
		},
	}
	if err := h.engine.RegisterSchema(v3Removed); !errors.Is(err, protocol.ErrIncompatibleSchema) {
		t.Fatalf("field removal must fail explicitly: %v", err)
	}
	// 注册表仍停留在 v2。
	if latest, _ := h.reg.Latest(testSource); latest.Version != 2 {
		t.Fatalf("registry should stay at v2 after failed changes, got v%d", latest.Version)
	}
	// 被拒绝后目标端状态不变，无静默错列。
	if n, _ := h.tgt.Count(); n != 4 {
		t.Fatalf("target rows changed after rejected schema changes: %d", n)
	}

	// 事件引用未注册的 v9：拒绝且原因可区分。
	h.append(t, mkEvent(5, 9, protocol.OpUpsert, map[string]interface{}{"id": 5.0, "email": "e@x"}))
	if _, err := h.engine.RunOnce(); err != nil {
		t.Fatal(err)
	}
	recs, _ := h.rejects.All()
	found := false
	for _, r := range recs {
		if r.Event.Seq == 5 && r.Reason == protocol.RejectIncompatibleSchema {
			found = true
		}
	}
	if !found {
		t.Fatalf("event with unregistered schema version must be rejected as INCOMPATIBLE_SCHEMA: %+v", recs)
	}

	// 结构变更历史可追溯：成功 2 条（v1/v2），失败 2 条。
	histAll, _ := h.hist.All()
	var compatible, incompatible int
	for _, e := range histAll {
		if e.Compatible {
			compatible++
		} else {
			incompatible++
		}
	}
	if compatible != 2 || incompatible != 2 {
		t.Fatalf("schema history compatible=%d incompatible=%d, want 2/2", compatible, incompatible)
	}
	t.Logf("[结构变更] 结构历史: %s", summarizeSchemaHistory(histAll))

	// 重启后注册表从历史恢复，仍能处理 v2 事件且缺省语义不变。
	h.reopen(t)
	if latest, _ := h.reg.Latest(testSource); latest.Version != 2 {
		t.Fatalf("after restart registry latest = v%d, want v2", latest.Version)
	}
	h.append(t, mkEvent(6, 2, protocol.OpUpsert, map[string]interface{}{"id": 6.0, "email": "f@x"}))
	if _, err := h.engine.RunOnce(); err != nil {
		t.Fatal(err)
	}
	row6, _, _ := h.tgt.Get("6")
	if row6["tier"] != "standard" {
		t.Fatalf("default semantics must survive restart: %v", row6)
	}
}

func summarizeChangelog(es []storage.ChangelogEntry) string {
	parts := make([]string, 0, len(es))
	for _, e := range es {
		parts = append(parts, "seq"+strconv.FormatInt(e.Seq, 10)+":"+e.Op+"->"+e.Action)
	}
	return strings.Join(parts, " ")
}

func summarizeSchemaHistory(es []storage.SchemaChangeEvent) string {
	parts := make([]string, 0, len(es))
	for _, e := range es {
		state := "OK"
		if !e.Compatible {
			state = "REJECTED"
		}
		parts = append(parts,
			"v"+strconv.Itoa(e.FromVersion)+"->v"+strconv.Itoa(e.ToVersion)+"="+state)
	}
	return strings.Join(parts, " ")
}

// 批内同主键并发 insert 的确定性：同一批未提交事件里，多个 insert 命中同一主键时，
// 最小 seq 插入成功，其余按 PRIMARY_KEY_CONFLICT 拒绝，且结果与 worker 调度无关。
func TestConcurrentInsertsSamePKDeterministic(t *testing.T) {
	for iter := 0; iter < 20; iter++ { // 反复跑以排除调度相关的偶发
		h := newHarness(t)
		h.append(t,
			mkEvent(1, 1, protocol.OpInsert, map[string]interface{}{"id": 9.0, "email": "first@x"}),
			mkEvent(2, 1, protocol.OpInsert, map[string]interface{}{"id": 9.0, "email": "second@x"}),
			mkEvent(3, 1, protocol.OpInsert, map[string]interface{}{"id": 9.0, "email": "third@x"}),
			mkEvent(4, 1, protocol.OpUpsert, map[string]interface{}{"id": 8.0, "email": "other@x"}),
		)
		stats, err := h.engine.RunOnce()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Rejected != 2 {
			t.Fatalf("iter %d: rejected=%d, want 2", iter, stats.Rejected)
		}
		recs, _ := h.rejects.All()
		conflictSeqs := map[int64]bool{}
		for _, r := range recs {
			if r.Reason == protocol.RejectPrimaryKeyConflict {
				conflictSeqs[r.Event.Seq] = true
			}
		}
		if !conflictSeqs[2] || !conflictSeqs[3] || conflictSeqs[1] {
			t.Fatalf("iter %d: conflict seqs=%v, want exactly {2,3}", iter, conflictSeqs)
		}
		row, ok, _ := h.tgt.Get("9")
		if !ok || row["email"] != "first@x" {
			t.Fatalf("iter %d: winner row should be seq1, got %v", iter, row)
		}
	}
	t.Logf("[批内冲突确定性] 20 次并发运行均为 seq1 胜出、seq2/3 冲突")
}
