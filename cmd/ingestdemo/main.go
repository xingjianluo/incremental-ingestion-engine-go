// ingestdemo 演示本地增量摄入：采集 -> 校验 -> 幂等落地 -> 位点提交，
// 并展示结构演进、被拒数据修复重投与可重复的结果对账。
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/ingest"
)

func main() {
	dir := filepath.Join(os.TempDir(), "ingestdemo")
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	fmt.Println("workdir:", dir)

	src := ingest.NewSource(filepath.Join(dir, "source.jsonl"))
	sink := ingest.NewSink(filepath.Join(dir, "sink.json"), filepath.Join(dir, "deadletter.jsonl"))
	offsets := ingest.NewOffsetStore(filepath.Join(dir, "offset"))
	// 结构注册表使用磁盘留痕版本：所有版本尝试（含不兼容的）都可在重启后追溯。
	reg, err := ingest.NewSchemaRegistryWithHistory(filepath.Join(dir, "schema-history.jsonl"))
	must(err)

	must(sink.Load())
	committed, err := offsets.Load()
	must(err)
	fmt.Printf("recovered committed offset: %d\n", committed)

	v1 := ingest.Schema{Version: 1, Fields: []ingest.Field{
		{Name: "name", Type: ingest.TypeString, Required: true},
		{Name: "age", Type: ingest.TypeInt, Required: true},
	}}
	must(reg.Register(v1))

	// 模拟上游变更流水（含坏记录）。
	must(src.Append(
		ingest.Event{Seq: 1, ID: "e1", Op: ingest.OpInsert, PK: "a", SchemaVersion: 1, Fields: map[string]any{"name": "alice", "age": 30}},
		ingest.Event{Seq: 2, ID: "e2", Op: ingest.OpInsert, PK: "b", SchemaVersion: 1, Fields: map[string]any{"name": "bob"}},                // 缺必需字段
		ingest.Event{Seq: 3, ID: "e3", Op: ingest.OpInsert, PK: "", SchemaVersion: 1, Fields: map[string]any{"name": "x", "age": 1}},         // 缺主键
		ingest.Event{Seq: 4, ID: "e4", Op: ingest.OpInsert, PK: "c", SchemaVersion: 1, Fields: map[string]any{"name": "c", "age": "thirty"}}, // 类型不匹配
		ingest.Event{Seq: 5, ID: "e5", Op: ingest.OpUpdate, PK: "a", SchemaVersion: 1, Fields: map[string]any{"name": "alice2", "age": 31}},
	))

	p := ingest.NewPipeline(src, sink, offsets, reg)
	p.Logf = func(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) }

	fmt.Println("== first run ==")
	must(p.Run(2))

	fmt.Println("== second run (idempotent, nothing to do) ==")
	must(p.Run(2))

	// 结构演进：新增字段带显式缺省值。
	v2 := ingest.Schema{Version: 2, Fields: []ingest.Field{
		{Name: "name", Type: ingest.TypeString, Required: true},
		{Name: "age", Type: ingest.TypeInt, Required: true},
		{Name: "email", Type: ingest.TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	fmt.Println("== upgrade schema to v2 (add email with default) ==")
	must(p.UpgradeSchema(v2))

	// 演示一次“不兼容演进尝试”：同样会被记录到 schema-history.jsonl。
	v3drop := ingest.Schema{Version: 3, Fields: []ingest.Field{
		{Name: "name", Type: ingest.TypeString, Required: true},
		{Name: "email", Type: ingest.TypeString, Required: false, HasDefault: true, Default: ""},
	}}
	if err := reg.Register(v3drop); err != nil {
		fmt.Printf("  incompatible v3 attempt recorded and rejected: %v\n", err)
	}

	must(src.Append(
		ingest.Event{Seq: 6, ID: "e6", Op: ingest.OpInsert, PK: "d", SchemaVersion: 2, Fields: map[string]any{"name": "dan", "age": 40, "email": "d@x.com"}},
		ingest.Event{Seq: 7, ID: "e7", Op: ingest.OpDelete, PK: "c", SchemaVersion: 2},
	))
	must(p.Run(2))

	// 被拒数据在上游修好后，通过修复队列重新投递（沿用原事件 ID）。
	// 同主键 z 给两条修正，演示确定性的 pk_conflict 裁决；
	// 另给一条与已生效记录同 seq 的冲突修正，演示绝不改写已有数据。
	fmt.Println("== repair resubmit (rejected records fixed upstream) ==")
	rq := ingest.NewRepairQueue(filepath.Join(dir, "repairs.jsonl"))
	must(rq.Append(
		ingest.Event{Seq: 2, ID: "e2", Op: ingest.OpInsert, PK: "b", SchemaVersion: 2, Fields: map[string]any{"name": "bob", "age": 25, "email": "b@x.com"}},
		// e4 原本在 seq=4 类型不对被拒；c 之后在 seq=7 被删。
		// 用 seq=8 重投表示“删除之后的重新插入”，seq 更新、安全生效。
		ingest.Event{Seq: 8, ID: "e4", Op: ingest.OpInsert, PK: "c", SchemaVersion: 2, Fields: map[string]any{"name": "carol", "age": 28, "email": "c@x.com"}},
		ingest.Event{Seq: 8, ID: "z-lo", Op: ingest.OpInsert, PK: "z", SchemaVersion: 2, Fields: map[string]any{"name": "zlo", "age": 1, "email": ""}},
		ingest.Event{Seq: 9, ID: "z-hi", Op: ingest.OpInsert, PK: "z", SchemaVersion: 2, Fields: map[string]any{"name": "zhi", "age": 2, "email": ""}},
		ingest.Event{Seq: 5, ID: "a-collide", Op: ingest.OpUpdate, PK: "a", SchemaVersion: 2, Fields: map[string]any{"name": "DANGER", "age": 99, "email": ""}},
	))
	res, err := p.ProcessRepairs(rq)
	must(err)
	fmt.Printf("  repair result: applied=%d skipped=%d stale=%d rejected=%d (offset stays %d)\n",
		res.Applied, res.Skipped, res.Stale, res.Rejected, offsets.Committed())
	fmt.Println("== repair again (fully idempotent) ==")
	res2, err := p.ProcessRepairs(rq)
	must(err)
	fmt.Printf("  repair result: applied=%d skipped=%d stale=%d rejected=%d\n",
		res2.Applied, res2.Skipped, res2.Stale, res2.Rejected)

	// 结果对账：来源流水 × 落地状态 × 处置结果，三方对齐，可重复执行。
	fmt.Println("== reconcile (repeatable) ==")
	rep, err := ingest.NewReconciler(src, sink, offsets, reg).WithRepairQueue(rq).Reconcile()
	must(err)
	fmt.Printf("  committedOffset=%d total=%d counts=%v finalReasonCounts=%v\n",
		rep.CommittedOffset, rep.Total, rep.Counts, rep.ReasonCounts)
	for _, r := range rep.Records {
		fmt.Printf("  seq=%d id=%-10s pk=%-2s %-13s repaired=%-5v reasons=%v\n",
			r.Seq, r.EventID, r.PK, r.Status, r.Repaired, r.Reasons)
	}
	for _, o := range rep.OrphanRepairs {
		fmt.Printf("  orphan-repair id=%-10s applied=%v\n", o.EventID, o.Applied)
	}
	fmt.Println("  schema change history:")
	for _, c := range rep.SchemaChanges {
		fmt.Printf("    v%d accepted=%-5v detail=%q\n", c.Version, c.Accepted, c.Detail)
	}

	fmt.Println("== final rows ==")
	for _, r := range sink.Rows() {
		fmt.Printf("  pk=%s fields=%v\n", r.PK, r.Fields)
	}
	fmt.Println("== dead letter ==")
	for _, r := range sink.Rejections() {
		fmt.Printf("  id=%s seq=%d reason=%s detail=%s\n", r.EventID, r.Seq, r.Reason, r.Detail)
	}
	fmt.Printf("committed offset: %d\n", offsets.Committed())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
