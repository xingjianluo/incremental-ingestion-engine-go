// ingestdemo 演示本地增量摄入：采集 -> 校验 -> 幂等落地 -> 位点提交，
// 并展示结构演进与重复运行后的确定状态。
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
	reg := ingest.NewSchemaRegistry(filepath.Join(dir, "schema-ledger.jsonl"))

	must(sink.Load())
	must(reg.Load())
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

	must(src.Append(
		ingest.Event{Seq: 6, ID: "e6", Op: ingest.OpInsert, PK: "d", SchemaVersion: 2, Fields: map[string]any{"name": "dan", "age": 40, "email": "d@x.com"}},
		ingest.Event{Seq: 7, ID: "e7", Op: ingest.OpDelete, PK: "c", SchemaVersion: 2},
	))
	must(p.Run(2))

	fmt.Println("== final rows ==")
	for _, r := range sink.Rows() {
		fmt.Printf("  pk=%s fields=%v\n", r.PK, r.Fields)
	}
	fmt.Println("== dead letter ==")
	for _, r := range sink.Rejections() {
		fmt.Printf("  id=%s seq=%d reason=%s detail=%s\n", r.EventID, r.Seq, r.Reason, r.Detail)
	}
	fmt.Printf("committed offset: %d\n", offsets.Committed())

	// 修复重投：上游修好被拒记录后重新投递（不推进位点）。
	fmt.Println("== resubmit fixed records ==")
	out, err := p.Resubmit([]ingest.Event{
		{Seq: 2, ID: "e2", Op: ingest.OpInsert, PK: "b", SchemaVersion: 2, Fields: map[string]any{"name": "bob", "age": 25}},
		{Seq: 4, ID: "e4", Op: ingest.OpInsert, PK: "c", SchemaVersion: 2, Fields: map[string]any{"name": "cat", "age": 33}},
	})
	must(err)
	fmt.Printf("  applied=%v skipped=%v rejected=%v\n", out.Applied, out.Skipped, out.Rejected)

	// 主键冲突：修正记录位点不晚于目标端该主键已应用位点 -> pk_conflict，不改写已有数据。
	fmt.Println("== resubmit with pk conflict (stale seq) ==")
	out2, err := p.Resubmit([]ingest.Event{
		{Seq: 1, ID: "fix-a", Op: ingest.OpUpdate, PK: "a", SchemaVersion: 2, Fields: map[string]any{"name": "alice-hijack", "age": 99}},
	})
	must(err)
	fmt.Printf("  applied=%v skipped=%v rejected=%v\n", out2.Applied, out2.Skipped, out2.Rejected)

	// 同一批修正数据重复提交：幂等。
	fmt.Println("== resubmit same batch again (idempotent) ==")
	out3, err := p.Resubmit([]ingest.Event{
		{Seq: 2, ID: "e2", Op: ingest.OpInsert, PK: "b", SchemaVersion: 2, Fields: map[string]any{"name": "bob", "age": 25}},
	})
	must(err)
	fmt.Printf("  applied=%v skipped=%v rejected=%v\n", out3.Applied, out3.Skipped, out3.Rejected)
	fmt.Printf("committed offset after resubmits: %d (unchanged)\n", offsets.Committed())

	// 结构变更流水：含被判定不兼容的尝试，重启后依然可查。
	fmt.Println("== schema ledger (all attempts, incl. rejected) ==")
	badV3 := ingest.Schema{Version: 3, Fields: []ingest.Field{
		{Name: "name", Type: ingest.TypeString, Required: true},
		{Name: "age", Type: ingest.TypeInt, Required: true},
	}}
	if err := reg.Register(badV3); err != nil {
		fmt.Printf("  v3 attempt rejected: %v\n", err)
	}
	for _, a := range reg.Attempts() {
		fmt.Printf("  v%d accepted=%v reason=%s\n", a.Version, a.Accepted, a.Reason)
	}

	// 结果对账：每条记录的最终结论，可重复执行、结论一致。
	fmt.Println("== reconcile ==")
	rep, err := p.Reconcile()
	must(err)
	for _, e := range rep.Entries {
		fmt.Printf("  id=%s seq=%d pk=%s -> %s %s %s\n", e.EventID, e.Seq, e.PK, e.Disposition, e.Reason, e.Detail)
	}
	for _, e := range rep.External {
		fmt.Printf("  external id=%s -> %s %s\n", e.EventID, e.Disposition, e.Detail)
	}
	fmt.Printf("  counts: %v\n", rep.Counts)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
