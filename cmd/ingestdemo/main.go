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
	reg := ingest.NewSchemaRegistry()

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
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
