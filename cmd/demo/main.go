// Command demo 在本地 data/ 目录下模拟一次完整的增量摄入流程：
// 造数（含主键冲突、缺必需字段、类型不匹配的坏记录）-> 并发摄入 ->
// 重复运行（验证幂等）-> 结构加字段（兼容回填）-> 结构删字段（显式失败留痕），
// 并打印位点、提交顺序、拒绝记录、变更流水与最终状态。
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/ingest"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/storage"
)

const sourceName = "demo-users"

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	dataDir := filepath.Join(".", "data")
	if err := os.RemoveAll(dataDir); err != nil {
		log.Fatal(err)
	}

	src, err := storage.NewFileEventSource(dataDir, sourceName)
	must(err)
	tgt, err := storage.NewFileTargetStore(dataDir, sourceName)
	must(err)
	rejects, err := storage.NewFileRejectLog(dataDir, sourceName)
	must(err)
	cl, err := storage.NewFileChangelog(dataDir, sourceName)
	must(err)
	hist, err := storage.NewSchemaHistory(dataDir, sourceName)
	must(err)
	offsets, err := ingest.NewOffsetStore(dataDir, sourceName)
	must(err)

	reg := protocol.NewSchemaRegistry()
	eng := ingest.NewEngine(src, tgt, rejects, cl, hist, reg, offsets, ingest.Config{
		BatchSize:   4,
		Concurrency: 3,
		Logger:      logger,
	})

	// ---- v1 schema ----
	v1 := &protocol.Schema{
		SourceName: sourceName,
		Version:    1,
		PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "email", Type: protocol.TypeString, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
		},
	}
	must(eng.RegisterSchema(v1))

	now := time.Now()
	eventsV1 := []protocol.ChangeEvent{
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(1), "email": "a@x.com", "age": float64(30)}, EmittedAt: now},
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(2), "email": "b@x.com", "age": float64(31)}, EmittedAt: now},
		// seq3: insert，主键冲突（id=1 已存在）
		{Op: protocol.OpInsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(1), "email": "dup@x.com", "age": float64(99)}, EmittedAt: now},
		// seq4: 缺失必需字段 email
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(4), "age": float64(40)}, EmittedAt: now},
		// seq5: 类型不匹配（age 给成字符串）
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(5), "email": "e@x.com", "age": "oops"}, EmittedAt: now},
		// seq6: 缺失主键
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"email": "nopk@x.com"}, EmittedAt: now},
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(7), "email": "g@x.com"}, EmittedAt: now},
		// seq8: 更新已存在记录
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(1), "email": "a@x.com", "age": float64(31)}, EmittedAt: now},
		// seq9: 删除 id=2
		{Op: protocol.OpDelete, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(2)}, EmittedAt: now},
		// seq10: 载荷出现未声明字段，按结构不兼容拒绝
		{Op: protocol.OpUpsert, SchemaVer: 1, Payload: map[string]interface{}{"id": float64(10), "email": "h@x.com", "sneaky": float64(1)}, EmittedAt: now},
	}
	for i := range eventsV1 {
		eventsV1[i].Seq = int64(i + 1)
		eventsV1[i].SourceName = sourceName
		must(src.Append(eventsV1[i]))
	}

	fmt.Println("================ 第 1 轮摄入（3 worker 并发，batch=4） ================")
	stats1, err := eng.RunOnce()
	must(err)
	printSummary("round-1", stats1, tgt, rejects, cl, offsets)

	// ---- v2 schema：新增可选字段 tier，带稳定缺省值，历史行回填 ----
	v2 := &protocol.Schema{
		SourceName: sourceName,
		Version:    2,
		PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "email", Type: protocol.TypeString, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
			{Name: "tier", Type: protocol.TypeString, Required: false, HasDefault: true, Default: "standard"},
		},
	}
	must(eng.RegisterSchema(v2))
	newEvents := []protocol.ChangeEvent{
		// seq11: v2 记录（显式 tier）
		{Op: protocol.OpUpsert, SchemaVer: 2, Payload: map[string]interface{}{"id": float64(11), "email": "k@x.com", "tier": "gold"}, EmittedAt: now},
		// seq12: v2 记录缺 tier -> 按缺省语义补 standard
		{Op: protocol.OpUpsert, SchemaVer: 2, Payload: map[string]interface{}{"id": float64(12), "email": "l@x.com", "age": float64(50)}, EmittedAt: now},
	}
	for i := range newEvents {
		newEvents[i].Seq = int64(11 + i)
		newEvents[i].SourceName = sourceName
		must(src.Append(newEvents[i]))
	}
	fmt.Println("================ 第 2 轮摄入（结构 v2 后） ================")
	stats2, err := eng.RunOnce()
	must(err)
	printSummary("round-2", stats2, tgt, rejects, cl, offsets)

	fmt.Println("================ 第 3 轮：重复运行（不得产生重复或丢失） ================")
	stats3, err := eng.RunOnce()
	must(err)
	printSummary("round-3(replay)", stats3, tgt, rejects, cl, offsets)

	// 第 3.5 轮：模拟"消息重复投递"——位点文件丢失（watermark 归 0）但目标端状态还在，
	// seq 1..12 被重新处理一遍。存储层按 Seq 幂等，结果不得出现重复或丢失。
	fmt.Println("================ 第 3.5 轮：强制重投 seq 1..12（幂等性验证） ================")
	dupOffsets, err := ingest.NewOffsetStore(filepath.Join(dataDir, "replay"), sourceName)
	must(err)
	engDup := ingest.NewEngine(src, tgt, rejects, cl, hist, reg, dupOffsets, ingest.Config{
		BatchSize: 5, Concurrency: 4, Logger: logger,
	})
	statsDup, err := engDup.RunOnce()
	must(err)
	fmt.Printf("[duplicate-delivery] processed=%d (含 %d 条 duplicate-skip)，目标端行数仍为 %d，拒绝记录仍为 %d，流水仍为 %d\n",
		statsDup.Processed, statsDup.DuplicateSkip, mustCount(tgt), len(mustRejects(rejects)), len(mustChangelog(cl)))

	// ---- v3 schema：删除 email（模拟删字段/改名），必须显式失败 ----
	v3 := &protocol.Schema{
		SourceName: sourceName,
		Version:    3,
		PrimaryKey: []string{"id"},
		Fields: []protocol.Field{
			{Name: "id", Type: protocol.TypeInt, Required: true},
			{Name: "age", Type: protocol.TypeInt, Required: false},
			{Name: "tier", Type: protocol.TypeString, Required: false, HasDefault: true, Default: "standard"},
		},
	}
	fmt.Println("================ 注册不兼容结构 v3（删除 email），预期失败 ================")
	if err := eng.RegisterSchema(v3); err != nil {
		fmt.Printf("[expected-failure] register v3 rejected: %v\n", err)
	} else {
		log.Fatal("ERROR: incompatible schema v3 should have been rejected")
	}

	// 打印结构变更历史
	schEvents, _ := hist.All()
	fmt.Println("---- 结构变更历史（schema history） ----")
	for _, e := range schEvents {
		fmt.Printf("  v%d->v%d compatible=%-5v backfilled=%d desc=%s\n",
			e.FromVersion, e.ToVersion, e.Compatible, e.BackfilledRows, e.Description)
	}

	// 模拟重启：重新打开所有存储并再跑一轮，验证从最近已提交位点恢复
	fmt.Println("================ 模拟进程重启，重新打开存储 ================")
	src2, _ := storage.NewFileEventSource(dataDir, sourceName)
	tgt2, _ := storage.NewFileTargetStore(dataDir, sourceName)
	rej2, _ := storage.NewFileRejectLog(dataDir, sourceName)
	cl2, _ := storage.NewFileChangelog(dataDir, sourceName)
	hist2, _ := storage.NewSchemaHistory(dataDir, sourceName)
	off2, _ := ingest.NewOffsetStore(dataDir, sourceName)
	reg2 := protocol.NewSchemaRegistry()
	must(hist2.RestoreRegistry(reg2))
	eng2 := ingest.NewEngine(src2, tgt2, rej2, cl2, hist2, reg2, off2, ingest.Config{
		BatchSize: 4, Concurrency: 3, Logger: logger,
	})
	fmt.Printf("[recover] 恢复出的已提交位点 = %d（来源最大 seq = %d）\n",
		eng2.RecoveredOffset(), mustMax(src2))
	stats4, err := eng2.RunOnce()
	must(err)
	if stats4.Processed != 0 {
		log.Fatalf("ERROR: after restart expected 0 new events, got %d", stats4.Processed)
	}
	fmt.Printf("[recover] 重启后无新事件需要处理，目标端行数=%d，拒绝记录数=%d\n",
		mustCount(tgt2), len(mustRejects(rej2)))

	fmt.Println("\n演示完成。落盘文件见 ./data 目录。")
}

func printSummary(tag string, s ingest.RunStats, tgt storage.TargetStore,
	rejects storage.RejectLog, cl storage.ChangeLogger, offsets *ingest.OffsetStore) {
	fmt.Printf("---- %s ----\n", tag)
	fmt.Printf("stats: processed=%d applied=%d rejected=%d dup_skip=%d by_reason=%v\n",
		s.Processed, s.Applied, s.Rejected, s.DuplicateSkip, s.ByReason)
	fmt.Printf("watermark: %d -> %d, commit_order=%v\n",
		s.CommittedFrom, s.CommittedTo, offsets.CommitOrder())
	rows, _ := tgt.All()
	fmt.Printf("target rows (%d):\n", len(rows))
	for pk, row := range rows {
		fmt.Printf("  pk=%s %v\n", pk, row)
	}
	recs, _ := rejects.All()
	fmt.Printf("rejected records (%d):\n", len(recs))
	for _, r := range recs {
		fmt.Printf("  seq=%d reason=%s detail=%s payload=%v\n",
			r.Event.Seq, r.Reason, r.Detail, r.Event.Payload)
	}
	entries, _ := cl.All()
	fmt.Printf("changelog (%d):\n", len(entries))
	for _, e := range entries {
		fmt.Printf("  seq=%d op=%-6s pk=%-2s action=%s schema=v%d\n",
			e.Seq, e.Op, e.PK, e.Action, e.SchemaVer)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func mustMax(s storage.EventSource) int64 {
	n, err := s.MaxSeq()
	must(err)
	return n
}
func mustCount(t storage.TargetStore) int {
	n, err := t.Count()
	must(err)
	return n
}
func mustRejects(l storage.RejectLog) []protocol.RejectedRecord {
	rs, err := l.All()
	must(err)
	return rs
}
func mustChangelog(l storage.ChangeLogger) []storage.ChangelogEntry {
	es, err := l.All()
	must(err)
	return es
}
