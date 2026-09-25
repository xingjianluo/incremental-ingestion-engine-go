package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
)

func TestFileEventSourceAppendContiguousAndReread(t *testing.T) {
	dir := t.TempDir()
	src, err := NewFileEventSource(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	ev := func(seq int64) protocol.ChangeEvent {
		return protocol.ChangeEvent{Seq: seq, SourceName: "s", Op: protocol.OpUpsert, SchemaVer: 1,
			Payload: map[string]interface{}{"id": float64(seq)}, EmittedAt: time.Now()}
	}
	if err := src.Append(ev(1)); err != nil {
		t.Fatal(err)
	}
	if err := src.Append(ev(3)); err == nil {
		t.Fatal("non-contiguous append should fail")
	}
	if err := src.Append(ev(2)); err != nil {
		t.Fatal(err)
	}
	got, err := src.Read(2, 10)
	if err != nil || len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("read got %+v err=%v", got, err)
	}
	// 重新打开，事件仍在（持久化）。
	src2, err := NewFileEventSource(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	if max, _ := src2.MaxSeq(); max != 2 {
		t.Fatalf("max seq after reopen = %d, want 2", max)
	}
	if filepath.Base(src2.path) != "s.jsonl" {
		t.Fatalf("unexpected path %s", src2.path)
	}
}

func TestTargetApplyIdempotentConflictAndRestart(t *testing.T) {
	dir := t.TempDir()
	tgt, err := NewFileTargetStore(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	e1 := protocol.ChangeEvent{Seq: 1, Op: protocol.OpInsert, Payload: map[string]interface{}{"id": 1}}
	dup, conflict, err := tgt.Apply(e1, "1", map[string]interface{}{"id": int64(1), "v": "a"})
	if err != nil || dup || conflict {
		t.Fatalf("first apply: dup=%v conflict=%v err=%v", dup, conflict, err)
	}
	// 重复投递同一 Seq：幂等跳过。
	dup, conflict, err = tgt.Apply(e1, "1", map[string]interface{}{"id": int64(1), "v": "a"})
	if err != nil || !dup || conflict {
		t.Fatalf("replay same seq: dup=%v conflict=%v err=%v", dup, conflict, err)
	}
	// 不同 Seq 的 insert 同主键：冲突。
	e2 := protocol.ChangeEvent{Seq: 2, Op: protocol.OpInsert}
	dup, conflict, err = tgt.Apply(e2, "1", map[string]interface{}{"id": int64(1), "v": "b"})
	if err != nil || dup || !conflict {
		t.Fatalf("pk conflict: dup=%v conflict=%v err=%v", dup, conflict, err)
	}
	// upsert 覆盖，delete 删除。
	e3 := protocol.ChangeEvent{Seq: 3, Op: protocol.OpUpsert}
	if _, conflict, err = tgt.Apply(e3, "1", map[string]interface{}{"id": int64(1), "v": "c"}); err != nil || conflict {
		t.Fatalf("upsert failed: conflict=%v err=%v", conflict, err)
	}
	row, ok, _ := tgt.Get("1")
	if !ok || row["v"] != "c" {
		t.Fatalf("upsert row wrong: %v", row)
	}
	e4 := protocol.ChangeEvent{Seq: 4, Op: protocol.OpDelete}
	if _, _, err = tgt.Apply(e4, "1", nil); err != nil {
		t.Fatal(err)
	}
	if n, _ := tgt.Count(); n != 0 {
		t.Fatalf("count after delete = %d, want 0", n)
	}
	// 重启：appliedSeq 必须仍然记住 seq 1..4，重复投递不会复活数据。
	tgt2, err := NewFileTargetStore(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	if dup, _, err = tgt2.Apply(e1, "1", map[string]interface{}{"id": int64(1), "v": "a"}); err != nil || !dup {
		t.Fatalf("after restart seq1 should be dup: dup=%v err=%v", dup, err)
	}
	if n, _ := tgt2.Count(); n != 0 {
		t.Fatalf("resurrected rows after replay post-delete: %d", n)
	}
}

func TestTargetBackfillDefaults(t *testing.T) {
	dir := t.TempDir()
	tgt, _ := NewFileTargetStore(dir, "s")
	_, _, _ = tgt.Apply(protocol.ChangeEvent{Seq: 1, Op: protocol.OpInsert}, "1",
		map[string]interface{}{"id": int64(1)})
	_, _, _ = tgt.Apply(protocol.ChangeEvent{Seq: 2, Op: protocol.OpInsert}, "2",
		map[string]interface{}{"id": int64(2)})
	n, err := tgt.BackfillDefaults([]protocol.Field{
		{Name: "tier", HasDefault: true, Default: "std"},
		{Name: "note"}, // nullable -> nil
	})
	if err != nil || n != 2 {
		t.Fatalf("backfill n=%d err=%v", n, err)
	}
	row, ok, _ := tgt.Get("1")
	if !ok || row["tier"] != "std" || row["note"] != nil {
		t.Fatalf("backfilled row wrong: %v", row)
	}
}

func TestRejectLogIdempotentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	log1, err := NewFileRejectLog(dir, "s")
	if err != nil {
		t.Fatal(err)
	}
	rec := protocol.RejectedRecord{
		Event:      protocol.ChangeEvent{Seq: 7, SourceName: "s"},
		Reason:     protocol.RejectTypeMismatch,
		Detail:     "bad",
		RejectedAt: time.Now(),
	}
	appended, err := log1.Append(rec)
	if err != nil || !appended {
		t.Fatalf("first append: %v %v", appended, err)
	}
	appended, err = log1.Append(rec)
	if err != nil || appended {
		t.Fatalf("second append should be idempotent no-op: %v %v", appended, err)
	}
	log2, _ := NewFileRejectLog(dir, "s")
	all, _ := log2.All()
	if len(all) != 1 || all[0].Reason != protocol.RejectTypeMismatch {
		t.Fatalf("reject log after restart wrong: %+v", all)
	}
	appended, _ = log2.Append(rec)
	if appended {
		t.Fatal("append after restart must remain idempotent")
	}
}

func TestChangelogIdempotent(t *testing.T) {
	dir := t.TempDir()
	cl, _ := NewFileChangelog(dir, "s")
	e := ChangelogEntry{Seq: 1, Source: "s", Action: "inserted", AppliedAt: time.Now()}
	if ok, _ := cl.Append(e); !ok {
		t.Fatal("first changelog append should record")
	}
	if ok, _ := cl.Append(e); ok {
		t.Fatal("second changelog append should be deduped")
	}
	all, _ := cl.All()
	if len(all) != 1 {
		t.Fatalf("changelog len=%d", len(all))
	}
}
