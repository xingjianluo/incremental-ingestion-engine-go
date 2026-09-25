package ingest

import (
	"errors"
	"io"
	"log"
	"sync"
	"testing"
)

func TestOffsetCommitProtocol(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(dir, "src")
	if err != nil {
		t.Fatal(err)
	}
	if s.Committed() != 0 {
		t.Fatalf("initial offset = %d, want 0", s.Committed())
	}
	if err := s.Commit(1); err != nil {
		t.Fatal(err)
	}
	// 回退（含重复提交）必须拒绝。
	if err := s.Commit(1); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("re-commit same seq: want ErrOffsetRollback, got %v", err)
	}
	if err := s.Commit(0); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("commit backwards: want ErrOffsetRollback, got %v", err)
	}
	// 跳段必须拒绝，且位点保持不变（绝不"从中间继续"）。
	if err := s.Commit(3); !errors.Is(err, ErrOffsetGap) {
		t.Fatalf("gap commit: want ErrOffsetGap, got %v", err)
	}
	if s.Committed() != 1 {
		t.Fatalf("offset changed after rejected commits: %d", s.Committed())
	}
	if err := s.Commit(2); err != nil {
		t.Fatal(err)
	}
	if got := s.CommitOrder(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("commit order = %v, want [1 2]", got)
	}
}

func TestOffsetRecoverFromDisk(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewOffsetStore(dir, "src")
	for i := int64(1); i <= 5; i++ {
		if err := s.Commit(i); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟进程重启：重新打开位点文件，恢复到最近一次已提交位点。
	s2, err := NewOffsetStore(dir, "src")
	if err != nil {
		t.Fatal(err)
	}
	if s2.Committed() != 5 {
		t.Fatalf("recovered offset = %d, want 5", s2.Committed())
	}
	if err := s2.Commit(5); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("commit recovered offset again: want rollback, got %v", err)
	}
	if err := s2.Commit(6); err != nil {
		t.Fatalf("commit next after recovery: %v", err)
	}
}

// 并发竞争提交：多个 goroutine 抢着推进位点，最终 watermark 必须恰好为 1..target
// 依次各提交一次；除成功路径外的提交都必须收到 rollback/gap 拒绝。
func TestOffsetConcurrentMonotonic(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewOffsetStore(dir, "src")
	const target = int64(100)
	const workers = 16

	var wg sync.WaitGroup
	var mu sync.Mutex
	rejects := map[string]int{}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c := s.Committed()
				if c >= target {
					return
				}
				err := s.Commit(c + 1)
				switch {
				case err == nil:
					// 抢到下一个位点。
				case errors.Is(err, ErrOffsetRollback):
					mu.Lock()
					rejects["rollback"]++
					mu.Unlock()
				case errors.Is(err, ErrOffsetGap):
					mu.Lock()
					rejects["gap"]++
					mu.Unlock()
				default:
					t.Errorf("unexpected commit error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if s.Committed() != target {
		t.Fatalf("final offset = %d, want %d", s.Committed(), target)
	}
	order := s.CommitOrder()
	if len(order) != int(target) {
		t.Fatalf("commit order length = %d, want %d", len(order), target)
	}
	for i, seq := range order {
		if seq != int64(i+1) {
			t.Fatalf("commit order not strictly +1 at index %d: got %d, want %d; order=%v",
				i, seq, i+1, order)
		}
	}
	t.Logf("[位点并发] workers=%d 最终 watermark=%d 成功提交顺序=%v 被拒绝提交(rollback/gap)=%v",
		workers, target, order, rejects)
	if rejects["rollback"]+rejects["gap"] == 0 {
		t.Log("（提示：本轮竞争未产生落败提交，属调度抖动；单调性断言仍成立）")
	}
}

// tWriter 把引擎运行日志（逐条提交位点等）转发到 t.Log，
// 配合 go test -v 即可在单测日志中看到位点、提交顺序与拒绝/落地明细。
type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

var _ io.Writer = tWriter{}

// newTestLogger 构造输出到测试日志的 logger。
func newTestLogger(t *testing.T) *log.Logger {
	return log.New(tWriter{t: t}, "", 0)
}
