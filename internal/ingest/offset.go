package ingest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrOffsetRegression 表示位点回退。
var ErrOffsetRegression = errors.New("offset regression: committed offset must be monotonic")

// ErrOffsetGap 表示位点跳段。
var ErrOffsetGap = errors.New("offset gap: commit must continue from the last committed offset")

// OffsetStore 持久化已提交位点，保证单调推进。
// 提交采用 CAS：Commit(prev, next) 要求 prev 等于当前已提交位点，
// 因此回退（next <= committed）与跳段（prev 落后于 committed）都会被拒绝。
type OffsetStore struct {
	path      string
	mu        sync.Mutex
	committed uint64
}

// NewOffsetStore 创建位点存储。
func NewOffsetStore(path string) *OffsetStore { return &OffsetStore{path: path} }

// Load 恢复最近已提交位点（进程中断后从这里恢复）。
func (s *OffsetStore) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.committed = 0
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("corrupt offset file %s: %w", s.path, err)
	}
	s.committed = v
	return v, nil
}

// Commit 以 CAS 方式提交位点：prev 必须等于当前已提交位点，next 必须大于 prev。
func (s *OffsetStore) Commit(prev, next uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev != s.committed {
		// prev 落后（并发竞争失败）或超前（跳段）都拒绝。
		return fmt.Errorf("%w: prev=%d committed=%d", ErrOffsetGap, prev, s.committed)
	}
	if next <= s.committed {
		return fmt.Errorf("%w: next=%d committed=%d", ErrOffsetRegression, next, s.committed)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(next, 10)), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	s.committed = next
	return nil
}

// Committed 返回当前已提交位点。
func (s *OffsetStore) Committed() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}
