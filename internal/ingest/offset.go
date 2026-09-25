// Package ingest 实现断点与并发协同：位点单调提交、中断恢复、回退/跳段拒绝。
package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// 位点提交协议：
//   - 已提交位点（committed watermark）只能单调推进；
//   - 允许提交的下一个位点必须恰好等于 current+1；
//   - 回退（< current）与跳段（> current+1）一律显式拒绝，绝不"从中间继续"；
//   - 位点持久化到本地文件并 fsync，进程重启后从最近一次已提交位点恢复。
var (
	// ErrOffsetRollback 表示尝试把位点回退到更小的值。
	ErrOffsetRollback = errors.New("offset rollback rejected")
	// ErrOffsetGap 表示尝试跳过尚未提交的位点。
	ErrOffsetGap = errors.New("offset gap rejected")
)

// offsetFile 是位点文件的落盘格式。
type offsetFile struct {
	Source    string `json:"source"`
	Committed int64  `json:"committed"`
}

// OffsetStore 管理单个来源的已提交位点，读写均落盘。
// 文件路径：<dir>/offsets/<source>.offset.json，临时文件 + rename 原子替换。
type OffsetStore struct {
	mu          sync.Mutex
	path        string
	source      string
	committed   int64
	commitOrder []int64 // 记录进程内实际成功提交的位点顺序，供测试与日志打印
}

// NewOffsetStore 打开位点文件；文件不存在时位点初始化为 0，存在则恢复。
func NewOffsetStore(dir, source string) (*OffsetStore, error) {
	if dir == "" || source == "" {
		return nil, fmt.Errorf("offset dir and source are required")
	}
	path := filepath.Join(dir, "offsets", source+".offset.json")
	s := &OffsetStore{path: path, source: source}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *OffsetStore) loadLocked() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 首次运行，位点 0
		}
		return err
	}
	var f offsetFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return fmt.Errorf("corrupt offset file %s: %w", s.path, err)
	}
	if f.Source != s.source {
		return fmt.Errorf("offset file %s belongs to source %q, want %q", s.path, f.Source, s.source)
	}
	s.committed = f.Committed
	return nil
}

func (s *OffsetStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	f := offsetFile{Source: s.source, Committed: s.committed}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Committed 返回当前已提交位点。
func (s *OffsetStore) Committed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// Commit 尝试把位点推进到 seq：必须恰好为 committed+1，否则按原因返回错误。
// 提交成功后立即原子落盘并 fsync。
func (s *OffsetStore) Commit(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case seq <= s.committed:
		return fmt.Errorf("%w: current=%d requested=%d", ErrOffsetRollback, s.committed, seq)
	case seq > s.committed+1:
		return fmt.Errorf("%w: current=%d requested=%d (missing %d..%d)",
			ErrOffsetGap, s.committed, seq, s.committed+1, seq-1)
	}
	prev := s.committed
	s.committed = seq
	if err := s.persistLocked(); err != nil {
		s.committed = prev // 落盘失败回滚内存态，保持文件与内存一致
		return err
	}
	s.commitOrder = append(s.commitOrder, seq)
	return nil
}

// CommitOrder 返回进程内实际成功提交的位点顺序快照。
func (s *OffsetStore) CommitOrder() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.commitOrder))
	copy(out, s.commitOrder)
	return out
}
