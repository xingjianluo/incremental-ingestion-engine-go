// Package storage 用本地文件模拟数据源（事件日志）与目标端（物化表），
// 同时持久化拒绝记录与变更流水，所有状态均落盘以便重复运行与排查。
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
)

// EventSource 是变更事件来源的读接口。Seq 在来源内从 1 开始连续递增。
type EventSource interface {
	// Read 从 fromSeq（含）开始顺序读取事件，最多 limit 条；无更多数据时返回空切片。
	Read(fromSeq int64, limit int) ([]protocol.ChangeEvent, error)
	// MaxSeq 返回当前已写入的最大 Seq；无数据时返回 0。
	MaxSeq() (int64, error)
	// Name 返回来源名称。
	Name() string
}

// FileEventSource 以本地 JSONL 文件模拟来源事件日志，支持进程重启后重读。
// 文件路径：<dir>/sources/<name>.jsonl，每行一条 ChangeEvent。
type FileEventSource struct {
	mu     sync.Mutex
	path   string
	name   string
	events []protocol.ChangeEvent
	loaded bool
}

// NewFileEventSource 打开（不存在则创建）一个文件事件源，并把历史事件载入内存。
func NewFileEventSource(dir, name string) (*FileEventSource, error) {
	if dir == "" || name == "" {
		return nil, fmt.Errorf("source dir and name are required")
	}
	path := filepath.Join(dir, "sources", name+".jsonl")
	s := &FileEventSource{path: path, name: name}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileEventSource) loadLocked() error {
	s.events = nil
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.loaded = true
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e protocol.ChangeEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("corrupt source file %s: %w", s.path, err)
		}
		s.events = append(s.events, e)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	s.loaded = true
	return nil
}

// Append 向来源追加一条事件并立即刷盘；Seq 必须恰好等于当前 MaxSeq+1，
// 以模拟来源端的连续日志语义。
func (s *FileEventSource) Append(e protocol.ChangeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.SourceName == "" {
		e.SourceName = s.name
	}
	if e.SourceName != s.name {
		return fmt.Errorf("event source name %q does not match log %q", e.SourceName, s.name)
	}
	want := int64(len(s.events)) + 1
	if e.Seq != want {
		return fmt.Errorf("event seq must be contiguous: want %d, got %d", want, e.Seq)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	s.events = append(s.events, e)
	return nil
}

// Read 实现 EventSource。fromSeq 从 1 开始计数。
func (s *FileEventSource) Read(fromSeq int64, limit int) ([]protocol.ChangeEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fromSeq < 1 {
		return nil, fmt.Errorf("fromSeq must be >= 1, got %d", fromSeq)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0, got %d", limit)
	}
	start := int(fromSeq - 1)
	if start > len(s.events) {
		start = len(s.events)
	}
	end := len(s.events)
	if start+limit < end {
		end = start + limit
	}
	out := make([]protocol.ChangeEvent, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, s.events[i])
	}
	return out, nil
}

// MaxSeq 实现 EventSource。
func (s *FileEventSource) MaxSeq() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.events)), nil
}

// Name 实现 EventSource。
func (s *FileEventSource) Name() string { return s.name }
