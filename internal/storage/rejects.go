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

// RejectLog 持久化被拒绝的记录及其可区分原因，保证不静默丢弃。
type RejectLog interface {
	// Append 记录一条拒绝；同一事件 Seq 重复拒绝时幂等跳过（appended=false）。
	Append(rec protocol.RejectedRecord) (appended bool, err error)
	// All 返回全部拒绝记录，按 Seq 升序。
	All() ([]protocol.RejectedRecord, error)
}

// FileRejectLog 以 JSONL 文件持久化拒绝记录（只追加）。
// 文件路径：<dir>/rejects/<source>.rejects.jsonl
type FileRejectLog struct {
	mu    sync.Mutex
	path  string
	seen  map[int64]bool
	items []protocol.RejectedRecord
}

// NewFileRejectLog 打开（不存在则创建）拒绝记录文件。
func NewFileRejectLog(dir, source string) (*FileRejectLog, error) {
	if dir == "" || source == "" {
		return nil, fmt.Errorf("reject log dir and source are required")
	}
	path := filepath.Join(dir, "rejects", source+".rejects.jsonl")
	l := &FileRejectLog{path: path, seen: map[int64]bool{}}
	if err := l.loadLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *FileRejectLog) loadLocked() error {
	f, err := os.Open(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var rec protocol.RejectedRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return fmt.Errorf("corrupt reject log %s: %w", l.path, err)
		}
		l.items = append(l.items, rec)
		l.seen[rec.Event.Seq] = true
	}
	return sc.Err()
}

func (l *FileRejectLog) Append(rec protocol.RejectedRecord) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen[rec.Event.Seq] {
		return false, nil // 重复运行时同一坏记录只记一次
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return false, err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	data, err := json.Marshal(rec)
	if err != nil {
		return false, err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return false, err
	}
	if err := f.Sync(); err != nil {
		return false, err
	}
	l.items = append(l.items, rec)
	l.seen[rec.Event.Seq] = true
	return true, nil
}

func (l *FileRejectLog) All() ([]protocol.RejectedRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]protocol.RejectedRecord, len(l.items))
	copy(out, l.items)
	return out, nil
}
