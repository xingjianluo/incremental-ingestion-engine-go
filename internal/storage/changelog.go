package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ChangelogEntry 是一条已落地变更的流水记录，用于解释最终状态的由来。
type ChangelogEntry struct {
	Seq       int64     `json:"seq"`
	Source    string    `json:"source"`
	Op        string    `json:"op"`
	PK        string    `json:"pk"`
	Action    string    `json:"action"` // inserted / updated / deleted / duplicate-skip
	SchemaVer int       `json:"schema_ver"`
	Detail    string    `json:"detail,omitempty"`
	AppliedAt time.Time `json:"applied_at"`
}

// ChangeLogger 持久化变更流水（只追加）。
type ChangeLogger interface {
	Append(e ChangelogEntry) (appended bool, err error)
	All() ([]ChangelogEntry, error)
}

// FileChangelog 以 JSONL 文件持久化变更流水。
// 文件路径：<dir>/changelog/<source>.changelog.jsonl
type FileChangelog struct {
	mu    sync.Mutex
	path  string
	seen  map[int64]bool
	items []ChangelogEntry
}

// NewFileChangelog 打开（不存在则创建）变更流水文件。
func NewFileChangelog(dir, source string) (*FileChangelog, error) {
	if dir == "" || source == "" {
		return nil, fmt.Errorf("changelog dir and source are required")
	}
	path := filepath.Join(dir, "changelog", source+".changelog.jsonl")
	l := &FileChangelog{path: path, seen: map[int64]bool{}}
	if err := l.loadLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *FileChangelog) loadLocked() error {
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
		var e ChangelogEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("corrupt changelog %s: %w", l.path, err)
		}
		l.items = append(l.items, e)
		l.seen[e.Seq] = true
	}
	return sc.Err()
}

func (l *FileChangelog) Append(e ChangelogEntry) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen[e.Seq] {
		return false, nil // 重放时流水不重复
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return false, err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	data, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return false, err
	}
	if err := f.Sync(); err != nil {
		return false, err
	}
	l.items = append(l.items, e)
	l.seen[e.Seq] = true
	return true, nil
}

func (l *FileChangelog) All() ([]ChangelogEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]ChangelogEntry, len(l.items))
	copy(out, l.items)
	return out, nil
}
