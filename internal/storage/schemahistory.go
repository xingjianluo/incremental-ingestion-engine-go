package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
)

// SchemaChangeEvent 记录一次结构变化的处置结果，保证结构变化可追溯。
// 兼容变化会持久化完整 Schema，使进程重启后可从磁盘重建注册表。
type SchemaChangeEvent struct {
	Source         string           `json:"source"`
	FromVersion    int              `json:"from_version"`
	ToVersion      int              `json:"to_version"`
	Compatible     bool             `json:"compatible"`
	Description    string           `json:"description"`
	BackfilledRows int              `json:"backfilled_rows"`
	Schema         *protocol.Schema `json:"schema,omitempty"`
	OccurredAt     time.Time        `json:"occurred_at"`
}

// SchemaHistory 以只追加 JSONL 持久化结构变化历史（包括被拒绝的不兼容变化）。
type SchemaHistory struct {
	mu    sync.Mutex
	path  string
	items []SchemaChangeEvent
}

// NewSchemaHistory 打开（不存在则创建）结构变化历史文件。
func NewSchemaHistory(dir, source string) (*SchemaHistory, error) {
	if dir == "" || source == "" {
		return nil, fmt.Errorf("schema history dir and source are required")
	}
	path := filepath.Join(dir, "schemas", source+".schema-history.jsonl")
	h := &SchemaHistory{path: path}
	if err := h.loadLocked(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *SchemaHistory) loadLocked() error {
	f, err := os.Open(h.path)
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
		var e SchemaChangeEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("corrupt schema history %s: %w", h.path, err)
		}
		h.items = append(h.items, e)
	}
	return sc.Err()
}

// Append 追加一条结构变化记录并刷盘。
func (h *SchemaHistory) Append(e SchemaChangeEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(h.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(h.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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
	h.items = append(h.items, e)
	return nil
}

// All 返回全部结构变化记录。
func (h *SchemaHistory) All() ([]SchemaChangeEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]SchemaChangeEvent, len(h.items))
	copy(out, h.items)
	return out, nil
}

// RestoreRegistry 依据磁盘上的结构变更历史，按版本顺序重建内存注册表。
// 仅回放 compatible=true 且带完整 Schema 的记录；被拒绝的不兼容版本跳过。
// 重建会再次经过兼容性校验，保证恢复出的注册表自洽。
func (h *SchemaHistory) RestoreRegistry(reg *protocol.SchemaRegistry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.items {
		if !e.Compatible || e.Schema == nil {
			continue // 跳过被拒绝的不兼容版本
		}
		if err := reg.Register(e.Schema); err != nil {
			return fmt.Errorf("restore schema source=%s v%d: %w", e.Source, e.ToVersion, err)
		}
	}
	return nil
}
