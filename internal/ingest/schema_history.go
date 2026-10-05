package ingest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SchemaChange 记录一次结构版本注册尝试（无论兼容与否，都留痕、可追溯、重启可查）。
// 即使是被判定为不能安全兼容的版本，也能凭 Fields 快照解释
// “某条记录为什么被判成结构不兼容”。
type SchemaChange struct {
	Version  int       `json:"version"`  // 尝试注册的版本号
	Accepted bool      `json:"accepted"` // 该次演进是否被接受（兼容）
	Detail   string    `json:"detail,omitempty"`
	Fields   []Field   `json:"fields"` // 该版本的字段快照
	At       time.Time `json:"at"`     // 尝试时间（仅展示用，不参与对账计数）
}

// NewSchemaRegistryWithHistory 创建带磁盘留痕的结构注册表：
// 所有注册尝试（含被拒绝的不兼容演进）都会追加到 historyPath（JSONL），
// 进程重启后通过 History/HistoryForVersion 依然可查。
// 文件不存在时从空历史开始。
func NewSchemaRegistryWithHistory(historyPath string) (*SchemaRegistry, error) {
	r := NewSchemaRegistry()
	r.historyPath = historyPath
	f, err := os.Open(historyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var c SchemaChange
		if err := json.Unmarshal(line, &c); err != nil {
			return nil, fmt.Errorf("corrupt schema history %s: %w", historyPath, err)
		}
		r.history = append(r.history, c)
	}
	return r, sc.Err()
}

// recordChangeLocked 记录一次注册尝试。
// 同一（版本、是否接受、详情、字段集）的尝试只留一次，
// 因此进程重启后重复注册同一批结构不会产生重复流水。
func (r *SchemaRegistry) recordChangeLocked(c SchemaChange) error {
	c.Fields = append([]Field(nil), c.Fields...)
	sig := c.signature()
	for _, old := range r.history {
		if old.signature() == sig {
			return nil
		}
	}
	if c.At.IsZero() {
		c.At = time.Now().UTC()
	}
	r.history = append(r.history, c)
	if r.historyPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(r.historyPath), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(r.historyPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// signature 是去重签名：时间戳不参与（同内容尝试视为重复）。
func (c SchemaChange) signature() string {
	b, _ := json.Marshal(struct {
		Version  int
		Accepted bool
		Detail   string
		Fields   []Field
	}{c.Version, c.Accepted, c.Detail, c.Fields})
	return string(b)
}

// History 返回按尝试先后排列的结构变更流水（副本）。
func (r *SchemaRegistry) History() []SchemaChange {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SchemaChange, len(r.history))
	copy(out, r.history)
	return out
}

// HistoryForVersion 返回指定版本的全部注册尝试（含被拒绝的尝试）。
func (r *SchemaRegistry) HistoryForVersion(version int) []SchemaChange {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []SchemaChange
	for _, c := range r.history {
		if c.Version == version {
			out = append(out, c)
		}
	}
	return out
}
