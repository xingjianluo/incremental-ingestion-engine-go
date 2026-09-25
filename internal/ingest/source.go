package ingest

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// Source 以本地 JSONL 文件模拟数据源（变更流水）。
// 每行一个 Event，行号（从 0 开始）即事件位点。
type Source struct {
	path string
	mu   sync.Mutex
}

// NewSource 创建本地文件数据源。
func NewSource(path string) *Source { return &Source{path: path} }

// Append 向来源追加事件（模拟上游写入）。
func (s *Source) Append(events ...Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.Flush()
}

// ReadFrom 从位点 offset 开始读取最多 limit 条事件。
// limit <= 0 表示读到末尾。
func (s *Source) ReadFrom(offset uint64, limit int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Event
	var idx uint64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if idx >= offset {
			var e Event
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, err
			}
			out = append(out, e)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		idx++
	}
	return out, sc.Err()
}
