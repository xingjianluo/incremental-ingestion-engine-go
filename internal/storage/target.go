package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/xingjianluo/incremental-ingestion-engine-go/internal/protocol"
)

// TargetStore 是目标端物化表接口，以本地文件持久化。
// Apply 必须幂等：同一事件（以 Seq 标识）重复落地不得产生重复或丢失。
type TargetStore interface {
	// Apply 将一条事件应用到目标端。
	// dup=true 表示该 Seq 已应用过（重复投递，调用方应跳过而非视为冲突）。
	// conflict=true 仅对 insert 操作出现：同主键的*不同*记录已存在。
	Apply(ev protocol.ChangeEvent, pk string, data map[string]interface{}) (dup, conflict bool, err error)
	// Get 按主键读取当前物化记录（不含内部元数据）。
	Get(pk string) (map[string]interface{}, bool, error)
	// All 返回全部物化记录的快照（不含内部元数据）。
	All() (map[string]map[string]interface{}, error)
	// Count 返回记录条数。
	Count() (int, error)
	// BackfillDefaults 为所有现存行补上缺失的新字段：有缺省值用缺省值，否则补 nil。
	// 返回被补全的行数，结果立即持久化。
	BackfillDefaults(fields []protocol.Field) (int, error)
	// PrepareBatch 在并发处理之前、于锁内按 seq 顺序预解析一批事件，
	// 确定性地给出每个 seq 的处置：dup（已应用，重投）、conflict（insert 主键冲突，
	// 含批内同主键多个 insert 的情况，最小 seq 胜出）、ok。
	// 预解析不修改任何状态，只用于消除并发应用顺序带来的二义。
	PrepareBatch(evs []protocol.ChangeEvent, pks []string) BatchPlan
}

// SeqDisposition 是单个 seq 在批次中的预解析处置。
type SeqDisposition struct {
	Dup      bool
	Conflict bool
}

// BatchPlan 是一批事件的确定性预解析结果。
type BatchPlan struct {
	BySeq map[int64]SeqDisposition
}

// PrepareBatch 实现 TargetStore。
func (s *FileTargetStore) PrepareBatch(evs []protocol.ChangeEvent, pks []string) BatchPlan {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan := BatchPlan{BySeq: map[int64]SeqDisposition{}}
	// 模拟应用整批后的"存活行"，初始为当前已持久化的主键集合。
	alive := map[string]bool{}
	for pk := range s.state.Rows {
		alive[pk] = true
	}
	for i, ev := range evs {
		pk := pks[i]
		if s.state.AppliedSeq[ev.Seq] {
			// 已应用过：重投，按幂等跳过（连冲突都不再报）。
			plan.BySeq[ev.Seq] = SeqDisposition{Dup: true}
			continue
		}
		switch ev.Op {
		case protocol.OpInsert:
			if alive[pk] {
				// 批内更早的同主键事件或历史行已占用该键：确定性冲突。
				plan.BySeq[ev.Seq] = SeqDisposition{Conflict: true}
				continue
			}
			alive[pk] = true
		case protocol.OpUpsert:
			alive[pk] = true
		case protocol.OpDelete:
			delete(alive, pk)
		}
	}
	return plan
}

// targetState 是目标端落盘结构。
type targetState struct {
	Source     string                            `json:"source"`
	Rows       map[string]map[string]interface{} `json:"rows"`
	AppliedSeq map[int64]bool                    `json:"applied_seq"`
	LastSeq    int64                             `json:"last_seq"`
}

// FileTargetStore 以 JSON 文件持久化的本地目标端。
// 文件路径：<dir>/targets/<source>.json，写入采用临时文件 + rename 原子替换。
type FileTargetStore struct {
	mu     sync.Mutex
	path   string
	source string
	state  targetState
}

// NewFileTargetStore 打开（不存在则初始化）来源对应的目标端存储。
func NewFileTargetStore(dir, source string) (*FileTargetStore, error) {
	if dir == "" || source == "" {
		return nil, fmt.Errorf("target dir and source are required")
	}
	path := filepath.Join(dir, "targets", source+".json")
	s := &FileTargetStore{path: path, source: source}
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *FileTargetStore) loadLocked() error {
	s.state = targetState{
		Source:     s.source,
		Rows:       map[string]map[string]interface{}{},
		AppliedSeq: map[int64]bool{},
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, &s.state); err != nil {
		return fmt.Errorf("corrupt target file %s: %w", s.path, err)
	}
	if s.state.Rows == nil {
		s.state.Rows = map[string]map[string]interface{}{}
	}
	if s.state.AppliedSeq == nil {
		s.state.AppliedSeq = map[int64]bool{}
	}
	return nil
}

func (s *FileTargetStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
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
	return nil
}

func (s *FileTargetStore) Apply(ev protocol.ChangeEvent, pk string, data map[string]interface{}) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 同一 Seq 已应用：重复投递，幂等跳过。
	if s.state.AppliedSeq[ev.Seq] {
		return true, false, nil
	}

	conflict := false
	switch ev.Op {
	case protocol.OpInsert:
		if _, exists := s.state.Rows[pk]; exists {
			// 冲突：不改动任何状态、不登记 appliedSeq、不落盘。
			// 因此重复投递同一冲突事件时仍会稳定地按主键冲突被拒绝，原因可区分。
			conflict = true
			break
		}
		row := cloneMap(data)
		s.state.Rows[pk] = row
	case protocol.OpUpsert:
		s.state.Rows[pk] = cloneMap(data)
	case protocol.OpDelete:
		delete(s.state.Rows, pk)
	default:
		return false, false, fmt.Errorf("unknown op type %q for seq %d", ev.Op, ev.Seq)
	}

	if conflict {
		return false, true, nil
	}

	// 只有真正应用成功的事件才登记幂等标记。
	s.state.AppliedSeq[ev.Seq] = true
	if ev.Seq > s.state.LastSeq {
		s.state.LastSeq = ev.Seq
	}
	if err := s.persistLocked(); err != nil {
		return false, false, err
	}
	return false, false, nil
}

func cloneMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *FileTargetStore) Get(pk string) (map[string]interface{}, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.state.Rows[pk]
	if !ok {
		return nil, false, nil
	}
	return cloneMap(row), true, nil
}

func (s *FileTargetStore) All() (map[string]map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]interface{}, len(s.state.Rows))
	for pk, row := range s.state.Rows {
		out[pk] = cloneMap(row)
	}
	return out, nil
}

func (s *FileTargetStore) Count() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state.Rows), nil
}

// BackfillDefaults 为现存行补全新增字段的稳定缺省值。
func (s *FileTargetStore) BackfillDefaults(fields []protocol.Field) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	touched := 0
	for pk, row := range s.state.Rows {
		changed := false
		for _, f := range fields {
			if _, present := row[f.Name]; present {
				continue
			}
			if f.HasDefault {
				row[f.Name] = f.Default
			} else {
				row[f.Name] = nil
			}
			changed = true
		}
		if changed {
			s.state.Rows[pk] = row
			touched++
		}
	}
	if touched > 0 {
		if err := s.persistLocked(); err != nil {
			return 0, err
		}
	}
	return touched, nil
}
