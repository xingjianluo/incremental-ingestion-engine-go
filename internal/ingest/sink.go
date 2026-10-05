package ingest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Row 是目标端的一行数据。
type Row struct {
	PK     string         `json:"pk"`
	Fields map[string]any `json:"fields"`
}

// Sink 以本地 JSON 文件模拟目标端，按主键幂等落地。
// 幂等保证：同一事件 ID 只应用一次；同一主键上 Seq 不大于已应用 Seq 的事件被跳过。
type Sink struct {
	dataPath   string
	rejectPath string

	mu          sync.Mutex
	rows        map[string]Row
	lastSeqByPK map[string]uint64
	appliedIDs  map[string]struct{}
	rejections  []Rejection
}

// NewSink 创建本地文件目标端。
func NewSink(dataPath, rejectPath string) *Sink {
	return &Sink{dataPath: dataPath, rejectPath: rejectPath}
}

type sinkState struct {
	Rows        map[string]Row    `json:"rows"`
	LastSeqByPK map[string]uint64 `json:"lastSeqByPk"`
	AppliedIDs  []string          `json:"appliedIds"`
}

// Load 从磁盘恢复目标端状态。
func (s *Sink) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = map[string]Row{}
	s.lastSeqByPK = map[string]uint64{}
	s.appliedIDs = map[string]struct{}{}
	s.rejections = nil

	b, err := os.ReadFile(s.dataPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && len(b) > 0 {
		var st sinkState
		if err := json.Unmarshal(b, &st); err != nil {
			return err
		}
		s.rows = st.Rows
		s.lastSeqByPK = st.LastSeqByPK
		for _, id := range st.AppliedIDs {
			s.appliedIDs[id] = struct{}{}
		}
	}
	// 恢复死信记录。
	f, err := os.Open(s.rejectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Rejection
		if err := json.Unmarshal(line, &r); err != nil {
			return err
		}
		s.rejections = append(s.rejections, r)
	}
	return sc.Err()
}

// Apply 幂等地应用一批事件（Fields 应已投影到最新结构）。
// 返回本批实际生效（非重复）的事件数。
func (s *Sink) Apply(events []Event) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	applied := 0
	for _, e := range events {
		if _, dup := s.appliedIDs[e.ID]; dup {
			continue // 重复消费：同一事件 ID 只应用一次
		}
		if last, ok := s.lastSeqByPK[e.PK]; ok && e.Seq <= last {
			continue // 同一主键上的过期事件
		}
		switch e.Op {
		case OpInsert, OpUpdate:
			s.rows[e.PK] = Row{PK: e.PK, Fields: e.Fields}
		case OpDelete:
			delete(s.rows, e.PK)
		}
		s.lastSeqByPK[e.PK] = e.Seq
		s.appliedIDs[e.ID] = struct{}{}
		applied++
	}
	if err := s.persistLocked(); err != nil {
		return 0, err
	}
	return applied, nil
}

// ApplyResubmit 原子地应用一批修复重投的事件（调用方已完成校验与投影，
// 且批内同一主键已确定唯一候选）。与 Apply 的区别：撞上目标端已存在
// 主键且无法凭来源顺序安全判定先后（seq <= 该主键已应用位点）时，
// 不静默跳过，而是返回 RejectPKConflict 拒绝记录；绝不改写已有数据。
// 返回：实际生效的事件 ID、幂等跳过的事件 ID（此前已应用过）、冲突拒绝记录。
func (s *Sink) ApplyResubmit(events []Event) (applied, skipped []string, conflicts []Rejection, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		if _, dup := s.appliedIDs[e.ID]; dup {
			skipped = append(skipped, e.ID) // 同一修正数据重复提交：幂等跳过
			continue
		}
		if last, ok := s.lastSeqByPK[e.PK]; ok && e.Seq <= last {
			conflicts = append(conflicts, Rejection{
				EventID: e.ID, Seq: e.Seq,
				Reason: RejectPKConflict,
				Detail: fmt.Sprintf("pk %q already applied at seq %d, cannot safely order resubmitted seq %d; existing row left untouched",
					e.PK, last, e.Seq),
			})
			continue
		}
		switch e.Op {
		case OpInsert, OpUpdate:
			s.rows[e.PK] = Row{PK: e.PK, Fields: e.Fields}
		case OpDelete:
			delete(s.rows, e.PK)
		}
		s.lastSeqByPK[e.PK] = e.Seq
		s.appliedIDs[e.ID] = struct{}{}
		applied = append(applied, e.ID)
	}
	if err := s.persistLocked(); err != nil {
		return nil, nil, nil, err
	}
	return applied, skipped, conflicts, nil
}

// HasApplied 报告事件 ID 是否已应用过（对账与重投判定用）。
func (s *Sink) HasApplied(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.appliedIDs[id]
	return ok
}

// LastSeqByPK 返回各主键已应用的最大来源位点（对账用，返回副本）。
func (s *Sink) LastSeqByPK() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64, len(s.lastSeqByPK))
	for k, v := range s.lastSeqByPK {
		out[k] = v
	}
	return out
}

// AppliedIDs 返回全部已应用事件 ID（对账用，返回副本）。
func (s *Sink) AppliedIDs() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]struct{}, len(s.appliedIDs))
	for id := range s.appliedIDs {
		out[id] = struct{}{}
	}
	return out
}

func (s *Sink) persistLocked() error {
	ids := make([]string, 0, len(s.appliedIDs))
	for id := range s.appliedIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	st := sinkState{Rows: s.rows, LastSeqByPK: s.lastSeqByPK, AppliedIDs: ids}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.dataPath), 0o755); err != nil {
		return err
	}
	tmp := s.dataPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.dataPath)
}

// AddRejections 把被拒绝的记录追加到死信文件（原因可区分、可追溯）。
func (s *Sink) AddRejections(rs []Rejection) error {
	if len(rs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.rejectPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.rejectPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, r := range rs {
		// 死信去重：同一事件同一原因只记一次（重复消费时保持可预期）。
		dup := false
		for _, old := range s.rejections {
			if old.EventID == r.EventID && old.Reason == r.Reason {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
		s.rejections = append(s.rejections, r)
	}
	return w.Flush()
}

// Rows 返回当前目标端全部行（按主键排序）。
func (s *Sink) Rows() []Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Row, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PK < out[j].PK })
	return out
}

// Rejections 返回累计的拒绝记录。
func (s *Sink) Rejections() []Rejection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Rejection, len(s.rejections))
	copy(out, s.rejections)
	return out
}

// BackfillDefaults 为存量行补齐新增字段的缺省值（结构演进时调用）。
func (s *Sink) BackfillDefaults(fields []Field) error {
	if len(fields) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for pk, row := range s.rows {
		for _, f := range fields {
			if _, ok := row.Fields[f.Name]; !ok {
				row.Fields[f.Name] = f.Default
			}
		}
		s.rows[pk] = row
	}
	return s.persistLocked()
}
