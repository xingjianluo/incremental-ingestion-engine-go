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

// RepairBatchResult 是一批修复记录在目标端的处置结果。
// 同一批次内同一主键的冲突裁决在 Sink 锁内原子完成，
// 因此重复运行与并发运行得到完全一致的结果。
type RepairBatchResult struct {
	Applied   []string    // 本次新生效的事件 ID
	Skipped   []string    // 事件 ID 已生效过（重复重投，幂等跳过）
	Stale     []string    // seq 旧于该主键已生效 seq（确定性过期，不改写新数据）
	Conflicts []Rejection // 主键冲突：无法凭来源顺序安全裁决先后
}

// ApplyRepairs 落地一批修复记录（Fields 应已投影到最新结构）。
//
// 确定性裁决规则：
//   - 已生效过的事件 ID：跳过（重复重投不产生重复数据）；
//   - 同一批内同一主键只允许一条记录生效：seq 最大者胜出，
//     seq 相同则事件 ID 字典序最小者胜出，再相同取批次内最先出现者；
//     其余同主键记录一律以 pk_conflict 拒绝（谁生效、谁被拒与文件顺序/并发无关）；
//   - 胜出记录若该主键从未落地：生效；
//   - 胜出记录 seq 新于该主键已生效 seq：生效（可凭来源顺序安全判定更新）；
//   - seq 等于已生效 seq：同一来源位置却是另一条已生效记录，无法安全判定先后
//     -> pk_conflict 拒绝，绝不改写已有数据；
//   - seq 旧于已生效 seq：确定性过期（stale），不回滚更新的数据。
func (s *Sink) ApplyRepairs(events []Event) (RepairBatchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out RepairBatchResult

	// 1) 每个主键在批次内选出唯一胜出者（与输入顺序无关的确定性裁决）。
	winnerOf := map[string]int{} // pk -> 在 events 中的下标
	for i, e := range events {
		w, ok := winnerOf[e.PK]
		if !ok || repairPrecedes(events[w], e) {
			winnerOf[e.PK] = i
		}
	}

	// 2) 逐条裁决，全部在锁内完成，避免 check-then-apply 竞争。
	for i, e := range events {
		if _, dup := s.appliedIDs[e.ID]; dup {
			out.Skipped = append(out.Skipped, e.ID)
			continue
		}
		if winnerOf[e.PK] != i {
			w := events[winnerOf[e.PK]]
			out.Conflicts = append(out.Conflicts, Rejection{
				EventID: e.ID, Seq: e.Seq, Reason: RejectPKConflict,
				Detail: fmt.Sprintf("pk %q appears multiple times in one repair batch; winner is event %q (seq=%d); duplicate corrective write rejected",
					e.PK, w.ID, w.Seq),
			})
			continue
		}
		if last, exists := s.lastSeqByPK[e.PK]; exists {
			switch {
			case e.Seq < last:
				out.Stale = append(out.Stale, e.ID)
				continue
			case e.Seq == last:
				out.Conflicts = append(out.Conflicts, Rejection{
					EventID: e.ID, Seq: e.Seq, Reason: RejectPKConflict,
					Detail: fmt.Sprintf("pk %q already applied at the same source seq=%d by another event; precedence cannot be decided safely, existing data kept",
						e.PK, last),
				})
				continue
			}
		}
		switch e.Op {
		case OpInsert, OpUpdate:
			s.rows[e.PK] = Row{PK: e.PK, Fields: e.Fields}
		case OpDelete:
			delete(s.rows, e.PK)
		}
		s.lastSeqByPK[e.PK] = e.Seq
		s.appliedIDs[e.ID] = struct{}{}
		out.Applied = append(out.Applied, e.ID)
	}
	if err := s.persistLocked(); err != nil {
		return RepairBatchResult{}, err
	}
	return out, nil
}

// repairPrecedes 报告在“同主键取最新”的裁决中，候选 c 是否应取代当前胜者 cur：
// seq 更大者胜；seq 相同则事件 ID 字典序更小者胜（结果确定、可复现）。
func repairPrecedes(cur, c Event) bool {
	if c.Seq != cur.Seq {
		return c.Seq > cur.Seq
	}
	if c.ID != cur.ID {
		return c.ID < cur.ID
	}
	return false // 完全同 seq 同 ID：保留先出现者
}

// Applied 报告某事件 ID 是否已经生效。
func (s *Sink) Applied(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.appliedIDs[id]
	return ok
}

// LastSeq 返回某主键已生效的最大来源 seq。
func (s *Sink) LastSeq(pk string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.lastSeqByPK[pk]
	return v, ok
}

// HasRow 报告目标端当前是否存在该主键的行（delete 后为 false）。
func (s *Sink) HasRow(pk string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[pk]
	return ok
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
