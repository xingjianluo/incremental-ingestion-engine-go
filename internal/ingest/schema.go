package ingest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ErrIncompatibleSchema 表示无法安全兼容的结构变化。
var ErrIncompatibleSchema = errors.New("incompatible schema change")

// FieldType 是字段类型。
type FieldType string

const (
	TypeString FieldType = "string"
	TypeInt    FieldType = "int"
	TypeFloat  FieldType = "float"
	TypeBool   FieldType = "bool"
)

// Field 描述一个字段。
type Field struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	// HasDefault 为 true 时 Default 生效；新增字段必须显式给出缺省值，
	// 以保证缺省语义确定且稳定。
	HasDefault bool `json:"hasDefault,omitempty"`
	Default    any  `json:"default,omitempty"`
}

// Schema 是一个版本的来源结构。
type Schema struct {
	Version int     `json:"version"`
	Fields  []Field `json:"fields"`
}

func (s Schema) field(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// SchemaRegistry 管理结构版本并校验兼容性。
// 若构造时给定 ledgerPath，所有注册尝试（含被判定不兼容的）都会
// 追加到本地 JSONL 变更流水，进程重启后经 Load 恢复，依然可查可解释。
type SchemaRegistry struct {
	mu         sync.Mutex
	schemas    map[int]Schema
	latest     int
	ledgerPath string
	attempts   []SchemaAttempt
}

// NewSchemaRegistry 创建注册表；可选传入变更流水文件路径以持久化所有注册尝试。
func NewSchemaRegistry(ledgerPath ...string) *SchemaRegistry {
	r := &SchemaRegistry{schemas: map[int]Schema{}}
	if len(ledgerPath) > 0 {
		r.ledgerPath = ledgerPath[0]
	}
	return r
}

// Load 从变更流水恢复：重放已接受的版本重建注册表，并恢复全部尝试记录。
func (r *SchemaRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas = map[int]Schema{}
	r.latest = 0
	r.attempts = nil
	if r.ledgerPath == "" {
		return nil
	}
	f, err := os.Open(r.ledgerPath)
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
		var a SchemaAttempt
		if err := json.Unmarshal(line, &a); err != nil {
			return fmt.Errorf("corrupt schema ledger %s: %w", r.ledgerPath, err)
		}
		r.attempts = append(r.attempts, a)
		if a.Accepted {
			r.schemas[a.Version] = Schema{Version: a.Version, Fields: a.Fields}
			if a.Version > r.latest {
				r.latest = a.Version
			}
		}
	}
	return sc.Err()
}

// Attempts 返回全部注册尝试（含被拒绝的），按发生顺序排列。
func (r *SchemaRegistry) Attempts() []SchemaAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SchemaAttempt, len(r.attempts))
	copy(out, r.attempts)
	return out
}

// recordAttemptLocked 记录一次注册尝试并追加到变更流水（若启用）。
func (r *SchemaRegistry) recordAttemptLocked(s Schema, err error) error {
	a := SchemaAttempt{Version: s.Version, Fields: s.Fields, Accepted: err == nil}
	if err != nil {
		a.Reason = err.Error()
	}
	r.attempts = append(r.attempts, a)
	if r.ledgerPath == "" {
		return nil
	}
	if mkErr := os.MkdirAll(filepath.Dir(r.ledgerPath), 0o755); mkErr != nil {
		return mkErr
	}
	f, mkErr := os.OpenFile(r.ledgerPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if mkErr != nil {
		return mkErr
	}
	defer f.Close()
	b, mkErr := json.Marshal(a)
	if mkErr != nil {
		return mkErr
	}
	if _, mkErr := f.Write(append(b, '\n')); mkErr != nil {
		return mkErr
	}
	return nil
}

// Register 注册新版本结构；不兼容的演进返回 ErrIncompatibleSchema 及可追溯详情。
// 无论接受与否，本次尝试都会记入变更流水（若启用），重启后仍可追溯。
// 兼容规则：
//   - 新增字段：必须显式携带缺省值（HasDefault），允许；
//   - 删除字段：拒绝（可能造成目标端静默错列）；
//   - 字段改名：表现为“删除+新增”，同样拒绝，并在详情中提示疑似改名；
//   - 已有字段类型变更、或由可选变为必需：拒绝。
func (r *SchemaRegistry) Register(s Schema) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.registerLocked(s)
	if recErr := r.recordAttemptLocked(s, err); recErr != nil && err == nil {
		return recErr
	}
	return err
}

func (r *SchemaRegistry) registerLocked(s Schema) error {
	prev, ok := r.schemas[r.latest]
	if !ok {
		if s.Version != 1 {
			return fmt.Errorf("%w: first schema version must be 1, got %d", ErrIncompatibleSchema, s.Version)
		}
		r.schemas[s.Version] = s
		r.latest = s.Version
		return nil
	}
	if s.Version != prev.Version+1 {
		return fmt.Errorf("%w: version must increase by 1 (prev=%d got=%d)", ErrIncompatibleSchema, prev.Version, s.Version)
	}
	for _, pf := range prev.Fields {
		nf, found := s.field(pf.Name)
		if !found {
			// 检查是否疑似改名：存在同类型的新增字段。
			hint := ""
			for _, c := range s.Fields {
				if _, existed := prev.field(c.Name); !existed && c.Type == pf.Type {
					hint = fmt.Sprintf(" (possible rename to %q)", c.Name)
					break
				}
			}
			return fmt.Errorf("%w: field %q removed in version %d%s", ErrIncompatibleSchema, pf.Name, s.Version, hint)
		}
		if nf.Type != pf.Type {
			return fmt.Errorf("%w: field %q type changed %s -> %s in version %d",
				ErrIncompatibleSchema, pf.Name, pf.Type, nf.Type, s.Version)
		}
		if nf.Required && !pf.Required {
			return fmt.Errorf("%w: field %q became required in version %d", ErrIncompatibleSchema, pf.Name, s.Version)
		}
	}
	for _, nf := range s.Fields {
		if _, existed := prev.field(nf.Name); !existed && !nf.HasDefault {
			return fmt.Errorf("%w: new field %q in version %d must declare an explicit default",
				ErrIncompatibleSchema, nf.Name, s.Version)
		}
	}
	r.schemas[s.Version] = s
	r.latest = s.Version
	return nil
}

// Get 返回指定版本的结构。
func (r *SchemaRegistry) Get(version int) (Schema, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schemas[version]
	return s, ok
}

// Latest 返回最新版本结构。
func (r *SchemaRegistry) Latest() (Schema, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schemas[r.latest]
	return s, ok
}

// checkType 校验 JSON 解码后的值是否匹配字段类型。
func checkType(ft FieldType, v any) bool {
	switch ft {
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	case TypeInt:
		f, ok := v.(float64)
		return ok && f == float64(int64(f))
	case TypeFloat:
		_, ok := v.(float64)
		return ok
	default:
		return false
	}
}

// Validate 按事件自身声明的结构版本校验记录：
// 必需字段缺失或类型不匹配时返回可区分的拒绝原因。
func (r *SchemaRegistry) Validate(e Event) *Rejection {
	schema, ok := r.Get(e.SchemaVersion)
	if !ok {
		return &Rejection{EventID: e.ID, Seq: e.Seq, Reason: RejectUnknownSchema,
			Detail: fmt.Sprintf("schema version %d not registered", e.SchemaVersion)}
	}
	for _, f := range schema.Fields {
		v, present := e.Fields[f.Name]
		if !present || v == nil {
			if f.Required {
				return &Rejection{EventID: e.ID, Seq: e.Seq, Reason: RejectMissingField,
					Detail: fmt.Sprintf("required field %q missing (schema v%d)", f.Name, schema.Version)}
			}
			continue
		}
		if !checkType(f.Type, v) {
			return &Rejection{EventID: e.ID, Seq: e.Seq, Reason: RejectTypeMismatch,
				Detail: fmt.Sprintf("field %q expects %s, got %T(%v)", f.Name, f.Type, v, v)}
		}
	}
	return nil
}

// Project 把事件载荷投影到最新结构：
//   - 缺失的新增字段填入其声明的缺省值（语义确定且稳定）；
//   - 载荷中出现最新结构里不存在的字段（来源于已删除/改名字段）时，
//     返回 ErrIncompatibleSchema，绝不静默错列。
func (r *SchemaRegistry) Project(e Event) (map[string]any, error) {
	latest, ok := r.Latest()
	if !ok {
		return nil, fmt.Errorf("%w: no schema registered", ErrIncompatibleSchema)
	}
	out := make(map[string]any, len(latest.Fields))
	for _, f := range latest.Fields {
		if v, present := e.Fields[f.Name]; present && v != nil {
			out[f.Name] = v
		} else if f.HasDefault {
			out[f.Name] = f.Default
		}
	}
	var extra []string
	for name := range e.Fields {
		if _, ok := latest.field(name); !ok {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return nil, fmt.Errorf("%w: event %s (schema v%d) carries fields %v absent from latest schema v%d",
			ErrIncompatibleSchema, e.ID, e.SchemaVersion, extra, latest.Version)
	}
	return out, nil
}
