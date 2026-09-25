package protocol

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// 兼容性规则（详见 docs/ingestion-protocol.md）：
//   - 新增字段：允许。新字段必须具备稳定缺省语义：
//     HasDefault=true 时用其 Default；非必需（Required=false）时缺省为 nil。
//     历史记录以同一缺省值回填，新记录缺字段时按同一规则补全。
//   - 删除字段 / 改名字段（即删旧名+加新名）/ 修改字段类型 / 修改主键 /
//     已声明缺省值被改动 / 必需性收紧：不允许静默兼容，显式失败。
var (
	// ErrIncompatibleSchema 表示新旧 Schema 之间存在无法安全兼容的变化。
	ErrIncompatibleSchema = errors.New("incompatible schema change")
	// ErrSchemaNotFound 表示事件携带的 Schema 版本未注册。
	ErrSchemaNotFound = errors.New("schema version not found")
)

// SchemaRegistry 保存来源各版本的 Schema，并负责兼容性判定。
type SchemaRegistry struct {
	sources map[string][]*Schema
}

// NewSchemaRegistry 创建空的 Schema 注册表。
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{sources: map[string][]*Schema{}}
}

func incompatible(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrIncompatibleSchema, fmt.Sprintf(format, args...))
}

// Register 注册一个新版本。首个版本直接登记；之后的版本必须满足兼容规则，
// 且版本号恰好为上一版本 +1，否则返回包装了 ErrIncompatibleSchema 的错误。
func (r *SchemaRegistry) Register(s *Schema) error {
	if s == nil {
		return incompatible("nil schema")
	}
	if s.Version < 1 {
		return incompatible("schema version must start from 1, got %d", s.Version)
	}
	if len(s.PrimaryKey) == 0 {
		return incompatible("source %q schema v%d declares no primary key", s.SourceName, s.Version)
	}
	for i := range s.Fields {
		f := &s.Fields[i]
		switch f.Type {
		case TypeString, TypeInt, TypeFloat, TypeBool:
		default:
			return incompatible("field %q has unknown type %q", f.Name, f.Type)
		}
	}

	versions := r.sources[s.SourceName]
	var old *Schema
	if len(versions) > 0 {
		old = versions[len(versions)-1]
	}
	if _, err := CheckCompatibility(old, s); err != nil {
		return err
	}
	r.sources[s.SourceName] = append(versions, s)
	return nil
}

// Get 取出来源指定版本的 Schema；不存在返回 ErrSchemaNotFound。
func (r *SchemaRegistry) Get(source string, version int) (*Schema, error) {
	for _, s := range r.sources[source] {
		if s.Version == version {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: source=%s version=%d", ErrSchemaNotFound, source, version)
}

// Latest 取出来源最新版本的 Schema。
func (r *SchemaRegistry) Latest(source string) (*Schema, error) {
	versions := r.sources[source]
	if len(versions) == 0 {
		return nil, fmt.Errorf("%w: source=%s has no registered schema", ErrSchemaNotFound, source)
	}
	return versions[len(versions)-1], nil
}

// CheckCompatibility 判定 next 相对 old 是否可安全兼容，并返回可读的变化描述。
// old 为 nil 表示首个版本，直接通过。
func CheckCompatibility(old, next *Schema) (string, error) {
	if old == nil {
		return fmt.Sprintf("register initial schema v%d (%d fields, pk=%s)",
			next.Version, len(next.Fields), strings.Join(next.PrimaryKey, ",")), nil
	}

	if next.SourceName != old.SourceName {
		return "", incompatible("source name changed from %q to %q", old.SourceName, next.SourceName)
	}
	if next.Version != old.Version+1 {
		return "", incompatible("schema version must advance by exactly 1: v%d -> v%d", old.Version, next.Version)
	}

	// 主键必须保持不变（顺序敏感）。
	if len(next.PrimaryKey) != len(old.PrimaryKey) {
		return "", incompatible("primary key changed: %v -> %v", old.PrimaryKey, next.PrimaryKey)
	}
	for i := range old.PrimaryKey {
		if old.PrimaryKey[i] != next.PrimaryKey[i] {
			return "", incompatible("primary key changed: %v -> %v", old.PrimaryKey, next.PrimaryKey)
		}
	}

	var added, notes []string
	oldFields := map[string]*Field{}
	for i := range old.Fields {
		oldFields[old.Fields[i].Name] = &old.Fields[i]
	}
	nextFields := map[string]*Field{}
	for i := range next.Fields {
		nextFields[next.Fields[i].Name] = &next.Fields[i]
	}

	// 删除 / 改名：next 中消失的旧字段一律拒绝（改名会表现为旧名消失）。
	for name := range oldFields {
		if nextFields[name] == nil {
			return "", incompatible("field %q was removed or renamed (removal/rename is not auto-compatible)", name)
		}
	}
	for name, nf := range nextFields {
		of, existed := oldFields[name]
		if !existed {
			// 新增字段必须有稳定缺省语义。
			if nf.Required && !nf.HasDefault {
				return "", incompatible("new required field %q has no declared default", name)
			}
			if nf.HasDefault {
				if err := typeCheckDefault(nf); err != nil {
					return "", err
				}
				added = append(added, fmt.Sprintf("%s(+default=%v)", name, nf.Default))
			} else {
				added = append(added, fmt.Sprintf("%s(+nullable, default=nil)", name))
			}
			continue
		}
		// 既有字段：类型、缺省值不得变化；必需性只能放宽不能收紧。
		if of.Type != nf.Type {
			return "", incompatible("field %q type changed: %s -> %s", name, of.Type, nf.Type)
		}
		if of.HasDefault != nf.HasDefault || (of.HasDefault && fmt.Sprintf("%v", of.Default) != fmt.Sprintf("%v", nf.Default)) {
			return "", incompatible("field %q default semantics changed: %v -> %v", name, of.Default, nf.Default)
		}
		if !of.Required && nf.Required {
			return "", incompatible("field %q was tightened from optional to required", name)
		}
		if of.Required && !nf.Required {
			notes = append(notes, fmt.Sprintf("%s(relaxed optional)", name))
		}
	}

	sort.Strings(added)
	sort.Strings(notes)
	changes := append(append([]string{}, added...), notes...)
	if len(changes) == 0 {
		changes = []string{"no field changes"}
	}
	return fmt.Sprintf("compatible change v%d -> v%d: %s", old.Version, next.Version, strings.Join(changes, "; ")), nil
}

// typeCheckDefault 校验声明的缺省值与其字段类型一致。
func typeCheckDefault(f *Field) error {
	dummy := &Schema{}
	if err := dummy.ValidateValue(f, f.Default); err != nil {
		return incompatible("field %q default value does not match its type: %v", f.Name, err)
	}
	return nil
}
