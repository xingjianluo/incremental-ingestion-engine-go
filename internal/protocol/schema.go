package protocol

import (
	"fmt"
	"math"
)

// FieldType 是受支持的字段值类型。
type FieldType string

const (
	TypeString FieldType = "string"
	TypeInt    FieldType = "int"
	TypeFloat  FieldType = "float"
	TypeBool   FieldType = "bool"
)

// Field 描述一个字段的结构与约束。
type Field struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	// HasDefault 为 true 时，新增字段对历史/缺省记录使用 Default 作为稳定缺省值。
	HasDefault bool        `json:"has_default"`
	Default    interface{} `json:"default,omitempty"`
}

// Schema 是某个来源某一版本的结构定义。Version 单调递增，从 1 开始。
type Schema struct {
	SourceName string   `json:"source_name"`
	Version    int      `json:"version"`
	PrimaryKey []string `json:"primary_key"`
	Fields     []Field  `json:"fields"`
}

// FieldByName 返回指定名称的字段定义，不存在返回 nil。
func (s *Schema) FieldByName(name string) *Field {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i]
		}
	}
	return nil
}

// isIntegralNumber 判断数值是否为整数值（JSON 解码后数字统一为 float64）。
func isIntegralNumber(v float64) bool {
	return v == math.Trunc(v) && !math.IsInf(v, 0)
}

// ValidateValue 校验单个字段值是否匹配声明类型。nil 表示缺省，不在此处判定。
func (s *Schema) ValidateValue(f *Field, v interface{}) error {
	switch f.Type {
	case TypeString:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("field %q expects string, got %T", f.Name, v)
		}
	case TypeInt:
		switch n := v.(type) {
		case int, int64:
			// 原生整数，通过。
		case float64:
			if !isIntegralNumber(n) {
				return fmt.Errorf("field %q expects int, got non-integral number %v", f.Name, n)
			}
		default:
			return fmt.Errorf("field %q expects int, got %T", f.Name, v)
		}
	case TypeFloat:
		switch v.(type) {
		case float64, int, int64:
			// 数值即可。
		default:
			return fmt.Errorf("field %q expects float, got %T", f.Name, v)
		}
	case TypeBool:
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("field %q expects bool, got %T", f.Name, v)
		}
	default:
		return fmt.Errorf("field %q has unknown declared type %q", f.Name, f.Type)
	}
	return nil
}

// ValidateRecord 按主键、必需字段、类型规则校验一条记录。
// 返回拒绝原因（空串表示通过）与可读细节。
// 校验顺序固定：主键缺失 -> 必需字段缺失 -> 类型不匹配 -> 未声明字段。
func (s *Schema) ValidateRecord(rec map[string]interface{}) (RejectReason, string) {
	// 1) 主键字段必须存在且非空。
	for _, pk := range s.PrimaryKey {
		v, ok := rec[pk]
		if !ok || v == nil {
			return RejectMissingPrimaryKey, fmt.Sprintf("primary key field %q is missing", pk)
		}
	}
	// 2) 必需字段必须存在且非 nil；3) 在场字段类型必须匹配。
	for i := range s.Fields {
		f := &s.Fields[i]
		v, present := rec[f.Name]
		if !present || v == nil {
			if f.Required {
				return RejectMissingRequiredField, fmt.Sprintf("required field %q is missing", f.Name)
			}
			continue
		}
		if err := s.ValidateValue(f, v); err != nil {
			return RejectTypeMismatch, err.Error()
		}
	}
	// 4) 载荷中不得出现 Schema 未声明的字段，防止静默错列。
	for k := range rec {
		if s.FieldByName(k) == nil {
			return RejectIncompatibleSchema, fmt.Sprintf("payload contains undeclared field %q", k)
		}
	}
	return "", ""
}

// CoerceRecord 在记录已通过校验后，产出可落地的规范化记录：
//   - int 字段的 float64 整数值转换为 int64，避免目标端出现 3 与 3.0 二义；
//   - 缺失的非必需字段按稳定缺省语义补全：HasDefault 用 Default，否则补 nil；
//   - 仅保留 Schema 声明过的字段。
func (s *Schema) CoerceRecord(rec map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(s.Fields))
	for i := range s.Fields {
		f := &s.Fields[i]
		v, present := rec[f.Name]
		switch {
		case present && v != nil:
			if f.Type == TypeInt {
				switch n := v.(type) {
				case float64:
					v = int64(n)
				case int:
					v = int64(n)
				}
			}
			out[f.Name] = v
		case f.HasDefault:
			out[f.Name] = f.Default
		default:
			out[f.Name] = nil
		}
	}
	return out
}

// PrimaryKeyValue 以固定顺序拼接主键值，用作目标端行键。
func (s *Schema) PrimaryKeyValue(rec map[string]interface{}) string {
	key := ""
	for i, pk := range s.PrimaryKey {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", rec[pk])
	}
	return key
}
