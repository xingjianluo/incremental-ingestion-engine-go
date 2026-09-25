package protocol

import (
	"errors"
	"strings"
	"testing"
)

func sampleSchema() *Schema {
	return &Schema{
		SourceName: "users",
		Version:    1,
		PrimaryKey: []string{"id"},
		Fields: []Field{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "email", Type: TypeString, Required: true},
			{Name: "age", Type: TypeInt, Required: false},
			{Name: "active", Type: TypeBool, Required: false},
		},
	}
}

func TestValidateRecord(t *testing.T) {
	s := sampleSchema()
	cases := []struct {
		name   string
		rec    map[string]interface{}
		reason RejectReason
	}{
		{"ok-complete", map[string]interface{}{"id": float64(1), "email": "a@x", "age": float64(20), "active": true}, ""},
		{"ok-optional-missing", map[string]interface{}{"id": float64(1), "email": "a@x"}, ""},
		{"missing-pk", map[string]interface{}{"email": "a@x"}, RejectMissingPrimaryKey},
		{"pk-null", map[string]interface{}{"id": nil, "email": "a@x"}, RejectMissingPrimaryKey},
		{"missing-required", map[string]interface{}{"id": float64(1)}, RejectMissingRequiredField},
		{"type-mismatch-int-as-string", map[string]interface{}{"id": float64(1), "email": "a@x", "age": "x"}, RejectTypeMismatch},
		{"type-mismatch-fractional-int", map[string]interface{}{"id": float64(1), "email": "a@x", "age": 1.5}, RejectTypeMismatch},
		{"type-mismatch-bool", map[string]interface{}{"id": float64(1), "email": "a@x", "active": "yes"}, RejectTypeMismatch},
		{"type-mismatch-string-as-int", map[string]interface{}{"id": "1", "email": "a@x"}, RejectTypeMismatch},
		{"undeclared-field", map[string]interface{}{"id": float64(1), "email": "a@x", "ghost": float64(1)}, RejectIncompatibleSchema},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reason, detail := s.ValidateRecord(c.rec)
			if reason != c.reason {
				t.Fatalf("want reason %q, got %q (detail=%s)", c.reason, reason, detail)
			}
		})
	}
}

func TestCoerceRecordDefaults(t *testing.T) {
	s := &Schema{
		SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
		Fields: []Field{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "tier", Type: TypeString, Required: false, HasDefault: true, Default: "standard"},
			{Name: "note", Type: TypeString, Required: false}, // nullable -> nil
		},
	}
	out := s.CoerceRecord(map[string]interface{}{"id": float64(7)})
	if out["id"] != int64(7) {
		t.Fatalf("int coercion failed: %v(%T)", out["id"], out["id"])
	}
	if out["tier"] != "standard" {
		t.Fatalf("default not applied: %v", out["tier"])
	}
	if out["note"] != nil {
		t.Fatalf("nullable missing field should be nil: %v", out["note"])
	}
}

func TestSchemaCompatibility(t *testing.T) {
	v1 := sampleSchema()

	// 兼容：新增带缺省值的字段
	v2 := &Schema{
		SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
		Fields: append(append([]Field{}, v1.Fields...),
			Field{Name: "tier", Type: TypeString, Required: false, HasDefault: true, Default: "std"}),
	}
	if desc, err := CheckCompatibility(v1, v2); err != nil {
		t.Fatalf("add nullable-default field should be compatible: %v", err)
	} else {
		t.Logf("compatible: %s", desc)
	}

	// 兼容：新增 nullable 字段（缺省 nil）
	v2b := &Schema{
		SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
		Fields: append(append([]Field{}, v1.Fields...),
			Field{Name: "nick", Type: TypeString, Required: false}),
	}
	if _, err := CheckCompatibility(v1, v2b); err != nil {
		t.Fatalf("add nullable field should be compatible: %v", err)
	}

	bad := []struct {
		name      string
		base      *Schema
		next      *Schema
		wantMsgIn string
	}{
		{"remove-field", v2, &Schema{SourceName: "users", Version: 3, PrimaryKey: []string{"id"},
			Fields: []Field{{Name: "id", Type: TypeInt, Required: true}, {Name: "age", Type: TypeInt}}},
			"removed or renamed"},
		{"rename-field", v1, &Schema{SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
			Fields: []Field{
				{Name: "id", Type: TypeInt, Required: true},
				{Name: "mail", Type: TypeString, Required: true}, // email 改名
				{Name: "age", Type: TypeInt}, {Name: "active", Type: TypeBool},
			}},
			"removed or renamed"},
		{"change-type", v1, &Schema{SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
			Fields: []Field{
				{Name: "id", Type: TypeString, Required: true}, // 类型变了
				{Name: "email", Type: TypeString, Required: true},
				{Name: "age", Type: TypeInt}, {Name: "active", Type: TypeBool},
			}},
			"type changed"},
		{"pk-changed", v1, &Schema{SourceName: "users", Version: 2, PrimaryKey: []string{"email"},
			Fields: v1.Fields},
			"primary key changed"},
		{"required-tighten", v1, &Schema{SourceName: "users", Version: 2, PrimaryKey: []string{"id"},
			Fields: []Field{
				{Name: "id", Type: TypeInt, Required: true},
				{Name: "email", Type: TypeString, Required: true},
				{Name: "age", Type: TypeInt, Required: true}, // 收紧
				{Name: "active", Type: TypeBool},
			}},
			"tightened"},
		{"new-required-no-default", v2, &Schema{SourceName: "users", Version: 3, PrimaryKey: []string{"id"},
			Fields: append(append([]Field{}, v2.Fields...),
				Field{Name: "tenant", Type: TypeString, Required: true})},
			"no declared default"},
		{"version-skipped", v1, &Schema{SourceName: "users", Version: 5, PrimaryKey: []string{"id"}, Fields: v1.Fields},
			"advance by exactly 1"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := CheckCompatibility(c.base, c.next)
			if !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatalf("%s: want ErrIncompatibleSchema, got %v", c.name, err)
			}
			if !strings.Contains(err.Error(), c.wantMsgIn) {
				t.Fatalf("%s: error %q does not explain %q", c.name, err.Error(), c.wantMsgIn)
			}
			t.Logf("%s rejected: %v", c.name, err)
		})
	}
}

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewSchemaRegistry()
	v1 := sampleSchema()
	if err := r.Register(v1); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Latest("users"); err != nil || got.Version != 1 {
		t.Fatalf("latest wrong: %v %v", got, err)
	}
	if _, err := r.Get("users", 9); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("want ErrSchemaNotFound, got %v", err)
	}
}
