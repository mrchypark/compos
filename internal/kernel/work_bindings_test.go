package kernel

import "testing"

func TestTaskDataBindingValidation(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"input_schema":  map[string]any{"type": "object", "required": []any{"object_id"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}}},
			"data_bindings": []any{map[string]any{"id": "quote", "object_type_id": "otp_quote", "schema_version": 1, "context_key": "object_id", "writable_fields": []any{"amount"}, "allowed_operations": []any{"object.update"}}},
		}
	}
	if err := validateDataBindings(base()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]any, map[string]any)
	}{
		{"missing input pin", func(s, b map[string]any) { b["context_key"] = "unbound" }},
		{"zero schema", func(s, b map[string]any) { b["schema_version"] = 0 }},
		{"arbitrary command", func(s, b map[string]any) { b["allowed_operations"] = []any{"spec.publish"} }},
		{"layout is not domain", func(s, b map[string]any) { b["layout"] = "two-columns" }},
		{"duplicate binding", func(s, b map[string]any) { s["data_bindings"] = []any{b, b} }},
		{"invalid direction", func(s, b map[string]any) {
			b["relation_path"] = []any{map[string]any{"relation_definition_id": "r", "version": 1, "direction": "both"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := base()
			b := s["data_bindings"].([]any)[0].(map[string]any)
			tc.change(s, b)
			if err := validateDataBindings(s); err == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
}

func TestBindingReplacementCannotEraseProtectedFields(t *testing.T) {
	b := map[string]any{"writable_fields": []any{"amount"}}
	old := map[string]any{"amount": 1, "owner": "finance"}
	for _, payload := range []map[string]any{
		{"values": map[string]any{"amount": 2}},
		{"values": map[string]any{"amount": 2, "owner": "sales"}},
		{"title": "renamed"},
	} {
		if err := validateBindingFields(b, payload, old); err == nil {
			t.Fatalf("protected mutation accepted: %v", payload)
		}
	}
	if err := validateBindingFields(b, map[string]any{"values": map[string]any{"amount": 2, "owner": "finance"}}, old); err != nil {
		t.Fatal(err)
	}
}
