package kernel

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateFieldSchemaSupportsFourFieldTypes(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"priority", "tags", "status", "homepage"},
		"properties": map[string]any{
			"priority": map[string]any{
				"type":       "string",
				"field_type": "select",
				"enum":       []any{"low", "medium", "high"},
			},
			"tags": map[string]any{
				"type":        "array",
				"field_type":  "multi_select",
				"uniqueItems": true,
				"items": map[string]any{
					"type": "string",
					"enum": []any{"bug", "feature", "ops"},
				},
			},
			"status": map[string]any{
				"type":       "string",
				"field_type": "status",
				"enum":       []any{"todo", "in_progress", "done"},
			},
			"homepage": map[string]any{
				"type":       []any{"string", "null"},
				"field_type": "url",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := validateFieldSchema(raw)
	if err != nil {
		t.Fatalf("expected valid schema, got %v", err)
	}

	valid := map[string]any{
		"priority": "high",
		"tags":     []any{"bug", "ops"},
		"status":   "in_progress",
		"homepage": "https://example.com/tasks/1",
	}
	if err := validateValues(valid, schema, "values"); err != nil {
		t.Fatalf("expected valid values, got %v", err)
	}

	nullableURL := map[string]any{
		"priority": "low",
		"tags":     []any{},
		"status":   "todo",
		"homepage": nil,
	}
	if err := validateValues(nullableURL, schema, "values"); err != nil {
		t.Fatalf("expected nullable url to pass, got %v", err)
	}

	invalidCases := []struct {
		name   string
		values map[string]any
	}{
		{
			name:   "invalid select option",
			values: map[string]any{"priority": "urgent", "tags": []any{"bug"}, "status": "todo", "homepage": "https://example.com"},
		},
		{
			name:   "invalid multi_select option",
			values: map[string]any{"priority": "low", "tags": []any{"unknown"}, "status": "todo", "homepage": "https://example.com"},
		},
		{
			name:   "duplicate multi_select items",
			values: map[string]any{"priority": "low", "tags": []any{"bug", "bug"}, "status": "todo", "homepage": "https://example.com"},
		},
		{
			name:   "invalid status option",
			values: map[string]any{"priority": "low", "tags": []any{"bug"}, "status": "archived", "homepage": "https://example.com"},
		},
		{
			name:   "invalid url value",
			values: map[string]any{"priority": "low", "tags": []any{"bug"}, "status": "todo", "homepage": "not-a-url"},
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateValues(tc.values, schema, "values")
			var cmdErr *CommandError
			if !errors.As(err, &cmdErr) || cmdErr.Outcome != "schema_mismatch" {
				t.Fatalf("expected schema_mismatch, got %v", err)
			}
		})
	}
}

func TestValidateFieldSchemaRejectsInvalidFieldTypes(t *testing.T) {
	invalidSchemas := []struct {
		name   string
		schema map[string]any
	}{
		{
			name:   "field_type on root",
			schema: map[string]any{"type": "object", "field_type": "select", "enum": []any{"a"}},
		},
		{
			name: "unsupported field_type",
			schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"owner": map[string]any{"type": "string", "field_type": "person"}},
			},
		},
		{
			name: "select without enum",
			schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"kind": map[string]any{"type": "string", "field_type": "select"}},
			},
		},
		{
			name: "status with non-string type",
			schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"state": map[string]any{"type": "integer", "field_type": "status", "enum": []any{1, 2}}},
			},
		},
		{
			name: "multi_select without items enum",
			schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"tags": map[string]any{"type": "array", "field_type": "multi_select", "items": map[string]any{"type": "string"}}},
			},
		},
		{
			name: "url with email format",
			schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"link": map[string]any{"type": "string", "field_type": "url", "format": "email"}},
			},
		},
	}

	for _, tc := range invalidSchemas {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.schema)
			if err != nil {
				t.Fatal(err)
			}
			_, err = validateFieldSchema(raw)
			var cmdErr *CommandError
			if !errors.As(err, &cmdErr) || cmdErr.Outcome != "schema_mismatch" {
				t.Fatalf("expected schema_mismatch, got %v", err)
			}
		})
	}
}
