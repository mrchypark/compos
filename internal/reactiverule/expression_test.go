package reactiverule

import "testing"

func TestBoundedJsonLogic(t *testing.T) {
	predicate := map[string]any{"and": []any{
		map[string]any{"===": []any{map[string]any{"var": "payload.state"}, "ready"}},
		map[string]any{">=": []any{map[string]any{"var": "payload.amount"}, float64(100)}},
	}}
	if err := ValidatePredicate(predicate); err != nil {
		t.Fatal(err)
	}
	matched, err := Match(predicate, map[string]any{"payload": map[string]any{"state": "ready", "amount": float64(100)}})
	if err != nil || !matched {
		t.Fatalf("matched=%v err=%v", matched, err)
	}
	if err := ValidatePredicate(map[string]any{"log": []any{"secret"}}); err == nil {
		t.Fatal("unsafe operator was accepted")
	}
}

func TestClosedTemplateReferences(t *testing.T) {
	valid := map[string]any{"id": map[string]any{"$from": map[string]any{"source": "event.payload", "path": "/object/id"}}}
	if err := ValidateTemplate(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []any{
		map[string]any{"$from": map[string]any{"source": "secrets", "path": "/token"}},
		map[string]any{"$from": map[string]any{"source": "event.payload", "path": "object/id"}},
		map[string]any{"$from": map[string]any{"source": "event.payload", "path": ""}, "literal": true},
	} {
		if err := ValidateTemplate(invalid); err == nil {
			t.Fatalf("invalid template accepted: %#v", invalid)
		}
	}
}
