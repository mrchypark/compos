package kernel

import (
	"errors"
	"testing"
)

func TestWorkReferencePlanOrdersAndResolvesNewRecords(t *testing.T) {
	parent := Command{CommandID: "submit-quote", IdempotencyKey: "submit-quote", WorkspaceID: "wks_1"}
	changes := []workChange{
		{Index: 0, BindingID: "item", ClientRef: "item_1", CommandType: "object.create", Payload: map[string]any{"object_type_key": "items", "relations": map[string]any{"quote": map[string]any{"client_ref": "quote"}}}},
		{Index: 1, BindingID: "quote", ClientRef: "quote", CommandType: "object.create", Payload: map[string]any{"object_type_key": "quotes", "relations": map[string]any{"customer": "obj_customer"}}},
		{Index: 2, BindingID: "item", ClientRef: "item_2", CommandType: "object.create", Payload: map[string]any{"object_type_key": "items", "relations": map[string]any{"quote": map[string]any{"client_ref": "quote"}}}},
	}
	plan, err := planWorkReferences(parent, changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.changes) != 3 || plan.changes[0].ClientRef != "quote" || plan.changes[1].ClientRef != "item_1" || plan.changes[2].ClientRef != "item_2" {
		t.Fatalf("create order = %#v", plan.changes)
	}
	quote := plan.objects["quote"]
	if quote.ID != aggregateID("obj", "submit-quote:change:1") {
		t.Fatalf("quote ID = %q", quote.ID)
	}
	if got := plan.changes[1].Payload["relations"].(map[string]any)["quote"]; got != quote.ID {
		t.Fatalf("item relation = %#v, want %q", got, quote.ID)
	}
	resolved, err := resolveWorkSubmissionRefs(map[string]any{"quote_id": map[string]any{"client_ref": "quote"}}, plan.objects)
	if err != nil || resolved.(map[string]any)["quote_id"] != quote.ID {
		t.Fatalf("submission refs = %#v, %v", resolved, err)
	}
}

func TestWorkReferencePlanRejectsCyclesAndUnknownReferences(t *testing.T) {
	parent := Command{CommandID: "submit-cycle", IdempotencyKey: "submit-cycle", WorkspaceID: "wks_1"}
	cycle := []workChange{
		{Index: 0, BindingID: "a", ClientRef: "a", CommandType: "object.create", Payload: map[string]any{"relations": map[string]any{"b": map[string]any{"client_ref": "b"}}}},
		{Index: 1, BindingID: "b", ClientRef: "b", CommandType: "object.create", Payload: map[string]any{"relations": map[string]any{"a": map[string]any{"client_ref": "a"}}}},
	}
	if _, err := planWorkReferences(parent, cycle); err == nil {
		t.Fatal("cyclic refs accepted")
	}
	unknown := []workChange{{Index: 0, BindingID: "item", ClientRef: "item", CommandType: "object.create", Payload: map[string]any{"relations": map[string]any{"quote": map[string]any{"client_ref": "missing"}}}}}
	if _, err := planWorkReferences(parent, unknown); err == nil {
		t.Fatal("unknown ref accepted")
	} else {
		assertReferenceDetails(t, err, 0, "item", "item", "relations")
	}
}

func TestWorkReferencePlanIdentifiesDuplicateAndInvalidClientRefs(t *testing.T) {
	parent := Command{CommandID: "submit-refs", IdempotencyKey: "submit-refs", WorkspaceID: "wks_1"}
	duplicate := []workChange{
		{Index: 0, BindingID: "quote", ClientRef: "quote", CommandType: "object.create", Payload: map[string]any{}},
		{Index: 1, BindingID: "item", ClientRef: "quote", CommandType: "object.create", Payload: map[string]any{}},
	}
	if _, err := planWorkReferences(parent, duplicate); err == nil {
		t.Fatal("duplicate ref accepted")
	} else {
		assertReferenceDetails(t, err, 1, "quote", "item", "client_ref")
	}
	invalid := []workChange{{Index: 0, BindingID: "item", ClientRef: "item", CommandType: "relation.link", Payload: map[string]any{}}}
	if _, err := planWorkReferences(parent, invalid); err == nil {
		t.Fatal("invalid ref accepted")
	} else {
		assertReferenceDetails(t, err, 0, "item", "item", "client_ref")
	}
}

func assertReferenceDetails(t *testing.T, err error, index int, clientRef, bindingID, field string) {
	t.Helper()
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Details == nil || commandErr.Details.ChangeIndex == nil {
		t.Fatalf("error has no details: %v", err)
	}
	if *commandErr.Details.ChangeIndex != index || commandErr.Details.ClientRef != clientRef || commandErr.Details.BindingID != bindingID || commandErr.Details.Field != field || commandErr.Details.Code != commandErr.Outcome {
		t.Fatalf("details=%+v outcome=%s", commandErr.Details, commandErr.Outcome)
	}
}
