package knowledge_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/knowledge"
	"github.com/mrchypark/compos/internal/store"
)

func TestProjectionSnapshotIsDeterministicAndRebuildable(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "projection.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	executor := kernel.NewExecutor(s.DB())
	if _, err := executor.Execute(ctx, "dev", projectionCommand("type", "object_type.define", "object_type", map[string]any{"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object"}})); err != nil {
		t.Fatal(err)
	}
	person := "---\ntype: concept\ntitle: Person\n---\nPerson"
	customer := "---\ntype: concept\ntitle: Customer\ncompos:\n  relations:\n    - predicate: hasOwner\n      target: person\n  grounds:\n    - entity_definition: customers\n---\nCustomer"
	for id, item := range map[string][2]string{"person": {"person.md", person}, "customer": {"customer.md", customer}} {
		if _, err := executor.Execute(ctx, "dev", projectionCommand("accept-"+id, "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": item[0], "source_markdown": item[1]})); err != nil {
			t.Fatal(err)
		}
	}
	full, err := knowledge.Snapshot(ctx, s, "wks_1")
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != 4 || full[0].Kind != "concept" || full[2].Kind != "relation" || full[3].Kind != "grounding" {
		t.Fatalf("snapshot order/content=%#v", full)
	}
	state, sequences, digest, err := knowledge.SnapshotState(ctx, s)
	if err != nil || len(state) != len(full) || sequences["wks_1"] != 3 || digest != knowledge.ProjectionDigest(full) {
		t.Fatalf("atomic projection state records=%d sequences=%v digest=%q err=%v", len(state), sequences, digest, err)
	}
	again, err := knowledge.Snapshot(ctx, s, "wks_1")
	if err != nil || knowledge.ProjectionDigest(full) != knowledge.ProjectionDigest(again) {
		t.Fatalf("snapshot is not deterministic: err=%v", err)
	}
	incremental, err := knowledge.ConceptSnapshot(ctx, s, "wks_1", "core", "customer")
	if err != nil || len(incremental) != 3 {
		t.Fatalf("concept snapshot=%#v err=%v", incremental, err)
	}
	var payload string
	if err := s.DB().QueryRow(`SELECT payload_json FROM projection_events WHERE workspace_id='wks_1' AND aggregate_type='knowledge_document' ORDER BY projection_sequence DESC LIMIT 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	_ = json.Unmarshal([]byte(payload), &event)
	if event["operation"] != "accepted" || event["content_hash"] == nil || event["concept_id"] == nil || event["bundle_id"] != "core" || event["workspace_id"] != "wks_1" {
		t.Fatalf("projection event=%v", event)
	}
	if _, err := executor.Execute(ctx, "dev", projectionCommand("remove-customer", "knowledge.remove", "knowledge_document", map[string]any{"bundle_id": "core", "concept_id": "customer"})); err != nil {
		t.Fatal(err)
	}
	incremental, err = knowledge.ConceptSnapshot(ctx, s, "wks_1", "core", "customer")
	if err != nil || len(incremental) != 0 {
		t.Fatalf("removed concept snapshot=%#v err=%v", incremental, err)
	}
	if _, err := executor.Execute(ctx, "dev", projectionCommand("reaccept-customer", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": customer})); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := knowledge.Snapshot(ctx, s, "wks_1")
	if err != nil || knowledge.ProjectionDigest(full) != knowledge.ProjectionDigest(rebuilt) {
		t.Fatalf("rebuild differs after remove/reaccept: err=%v\nfull=%#v\nrebuilt=%#v", err, full, rebuilt)
	}
}

func projectionCommand(id, commandType, aggregateType string, payload any) kernel.Command {
	raw, _ := json.Marshal(payload)
	return kernel.Command{
		CommandID: id, IdempotencyKey: id, WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"},
		CommandType: commandType, Target: kernel.Target{AggregateType: aggregateType}, Preconditions: []kernel.Precondition{}, Payload: raw, Causation: kernel.Causation{Source: "api"},
	}
}
