package kernel

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestDomainEventValidation(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "semantic.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	executor := testExecutor(s.DB())
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

	event := execute(t, executor, "dev", command("cmd_event", "idem_event", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "customer.enriched", "revision": 1, "kind": "event",
		"spec": map[string]any{"name": "Customer enriched", "payload_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"customer_id"}, "properties": map[string]any{"customer_id": map[string]any{"type": "string"}}}},
	}))
	execute(t, executor, "dev", command("cmd_emit", "idem_emit", "event.emit", "domain_event", nil, map[string]any{"event_revision_id": event.Aggregate.ID, "data": map[string]any{"customer_id": "customer_1"}}))
	if _, err := executor.Execute(ctx, "dev", command("cmd_bad_emit", "idem_bad_emit", "event.emit", "domain_event", nil, map[string]any{"event_revision_id": event.Aggregate.ID, "data": map[string]any{"unexpected": true}})); err == nil {
		t.Fatal("invalid Domain Event payload was accepted")
	}

}

func TestRetiredKnowledgeCommandsRejectedByBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "rhiza"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var s *store.Store
			var err error
			if backend == "rhiza" {
				s = openRhizaKernelStore(t, ctx, "retired-knowledge")
			} else {
				s, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "retired.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
			}
			defer s.Close()
			executor := testStoreExecutor(s)
			for _, name := range []string{"knowledge.accept", "knowledge.remove", "knowledge.propose", "knowledge.review"} {
				if _, ok := definition(name); ok {
					t.Fatalf("retired command registered: %s", name)
				}
				if _, err := executor.Execute(ctx, "dev", command(name, name, name, "knowledge_document", nil, map[string]any{})); err == nil {
					t.Fatalf("retired command accepted: %s", name)
				}
			}
			// Ontology publication and Task references remain independent of Knowledge.
			if backend == "sqlite" {
				now := "2026-01-01T00:00:00Z"
				if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
					t.Fatal(err)
				}

			}
			ontology := execute(t, executor, "dev", command("ontology", "ontology", "spec.publish", "spec_revision", nil, map[string]any{
				"family_id": "crm", "revision": 1, "kind": "ontology",
				"spec": map[string]any{"name": "CRM", "concepts": []any{map[string]any{"id": "Customer"}}, "relations": []any{}},
			}))
			execute(t, executor, "dev", command("ontology-task", "ontology-task", "spec.publish", "spec_revision", nil, map[string]any{
				"family_id": "crm-task", "revision": 1, "kind": "task",
				"spec": map[string]any{"name": "CRM task", "goal": "Review customer", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "completion": map[string]any{"kind": "submission_valid"}, "ontology_context": map[string]any{"ontology_revision_ids": []any{ontology.Aggregate.ID}, "concept_refs": []any{"Customer"}}},
			}))
		})
	}
}
