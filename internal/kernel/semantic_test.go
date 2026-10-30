package kernel

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestDomainEventAndKnowledgeProposalPromotion(t *testing.T) {
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
	if _, err := s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(s.DB())
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

	accepted := "---\ntype: Ontology Concept\ntitle: Customer\n---\nAn external organization."
	execute(t, executor, "dev", command("cmd_knowledge", "idem_knowledge", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": accepted}))
	event := execute(t, executor, "dev", command("cmd_event", "idem_event", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "customer.enriched", "revision": 1, "kind": "event",
		"spec": map[string]any{"name": "Customer enriched", "payload_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"customer_id"}, "properties": map[string]any{"customer_id": map[string]any{"type": "string"}}}},
	}))
	execute(t, executor, "dev", command("cmd_emit", "idem_emit", "event.emit", "domain_event", nil, map[string]any{"event_revision_id": event.Aggregate.ID, "data": map[string]any{"customer_id": "customer_1"}}))
	if _, err := executor.Execute(ctx, "dev", command("cmd_bad_emit", "idem_bad_emit", "event.emit", "domain_event", nil, map[string]any{"event_revision_id": event.Aggregate.ID, "data": map[string]any{"unexpected": true}})); err == nil {
		t.Fatal("invalid Domain Event payload was accepted")
	}

	ontology := map[string]any{"name": "CRM", "concepts": []any{map[string]any{"id": "Customer"}}, "relations": []any{}}
	proposal := execute(t, executor, "dev", command("cmd_propose", "idem_propose", "knowledge.propose", "knowledge_proposal", nil, map[string]any{
		"sources": []any{map[string]any{"bundle_id": "core", "concept_id": "customer"}}, "proposal": ontology,
	}))
	proposalID := proposal.Aggregate.ID
	var proposalPayload string
	if err := s.DB().QueryRow(`SELECT payload_json FROM journal_records WHERE stream_id=? AND kind='knowledge.proposed'`, proposalID).Scan(&proposalPayload); err != nil || !strings.Contains(proposalPayload, `"revision_id":"sha256:`) {
		t.Fatalf("proposal did not pin source content: %s err=%v", proposalPayload, err)
	}
	if _, err := s.DB().Exec(`INSERT INTO actor_capabilities(id,workspace_id,principal_id,actor_type,actor_id,command_type,causation_source,relation_definition_id,idempotency_key_prefix,created_at,updated_at) VALUES('cap_review','wks_1','dev','service','review-agent','knowledge.review','api','','agent:',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	agentReview := command("cmd_agent_review", "agent:review", "knowledge.review", "knowledge_proposal", &proposalID, map[string]any{"decision": "reject"})
	agentReview.RequestedActor = &Actor{Type: "service", ID: "review-agent"}
	if _, err := executor.Execute(ctx, "dev", agentReview); err == nil || err.(*CommandError).Outcome != "forbidden" {
		t.Fatalf("non-human review was not rejected: %v", err)
	}
	execute(t, executor, "dev", command("cmd_review", "idem_review", "knowledge.review", "knowledge_proposal", &proposalID, map[string]any{"decision": "accept", "family_id": "crm", "revision": 1}))

	var ontologyID, stored string
	if err := s.DB().QueryRow(`SELECT id,payload_json FROM spec_revisions WHERE workspace_id='wks_1' AND kind='ontology'`).Scan(&ontologyID, &stored); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(stored), &got); err != nil || got["name"] != "CRM" {
		t.Fatalf("ontology=%s err=%v", stored, err)
	}
	var emitted, acceptedCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE kind='domain.event.emitted'`).Scan(&emitted); err != nil || emitted != 1 {
		t.Fatalf("domain events=%d err=%v", emitted, err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='knowledge.proposal.accepted' AND spec_revision_id=?`, proposalID, ontologyID).Scan(&acceptedCount); err != nil || acceptedCount != 1 {
		t.Fatalf("accepted proposal facts=%d err=%v", acceptedCount, err)
	}
	if _, err := executor.Execute(ctx, "dev", command("cmd_review_again", "idem_review_again", "knowledge.review", "knowledge_proposal", &proposalID, map[string]any{"decision": "reject"})); err == nil {
		t.Fatal("proposal was reviewed twice")
	}

	stale := execute(t, executor, "dev", command("cmd_propose_stale", "idem_propose_stale", "knowledge.propose", "knowledge_proposal", nil, map[string]any{
		"sources": []any{map[string]any{"bundle_id": "core", "concept_id": "customer"}}, "proposal": ontology,
	}))
	changed := "---\ntype: Ontology Concept\ntitle: Customer\n---\nA changed source."
	execute(t, executor, "dev", command("cmd_knowledge_changed", "idem_knowledge_changed", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": changed}))
	staleID := stale.Aggregate.ID
	if _, err := executor.Execute(ctx, "dev", command("cmd_review_stale", "idem_review_stale", "knowledge.review", "knowledge_proposal", &staleID, map[string]any{"decision": "accept", "family_id": "crm-stale", "revision": 1})); err == nil {
		t.Fatal("proposal with changed Knowledge evidence was accepted")
	}
}
