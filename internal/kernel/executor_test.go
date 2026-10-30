package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestSQLiteExecutor_generatesUniqueNumbersAndDerivedRelationSchemas(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
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
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 1, 2, 3, 0, time.UTC) }

	define := command("cmd_type_1", "idem_type_1", "object_type.define", "object_type", nil, map[string]any{
		"key": "purchase_orders", "singular": "Purchase order", "plural": "Purchase orders",
		"field_schema": map[string]any{"type": "object", "properties": map[string]any{"number": map[string]any{"type": "string", "readOnly": true, "x-compos-generation": map[string]any{"version": 1, "on": "create", "template": "PO-{{today:YYMMDD}}-{{sequence:3}}", "timezone": "Asia/Seoul", "reset": "day", "unique": true}}}},
	})
	receipt, err := executor.Execute(ctx, "dev", define)
	if err != nil {
		t.Fatal(err)
	}
	typeID := receipt.Aggregate.ID
	for i, want := range []string{"PO-260101-001", "PO-260101-002"} {
		create := command("cmd_object_"+string(rune('1'+i)), "idem_object_"+string(rune('1'+i)), "object.create", "object_instance", nil, map[string]any{"object_type_key": "purchase_orders", "title": "PO", "values": map[string]any{}})
		created, err := executor.Execute(ctx, "dev", create)
		if err != nil {
			t.Fatal(err)
		}
		var payload string
		if err := s.DB().QueryRow(`SELECT payload_json FROM object_instances WHERE id=?`, created.Aggregate.ID).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var values map[string]any
		_ = json.Unmarshal([]byte(payload), &values)
		if values["number"] != want {
			t.Fatalf("number=%v want %s", values["number"], want)
		}
	}

	other := command("cmd_type_2", "idem_type_2", "object_type.define", "object_type", nil, map[string]any{"key": "vendors", "singular": "Vendor", "plural": "Vendors", "field_schema": map[string]any{"type": "object"}})
	otherReceipt, err := executor.Execute(ctx, "dev", other)
	if err != nil {
		t.Fatal(err)
	}
	relation := command("cmd_relation", "idem_relation", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "vendor_orders", "from_object_type_id": otherReceipt.Aggregate.ID, "to_object_type_id": typeID, "cardinality": "one_to_many", "required": false})
	relation.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: otherReceipt.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: typeID, ExpectedVersion: 1}}
	if _, err := executor.Execute(ctx, "dev", relation); err != nil {
		t.Fatal(err)
	}
	var relationJSON, digest string
	var schemaVersion int64
	if err := s.DB().QueryRow(`SELECT relation_schema_json,schema_version FROM object_types WHERE id=?`, typeID).Scan(&relationJSON, &schemaVersion); err != nil {
		t.Fatal(err)
	}
	if !json.Valid([]byte(relationJSON)) || relationJSON == `{"links":[]}` {
		t.Fatalf("relation schema was not derived: %s", relationJSON)
	}
	if err := s.DB().QueryRow(`SELECT digest FROM object_type_schema_revisions WHERE object_type_id=? AND schema_version=?`, typeID, schemaVersion).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest == "" {
		t.Fatal("full revision digest is missing")
	}

	for _, definition := range Registry() {
		if definition.WireName == "task.create" || definition.WireName == "task_submission.create" || definition.WireName == "task_submission.accept" {
			t.Fatalf("legacy task command remains registered: %s", definition.WireName)
		}
	}
}

func TestRetiredObjectTypeTemplateSetIsRejectedBySQLite(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := "otp_retired"
	_, err = NewExecutor(s.DB()).Execute(ctx, "dev", command("cmd_retired_template", "idem_retired_template", "object_type.template_set", "object_type", &target, map[string]any{}))
	var rejected *CommandError
	if !errors.As(err, &rejected) || rejected.Outcome != "validation_failed" {
		t.Fatalf("error=%v", err)
	}
}

func TestObjectValuesUpdateRepinsLatestSchemaRevision(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
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

	typeReceipt := execute(t, executor, "dev", command("cmd_schema_pin_type", "idem_schema_pin_type", "object_type.define", "object_type", nil, map[string]any{
		"key": "notes", "singular": "Note", "plural": "Notes", "field_schema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}},
	}))
	objectReceipt := execute(t, executor, "dev", command("cmd_schema_pin_object", "idem_schema_pin_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "notes", "title": "Old", "values": map[string]any{}}))
	typeID, objectID := typeReceipt.Aggregate.ID, objectReceipt.Aggregate.ID
	evolve := command("cmd_schema_pin_evolve", "idem_schema_pin_evolve", "object_type.schema_evolve", "object_type", &typeID, map[string]any{
		"field_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"body"}, "properties": map[string]any{"body": map[string]any{"type": "string"}}},
	})
	evolve.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: typeID, ExpectedVersion: 1}}
	execute(t, executor, "dev", evolve)

	titleOnly := command("cmd_schema_pin_title", "idem_schema_pin_title", "object.update", "object_instance", &objectID, map[string]any{"title": "Still old schema"})
	titleOnly.Preconditions = []Precondition{{AggregateType: "object_instance", AggregateID: objectID, ExpectedVersion: 1}}
	execute(t, executor, "dev", titleOnly)
	var pin int64
	if err := s.DB().QueryRow(`SELECT object_type_schema_version FROM object_instances WHERE id=?`, objectID).Scan(&pin); err != nil || pin != 1 {
		t.Fatalf("title-only update pin=%d err=%v", pin, err)
	}

	replaceValues := command("cmd_schema_pin_values", "idem_schema_pin_values", "object.update", "object_instance", &objectID, map[string]any{"values": map[string]any{"body": "Current"}})
	replaceValues.Preconditions = []Precondition{{AggregateType: "object_instance", AggregateID: objectID, ExpectedVersion: 2}}
	execute(t, executor, "dev", replaceValues)
	if err := s.DB().QueryRow(`SELECT object_type_schema_version FROM object_instances WHERE id=?`, objectID).Scan(&pin); err != nil || pin != 2 {
		t.Fatalf("values update pin=%d err=%v", pin, err)
	}
}

func TestReactiveRuleVisibilityAndActionAllowlist(t *testing.T) {
	for _, eventType := range []string{"object.created", "work.opened", "work.deadline.reached", "domain.event.emitted"} {
		if !ReactiveRuleTriggerEventAllowed(eventType) {
			t.Fatalf("public event %q is not visible", eventType)
		}
	}
	for _, eventType := range []string{"function.result_recorded", "agent.result_recorded", "workflow.completed", "reactive_rule.defined"} {
		if ReactiveRuleTriggerEventAllowed(eventType) {
			t.Fatalf("internal event %q is visible", eventType)
		}
	}
	for _, commandType := range []string{"object.create", "object.move", "relation.link"} {
		definition, _ := definition(commandType)
		if !definition.AutomationActionAllowed {
			t.Fatalf("documented action %q is not allowed", commandType)
		}
	}
	for _, commandType := range []string{"relation.unlink", "function.invoke", "event.emit"} {
		definition, _ := definition(commandType)
		if definition.AutomationActionAllowed {
			t.Fatalf("undocumented action %q is allowed", commandType)
		}
	}
}

func TestKnowledgeCommandsAreAtomicAndReplaceOutgoingState(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "knowledge.sqlite")})
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
	defined, err := executor.Execute(ctx, "dev", command("cmd_customers", "idem_customers", "object_type.define", "object_type", nil, map[string]any{
		"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object"},
	}))
	if err != nil {
		t.Fatal(err)
	}

	source := "---\ntype: Ontology Concept\ntitle: Customer\nunknown: keep\ncompos:\n  relations:\n    - predicate: hasOwner\n      target: people/person\n    - predicate: hasContract\n      target: contracts/contract\n  grounds:\n    - entity_definition: customers\n      mapping:\n        name: display_name\n---\nCustomer body"
	accepted, err := executor.Execute(ctx, "dev", command("cmd_accept", "idem_accept", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": source}))
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Aggregate.ID != "core:customer" {
		t.Fatalf("aggregate=%#v", accepted.Aggregate)
	}
	for table, want := range map[string]int{"knowledge_documents": 1, "knowledge_relations": 2, "ontology_groundings": 1} {
		var got int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, got, want, err)
		}
	}
	var entityID, updatedAt string
	if err := s.DB().QueryRow(`SELECT entity_definition_id FROM ontology_groundings`).Scan(&entityID); err != nil || entityID != defined.Aggregate.ID {
		t.Fatalf("grounding entity=%q err=%v", entityID, err)
	}
	if err := s.DB().QueryRow(`SELECT updated_at FROM knowledge_documents`).Scan(&updatedAt); err != nil {
		t.Fatal(err)
	}
	executor.now = func() time.Time { return time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC) }
	if _, err := executor.Execute(ctx, "dev", command("cmd_accept_same", "idem_accept_same", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": source})); err != nil {
		t.Fatal(err)
	}
	var unchanged string
	_ = s.DB().QueryRow(`SELECT updated_at FROM knowledge_documents`).Scan(&unchanged)
	if unchanged != updatedAt {
		t.Fatalf("same content changed updated_at: %s -> %s", updatedAt, unchanged)
	}

	changed := strings.Replace(source, "    - predicate: hasContract\n      target: contracts/contract\n", "", 1)
	if _, err := executor.Execute(ctx, "dev", command("cmd_accept_changed", "idem_accept_changed", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": changed})); err != nil {
		t.Fatal(err)
	}
	var relationCount int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM knowledge_relations`).Scan(&relationCount)
	if relationCount != 1 {
		t.Fatalf("stale relations remain: %d", relationCount)
	}

	bad := strings.Replace(source, "entity_definition: customers", "entity_definition: missing", 1)
	if _, err := executor.Execute(ctx, "dev", command("cmd_bad", "idem_bad", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "bad.md", "source_markdown": bad})); err == nil {
		t.Fatal("expected invalid grounding rejection")
	}
	var badRows, badLedger int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM knowledge_documents WHERE concept_id='bad'`).Scan(&badRows)
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM command_ledger WHERE command_id='cmd_bad'`).Scan(&badLedger)
	if badRows != 0 || badLedger != 0 {
		t.Fatalf("invalid accept was not rolled back: documents=%d ledger=%d", badRows, badLedger)
	}

	if _, err := executor.Execute(ctx, "dev", command("cmd_remove", "idem_remove", "knowledge.remove", "knowledge_document", nil, map[string]any{"bundle_id": "core", "concept_id": "customer"})); err != nil {
		t.Fatal(err)
	}
	var remaining int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM knowledge_documents`).Scan(&remaining)
	if remaining != 0 {
		t.Fatalf("knowledge document remains after remove: %d", remaining)
	}
}

func command(id, idempotency, commandType, aggregateType string, aggregateID *string, payload any) Command {
	raw, _ := json.Marshal(payload)
	return Command{CommandID: id, IdempotencyKey: idempotency, WorkspaceID: "wks_1", RequestedActor: &Actor{Type: "user", ID: "dev"}, CommandType: commandType, Target: Target{AggregateType: aggregateType, AggregateID: aggregateID}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: "api"}}
}
