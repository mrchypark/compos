package kernel

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestRhizaWorkSubmitChangesIsOneAtomicSubmission(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-work-changes")
	defer s.Close()
	e := testStoreExecutor(s)
	e.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	rootType := execute(t, e, "dev", command("change-root-type", "change-root-type", "object_type.define", "object_type", nil, map[string]any{"key": "quotes", "singular": "Quote", "plural": "Quotes", "field_schema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"amount": map[string]any{"type": "number"}}}}))
	itemType := execute(t, e, "dev", command("change-item-type", "change-item-type", "object_type.define", "object_type", nil, map[string]any{"key": "quote_items", "singular": "Quote item", "plural": "Quote items", "field_schema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"number": map[string]any{"type": "string", "readOnly": true, "x-compos-generation": map[string]any{"version": 1, "on": "create", "template": "IT-{{sequence:3}}", "timezone": "UTC", "reset": "never", "unique": true}}}}}))
	relationCommand := command("change-relation", "change-relation", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "quote", "from_object_type_id": itemType.Aggregate.ID, "to_object_type_id": rootType.Aggregate.ID, "cardinality": "many_to_one", "required": false})
	relationCommand.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: itemType.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: rootType.Aggregate.ID, ExpectedVersion: 1}}
	relation := execute(t, e, "dev", relationCommand)
	root := execute(t, e, "dev", command("change-root", "change-root", "object.create", "object_instance", nil, map[string]any{"object_type_key": "quotes", "title": "Q-1", "values": map[string]any{"amount": 1}}))
	taskSpec := taskSpec("Edit quote", "Edit quote", "ignored")
	taskSpec["completion"] = map[string]any{"kind": "submission_valid"}
	taskSpec["data_bindings"] = []any{
		map[string]any{"id": "quote", "object_type_id": rootType.Aggregate.ID, "schema_version": 2, "context_key": "object_id", "writable_fields": []any{"amount"}, "allowed_operations": []any{"object.update"}},
		map[string]any{"id": "item", "object_type_id": itemType.Aggregate.ID, "schema_version": 2, "context_key": "object_id", "relation_path": []any{map[string]any{"relation_definition_id": relation.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create", "relation.link"}},
	}
	task := execute(t, e, "dev", command("change-task", "change-task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "edit-quote", "revision": 1, "kind": "task", "spec": taskSpec}))
	work := execute(t, e, "dev", command("change-work", "change-work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": root.Aggregate.ID}}))
	workID := work.Aggregate.ID
	execute(t, e, "dev", command("change-route", "change-route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	badWork := execute(t, e, "dev", command("change-bad-work", "change-bad-work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": root.Aggregate.ID}}))
	badWorkID := badWork.Aggregate.ID
	execute(t, e, "dev", command("change-bad-route", "change-bad-route", "work.route", "work_stream", &badWorkID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	bad := command("change-bad-submit", "change-bad-submit", "work.submit", "work_stream", &badWorkID, map[string]any{"submission": map[string]any{}, "changes": []any{
		map[string]any{"binding_id": "quote", "command_type": "object.update", "target": map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID}, "preconditions": []any{map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID, "expected_version": 1}}, "payload": map[string]any{"values": map[string]any{"amount": 2}}},
		map[string]any{"binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "quote_items", "title": "", "values": map[string]any{}, "relations": map[string]any{"quote": root.Aggregate.ID}}},
	}})
	if _, err := e.Execute(ctx, "dev", bad); err == nil {
		t.Fatal("invalid later change was accepted")
	}
	var unchanged float64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT value_number FROM object_field_index WHERE workspace_id='wks_1' AND object_instance_id=? AND field_name='amount'`, root.Aggregate.ID).Scan(&unchanged); err != nil || unchanged != 1 {
		t.Fatalf("failed submission changed root amount=%v err=%v", unchanged, err)
	}
	stale := command("change-stale", "change-stale", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{}, "changes": []any{
		map[string]any{"binding_id": "quote", "command_type": "object.update", "target": map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID}, "preconditions": []any{map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID, "expected_version": 99}}, "payload": map[string]any{"values": map[string]any{"amount": 2}}},
	}})
	if _, err := e.Execute(ctx, "dev", stale); err == nil {
		t.Fatal("stale object version accepted")
	}
	execute(t, e, "dev", command("change-cancel", "change-cancel", "work.cancel", "work_stream", &badWorkID, map[string]any{"reason": "closed"}))
	terminal := command("change-terminal", "change-terminal", "work.submit", "work_stream", &badWorkID, map[string]any{"submission": map[string]any{}, "changes": []any{
		map[string]any{"binding_id": "quote", "command_type": "object.update", "target": map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID}, "preconditions": []any{map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID, "expected_version": 1}}, "payload": map[string]any{"values": map[string]any{"amount": 2}}},
	}})
	if _, err := e.Execute(ctx, "dev", terminal); err == nil {
		t.Fatal("cancelled Work accepted changes")
	}
	submit := command("change-submit", "change-submit", "work.submit", "work_stream", &workID, map[string]any{
		"submission": map[string]any{},
		"changes": []any{
			map[string]any{"binding_id": "quote", "command_type": "object.update", "target": map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID}, "preconditions": []any{map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID, "expected_version": 1}}, "payload": map[string]any{"values": map[string]any{"amount": 2}}},
			map[string]any{"binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "quote_items", "title": "Line 1", "values": map[string]any{}, "relations": map[string]any{"quote": root.Aggregate.ID}}},
			map[string]any{"binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "quote_items", "title": "Line 2", "values": map[string]any{}, "relations": map[string]any{"quote": root.Aggregate.ID}}},
			map[string]any{"binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "quote_items", "title": "Line 3", "values": map[string]any{}, "relations": map[string]any{"quote": root.Aggregate.ID}}},
		},
	})
	// Both links plan against the same initial state. Only the second INSERT
	// violates uniqueness, after the root UPDATE and first INSERT have executed.
	unlinked := execute(t, e, "dev", command("change-unlinked", "change-unlinked", "object.create", "object_instance", nil, map[string]any{"object_type_key": "quote_items", "title": "Unlinked", "values": map[string]any{}}))
	link := map[string]any{"binding_id": "item", "command_type": "relation.link", "target": map[string]any{"aggregate_type": "object_relation"}, "preconditions": []any{
		map[string]any{"aggregate_type": "relation_definition", "aggregate_id": relation.Aggregate.ID, "expected_version": 1},
		map[string]any{"aggregate_type": "object_instance", "aggregate_id": root.Aggregate.ID, "expected_version": 1},
		map[string]any{"aggregate_type": "object_instance", "aggregate_id": unlinked.Aggregate.ID, "expected_version": 1},
	}, "payload": map[string]any{"relation_definition_id": relation.Aggregate.ID, "from_object_id": unlinked.Aggregate.ID, "to_object_id": root.Aggregate.ID}}
	var submitPayload map[string]any
	if err := json.Unmarshal(submit.Payload, &submitPayload); err != nil {
		t.Fatal(err)
	}
	constraintFailure := command("change-constraint", "change-constraint", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{}, "changes": []any{submitPayload["changes"].([]any)[1], link, link}})
	if _, err := e.Execute(ctx, "dev", constraintFailure); err == nil || !strings.Contains(err.Error(), "database constraint") {
		t.Fatalf("expected commit-time constraint failure, got %v", err)
	}
	var rootVersion, edges, failedFacts int64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_instances WHERE id=?`, root.Aggregate.ID).Scan(&rootVersion); err != nil || rootVersion != 1 {
		t.Fatalf("root update was not rolled back: version=%d err=%v", rootVersion, err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_relations WHERE from_object_instance_id=?`, unlinked.Aggregate.ID).Scan(&edges); err != nil || edges != 0 {
		t.Fatalf("link was not rolled back: edges=%d err=%v", edges, err)
	}
	var failedCounterRows int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT counter FROM field_sequence_counters WHERE workspace_id='wks_1' AND object_type_id=? AND field_name='number'`, itemType.Aggregate.ID).Scan(&failedCounterRows); err != nil || failedCounterRows != 1 {
		t.Fatalf("failed submission changed sequence counter=%d err=%v", failedCounterRows, err)
	}
	for _, table := range []string{"command_ledger", "activities", "projection_events", "outbox_events", "journal_records"} {
		if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM `+table+` WHERE command_id=?`, constraintFailure.CommandID).Scan(&failedFacts); err != nil || failedFacts != 0 {
			t.Fatalf("%s leaked failed facts=%d err=%v", table, failedFacts, err)
		}
	}
	bulkChanges := make([]any, 32)
	for i := range bulkChanges {
		bulkChanges[i] = submitPayload["changes"].([]any)[1]
	}
	tooLarge := command("change-too-large", "change-too-large", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{}, "changes": bulkChanges})
	if _, err := e.Execute(ctx, "dev", tooLarge); err == nil || !strings.Contains(err.Error(), "64-statement transaction limit") {
		t.Fatalf("expected explicit atomic capacity rejection, got %v", err)
	}
	receipt := execute(t, e, "dev", submit)
	duplicate := execute(t, e, "dev", submit)
	if duplicate.Outcome != "duplicate" || duplicate.ProjectionSequence == nil || receipt.ProjectionSequence == nil || *duplicate.ProjectionSequence != *receipt.ProjectionSequence {
		t.Fatalf("duplicate=%+v receipt=%+v", duplicate, receipt)
	}
	var amount float64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT value_number FROM object_field_index WHERE workspace_id='wks_1' AND object_instance_id=? AND field_name='amount'`, root.Aggregate.ID).Scan(&amount); err != nil || amount != 2 {
		t.Fatalf("root amount=%v err=%v", amount, err)
	}
	var children, projections int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_relations WHERE workspace_id='wks_1' AND relation_definition_id=? AND to_object_instance_id=?`, relation.Aggregate.ID, root.Aggregate.ID).Scan(&children); err != nil || children != 3 {
		t.Fatalf("children=%d err=%v", children, err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM projection_events WHERE workspace_id='wks_1' AND command_id=?`, receipt.CommandID).Scan(&projections); err != nil || projections != 5 {
		t.Fatalf("projections=%d err=%v", projections, err)
	}
	var repeatedEffects int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM (SELECT aggregate_type,aggregate_id FROM projection_events WHERE workspace_id='wks_1' AND command_id=? GROUP BY aggregate_type,aggregate_id HAVING COUNT(*) != 1)`, receipt.CommandID).Scan(&repeatedEffects); err != nil || repeatedEffects != 0 {
		t.Fatalf("projection aggregate identities must be unique: repeated=%d err=%v", repeatedEffects, err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM (SELECT aggregate_type,aggregate_id FROM activities WHERE workspace_id='wks_1' AND command_id=? GROUP BY aggregate_type,aggregate_id HAVING COUNT(*) != 1)`, receipt.CommandID).Scan(&repeatedEffects); err != nil || repeatedEffects != 0 {
		t.Fatalf("activity aggregate identities must be unique: repeated=%d err=%v", repeatedEffects, err)
	}
	rows, err := s.ReadRows(ctx, store.ReadLinearizable, `SELECT value_text FROM object_field_index WHERE workspace_id='wks_1' AND object_type_id=? AND field_name='number' ORDER BY value_text`, itemType.Aggregate.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var numbers []string
	for rows.Next() {
		var number string
		if err := rows.Scan(&number); err != nil {
			t.Fatal(err)
		}
		numbers = append(numbers, number)
	}
	if err := rows.Err(); err != nil || strings.Join(numbers, ",") != "IT-001,IT-002,IT-003,IT-004" {
		t.Fatalf("generated numbers=%v err=%v", numbers, err)
	}
	var counter int64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT counter FROM field_sequence_counters WHERE workspace_id='wks_1' AND object_type_id=? AND field_name='number' AND scope_key='never'`, itemType.Aggregate.ID).Scan(&counter); err != nil || counter != 4 {
		t.Fatalf("sequence counter=%d err=%v", counter, err)
	}
}

func TestWorkflowBindingRestrictionRejectsTaskBindingOutsideRoute(t *testing.T) {
	task := map[string]any{"data_bindings": []any{map[string]any{"id": "quote"}, map[string]any{"id": "item"}}}
	if err := enforceWorkflowBindingIDs([]string{"item"}, task, []workChange{{BindingID: "quote"}}); err == nil {
		t.Fatal("route allowed a Task binding outside its explicit list")
	}
	if err := enforceWorkflowBindingIDs([]string{"missing"}, task, nil); err == nil {
		t.Fatal("route accepted a binding absent from its Task")
	}
}
