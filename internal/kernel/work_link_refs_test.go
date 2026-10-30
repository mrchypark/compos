package kernel

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestWorkSubmitChangesLinksNewClientRefsSQLite(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'); INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	runWorkSubmitChangesLinksNewClientRefs(t, NewExecutor(s.DB()), s)
}

func TestWorkSubmitChangesLinksNewClientRefsRhiza(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-work-link-refs")
	defer s.Close()
	runWorkSubmitChangesLinksNewClientRefs(t, NewStoreExecutor(s), s)
}

func runWorkSubmitChangesLinksNewClientRefs(t *testing.T, e *Executor, s *store.Store) {
	t.Helper()
	ctx := context.Background()
	e.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	defineType := func(id, key string) Receipt {
		return execute(t, e, "dev", command(id, id, "object_type.define", "object_type", nil, map[string]any{"key": key, "singular": key, "plural": key, "field_schema": map[string]any{"type": "object", "additionalProperties": false}}))
	}
	customerType := defineType("link-customer-type", "link_customers")
	aType := defineType("link-a-type", "link_as")
	bType := defineType("link-b-type", "link_bs")
	defineRelation := func(id, key, from, to string, fromVersion, toVersion int64) Receipt {
		c := command(id, id, "relation_definition.define", "relation_definition", nil, map[string]any{"key": key, "from_object_type_id": from, "to_object_type_id": to, "cardinality": "many_to_one", "required": false})
		c.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: from, ExpectedVersion: fromVersion}, {AggregateType: "object_type", AggregateID: to, ExpectedVersion: toVersion}}
		return execute(t, e, "dev", c)
	}
	aCustomer := defineRelation("link-a-customer", "a_customer", aType.Aggregate.ID, customerType.Aggregate.ID, 1, 1)
	bCustomer := defineRelation("link-b-customer", "b_customer", bType.Aggregate.ID, customerType.Aggregate.ID, 1, 2)
	aB := defineRelation("link-a-b", "next", aType.Aggregate.ID, bType.Aggregate.ID, 2, 2)
	schemaVersion := func(typeID string) int64 {
		var version int64
		if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT schema_version FROM object_types WHERE workspace_id='wks_1' AND id=?`, typeID).Scan(&version); err != nil {
			t.Fatal(err)
		}
		return version
	}
	customer := execute(t, e, "dev", command("link-customer", "link-customer", "object.create", "object_instance", nil, map[string]any{"object_type_key": "link_customers", "title": "Customer", "values": map[string]any{}}))
	task := taskSpec("Link new records", "Link A to B", "ignored")
	task["input_schema"] = map[string]any{"type": "object", "additionalProperties": false, "required": []any{"customer_id"}, "properties": map[string]any{"customer_id": map[string]any{"type": "string"}}}
	task["completion"] = map[string]any{"kind": "submission_valid"}
	task["data_bindings"] = []any{
		map[string]any{"id": "a", "object_type_id": aType.Aggregate.ID, "schema_version": schemaVersion(aType.Aggregate.ID), "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": aCustomer.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create"}},
		map[string]any{"id": "b", "object_type_id": bType.Aggregate.ID, "schema_version": schemaVersion(bType.Aggregate.ID), "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": bCustomer.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create"}},
		// The terminal binding is B: customer -> A -> B. It may only add A->B.
		map[string]any{"id": "a-to-b", "object_type_id": bType.Aggregate.ID, "schema_version": schemaVersion(bType.Aggregate.ID), "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": aCustomer.Aggregate.ID, "version": 1, "direction": "incoming"}, map[string]any{"relation_definition_id": aB.Aggregate.ID, "version": 1, "direction": "outgoing"}}, "writable_fields": []any{}, "allowed_operations": []any{"relation.link"}},
	}
	taskReceipt := execute(t, e, "dev", command("link-task", "link-task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "link-new-records", "revision": 1, "kind": "task", "spec": task}))
	work := execute(t, e, "dev", command("link-work", "link-work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": taskReceipt.Aggregate.ID, "context": map[string]any{"customer_id": customer.Aggregate.ID}}))
	workID := work.Aggregate.ID
	execute(t, e, "dev", command("link-route", "link-route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	ref := func(value string) map[string]any { return map[string]any{"client_ref": value} }
	// Link is intentionally first. The server must retain its original identity,
	// inject endpoint version 1, and run it after both dependent creates.
	link := map[string]any{"binding_id": "a-to-b", "command_type": "relation.link", "target": map[string]any{"aggregate_type": "object_relation"}, "preconditions": []any{map[string]any{"aggregate_type": "relation_definition", "aggregate_id": aB.Aggregate.ID, "expected_version": 1}}, "payload": map[string]any{"relation_definition_id": aB.Aggregate.ID, "from_object_id": ref("a"), "to_object_id": ref("b")}}
	changes := []any{
		link,
		map[string]any{"client_ref": "b", "binding_id": "b", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "link_bs", "title": "B", "values": map[string]any{}, "relations": map[string]any{"b_customer": customer.Aggregate.ID}}},
		map[string]any{"client_ref": "a", "binding_id": "a", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "link_as", "title": "A", "values": map[string]any{}, "relations": map[string]any{"a_customer": customer.Aggregate.ID}}},
	}
	submit := command("link-submit", "link-submit", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{"a": ref("a"), "b": ref("b")}, "changes": changes})
	receipt := execute(t, e, "dev", submit)
	if receipt.ClientRefs["a"].ID == "" || receipt.ClientRefs["b"].ID == "" || receipt.ClientRefs["a"].Version != 1 || receipt.ClientRefs["b"].Version != 1 {
		t.Fatalf("client refs=%#v", receipt.ClientRefs)
	}
	var links int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_relations WHERE workspace_id='wks_1' AND relation_definition_id=? AND from_object_instance_id=? AND to_object_instance_id=?`, aB.Aggregate.ID, receipt.ClientRefs["a"].ID, receipt.ClientRefs["b"].ID).Scan(&links); err != nil || links != 1 {
		t.Fatalf("new-record link=%d err=%v", links, err)
	}
	duplicate := execute(t, e, "dev", submit)
	if duplicate.Outcome != "duplicate" || duplicate.ClientRefs["a"].ID != receipt.ClientRefs["a"].ID || duplicate.ClientRefs["b"].ID != receipt.ClientRefs["b"].ID {
		t.Fatalf("duplicate=%#v receipt=%#v", duplicate, receipt)
	}
}
