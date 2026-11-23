package kernel

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestWorkOpenValidatesBindingContextRoots(t *testing.T) {
	for _, rhizaStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "rhiza"}[rhizaStore], func(t *testing.T) {
			ctx := context.Background()
			var s *store.Store
			if rhizaStore {
				s = openRhizaKernelStore(t, ctx, "work-context-roots")
			} else {
				var err error
				s, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Exec(ctx, "work-context-fixture", `INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"); err != nil {
					t.Fatal(err)
				}

			}
			defer s.Close()

			var e *Executor
			if rhizaStore {
				e = testStoreExecutor(s)
			} else {
				e = testExecutor(s.DB())
			}
			e.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

			customerType := execute(t, e, "dev", command("context-customer-type", "context-customer-type", "object_type.define", "object_type", nil, map[string]any{"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object", "additionalProperties": false}}))
			quoteType := execute(t, e, "dev", command("context-quote-type", "context-quote-type", "object_type.define", "object_type", nil, map[string]any{"key": "quotes", "singular": "Quote", "plural": "Quotes", "field_schema": map[string]any{"type": "object", "additionalProperties": false}}))
			relation := command("context-quote-customer", "context-quote-customer", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "customer", "from_object_type_id": quoteType.Aggregate.ID, "to_object_type_id": customerType.Aggregate.ID, "cardinality": "many_to_one", "required": false})
			relation.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: quoteType.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: customerType.Aggregate.ID, ExpectedVersion: 1}}
			relationReceipt := execute(t, e, "dev", relation)
			var quoteSchemaVersion int64
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT schema_version FROM object_types WHERE workspace_id='wks_1' AND id=?`, quoteType.Aggregate.ID).Scan(&quoteSchemaVersion); err != nil {
				t.Fatal(err)
			}

			customer := execute(t, e, "dev", command("context-customer", "context-customer", "object.create", "object_instance", nil, map[string]any{"object_type_key": "customers", "title": "Customer", "values": map[string]any{}}))
			archivedCustomer := execute(t, e, "dev", command("context-archived-customer", "context-archived-customer", "object.create", "object_instance", nil, map[string]any{"object_type_key": "customers", "title": "Archived", "values": map[string]any{}}))
			wrongType := execute(t, e, "dev", command("context-quote", "context-quote", "object.create", "object_instance", nil, map[string]any{"object_type_key": "quotes", "title": "Quote", "values": map[string]any{}}))
			deleteArchived := command("context-archive", "context-archive", "object.delete", "object_instance", &archivedCustomer.Aggregate.ID, map[string]any{})
			deleteArchived.Preconditions = []Precondition{{AggregateType: "object_instance", AggregateID: archivedCustomer.Aggregate.ID, ExpectedVersion: 1}}
			execute(t, e, "dev", deleteArchived)

			task := map[string]any{
				"name": "Quote task", "goal": "Use a customer context",
				"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"customer_id"}, "properties": map[string]any{"customer_id": map[string]any{"type": "string"}}},
				"output_schema": map[string]any{"type": "object", "additionalProperties": false},
				"completion":    map[string]any{"kind": "submission_valid"},
				"data_bindings": []any{map[string]any{"id": "quote", "object_type_id": quoteType.Aggregate.ID, "schema_version": quoteSchemaVersion, "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": relationReceipt.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create"}}},
			}
			taskReceipt := execute(t, e, "dev", command("context-task", "context-task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "context-task", "revision": 1, "kind": "task", "spec": task}))

			open := func(id, root string) error {
				_, err := e.Execute(ctx, "dev", command(id, id, "work.open", "work_stream", nil, map[string]any{"task_revision_id": taskReceipt.Aggregate.ID, "context": map[string]any{"customer_id": root}}))
				return err
			}
			if err := open("context-valid", customer.Aggregate.ID); err != nil {
				t.Fatalf("valid alternate customer context: %v", err)
			}

			if err := s.Exec(ctx, "context-other-workspace", `INSERT INTO workspaces VALUES('wks_other','other','Other',1,?,?)`, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"); err != nil {
				t.Fatal(err)
			}

			foreignType := command("context-foreign-type", "context-foreign-type", "object_type.define", "object_type", nil, map[string]any{"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object", "additionalProperties": false}})
			foreignType.WorkspaceID = "wks_other"
			execute(t, e, "dev", foreignType)
			foreignObject := command("context-foreign-object", "context-foreign-object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "customers", "title": "Foreign", "values": map[string]any{}})
			foreignObject.WorkspaceID = "wks_other"
			foreignObjectReceipt := execute(t, e, "dev", foreignObject)

			for name, root := range map[string]string{
				"nonexistent": "obj_missing",
				"archived":    archivedCustomer.Aggregate.ID,
				"foreign":     foreignObjectReceipt.Aggregate.ID,
				"wrong_type":  wrongType.Aggregate.ID,
			} {
				if err := open("context-"+name, root); err == nil {
					t.Fatalf("%s binding context was accepted", name)
				} else {
					var commandErr *CommandError
					if !errors.As(err, &commandErr) || commandErr.Outcome != "precondition_failed" {
						t.Fatalf("%s error=%v, want precondition_failed", name, err)
					}
				}
			}
		})
	}
}
