package kernel

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestWorkSubmitChangesNewRecordsSQLite(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}

	runWorkSubmitChangesNewRecords(t, testExecutor(s.DB()), false)
}

func TestWorkSubmitChangesNewRecordsRhiza(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-work-new-records")
	defer s.Close()
	runWorkSubmitChangesNewRecords(t, testStoreExecutor(s), false)
}

func TestWorkSubmitChangesNewRecordsRhizaConcurrent(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-work-new-records-concurrent")
	defer s.Close()
	runWorkSubmitChangesNewRecords(t, testStoreExecutor(s), true)
}

func runWorkSubmitChangesNewRecords(t *testing.T, e *Executor, concurrent bool) {
	t.Helper()
	ctx := context.Background()
	e.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	generated := func(prefix string) map[string]any {
		return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"number": map[string]any{"type": "string", "readOnly": true, "x-compos-generation": map[string]any{"version": 1, "on": "create", "template": prefix + "-{{sequence:3}}", "timezone": "UTC", "reset": "never", "unique": true}}}}
	}
	customerType := execute(t, e, "dev", command("new-customer-type", "new-customer-type", "object_type.define", "object_type", nil, map[string]any{"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object", "additionalProperties": false}}))
	quoteType := execute(t, e, "dev", command("new-quote-type", "new-quote-type", "object_type.define", "object_type", nil, map[string]any{"key": "quotes", "singular": "Quote", "plural": "Quotes", "field_schema": generated("Q")}))
	itemType := execute(t, e, "dev", command("new-item-type", "new-item-type", "object_type.define", "object_type", nil, map[string]any{"key": "items", "singular": "Item", "plural": "Items", "field_schema": generated("I")}))
	quoteCustomer := command("new-quote-customer", "new-quote-customer", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "customer", "from_object_type_id": quoteType.Aggregate.ID, "to_object_type_id": customerType.Aggregate.ID, "cardinality": "many_to_one", "required": false})
	quoteCustomer.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: quoteType.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: customerType.Aggregate.ID, ExpectedVersion: 1}}
	quoteCustomerReceipt := execute(t, e, "dev", quoteCustomer)
	itemQuote := command("new-item-quote", "new-item-quote", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "quote", "from_object_type_id": itemType.Aggregate.ID, "to_object_type_id": quoteType.Aggregate.ID, "cardinality": "many_to_one", "required": false})
	itemQuote.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: itemType.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: quoteType.Aggregate.ID, ExpectedVersion: 2}}
	itemQuoteReceipt := execute(t, e, "dev", itemQuote)
	customer := execute(t, e, "dev", command("new-customer", "new-customer", "object.create", "object_instance", nil, map[string]any{"object_type_key": "customers", "title": "Existing customer", "values": map[string]any{}}))

	task := taskSpec("New quote", "Create quote and lines", "ignored")
	task["input_schema"] = map[string]any{"type": "object", "additionalProperties": false, "required": []any{"customer_id"}, "properties": map[string]any{"customer_id": map[string]any{"type": "string"}}}
	task["completion"] = map[string]any{"kind": "submission_valid"}
	task["data_bindings"] = []any{
		map[string]any{"id": "quote", "object_type_id": quoteType.Aggregate.ID, "schema_version": 3, "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": quoteCustomerReceipt.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create"}},
		map[string]any{"id": "item", "object_type_id": itemType.Aggregate.ID, "schema_version": 2, "context_key": "customer_id", "relation_path": []any{map[string]any{"relation_definition_id": quoteCustomerReceipt.Aggregate.ID, "version": 1, "direction": "incoming"}, map[string]any{"relation_definition_id": itemQuoteReceipt.Aggregate.ID, "version": 1, "direction": "incoming"}}, "writable_fields": []any{}, "allow_title": true, "allowed_operations": []any{"object.create"}},
	}
	taskReceipt := execute(t, e, "dev", command("new-task", "new-task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "new-quote", "revision": 1, "kind": "task", "spec": task}))
	openWork := func(id string) string {
		work := execute(t, e, "dev", command(id, id, "work.open", "work_stream", nil, map[string]any{"task_revision_id": taskReceipt.Aggregate.ID, "context": map[string]any{"customer_id": customer.Aggregate.ID}}))
		workID := work.Aggregate.ID
		execute(t, e, "dev", command(id+"-route", id+"-route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
		return workID
	}
	workID := openWork("new-work")

	ref := func(value string) map[string]any { return map[string]any{"client_ref": value} }
	changes := []any{
		map[string]any{"client_ref": "i1", "binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "items", "title": "Line 1", "values": map[string]any{}, "relations": map[string]any{"quote": ref("q")}}},
		map[string]any{"client_ref": "i2", "binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "items", "title": "Line 2", "values": map[string]any{}, "relations": map[string]any{"quote": ref("q")}}},
		map[string]any{"client_ref": "i3", "binding_id": "item", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "items", "title": "Line 3", "values": map[string]any{}, "relations": map[string]any{"quote": ref("q")}}},
		map[string]any{"client_ref": "q", "binding_id": "quote", "command_type": "object.create", "target": map[string]any{"aggregate_type": "object_instance"}, "payload": map[string]any{"object_type_key": "quotes", "title": "Quote", "values": map[string]any{}, "relations": map[string]any{"customer": customer.Aggregate.ID}}},
	}
	submit := command("new-submit", "new-submit", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{"quote": ref("q")}, "changes": changes})
	if concurrent {
		otherWorkID := openWork("new-work-other")
		other := command("new-submit-other", "new-submit-other", "work.submit", "work_stream", &otherWorkID, map[string]any{"submission": map[string]any{"quote": ref("q")}, "changes": changes})
		start := make(chan struct{})
		results := make(chan struct {
			command Command
			receipt Receipt
			err     error
		}, 2)
		var wg sync.WaitGroup
		for _, c := range []Command{submit, other} {
			wg.Add(1)
			go func(c Command) {
				defer wg.Done()
				<-start
				receipt, err := e.Execute(ctx, "dev", c)
				results <- struct {
					command Command
					receipt Receipt
					err     error
				}{c, receipt, err}
			}(c)
		}
		close(start)
		wg.Wait()
		close(results)
		receipts := []Receipt{}
		for result := range results {
			if result.err == nil {
				receipts = append(receipts, result.receipt)
				continue
			}
			receipts = append(receipts, execute(t, e, "dev", result.command))
		}
		quotes, items := []string{}, []string{}
		for _, receipt := range receipts {
			quotes = append(quotes, receipt.ClientRefs["q"].GeneratedValues["number"].(string))
			for _, ref := range []string{"i1", "i2", "i3"} {
				items = append(items, receipt.ClientRefs[ref].GeneratedValues["number"].(string))
			}
		}
		sort.Strings(quotes)
		sort.Strings(items)
		if len(receipts) != 2 || strings.Join(quotes, ",") != "Q-001,Q-002" || strings.Join(items, ",") != "I-001,I-002,I-003,I-004,I-005,I-006" {
			t.Fatalf("concurrent receipts=%#v quotes=%v items=%v", receipts, quotes, items)
		}
		return
	}
	receipt := execute(t, e, "dev", submit)
	if len(receipt.ClientRefs) != 4 || receipt.ClientRefs["q"].GeneratedValues["number"] != "Q-001" || receipt.ClientRefs["i1"].GeneratedValues["number"] != "I-001" || receipt.ClientRefs["i2"].GeneratedValues["number"] != "I-002" || receipt.ClientRefs["i3"].GeneratedValues["number"] != "I-003" {
		t.Fatalf("client refs=%#v", receipt.ClientRefs)
	}
	duplicate := execute(t, e, "dev", submit)
	if duplicate.Outcome != "duplicate" || duplicate.ClientRefs["q"].ID != receipt.ClientRefs["q"].ID || duplicate.ClientRefs["i3"].GeneratedValues["number"] != "I-003" {
		t.Fatalf("duplicate=%#v receipt=%#v", duplicate, receipt)
	}
}
