package kernel

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestKernelCommitsThroughEmbeddedRhiza(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "kernel-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	one := int64(1)
	if err := s.ApplyStatements(ctx, "kernel-fixture", []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}

	executor := NewStoreExecutor(s)
	define := command(
		"cmd_rhiza_type", "idem_rhiza_type", "object_type.define", "object_type", nil,
		map[string]any{"key": "contracts", "singular": "Contract", "plural": "Contracts", "field_schema": map[string]any{"type": "object"}},
	)
	receipt, err := executor.Execute(ctx, "dev", define)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := executor.Execute(ctx, "dev", define)
	if err != nil || duplicate.Outcome != "duplicate" || duplicate.ProjectionSequence == nil || receipt.ProjectionSequence == nil || *duplicate.ProjectionSequence != *receipt.ProjectionSequence {
		t.Fatalf("duplicate=%#v original=%#v err=%v", duplicate, receipt, err)
	}
	var ledger int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id=?`, receipt.CommandID).Scan(&ledger); err != nil || ledger != 1 {
		t.Fatalf("ledger count=%d err=%v", ledger, err)
	}
}

func TestRetiredObjectTypeTemplateSetIsRejectedByRhiza(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-retired-template")
	defer s.Close()
	target := "otp_retired"
	_, err := NewStoreExecutor(s).Execute(ctx, "dev", command("cmd_retired_template", "idem_retired_template", "object_type.template_set", "object_type", &target, map[string]any{}))
	var rejected *CommandError
	if !errors.As(err, &rejected) || rejected.Outcome != "validation_failed" {
		t.Fatalf("error=%v", err)
	}
}

func TestKernelRhizaPreconditionFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "kernel-conflict-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	one := int64(1)
	if err := s.ApplyStatements(ctx, "kernel-conflict-fixture", []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO object_types(id,workspace_id,key,singular,plural,schema_version,field_schema_json,relation_schema_json,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES('otp_guard','wks_1','guard','Guard','Guards',1,'{"type":"object"}','{"links":[]}',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','dev','dev')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO object_type_schema_revisions(workspace_id,object_type_id,schema_version,field_schema_json,relation_schema_json,digest,created_at,created_by_principal_id) VALUES('wks_1','otp_guard',1,'{"type":"object"}','{"links":[]}','sha256:test','2026-01-01T00:00:00Z','dev')`, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}
	guarded := command(
		"cmd_conflict", "idem_conflict", "object_type.define", "object_type", nil,
		map[string]any{"key": "should-not-exist", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}},
	)
	guarded.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: "otp_guard", ExpectedVersion: 2}}
	_, err = NewStoreExecutor(s).Execute(ctx, "dev", guarded)
	var rejected *CommandError
	if !errors.As(err, &rejected) || rejected.Outcome != "precondition_failed" {
		t.Fatalf("error=%v", err)
	}
	var objects, ledger int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_types WHERE key='should-not-exist'`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id='cmd_conflict'`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if objects != 0 || ledger != 0 {
		t.Fatalf("objects=%d ledger=%d", objects, ledger)
	}
}

func TestKernelRhizaRecognizesEveryRegisteredCommand(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "kernel-unsupported-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, definition := range definitions {
		name := strings.ReplaceAll(definition.WireName, ".", "_")
		var aggregateID *string
		if definition.TargetAggregateID == "required" {
			missing := "missing"
			aggregateID = &missing
		}
		c := command("cmd_registered_"+name, "idem_registered_"+name, definition.WireName, definition.AllowedTargetAggregateTypes[0], aggregateID, map[string]any{})
		_, err = NewStoreExecutor(s).executeRhiza(ctx, "dev", c)
		var rejected *CommandError
		if !errors.As(err, &rejected) || rejected.Outcome == "unsupported_storage" {
			t.Fatalf("definition[%d] %s error=%v", i, definition.WireName, err)
		}
		var ledger int
		if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id=?`, c.CommandID).Scan(&ledger); err != nil || ledger != 0 {
			t.Fatalf("definition[%d] %s ledger=%d err=%v", i, definition.WireName, ledger, err)
		}
	}
}

func TestRuntimeResultCommandsRequireReducerCausation(t *testing.T) {
	streamID := "wrk_1"
	payload := map[string]any{
		"invocation_id": "inv_1", "callback_id": "cb_1", "runtime_status": "succeeded", "output": map[string]any{},
		"adapter_inbound_event_id": "cb_1", "execution_id": "exec_1", "provider_key": "provider", "provider_run_ref": "run_1", "handler_version": "1", "attempt_count": 1,
	}
	for _, commandType := range []string{"function.record_result", "agent.record_result"} {
		c := command("cmd_"+strings.ReplaceAll(commandType, ".", "_"), "idem_"+commandType, commandType, "work_stream", &streamID, payload)
		_, err := (&Executor{}).Execute(context.Background(), "dev", c)
		var rejected *CommandError
		if !errors.As(err, &rejected) || rejected.Outcome != "forbidden" {
			t.Fatalf("%s error=%v", commandType, err)
		}
	}
}

func TestKernelRhizaObjectTypeAndRelationLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-lifecycle-test")
	defer s.Close()
	executor := NewStoreExecutor(s)

	left := execute(t, executor, "dev", command("cmd_left", "idem_left", "object_type.define", "object_type", nil, map[string]any{
		"key": "customers", "singular": "Customer", "plural": "Customers",
		"field_schema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}},
	}))
	right := execute(t, executor, "dev", command("cmd_right", "idem_right", "object_type.define", "object_type", nil, map[string]any{
		"key": "orders", "singular": "Order", "plural": "Orders", "field_schema": map[string]any{"type": "object"},
	}))
	leftID, rightID := left.Aggregate.ID, right.Aggregate.ID

	evolve := command("cmd_evolve", "idem_evolve", "object_type.schema_evolve", "object_type", &leftID, map[string]any{
		"field_schema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"name": map[string]any{"type": "string"}}},
	})
	evolve.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: leftID, ExpectedVersion: 1}}
	if receipt := execute(t, executor, "dev", evolve); receipt.Aggregate.Version != 2 {
		t.Fatalf("evolve aggregate=%+v", receipt.Aggregate)
	}

	relation := command("cmd_relation", "idem_relation", "relation_definition.define", "relation_definition", nil, map[string]any{
		"key": "customer_orders", "from_object_type_id": leftID, "to_object_type_id": rightID, "cardinality": "one_to_many", "required": false,
	})
	relation.Preconditions = []Precondition{
		{AggregateType: "object_type", AggregateID: leftID, ExpectedVersion: 2},
		{AggregateType: "object_type", AggregateID: rightID, ExpectedVersion: 1},
	}
	relationReceipt := execute(t, executor, "dev", relation)
	var leftRelation, rightRelation string
	var leftVersion, rightVersion, leftSchema, rightSchema int64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,version,schema_version FROM object_types WHERE id=?`, leftID).Scan(&leftRelation, &leftVersion, &leftSchema); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,version,schema_version FROM object_types WHERE id=?`, rightID).Scan(&rightRelation, &rightVersion, &rightSchema); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(leftRelation, relationReceipt.Aggregate.ID) || leftRelation != rightRelation || leftVersion != 3 || rightVersion != 2 || leftSchema != 3 || rightSchema != 2 {
		t.Fatalf("left=(%s,%d,%d) right=(%s,%d,%d)", leftRelation, leftVersion, leftSchema, rightRelation, rightVersion, rightSchema)
	}

	deleteRelation := command("cmd_relation_delete", "idem_relation_delete", "relation_definition.delete", "relation_definition", &relationReceipt.Aggregate.ID, map[string]any{})
	deleteRelation.Preconditions = []Precondition{{AggregateType: "relation_definition", AggregateID: relationReceipt.Aggregate.ID, ExpectedVersion: 1}}
	execute(t, executor, "dev", deleteRelation)
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,version FROM object_types WHERE id=?`, leftID).Scan(&leftRelation, &leftVersion); err != nil {
		t.Fatal(err)
	}
	if leftRelation != `{"links":[]}` || leftVersion != 4 {
		t.Fatalf("relation delete projection=%s version=%d", leftRelation, leftVersion)
	}

	deleteType := command("cmd_type_delete", "idem_type_delete", "object_type.delete", "object_type", &leftID, map[string]any{})
	deleteType.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: leftID, ExpectedVersion: 4}}
	execute(t, executor, "dev", deleteType)
	var archived string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT archived_at,version FROM object_types WHERE id=?`, leftID).Scan(&archived, &leftVersion); err != nil || archived == "" || leftVersion != 5 {
		t.Fatalf("archived=%q version=%d err=%v", archived, leftVersion, err)
	}
	var ledger, activity, outbox, projection int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM command_ledger`:    &ledger,
		`SELECT COUNT(*) FROM activities`:        &activity,
		`SELECT COUNT(*) FROM outbox_events`:     &outbox,
		`SELECT COUNT(*) FROM projection_events`: &projection,
	} {
		if err := s.ReadRow(ctx, store.ReadLinearizable, query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if ledger != 6 || activity != ledger || outbox != ledger || projection != ledger {
		t.Fatalf("ledger=%d activity=%d outbox=%d projection=%d", ledger, activity, outbox, projection)
	}
}

func TestKernelRhizaLifecycleFailuresRollBack(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-lifecycle-failure-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	typeReceipt := execute(t, executor, "dev", command("cmd_base", "idem_base", "object_type.define", "object_type", nil, map[string]any{
		"key": "items", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"},
	}))
	id := typeReceipt.Aggregate.ID

	stale := command("cmd_stale_evolve", "idem_stale_evolve", "object_type.schema_evolve", "object_type", &id, map[string]any{"field_schema": map[string]any{"type": "object"}})
	stale.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: id, ExpectedVersion: 2}}
	assertCommandOutcome(t, executor, stale, "precondition_failed")
	invalid := command("cmd_invalid_evolve", "idem_invalid_evolve", "object_type.schema_evolve", "object_type", &id, map[string]any{"field_schema": map[string]any{"type": "string"}})
	invalid.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: id, ExpectedVersion: 1}}
	assertCommandOutcome(t, executor, invalid, "schema_mismatch")
	var version, revisions, failedLedger int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_types WHERE id=?`, id).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_type_schema_revisions WHERE object_type_id=?`, id).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id IN ('cmd_stale_evolve','cmd_invalid_evolve')`).Scan(&failedLedger); err != nil {
		t.Fatal(err)
	}
	if version != 1 || revisions != 1 || failedLedger != 0 {
		t.Fatalf("version=%d revisions=%d failed ledger=%d", version, revisions, failedLedger)
	}

	other := execute(t, executor, "dev", command("cmd_failure_other", "idem_failure_other", "object_type.define", "object_type", nil, map[string]any{
		"key": "others", "singular": "Other", "plural": "Others", "field_schema": map[string]any{"type": "object"},
	}))
	relation := command("cmd_failure_relation", "idem_failure_relation", "relation_definition.define", "relation_definition", nil, map[string]any{
		"key": "item_other", "from_object_type_id": id, "to_object_type_id": other.Aggregate.ID, "cardinality": "many_to_many", "required": false,
	})
	relation.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: id, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: other.Aggregate.ID, ExpectedVersion: 1}}
	relationReceipt := execute(t, executor, "dev", relation)
	conflictingRelation := command("cmd_relation_conflict", "idem_relation_conflict", "relation_definition.define", "relation_definition", nil, map[string]any{
		"key": "item_other", "from_object_type_id": id, "to_object_type_id": other.Aggregate.ID, "cardinality": "one_to_many", "required": false,
	})
	conflictingRelation.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: id, ExpectedVersion: 2}, {AggregateType: "object_type", AggregateID: other.Aggregate.ID, ExpectedVersion: 2}}
	assertCommandOutcome(t, executor, conflictingRelation, "validation_failed")
	staleRelationDelete := command("cmd_stale_relation_delete", "idem_stale_relation_delete", "relation_definition.delete", "relation_definition", &relationReceipt.Aggregate.ID, map[string]any{})
	staleRelationDelete.Preconditions = []Precondition{{AggregateType: "relation_definition", AggregateID: relationReceipt.Aggregate.ID, ExpectedVersion: 2}}
	assertCommandOutcome(t, executor, staleRelationDelete, "precondition_failed")
	staleTypeDelete := command("cmd_stale_type_delete", "idem_stale_type_delete", "object_type.delete", "object_type", &id, map[string]any{})
	staleTypeDelete.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: id, ExpectedVersion: 99}}
	assertCommandOutcome(t, executor, staleTypeDelete, "precondition_failed")
	var activeRelation, archivedTypes, conflictLedger int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM relation_definitions WHERE id=? AND archived_at IS NULL`, relationReceipt.Aggregate.ID).Scan(&activeRelation); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_types WHERE id IN (?,?) AND archived_at IS NOT NULL`, id, other.Aggregate.ID).Scan(&archivedTypes); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id IN ('cmd_relation_conflict','cmd_stale_relation_delete','cmd_stale_type_delete')`).Scan(&conflictLedger); err != nil {
		t.Fatal(err)
	}
	if activeRelation != 1 || archivedTypes != 0 || conflictLedger != 0 {
		t.Fatalf("active relation=%d archived types=%d failed ledger=%d", activeRelation, archivedTypes, conflictLedger)
	}
}

func TestKernelRhizaDelegatedServiceCapability(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-service-capability-test")
	defer s.Close()
	one := int64(1)
	if err := s.ApplyStatements(ctx, "service-capability", []rhiza.SQLStatement{{SQL: `INSERT INTO actor_capabilities(id,workspace_id,principal_id,actor_type,actor_id,command_type,causation_source,relation_definition_id,idempotency_key_prefix,created_at,updated_at) VALUES('cap_1','wks_1','dev','service','importer','object_type.define','api','','service:','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	define := command("cmd_service", "service:type", "object_type.define", "object_type", nil, map[string]any{"key": "imports", "singular": "Import", "plural": "Imports", "field_schema": map[string]any{"type": "object"}})
	define.RequestedActor = &Actor{Type: "service", ID: "importer"}
	receipt := execute(t, NewStoreExecutor(s), "dev", define)
	var effective string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT effective_actor_json FROM command_ledger WHERE command_id=?`, receipt.CommandID).Scan(&effective); err != nil || effective != `{"type":"service","id":"importer"}` {
		t.Fatalf("effective=%s err=%v", effective, err)
	}
}

func TestKernelRhizaConcurrentDuplicateConverges(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-concurrent-duplicate-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	define := command("cmd_concurrent", "idem_concurrent", "object_type.define", "object_type", nil, map[string]any{
		"key": "concurrent", "singular": "Concurrent", "plural": "Concurrent", "field_schema": map[string]any{"type": "object"},
	})
	start := make(chan struct{})
	results := make(chan Receipt, 2)
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := executor.Execute(ctx, "dev", define)
			results <- receipt
			errors <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	outcomes := map[string]int{}
	for receipt := range results {
		outcomes[receipt.Outcome]++
	}
	if outcomes["applied"] != 1 || outcomes["duplicate"] != 1 {
		t.Fatalf("outcomes=%v", outcomes)
	}
	var objects, ledger int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_types WHERE key='concurrent'`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger WHERE command_id='cmd_concurrent'`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if objects != 1 || ledger != 1 {
		t.Fatalf("objects=%d ledger=%d", objects, ledger)
	}
}

func TestKernelRhizaRelationSchemasPreserveIncidentAndSelfRelations(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-relation-schema-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	ids := map[string]string{}
	for _, key := range []string{"a", "b", "c"} {
		receipt := execute(t, executor, "dev", command("cmd_type_"+key, "idem_type_"+key, "object_type.define", "object_type", nil, map[string]any{
			"key": key, "singular": strings.ToUpper(key), "plural": strings.ToUpper(key) + "s", "field_schema": map[string]any{"type": "object"},
		}))
		ids[key] = receipt.Aggregate.ID
	}
	defineRelation := func(commandID, key, from, to string, fromVersion, toVersion int64) Receipt {
		definition := command(commandID, "idem_"+commandID, "relation_definition.define", "relation_definition", nil, map[string]any{
			"key": key, "from_object_type_id": ids[from], "to_object_type_id": ids[to], "cardinality": "many_to_many", "required": false,
		})
		definition.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: ids[from], ExpectedVersion: fromVersion}}
		if from != to {
			definition.Preconditions = append(definition.Preconditions, Precondition{AggregateType: "object_type", AggregateID: ids[to], ExpectedVersion: toVersion})
		}
		return execute(t, executor, "dev", definition)
	}
	ac := defineRelation("cmd_ac", "a_c", "a", "c", 1, 1)
	ab := defineRelation("cmd_ab", "a_b", "a", "b", 2, 1)
	self := defineRelation("cmd_aa", "a_a", "a", "a", 3, 3)

	var relationJSON string
	var version, schemaVersion, revisions int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,version,schema_version FROM object_types WHERE id=?`, ids["a"]).Scan(&relationJSON, &version, &schemaVersion); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(relationJSON, ac.Aggregate.ID) || !strings.Contains(relationJSON, ab.Aggregate.ID) || !strings.Contains(relationJSON, self.Aggregate.ID) || version != 4 || schemaVersion != 4 {
		t.Fatalf("a schema=%s version=%d schema_version=%d", relationJSON, version, schemaVersion)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_type_schema_revisions WHERE object_type_id=?`, ids["a"]).Scan(&revisions); err != nil || revisions != 4 {
		t.Fatalf("a revisions=%d err=%v", revisions, err)
	}
	for key, included := range map[string]string{"b": ab.Aggregate.ID, "c": ac.Aggregate.ID} {
		if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json FROM object_types WHERE id=?`, ids[key]).Scan(&relationJSON); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(relationJSON, included) || strings.Contains(relationJSON, self.Aggregate.ID) {
			t.Fatalf("%s schema=%s", key, relationJSON)
		}
	}

	removeSelf := command("cmd_aa_delete", "idem_aa_delete", "relation_definition.delete", "relation_definition", &self.Aggregate.ID, map[string]any{})
	removeSelf.Preconditions = []Precondition{{AggregateType: "relation_definition", AggregateID: self.Aggregate.ID, ExpectedVersion: 1}}
	execute(t, executor, "dev", removeSelf)
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,version,schema_version FROM object_types WHERE id=?`, ids["a"]).Scan(&relationJSON, &version, &schemaVersion); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(relationJSON, self.Aggregate.ID) || !strings.Contains(relationJSON, ac.Aggregate.ID) || !strings.Contains(relationJSON, ab.Aggregate.ID) || version != 5 || schemaVersion != 5 {
		t.Fatalf("a schema after self delete=%s version=%d schema_version=%d", relationJSON, version, schemaVersion)
	}
}

func TestKernelRhizaWorldSpecWorkAndKernelFunction(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-world-work-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	execute(t, executor, "dev", command("rhz_cmd_type", "rhz_idem_type", "object_type.define", "object_type", nil, map[string]any{"key": "requests", "singular": "Request", "plural": "Requests", "field_schema": map[string]any{"type": "object", "additionalProperties": true, "state_machine": map[string]any{"initial": "requested", "transitions": map[string]any{"requested": []any{"approved"}, "approved": []any{}}}}}))
	object := execute(t, executor, "dev", command("rhz_cmd_object", "rhz_idem_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "requests", "title": "R-1", "values": map[string]any{}}))
	task := execute(t, executor, "dev", command("rhz_cmd_task", "rhz_idem_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "approval", "revision": 1, "kind": "task", "spec": taskSpec("Approval", "Approve", "approved")}))
	function := execute(t, executor, "dev", command("rhz_cmd_function", "rhz_idem_function", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "approve", "revision": 1, "kind": "function", "spec": map[string]any{
		"name": "Approve", "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}}},
		"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"id", "state_key", "version"}, "properties": map[string]any{"id": map[string]any{"type": "string"}, "state_key": map[string]any{"type": "string"}, "version": map[string]any{"type": "integer"}}},
		"binding":       map[string]any{"kind": "kernel_command", "command_type": "object.move", "to_state": "approved"},
	}}))
	work := execute(t, executor, "dev", command("rhz_cmd_work", "rhz_idem_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}}))
	workID := work.Aggregate.ID
	execute(t, executor, "dev", command("rhz_cmd_route", "rhz_idem_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	execute(t, executor, "dev", command("rhz_cmd_submit", "rhz_idem_submit", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{"decision": "approved"}}))
	receipt := execute(t, executor, "dev", command("rhz_cmd_invoke", "rhz_idem_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID, "expected_version": 1}}))
	if receipt.Aggregate.ID != object.Aggregate.ID || receipt.Aggregate.Version != 2 {
		t.Fatalf("invoke aggregate=%+v", receipt.Aggregate)
	}
	var state string
	var version int64
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT state_key,version FROM object_instances WHERE id=?`, object.Aggregate.ID).Scan(&state, &version); err != nil || state != "approved" || version != 2 {
		t.Fatalf("state=%q version=%d err=%v", state, version, err)
	}
	rows, err := s.ReadRows(ctx, store.ReadLinearizable, `SELECT kind FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? ORDER BY stream_sequence`, workID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatal(err)
		}
		got = append(got, kind)
	}
	want := []string{"work.opened", "work.routed", "work.submitted", "function.requested", "function.succeeded", "work.satisfied"}
	if canonical(got) != canonical(want) {
		t.Fatalf("journal=%v", got)
	}
}

func TestKernelRhizaExternalFunctionAndAgentCallbacks(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-external-agent-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	execute(t, executor, "dev", command("rhz_ext_type", "rhz_ext_type_i", "object_type.define", "object_type", nil, map[string]any{"key": "items", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}}))
	object := execute(t, executor, "dev", command("rhz_ext_object", "rhz_ext_object_i", "object.create", "object_instance", nil, map[string]any{"object_type_key": "items", "title": "I-1"}))
	taskSpec := map[string]any{
		"name": "Process", "goal": "Process item",
		"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}}},
		"output_schema": map[string]any{"type": "object", "additionalProperties": true},
		"completion":    map[string]any{"kind": "submission_valid"},
	}
	task := execute(t, executor, "dev", command("rhz_ext_task", "rhz_ext_task_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "process", "revision": 1, "kind": "task", "spec": taskSpec}))
	function := execute(t, executor, "dev", command("rhz_ext_function", "rhz_ext_function_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "send", "revision": 1, "kind": "function", "spec": map[string]any{
		"name": "Send", "input_schema": map[string]any{"type": "object", "additionalProperties": true}, "output_schema": map[string]any{"type": "object", "additionalProperties": true},
		"binding": map[string]any{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage"},
	}}))
	agent := execute(t, executor, "dev", command("rhz_ext_agent", "rhz_ext_agent_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "agent", "revision": 1, "kind": "participant", "spec": map[string]any{
		"name": "Worker", "instructions": "Process the item", "execution_policy": map[string]any{"handler_key": "agent.run", "handler_version": "1"},
	}}))
	work := execute(t, executor, "dev", command("rhz_ext_work", "rhz_ext_work_i", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}}))
	workID := work.Aggregate.ID
	execute(t, executor, "dev", command("rhz_ext_route", "rhz_ext_route_i", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	invoke := command("rhz_ext_invoke", "rhz_ext_invoke_i", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"text": "hello"}})
	execute(t, executor, "dev", invoke)
	var invocationID string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='function.requested'`, invoke.CommandID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	one := int64(1)
	if err := s.ApplyStatements(ctx, "rhz-ext-evidence", []rhiza.SQLStatement{{SQL: `INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, Args: []any{"ain_ext", "wks_1", "conductor", "cb_ext", "sha256:" + strings.Repeat("0", 64), `{}`, "2026-01-01T00:00:00Z"}, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	result := command("rhz_ext_result", "rhz_ext_result_i", "function.record_result", "work_stream", &workID, map[string]any{"invocation_id": invocationID, "callback_id": "cb_ext", "adapter_inbound_event_id": "cb_ext", "execution_id": "exec_ext", "provider_key": "connector.invoke", "provider_run_ref": "run_ext", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"ok": true}})
	result.Causation.Source = "adapter_reducer"
	execute(t, executor, "dev", result)
	var evidenceStatus string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_ext'`).Scan(&evidenceStatus); err != nil || evidenceStatus != "reduced" {
		t.Fatalf("evidence=%q err=%v", evidenceStatus, err)
	}
	if err := s.ApplyStatements(ctx, "rhz-ext-late-evidence", []rhiza.SQLStatement{{SQL: `INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, Args: []any{"ain_ext_late", "wks_1", "conductor", "cb_ext_late", "sha256:" + strings.Repeat("2", 64), `{}`, "2026-01-01T00:00:00Z"}, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	lateResult := command("rhz_ext_late_result", "rhz_ext_late_result_i", "function.record_result", "work_stream", &workID, map[string]any{"invocation_id": invocationID, "callback_id": "cb_ext_late", "adapter_inbound_event_id": "cb_ext_late", "execution_id": "exec_ext_late", "provider_key": "connector.invoke", "provider_run_ref": "run_ext_late", "handler_version": "1", "attempt_count": 2, "runtime_status": "succeeded", "output": map[string]any{"ok": true}})
	lateResult.Causation.Source = "adapter_reducer"
	assertCommandOutcome(t, executor, lateResult, "precondition_failed")
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_ext_late'`).Scan(&evidenceStatus); err != nil || evidenceStatus != "appended" {
		t.Fatalf("late evidence=%q err=%v", evidenceStatus, err)
	}

	routeAgent := command("rhz_agent_route", "rhz_agent_route_i", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "agent", "id": agent.Aggregate.ID}})
	execute(t, executor, "dev", routeAgent)
	var agentInvocation string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='agent.requested'`, routeAgent.CommandID).Scan(&agentInvocation); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyStatements(ctx, "rhz-agent-evidence", []rhiza.SQLStatement{{SQL: `INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, Args: []any{"ain_agent", "wks_1", "conductor", "cb_agent", "sha256:" + strings.Repeat("1", 64), `{}`, "2026-01-01T00:00:00Z"}, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	agentResult := command("rhz_agent_result", "rhz_agent_result_i", "agent.record_result", "work_stream", &workID, map[string]any{"invocation_id": agentInvocation, "callback_id": "cb_agent", "adapter_inbound_event_id": "cb_agent", "execution_id": "exec_agent", "provider_key": "agent.run", "provider_run_ref": "run_agent", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"done": true}})
	agentResult.Causation.Source = "adapter_reducer"
	execute(t, executor, "dev", agentResult)
	if err := s.ApplyStatements(ctx, "rhz-agent-late-evidence", []rhiza.SQLStatement{{SQL: `INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, Args: []any{"ain_agent_late", "wks_1", "conductor", "cb_agent_late", "sha256:" + strings.Repeat("3", 64), `{}`, "2026-01-01T00:00:00Z"}, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	lateAgentResult := command("rhz_agent_late_result", "rhz_agent_late_result_i", "agent.record_result", "work_stream", &workID, map[string]any{"invocation_id": agentInvocation, "callback_id": "cb_agent_late", "adapter_inbound_event_id": "cb_agent_late", "execution_id": "exec_agent_late", "provider_key": "agent.run", "provider_run_ref": "run_agent_late", "handler_version": "1", "attempt_count": 2, "runtime_status": "succeeded", "output": map[string]any{"done": true}})
	lateAgentResult.Causation.Source = "adapter_reducer"
	assertCommandOutcome(t, executor, lateAgentResult, "precondition_failed")
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_agent_late'`).Scan(&evidenceStatus); err != nil || evidenceStatus != "appended" {
		t.Fatalf("late agent evidence=%q err=%v", evidenceStatus, err)
	}
	var satisfied int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='work.satisfied'`, workID).Scan(&satisfied); err != nil || satisfied != 1 {
		t.Fatalf("satisfied=%d err=%v", satisfied, err)
	}
}

func TestKernelRhizaWorkflowDeadlineAndDomainEvent(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-workflow-deadline-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	execute(t, executor, "dev", command("rhz_wf_type", "rhz_wf_type_i", "object_type.define", "object_type", nil, map[string]any{"key": "cases", "singular": "Case", "plural": "Cases", "field_schema": map[string]any{"type": "object", "state_machine": map[string]any{"initial": "open", "transitions": map[string]any{"open": []any{}}}}}))
	object := execute(t, executor, "dev", command("rhz_wf_object", "rhz_wf_object_i", "object.create", "object_instance", nil, map[string]any{"object_type_key": "cases", "title": "C-1"}))
	task := execute(t, executor, "dev", command("rhz_wf_task", "rhz_wf_task_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "case", "revision": 1, "kind": "task", "spec": taskSpec("Case", "Handle case", "open")}))
	workflow := execute(t, executor, "dev", command("rhz_wf_spec", "rhz_wf_spec_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "case-flow", "revision": 1, "kind": "workflow", "spec": map[string]any{"name": "Case flow", "steps": []any{map[string]any{"id": "review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}}}))
	function := execute(t, executor, "dev", command("rhz_wf_function", "rhz_wf_function_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "case-function", "revision": 1, "kind": "function", "spec": map[string]any{"name": "Case function", "input_schema": map[string]any{"type": "object", "additionalProperties": true}, "output_schema": map[string]any{"type": "object", "additionalProperties": true}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID}}}))
	work := execute(t, executor, "dev", command("rhz_wf_work", "rhz_wf_work_i", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}}))
	workID := work.Aggregate.ID
	execute(t, executor, "dev", command("rhz_wf_route", "rhz_wf_route_i", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	invoke := command("rhz_wf_invoke", "rhz_wf_invoke_i", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID}})
	execute(t, executor, "dev", invoke)
	var invocationID string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='workflow.started'`, invoke.CommandID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	complete := command("rhz_wf_complete", "rhz_wf_complete_i", "workflow.complete", "work_stream", &workID, map[string]any{"invocation_id": invocationID, "runtime_status": "succeeded", "output": map[string]any{}})
	complete.Causation.Source = "workflow"
	execute(t, executor, "dev", complete)
	var completed int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind IN ('workflow.completed','function.succeeded','work.satisfied')`, workID).Scan(&completed); err != nil || completed != 3 {
		t.Fatalf("completed=%d err=%v", completed, err)
	}

	deadline := "2026-01-02T00:00:00Z"
	deadlineWork := execute(t, executor, "dev", command("rhz_deadline_work", "rhz_deadline_work_i", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}, "deadline_at": deadline}))
	deadlineID := deadlineWork.Aggregate.ID
	deadlineCommand := command("rhz_deadline", "rhz_deadline_i", "work.record_deadline", "work_stream", &deadlineID, map[string]any{"deadline_at": deadline})
	deadlineCommand.Causation.Source = "scheduler"
	execute(t, executor, "dev", deadlineCommand)
	execute(t, executor, "dev", command("rhz_deadline_cancel", "rhz_deadline_cancel_i", "work.cancel", "work_stream", &deadlineID, map[string]any{"reason": "closed"}))
	eventSpec := execute(t, executor, "dev", command("rhz_event_spec", "rhz_event_spec_i", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "case-event", "revision": 1, "kind": "event", "spec": map[string]any{"name": "Case event", "payload_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"case_id"}, "properties": map[string]any{"case_id": map[string]any{"type": "string"}}}}}))
	event := execute(t, executor, "dev", command("rhz_event_emit", "rhz_event_emit_i", "event.emit", "domain_event", nil, map[string]any{"event_revision_id": eventSpec.Aggregate.ID, "data": map[string]any{"case_id": object.Aggregate.ID}}))
	var journalEvent int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='domain.event.emitted'`, event.Aggregate.ID).Scan(&journalEvent); err != nil || journalEvent != 1 {
		t.Fatalf("events=%d err=%v", journalEvent, err)
	}
}

func TestKernelRhizaKnowledgePromotionAndRemoval(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-knowledge-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	source := "---\ntype: Ontology Concept\ntitle: Customer\n---\nAn external organization."
	execute(t, executor, "dev", command("rhz_knowledge_accept", "rhz_knowledge_accept_i", "knowledge.accept", "knowledge_document", nil, map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": source}))
	ontology := map[string]any{"name": "CRM", "concepts": []any{map[string]any{"id": "Customer"}}, "relations": []any{}}
	proposal := execute(t, executor, "dev", command("rhz_knowledge_propose", "rhz_knowledge_propose_i", "knowledge.propose", "knowledge_proposal", nil, map[string]any{"sources": []any{map[string]any{"bundle_id": "core", "concept_id": "customer"}}, "proposal": ontology}))
	proposalID := proposal.Aggregate.ID
	execute(t, executor, "dev", command("rhz_knowledge_review", "rhz_knowledge_review_i", "knowledge.review", "knowledge_proposal", &proposalID, map[string]any{"decision": "accept", "family_id": "crm", "revision": 1}))
	execute(t, executor, "dev", command("rhz_knowledge_remove", "rhz_knowledge_remove_i", "knowledge.remove", "knowledge_document", nil, map[string]any{"bundle_id": "core", "concept_id": "customer"}))
	var documents, ontologies, accepted int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM knowledge_documents`).Scan(&documents); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM spec_revisions WHERE kind='ontology'`).Scan(&ontologies); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='knowledge.proposal.accepted'`, proposalID).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	if documents != 0 || ontologies != 1 || accepted != 1 {
		t.Fatalf("documents=%d ontologies=%d accepted=%d", documents, ontologies, accepted)
	}
}

func TestKernelRhizaReactiveRuleLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-reactive-rule-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	rule := execute(t, executor, "dev", command("rhz_rule_define", "rhz_rule_define_i", "reactive_rule.define", "reactive_rule", nil, map[string]any{
		"automation_id": "auto-followup", "rule_key": "note-followup", "name": "Create follow-up", "requested_actor": map[string]any{"type": "automation", "id": "auto-followup"},
		"trigger":     map[string]any{"source": "kernel_canonical_event", "event_type": "object.created", "event_schema_version": 1, "aggregate_type": "object_instance", "allowed_causation_sources": []any{"api"}, "excluded_automation_ids": []any{}},
		"predicates":  map[string]any{"===": []any{map[string]any{"var": "payload.title"}, "Seed"}},
		"action":      map[string]any{"action_id": "create-followup", "kind": "emit_command", "command_type": "object.create", "target_template": map[string]any{"aggregate_type": "object_instance"}, "payload_template": map[string]any{"object_type_key": "notes", "title": "Follow-up"}, "preconditions_template": []any{}},
		"idempotency": map[string]any{}, "limits": map[string]any{"max_recursion_depth": 1, "max_emits_per_minute": 10, "max_emits_per_hour": 100, "max_pending_dispatches": 10, "failure_threshold": 3},
	}))
	ruleID := rule.Aggregate.ID
	var revisionID string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT id FROM reactive_rule_revisions WHERE rule_id=?`, ruleID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	change := func(id, typ string, version int64, payload map[string]any) Receipt {
		c := command(id, id+"_i", typ, "reactive_rule", &ruleID, payload)
		c.Preconditions = []Precondition{{AggregateType: "reactive_rule", AggregateID: ruleID, ExpectedVersion: version}}
		return execute(t, executor, "dev", c)
	}
	change("rhz_rule_publish", "reactive_rule.publish_revision", 1, map[string]any{"revision_id": revisionID, "activation_event_cursor": "0"})
	change("rhz_rule_disable", "reactive_rule.disable", 2, map[string]any{"reason": "pause", "circuit_breaker": true})
	change("rhz_rule_retire", "reactive_rule.retire", 3, map[string]any{"reason": "done"})
	change("rhz_rule_delete", "reactive_rule.delete", 4, map[string]any{"hard": false})
	var status string
	var archived string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT status,archived_at FROM reactive_rules WHERE id=?`, ruleID).Scan(&status, &archived); err != nil || status != "retired" || archived == "" {
		t.Fatalf("status=%q archived=%q err=%v", status, archived, err)
	}
}

func TestKernelRhizaObjectAndRelationInstanceLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openRhizaKernelStore(t, ctx, "kernel-object-relation-instance-test")
	defer s.Close()
	executor := NewStoreExecutor(s)
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	left := execute(t, executor, "dev", command("rhz_inst_left", "rhz_inst_left_i", "object_type.define", "object_type", nil, map[string]any{"key": "parents", "singular": "Parent", "plural": "Parents", "field_schema": map[string]any{"type": "object", "additionalProperties": true}}))
	right := execute(t, executor, "dev", command("rhz_inst_right", "rhz_inst_right_i", "object_type.define", "object_type", nil, map[string]any{"key": "children", "singular": "Child", "plural": "Children", "field_schema": map[string]any{"type": "object", "additionalProperties": true}}))
	relationCommand := command("rhz_inst_definition", "rhz_inst_definition_i", "relation_definition.define", "relation_definition", nil, map[string]any{"key": "children", "from_object_type_id": left.Aggregate.ID, "to_object_type_id": right.Aggregate.ID, "cardinality": "one_to_many", "required": false})
	relationCommand.Preconditions = []Precondition{{AggregateType: "object_type", AggregateID: left.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: right.Aggregate.ID, ExpectedVersion: 1}}
	definition := execute(t, executor, "dev", relationCommand)
	parent := execute(t, executor, "dev", command("rhz_inst_parent", "rhz_inst_parent_i", "object.create", "object_instance", nil, map[string]any{"object_type_key": "parents", "title": "P-1", "values": map[string]any{"name": "before"}}))
	child := execute(t, executor, "dev", command("rhz_inst_child", "rhz_inst_child_i", "object.create", "object_instance", nil, map[string]any{"object_type_key": "children", "title": "C-1"}))
	parentID := parent.Aggregate.ID
	linkCommand := command("rhz_inst_link", "rhz_inst_link_i", "relation.link", "object_relation", nil, map[string]any{"relation_definition_id": definition.Aggregate.ID, "from_object_id": parent.Aggregate.ID, "to_object_id": child.Aggregate.ID})
	linkCommand.Preconditions = []Precondition{{AggregateType: "relation_definition", AggregateID: definition.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_instance", AggregateID: parent.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_instance", AggregateID: child.Aggregate.ID, ExpectedVersion: 1}}
	link := execute(t, executor, "dev", linkCommand)
	updateCommand := command("rhz_inst_update", "rhz_inst_update_i", "object.update", "object_instance", &parentID, map[string]any{"title": "P-2", "values": map[string]any{"name": "after"}})
	updateCommand.Preconditions = []Precondition{{AggregateType: "object_instance", AggregateID: parent.Aggregate.ID, ExpectedVersion: 1}}
	execute(t, executor, "dev", updateCommand)
	linkID := link.Aggregate.ID
	unlinkCommand := command("rhz_inst_unlink", "rhz_inst_unlink_i", "relation.unlink", "object_relation", &linkID, map[string]any{"relation_definition_id": definition.Aggregate.ID})
	unlinkCommand.Preconditions = []Precondition{{AggregateType: "object_relation", AggregateID: link.Aggregate.ID, ExpectedVersion: 1}}
	execute(t, executor, "dev", unlinkCommand)
	deleteCommand := command("rhz_inst_delete", "rhz_inst_delete_i", "object.delete", "object_instance", &parentID, map[string]any{"hard": false})
	deleteCommand.Preconditions = []Precondition{{AggregateType: "object_instance", AggregateID: parent.Aggregate.ID, ExpectedVersion: 2}}
	execute(t, executor, "dev", deleteCommand)
	var relations, archived int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_relations WHERE id=?`, link.Aggregate.ID).Scan(&relations); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM object_instances WHERE id=? AND archived_at IS NOT NULL AND version=3`, parent.Aggregate.ID).Scan(&archived); err != nil {
		t.Fatal(err)
	}
	if relations != 0 || archived != 1 {
		t.Fatalf("relations=%d archived=%d", relations, archived)
	}
}

func openRhizaKernelStore(t *testing.T, ctx context.Context, cluster string) *store.Store {
	t.Helper()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: cluster, NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	one := int64(1)
	if err := s.ApplyStatements(ctx, cluster+"-fixture", []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
	}); err != nil {
		s.Close()
		t.Fatal(err)
	}
	return s
}

func assertCommandOutcome(t *testing.T, executor *Executor, c Command, outcome string) {
	t.Helper()
	_, err := executor.Execute(context.Background(), "dev", c)
	var rejected *CommandError
	if !errors.As(err, &rejected) || rejected.Outcome != outcome {
		t.Fatalf("command %s error=%v, want %s", c.CommandID, err, outcome)
	}
}
