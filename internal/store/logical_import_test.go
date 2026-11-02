package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestLogicalImportPreservesApplicationStateAndVerifiesAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	target, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "logical-import", NodeID: "n1", DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	input, digest := testLogicalImport(t, ctx, target)
	summary, err := ImportLogical(ctx, target, input, digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Tables) != len(logicalImportOrder) || summary.Digest == "" {
		t.Fatalf("unexpected safe summary: %#v", summary)
	}
	var counter int64
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT counter FROM field_sequence_counters WHERE workspace_id='ws'`).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter != 9007199254740993 {
		t.Fatalf("counter lost integer precision: %d", counter)
	}
	var currentPin, objectPin int64
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT schema_version FROM object_types WHERE workspace_id='ws' AND id='type'`).Scan(&currentPin); err != nil {
		t.Fatal(err)
	}
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT object_type_schema_version FROM object_instances WHERE workspace_id='ws' AND id='object'`).Scan(&objectPin); err != nil {
		t.Fatal(err)
	}
	if currentPin != 2 || objectPin != 1 {
		t.Fatalf("historical schema pin changed: type=%d object=%d", currentPin, objectPin)
	}
	var instances int64
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM object_instances WHERE workspace_id='ws'`).Scan(&instances); err != nil {
		t.Fatal(err)
	}
	if instances != 2 {
		t.Fatalf("imported %d objects, want current and historical pins", instances)
	}
	if err := target.ApplyStatements(ctx, "runtime-stale-pin", []rhiza.SQLStatement{{SQL: `INSERT INTO object_instances(id,workspace_id,object_type_id,object_type_schema_version,title,payload_json,state_key,archived_at,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, Args: []any{"runtime-stale", "ws", "type", int64(1), "Runtime", "{}", nil, nil, int64(1), "2026-10-08T00:00:00Z", "2026-10-08T00:00:00Z", "owner", "owner"}}}); err == nil {
		t.Fatal("normal runtime stale schema pin insert succeeded")
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target, err = OpenRhiza(ctx, rhiza.Config{ClusterID: "logical-import", NodeID: "n1", DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if _, err := VerifyLogicalImport(ctx, target, input, digest); err != nil {
		t.Fatalf("reopened target verification failed: %v", err)
	}
}

func TestLogicalImportRejectsInvalidInputBeforeWritesAndVerificationRejectsPartialTarget(t *testing.T) {
	ctx := context.Background()
	target, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "logical-invalid", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	input, digest := testLogicalImport(t, ctx, target)
	input.Source.SchemaIdentity = "wrong"
	if _, err := ImportLogical(ctx, target, input, digest); err == nil {
		t.Fatal("invalid source was imported")
	}
	var count int64
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid import wrote %d workspaces", count)
	}
	input.Source.SchemaIdentity = logicalSourceIdentity
	for _, name := range []string{"knowledge_documents", "knowledge_relations", "ontology_groundings", "workspace_memberships", "auth_provider_identities", "auth_sessions", "actor_capabilities", "principal_person_bindings", "person_binding_claims"} {
		input.Tables = append(input.Tables, LogicalImportTable{Name: name, Columns: []string{"id"}})
		if err := validateLogicalImport(input); err == nil {
			t.Fatalf("retired table accepted for import: %s", name)
		}
		input.Tables = input.Tables[:len(input.Tables)-1]
		input.ExcludedTables = append(input.ExcludedTables, LogicalExcludedTable{Name: name, Rows: 1})
	}
	if err := validateLogicalImport(input); err != nil {
		t.Fatalf("declared retired table exclusions rejected: %v", err)
	}
	if _, err := VerifyLogicalImport(ctx, target, input, digest); err == nil {
		t.Fatal("partial target passed verification")
	}
	if err := target.ApplyStatements(ctx, "partial-target", []rhiza.SQLStatement{{SQL: `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES ('partial','partial','Partial',1,'2026-10-08T00:00:00Z','2026-10-08T00:00:00Z')`}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportLogical(ctx, target, input, digest); err == nil {
		t.Fatal("nonempty target accepted import")
	}
}

func testLogicalImport(t *testing.T, ctx context.Context, target *Store) (*LogicalImport, string) {
	t.Helper()
	catalog, err := logicalCatalog(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	tables := make([]LogicalImportTable, 0, len(logicalImportOrder))
	byName := make(map[string]*LogicalImportTable, len(logicalImportOrder))
	for _, name := range logicalImportOrder {
		tables = append(tables, LogicalImportTable{Name: name, Columns: catalog[name].Columns})
		byName[name] = &tables[len(tables)-1]
	}
	time := "2026-10-08T00:00:00Z"
	hash := "sha256:" + strings.Repeat("a", 64)
	add := func(name string, values map[string]any) {
		table := byName[name]
		row := make([]any, len(table.Columns))
		for index, column := range table.Columns {
			row[index] = values[column]
		}
		table.Rows = append(table.Rows, row)
	}
	add("workspaces", map[string]any{"id": "ws", "key": "workspace", "name": "Workspace", "version": int64(1), "created_at": time, "updated_at": time})
	for index := 0; index < 65; index++ {
		add("workspaces", map[string]any{"id": "extra-" + strconv.Itoa(index), "key": "extra-" + strconv.Itoa(index), "name": "Extra", "version": int64(1), "created_at": time, "updated_at": time})
	}
	add("object_types", map[string]any{"id": "type", "workspace_id": "ws", "key": "things", "singular": "Thing", "plural": "Things", "schema_version": int64(2), "field_schema_json": "{}", "relation_schema_json": "{}", "state_machine_id": nil, "archived_at": nil, "version": int64(1), "created_at": time, "updated_at": time, "created_by_principal_id": "owner", "updated_by_principal_id": "owner"})
	add("object_type_schema_revisions", map[string]any{"workspace_id": "ws", "object_type_id": "type", "schema_version": int64(1), "field_schema_json": "{}", "relation_schema_json": "{}", "state_machine_id": nil, "digest": hash, "created_at": time, "created_by_principal_id": "owner"})
	add("object_type_schema_revisions", map[string]any{"workspace_id": "ws", "object_type_id": "type", "schema_version": int64(2), "field_schema_json": "{}", "relation_schema_json": "{}", "state_machine_id": nil, "digest": hash, "created_at": time, "created_by_principal_id": "owner"})
	add("object_instances", map[string]any{"id": "object", "workspace_id": "ws", "object_type_id": "type", "object_type_schema_version": int64(1), "title": "Object", "payload_json": "{}", "state_key": nil, "archived_at": nil, "version": int64(1), "created_at": time, "updated_at": time, "created_by_principal_id": "owner", "updated_by_principal_id": "owner"})
	add("object_instances", map[string]any{"id": "object-current", "workspace_id": "ws", "object_type_id": "type", "object_type_schema_version": int64(2), "title": "Current object", "payload_json": "{}", "state_key": nil, "archived_at": nil, "version": int64(1), "created_at": time, "updated_at": time, "created_by_principal_id": "owner", "updated_by_principal_id": "owner"})
	add("relation_definitions", map[string]any{"id": "relation", "workspace_id": "ws", "key": "related", "from_object_type_id": "type", "to_object_type_id": "type", "cardinality": "many_to_many", "required": int64(0), "archived_at": nil, "version": int64(1), "created_at": time, "updated_at": time, "created_by_principal_id": "owner", "updated_by_principal_id": "owner"})
	add("object_relations", map[string]any{"id": "edge", "workspace_id": "ws", "relation_definition_id": "relation", "from_object_instance_id": "object", "to_object_instance_id": "object", "version": int64(1), "created_at": time, "updated_at": time, "created_by_principal_id": "owner", "updated_by_principal_id": "owner"})
	add("field_sequence_counters", map[string]any{"workspace_id": "ws", "object_type_id": "type", "field_name": "ticket", "scope_key": "global", "counter": int64(9007199254740993)})
	add("object_field_index", map[string]any{"workspace_id": "ws", "object_type_id": "type", "object_instance_id": "object", "field_name": "score", "value_kind": "number", "value_text": nil, "value_number": 1.5, "value_boolean": nil})
	add("command_ledger", map[string]any{"command_id": "command", "workspace_id": "ws", "authenticated_principal_id": "owner", "requested_actor_json": "null", "effective_actor_json": "{}", "actor_context_hash": hash, "idempotency_key": "import-test", "command_type": "object.create", "target_json": "{}", "preconditions_json": "[]", "causation_json": "{}", "payload_hash": hash, "payload_json": "{}", "outcome": "applied", "result_json": "{}", "effective_at": time, "created_at": time})
	add("journal_records", map[string]any{"id": "journal", "workspace_id": "ws", "stream_id": "stream", "stream_sequence": int64(1), "kind": "work.opened", "spec_revision_id": nil, "participant_kind": nil, "participant_id": nil, "principal_id": "owner", "correlation_id": nil, "causation_id": nil, "command_id": "command", "payload_json": "{}", "created_at": time})
	payload := LogicalImport{Format: LogicalImportFormat, Source: LogicalImportSource{RhizaVersion: "v0.12.0", ClusterID: "old", NodeID: "old-node", ObjectProvider: "gcs", ObjectBucket: "source", ObjectPrefix: "", AppliedSlot: 99, SchemaIdentity: logicalSourceIdentity}, Tables: tables, ExcludedTables: []LogicalExcludedTable{{Name: "schema_metadata", Rows: 1}, {Name: "object_type_templates", Rows: 1}, {Name: "_rhiza_state", Rows: 4}, {Name: "sqlite_sequence", Rows: 0}}}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, digest, err := DecodeLogicalImport(data)
	if err != nil {
		t.Fatal(err)
	}
	return decoded, digest
}

func TestLogicalImportOrdersLinkedReactiveRuleRevisions(t *testing.T) {
	table := &LogicalImportTable{Name: "reactive_rule_revisions", Columns: []string{"id", "supersedes_revision_id"}, Rows: [][]any{{"child", "root"}, {"root", nil}}}
	rows, err := importRowsInOrder(table, logicalTableDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	if got := rows[0][0]; got != "root" {
		t.Fatalf("first revision = %q, want root", got)
	}
}

func TestHistoricalPinRestoreFailureRollsBackWholeGroup(t *testing.T) {
	ctx := context.Background()
	target, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "logical-rollback", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	input, digest := testLogicalImport(t, ctx, target)
	var objectTable LogicalImportTable
	for index := range input.Tables {
		if input.Tables[index].Name == "object_instances" {
			objectTable = LogicalImportTable{Name: input.Tables[index].Name, Columns: input.Tables[index].Columns, Rows: append([][]any(nil), input.Tables[index].Rows...)}
		}
		if input.Tables[index].Name == "object_instances" || input.Tables[index].Name == "object_field_index" || input.Tables[index].Name == "object_relations" || input.Tables[index].Name == "relation_cardinality_slots" {
			input.Tables[index].Rows = nil
		}
	}
	if _, err := ImportLogical(ctx, target, input, digest); err != nil {
		t.Fatal(err)
	}
	if objectTable.Name == "" {
		t.Fatal("missing object table")
	}
	byName := make(map[string]*LogicalImportTable, len(input.Tables))
	for index := range input.Tables {
		byName[input.Tables[index].Name] = &input.Tables[index]
	}
	byName["object_instances"] = &objectTable
	pins, err := logicalObjectSchemaPins(byName)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := importRowsInOrder(&objectTable, logicalTableDefinition{})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := logicalStatementGroups("object_instances", objectTable.Columns, rows, pins)
	if err != nil {
		t.Fatal(err)
	}
	var broken []rhiza.SQLStatement
	for _, group := range groups {
		if len(group) == 3 {
			broken = append([]rhiza.SQLStatement(nil), group...)
			break
		}
	}
	if len(broken) != 3 {
		t.Fatal("missing historical schema pin group")
	}
	broken[2].Args[3] = int64(999)
	if err := target.ApplyStatements(ctx, "broken-restore", broken); err == nil {
		t.Fatal("broken historical pin restore committed")
	}
	var count, pin int64
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM object_instances WHERE workspace_id='ws'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := target.ReadRow(ctx, ReadLinearizable, `SELECT schema_version FROM object_types WHERE workspace_id='ws' AND id='type'`).Scan(&pin); err != nil {
		t.Fatal(err)
	}
	if count != 0 || pin != 2 {
		t.Fatalf("rollback leaked object count=%d type pin=%d", count, pin)
	}
}

func TestDecodeLogicalImportRejectsTrailingDelimiter(t *testing.T) {
	if _, _, err := DecodeLogicalImport([]byte(`{}]`)); err == nil {
		t.Fatal("trailing delimiter was accepted")
	}
}

func TestLogicalImportRetriesOnlyCommitUnknown(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	if err := retryLogicalImportCommitUnknown(ctx, func() error {
		attempts++
		if attempts < 3 {
			return rhiza.ErrCommitUnknown
		}
		return nil
	}); err != nil || attempts != 3 {
		t.Fatalf("commit-unknown retry err=%v attempts=%d", err, attempts)
	}
	attempts = 0
	if err := retryLogicalImportCommitUnknown(ctx, func() error {
		attempts++
		return ErrMutationExecutionFailed
	}); !errors.Is(err, ErrMutationExecutionFailed) || attempts != 1 {
		t.Fatalf("execution failure retry err=%v attempts=%d", err, attempts)
	}
}
