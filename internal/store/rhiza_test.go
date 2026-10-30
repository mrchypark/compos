package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestComposGraphIndexesAreConfiguredOnce(t *testing.T) {
	custom := rhiza.GraphNodePropertyIndex{Label: "Custom", Property: "key"}
	concept := rhiza.GraphNodePropertyIndex{Label: "ComposConcept", Property: "projection_key"}
	config := withComposGraphIndexes(rhiza.Config{LocalGraphNodePropertyIndexes: []rhiza.GraphNodePropertyIndex{custom, concept}})
	want := []rhiza.GraphNodePropertyIndex{
		custom,
		concept,
		{Label: "ComposEntityDefinition", Property: "projection_key"},
		{Label: "ComposProjectionState", Property: "projection_key"},
	}
	if !reflect.DeepEqual(config.LocalGraphNodePropertyIndexes, want) {
		t.Fatalf("indexes=%#v", config.LocalGraphNodePropertyIndexes)
	}
}

func TestEmbeddedRhizaRejectsInteractiveTransactions(t *testing.T) {
	ctx := context.Background()
	s, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "compos-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Transact(ctx, "workspace-test", func(Tx) error { return nil })
	if !errors.Is(err, ErrInteractiveTransactionUnsupported) {
		t.Fatalf("error=%v", err)
	}
	var count int
	if err := s.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("workspace count=%d err=%v", count, err)
	}
}

func TestEmbeddedRhizaNativeExecAndReadBoundary(t *testing.T) {
	ctx := context.Background()
	s, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "native-boundary-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rows, err := s.ExecAffected(ctx, "native-workspace", `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES('native','native','Native',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`)
	if err != nil || rows != 1 {
		t.Fatalf("rows=%d error=%v", rows, err)
	}
	var name string
	row := s.ReadRow(ctx, ReadLinearizable, `SELECT name FROM workspaces WHERE id='native'`)
	if err := row.Scan(&name); err != nil || name != "Native" {
		t.Fatalf("name=%q error=%v version=%#v", name, err, row.Version())
	}
}

func TestEmbeddedRhizaEnforcesForeignKeys(t *testing.T) {
	ctx := context.Background()
	s, err := OpenRhiza(ctx, rhiza.Config{ClusterID: "foreign-key-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	one := int64(1)
	err = s.ApplyStatements(ctx, "orphan-membership", []rhiza.SQLStatement{{
		SQL: `INSERT INTO workspace_memberships(workspace_id,principal_id,role,created_at) VALUES('missing','dev','owner','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one,
	}})
	if !errors.Is(err, ErrMutationExecutionFailed) {
		t.Fatalf("orphan membership error=%v", err)
	}
	var count int
	if err := s.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspace_memberships`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("membership count=%d err=%v", count, err)
	}
}

func TestCompactSQLRequestIDFitsRhizaLimit(t *testing.T) {
	requestID := "conductor-inbound:" + strings.Repeat("x", 96)
	got := compactSQLRequestID(requestID)
	if len(got) != 64 || got != compactSQLRequestID(requestID) {
		t.Fatalf("compacted request id=%q length=%d", got, len(got))
	}
}

func TestRhizaBootstrapPublishesIdentityOnlyWithCompleteSchema(t *testing.T) {
	ctx := context.Background()
	cluster, err := rhiza.Open(ctx, rhiza.Config{ClusterID: "schema-publication", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	s := &Store{kind: "rhiza", rhiza: cluster}
	migrations, err := rhizaSchemaMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for i, migration := range migrations {
		if len(migration.Statements) > 64 {
			t.Fatalf("migration %d exceeds Rhiza statement limit", i)
		}
		if err := cluster.Migrate(ctx, migrations[:i+1]); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := s.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_metadata'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if i < len(migrations)-1 && count != 0 {
			t.Fatalf("schema readiness published before migration %d", len(migrations))
		}
	}
	last := migrations[len(migrations)-1]
	if len(last.Statements) != 2 {
		t.Fatal("metadata creation and identity must share final migration")
	}
	var identity string
	if err := s.ReadRow(ctx, ReadLinearizable, `SELECT identity FROM schema_metadata WHERE id=1`).Scan(&identity); err != nil || identity != SchemaIdentity {
		t.Fatalf("identity=%q err=%v", identity, err)
	}
	if err := s.initializeRhiza(ctx); err != nil {
		t.Fatalf("completed schema must reopen without migration changes: %v", err)
	}
	var templates int
	if err := s.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM sqlite_master WHERE name='object_type_templates'`).Scan(&templates); err != nil || templates != 0 {
		t.Fatalf("retired template table count=%d err=%v", templates, err)
	}
	if err := s.ApplyStatements(ctx, "old-schema-identity", []rhiza.SQLStatement{{SQL: `UPDATE schema_metadata SET identity=? WHERE id=1`, Args: []any{"compos-go-sqlite-world-spec-journal-reactive-agent-v2"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.initializeRhiza(ctx); err == nil || !strings.Contains(err.Error(), "unsupported Rhiza schema identity") {
		t.Fatalf("expected previous schema identity rejection, got %v", err)
	}
}

func TestRhizaBootstrapStillRejectsMetadataWithoutIdentity(t *testing.T) {
	ctx := context.Background()
	cluster, err := rhiza.Open(ctx, rhiza.Config{ClusterID: "schema-partial", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	statements, err := splitSchema()
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Migrate(ctx, []rhiza.Migration{{Version: 1, Name: "legacy-partial", Statements: []rhiza.SQLStatement{{SQL: statements[0]}}}}); err != nil {
		t.Fatal(err)
	}
	s := &Store{kind: "rhiza", rhiza: cluster}
	if err := s.initializeRhiza(ctx); err == nil || !strings.Contains(err.Error(), "refusing legacy or partially initialized") {
		t.Fatalf("expected partial schema rejection, got %v", err)
	}
}
