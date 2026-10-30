package rhizaprojection_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/knowledge"
	"github.com/mrchypark/compos/internal/rhizaprojection"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestEmbeddedRhizaProjectionReachable(t *testing.T) {
	ctx, s, p := openProjection(t)
	records := []knowledge.ProjectionRecord{
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "a", Title: "A"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "core", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "b"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "b", Title: "B"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "core", ConceptID: "b", RelationKind: "ontology", Predicate: "next", TargetRef: "c"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "c", Title: "C"},
	}
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 3)
	if err := p.Rebuild(ctx, records, map[string]int64{"w": 3}, "digest-1"); err != nil {
		t.Fatal(err)
	}
	result, err := p.Reachable(ctx, "w", "core", "a", 2, 10, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.ConceptIDs, []string{"b", "c"}) || result.ProjectionSequence != 3 || result.ProjectionGeneration != "digest-1" {
		t.Fatalf("unexpected reachability result: %#v", result)
	}
}

func TestEmbeddedRhizaProjectionReachableRejectsMissingOrStaleProjection(t *testing.T) {
	ctx, s, p := openProjection(t)
	records := []knowledge.ProjectionRecord{
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "a", Title: "A"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "core", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "b"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "b", Title: "B"},
	}
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 3)
	if err := p.Rebuild(ctx, records, map[string]int64{"w": 3}, "digest-1"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		workspace string
		concept   string
		minimum   int64
		want      error
	}{
		{name: "missing workspace cursor", workspace: "other", concept: "a", want: rhizaprojection.ErrStaleProjection},
		{name: "ahead cursor", workspace: "w", concept: "a", minimum: 4, want: rhizaprojection.ErrStaleProjection},
		{name: "missing source", workspace: "w", concept: "missing", want: rhizaprojection.ErrConceptNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := p.Reachable(ctx, test.workspace, "core", test.concept, 1, 10, 10, test.minimum)
			if !errors.Is(err, test.want) {
				t.Fatalf("Reachable() error = %v, want %v", err, test.want)
			}
		})
	}
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 4)
	if _, err := p.Reachable(ctx, "w", "core", "a", 1, 10, 10, 0); !errors.Is(err, rhizaprojection.ErrStaleProjection) {
		t.Fatalf("Reachable() after newer canonical knowledge event error = %v, want %v", err, rhizaprojection.ErrStaleProjection)
	}
}

func TestEmbeddedRhizaProjectionReachableRejectsIncompatibleStateSchema(t *testing.T) {
	ctx, s, p := openProjection(t)
	records := []knowledge.ProjectionRecord{
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "a", Title: "A"},
	}
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 1)
	if err := p.Rebuild(ctx, records, map[string]int64{"w": 1}, "digest-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.GraphExecute(ctx, "old-projection-schema", `MATCH (s:ComposProjectionState {scope: 'global'}) SET s.version = 0`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reachable(ctx, "w", "core", "a", 1, 10, 10, 0); !errors.Is(err, rhizaprojection.ErrStaleProjection) {
		t.Fatalf("Reachable() with old state schema error = %v, want %v", err, rhizaprojection.ErrStaleProjection)
	}
}

func TestEmbeddedRhizaProjectionReachableIsolatesBundleAndEnforcesLimits(t *testing.T) {
	ctx, s, p := openProjection(t)
	records := []knowledge.ProjectionRecord{
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "a", Title: "core A"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "core", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "b"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "core", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "c"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "b", Title: "B"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "c", Title: "C"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "other", ConceptID: "a", Title: "other A"},
		{Kind: "relation", WorkspaceID: "w", BundleID: "other", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "outside"},
		{Kind: "concept", WorkspaceID: "w", BundleID: "other", ConceptID: "outside", Title: "outside"},
		{Kind: "concept", WorkspaceID: "other-workspace", BundleID: "core", ConceptID: "a", Title: "other workspace A"},
		{Kind: "relation", WorkspaceID: "other-workspace", BundleID: "core", ConceptID: "a", RelationKind: "ontology", Predicate: "next", TargetRef: "elsewhere"},
		{Kind: "concept", WorkspaceID: "other-workspace", BundleID: "core", ConceptID: "elsewhere", Title: "elsewhere"},
	}
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 1)
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "other-workspace", 1)
	if err := p.Rebuild(ctx, records, map[string]int64{"w": 1, "other-workspace": 1}, "digest-1"); err != nil {
		t.Fatal(err)
	}
	result, err := p.Reachable(ctx, "w", "core", "a", 1, 10, 10, 0)
	if err != nil || !reflect.DeepEqual(result.ConceptIDs, []string{"b", "c"}) {
		t.Fatalf("core reachability result=%#v err=%v", result, err)
	}
	for _, limits := range []struct{ results, edges int }{{1, 10}, {10, 1}} {
		if _, err := p.Reachable(ctx, "w", "core", "a", 1, limits.results, limits.edges, 0); !errors.Is(err, rhizaprojection.ErrTraversalLimit) {
			t.Fatalf("Reachable(results=%d, edges=%d) error = %v, want %v", limits.results, limits.edges, err, rhizaprojection.ErrTraversalLimit)
		}
	}
}

func TestEmbeddedRhizaProjectionReachableRejectsGraphAheadOfCanonicalHead(t *testing.T) {
	ctx, s, p := openProjection(t)
	insertCanonicalKnowledgeProjectionEvent(t, ctx, s, "w", 3)
	if err := p.Rebuild(ctx, []knowledge.ProjectionRecord{{Kind: "concept", WorkspaceID: "w", BundleID: "core", ConceptID: "a", Title: "A"}}, map[string]int64{"w": 4}, "digest-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reachable(ctx, "w", "core", "a", 1, 10, 10, 0); !errors.Is(err, rhizaprojection.ErrStaleProjection) {
		t.Fatalf("Reachable() with graph cursor ahead of canonical head error = %v, want %v", err, rhizaprojection.ErrStaleProjection)
	}
}

func openProjection(t *testing.T) (context.Context, *store.Store, *rhizaprojection.Projection) {
	t.Helper()
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "projection-test-" + strings.ReplaceAll(t.Name(), "/", "-"), NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	p, err := rhizaprojection.Open(s)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, s, p
}

func insertCanonicalKnowledgeProjectionEvent(t *testing.T, ctx context.Context, s *store.Store, workspaceID string, sequence int64) {
	t.Helper()
	one := int64(1)
	sequenceText := strconv.FormatInt(sequence, 10)
	commandID := "cmd-" + workspaceID + "-" + sequenceText
	if err := s.ApplyStatements(ctx, "projection-head-"+workspaceID+"-"+sequenceText, []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES(?,?,'Workspace',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z') ON CONFLICT DO NOTHING`, Args: []any{workspaceID, workspaceID}},
		{SQL: `INSERT INTO command_ledger(command_id,workspace_id,authenticated_principal_id,requested_actor_json,effective_actor_json,actor_context_hash,idempotency_key,command_type,target_json,preconditions_json,causation_json,payload_hash,payload_json,outcome,result_json,effective_at,created_at) VALUES(?,?,?,'null','{}',?,?,'knowledge.accept','{}','[]','{}',?,'{}','applied','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, Args: []any{commandID, workspaceID, "test", "sha256:" + strings.Repeat("0", 64), "event-" + sequenceText, "sha256:" + strings.Repeat("1", 64)}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,?, 'knowledge_document','core:a',1,?,'{}','2026-01-01T00:00:00Z')`, Args: []any{"prj-" + workspaceID + "-" + sequenceText, workspaceID, commandID, sequence, "sha256:" + strings.Repeat("2", 64)}, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}
}
