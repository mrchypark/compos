package server

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestRhizaGraphReachabilityHTTP(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "server-graph-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	one := int64(1)
	if err := s.ApplyStatements(ctx, "graph-fixture", []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES('wks_graph','graph','Graph',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO knowledge_documents VALUES('wks_graph','core','a','concept','A',NULL,NULL,'{}','A','A','sha256:a','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','dev','dev')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO knowledge_documents VALUES('wks_graph','core','b','concept','B',NULL,NULL,'{}','B','B','sha256:b','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','dev','dev')`, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO knowledge_relations VALUES('wks_graph','core','a','ontology','next','b',0,NULL,'{}')`, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}
	handler := New(s)
	defer handler.Close()
	workspaceID := "wks_graph"
	deadline := time.Now().Add(10 * time.Second)
	for !handler.graphProjectionReady(workspaceID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	response := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/reachable-node-ids?bundle_id=core&concept_id=a&depth=1&min_projection_sequence=0", nil)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"concept_ids":["b"]`)) || !bytes.Contains(response.Body.Bytes(), []byte(`"source":"rhiza_graph"`)) {
		t.Fatalf("reachable status=%d body=%s", response.Code, response.Body.String())
	}
	t.Run("updating projection fails closed", func(t *testing.T) {
		handler.graphProjectionSync.Lock()
		defer handler.graphProjectionSync.Unlock()
		response := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/reachable-node-ids?bundle_id=core&concept_id=a", nil)
		if response.Code != http.StatusServiceUnavailable || !bytes.Contains(response.Body.Bytes(), []byte(`"stale_projection"`)) {
			t.Fatalf("updating status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("missing graph source is not empty success", func(t *testing.T) {
		// Simulate a canonical Concept omitted from an otherwise current graph.
		if err := s.ApplyStatements(ctx, "unprojected-concept", []rhiza.SQLStatement{
			{SQL: `INSERT INTO knowledge_documents VALUES('wks_graph','core','missing','concept','Missing',NULL,NULL,'{}','Missing','Missing','sha256:missing','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','dev','dev')`, ExpectedRowsAffected: &one},
		}); err != nil {
			t.Fatal(err)
		}
		response := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/reachable-node-ids?bundle_id=core&concept_id=missing", nil)
		if response.Code != http.StatusServiceUnavailable || !bytes.Contains(response.Body.Bytes(), []byte(`"stale_projection"`)) {
			t.Fatalf("missing graph source status=%d body=%s", response.Code, response.Body.String())
		}
	})
}
