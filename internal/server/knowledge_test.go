package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/knowledge"
	rhizaprojection "github.com/mrchypark/compos/internal/rhizaprojection"
	"github.com/mrchypark/compos/internal/store"
)

func TestKnowledgeOntologyJourney(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "knowledge.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	workspace := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Knowledge", "key": "knowledge", "idempotency_key": "workspace"})
	var workspaceBody map[string]any
	_ = json.Unmarshal(workspace.Body.Bytes(), &workspaceBody)
	workspaceID := workspaceBody["data"].(map[string]any)["id"].(string)

	defined := submitKnowledgeTestCommand(t, handler, workspaceID, "define-customers", "object_type.define", "object_type", map[string]any{
		"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object"},
	})
	var definedBody map[string]any
	_ = json.Unmarshal(defined.Body.Bytes(), &definedBody)
	customerTypeID := definedBody["aggregate"].(map[string]any)["id"].(string)

	person := "---\ntype: Ontology Concept\ntitle: Person\ncompos:\n  relations:\n    - predicate: knowsCustomer\n      target: customer\n  grounds:\n    - entity_definition: people\n---\nA human participant."
	contract := "---\ntype: Ontology Concept\ntitle: Contract\n---\nA commercial agreement."
	customer := "---\ntype: Ontology Concept\ntitle: Customer\ndescription: Enterprise account\nunknown: preserved\ncompos:\n  relations:\n    - predicate: hasOwner\n      target: person\n      cardinality: many_to_one\n    - predicate: hasContract\n      target: contract\n  grounds:\n    - entity_definition: customers\n      mapping:\n        name: display_name\n---\nEnterprise account knowledge."
	for i, item := range []struct{ path, source string }{{"person.md", person}, {"contract.md", contract}, {"customer.md", customer}} {
		response := submitKnowledgeTestCommand(t, handler, workspaceID, "accept-"+string(rune('a'+i)), "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": item.path, "source_markdown": item.source})
		if response.Code != http.StatusOK {
			t.Fatalf("accept %s status=%d body=%s", item.path, response.Code, response.Body.String())
		}
	}

	list := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge?bundle_id=core", nil)
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"concept_id":"customer"`)) || bytes.Contains(list.Body.Bytes(), []byte(`body_markdown`)) {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	source := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source", nil)
	if source.Code != http.StatusOK || source.Body.String() != customer || source.Header().Get("Content-Type") != "text/markdown; charset=utf-8" {
		t.Fatalf("source status=%d content-type=%q body=%s", source.Code, source.Header().Get("Content-Type"), source.Body.String())
	}
	document := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer", nil)
	if document.Code != http.StatusOK || !bytes.Contains(document.Body.Bytes(), []byte(`"unknown":"preserved"`)) || !bytes.Contains(document.Body.Bytes(), []byte(`"entity_definition_key":"customers"`)) {
		t.Fatalf("document status=%d body=%s", document.Code, document.Body.String())
	}
	var documentBody map[string]any
	_ = json.Unmarshal(document.Body.Bytes(), &documentBody)
	revisionID := documentBody["data"].(map[string]any)["revision_id"].(string)
	crlf := strings.ReplaceAll(customer, "\n", "\r\n")
	response := submitKnowledgeTestCommand(t, handler, workspaceID, "accept-crlf", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": crlf})
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	// Simulate an existing database where both sources retained the old normalized hash.
	if _, err := s.DB().Exec(`UPDATE knowledge_documents SET content_hash=? WHERE workspace_id=? AND bundle_id='core' AND concept_id='customer'`, revisionID, workspaceID); err != nil {
		t.Fatal(err)
	}
	crlfDocument := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer", nil)
	_ = json.Unmarshal(crlfDocument.Body.Bytes(), &documentBody)
	crlfRevisionID := documentBody["data"].(map[string]any)["revision_id"].(string)
	lfSource := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source&revision_id="+revisionID, nil)
	crlfSource := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source&revision_id="+crlfRevisionID, nil)
	if revisionID == crlfRevisionID || lfSource.Body.String() != customer || crlfSource.Body.String() != crlf {
		t.Fatalf("byte-distinct revisions collapsed: lf=%q crlf=%q", revisionID, crlfRevisionID)
	}

	traversal := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/traverse?bundle_id=core&concept_id=customer&direction=out&depth=8", nil)
	if traversal.Code != http.StatusOK || !bytes.Contains(traversal.Body.Bytes(), []byte(`"concept_id":"person"`)) || !bytes.Contains(traversal.Body.Bytes(), []byte(`"concept_id":"contract"`)) {
		t.Fatalf("traversal status=%d body=%s", traversal.Code, traversal.Body.String())
	}
	if bytes.Count(traversal.Body.Bytes(), []byte(`"concept_id":"customer"`)) != 2 { // one result node plus query echo
		t.Fatalf("cycle was not bounded: %s", traversal.Body.String())
	}
	groundings := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/groundings?bundle_id=core&entity_definition_id="+customerTypeID, nil)
	if groundings.Code != http.StatusOK || !bytes.Contains(groundings.Body.Bytes(), []byte(`"concept_id":"customer"`)) {
		t.Fatalf("groundings status=%d body=%s", groundings.Code, groundings.Body.String())
	}

	changed := bytes.ReplaceAll([]byte(customer), []byte("    - predicate: hasContract\n      target: contract\n"), nil)
	response = submitKnowledgeTestCommand(t, handler, workspaceID, "accept-changed", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "customer.md", "source_markdown": string(changed)})
	if response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	historical := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source&revision_id="+revisionID, nil)
	if historical.Code != http.StatusOK || historical.Body.String() != customer {
		t.Fatalf("historical source status=%d body=%s", historical.Code, historical.Body.String())
	}
	traversal = request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/traverse?bundle_id=core&concept_id=customer&depth=1", nil)
	if bytes.Contains(traversal.Body.Bytes(), []byte(`"concept_id":"contract"`)) {
		t.Fatalf("stale relation remains: %s", traversal.Body.String())
	}

	invalid := submitKnowledgeTestCommand(t, handler, workspaceID, "accept-invalid", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "invalid.md", "source_markdown": "---\ntype: concept\ncompos:\n  grounds:\n    - entity_definition: missing\n---\n"})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid grounding status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	var invalidRows int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM knowledge_documents WHERE workspace_id=? AND concept_id='invalid'`, workspaceID).Scan(&invalidRows)
	if invalidRows != 0 {
		t.Fatal("invalid grounding left a document")
	}
	removed := submitKnowledgeTestCommand(t, handler, workspaceID, "remove-customer", "knowledge.remove", "knowledge_document", map[string]any{"bundle_id": "core", "concept_id": "customer"})
	if removed.Code != http.StatusOK {
		t.Fatal(removed.Body.String())
	}
	current := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source", nil)
	historical = request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/knowledge/core/customer?format=source&revision_id="+revisionID, nil)
	var tombstones int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM activities WHERE command_id='remove-customer' AND json_extract(payload_json,'$.tombstone_id')='remove-customer'`).Scan(&tombstones)
	if current.Code != http.StatusNotFound || historical.Code != http.StatusOK || historical.Body.String() != customer || tombstones != 1 {
		t.Fatalf("removal history current=%d historical=%d tombstones=%d", current.Code, historical.Code, tombstones)
	}
}

func TestSQLiteOntologyTraversalHasAHardPathBudget(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "bounded.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	workspace := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Bounded", "key": "bounded", "idempotency_key": "workspace"})
	var body map[string]any
	_ = json.Unmarshal(workspace.Body.Bytes(), &body)
	workspaceID := body["data"].(map[string]any)["id"].(string)
	now := "2026-08-29T00:00:00Z"
	layers := [][]string{{"root"}}
	for layer := 1; layer <= 4; layer++ {
		nodes := make([]string, 11)
		for i := range nodes {
			nodes[i] = fmt.Sprintf("l%d-%02d", layer, i)
		}
		layers = append(layers, nodes)
	}
	for _, layer := range layers {
		for _, conceptID := range layer {
			if _, err := s.DB().Exec(`INSERT INTO knowledge_documents VALUES(?,?,?,?,NULL,NULL,NULL,'{}','','','sha256:test',?,?,?,?)`, workspaceID, "dense", conceptID, "concept", now, now, "test", "test"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for layer := 0; layer < len(layers)-1; layer++ {
		for _, source := range layers[layer] {
			for _, target := range layers[layer+1] {
				if _, err := s.DB().Exec(`INSERT INTO knowledge_relations VALUES(?,?,?,'ontology','next',?,0,NULL,'{}')`, workspaceID, "dense", source, target); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	_, err = handler.sqliteTraversal(context.Background(), workspaceID, "dense", "root", "out", 4)
	if !errors.Is(err, rhizaprojection.ErrTraversalLimit) {
		t.Fatalf("dense traversal error=%v, want resource limit", err)
	}
	response := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/ontology/traverse?bundle_id=dense&concept_id=root&depth=4", nil)
	if response.Code != 422 || !bytes.Contains(response.Body.Bytes(), []byte(`"code":"resource_limit_exceeded"`)) {
		t.Fatalf("dense traversal status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestKnowledgeCommandAllowsFullOKFDocumentThroughHTTP(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "boundary.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	workspace := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Boundary", "key": "boundary", "idempotency_key": "workspace"})
	var body map[string]any
	_ = json.Unmarshal(workspace.Body.Bytes(), &body)
	workspaceID := body["data"].(map[string]any)["id"].(string)
	prefix := "---\ntype: concept\n---\n"
	source := prefix + strings.Repeat("\x01", knowledge.MaxDocumentBytes-len(prefix))
	response := submitKnowledgeTestCommand(t, handler, workspaceID, "at-limit", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "limit.md", "source_markdown": source})
	if response.Code != http.StatusOK {
		t.Fatalf("valid %d-byte source rejected: %s", len(source), response.Body.String())
	}
	over := source + "x"
	response = submitKnowledgeTestCommand(t, handler, workspaceID, "over-limit", "knowledge.accept", "knowledge_document", map[string]any{"bundle_id": "core", "path": "over.md", "source_markdown": over})
	if response.Code != http.StatusBadRequest || !bytes.Contains(response.Body.Bytes(), []byte("exceeds")) {
		t.Fatalf("oversized source status=%d body=%s", response.Code, response.Body.String())
	}
}

func submitKnowledgeTestCommand(t *testing.T, handler http.Handler, workspaceID, id, commandType, aggregateType string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	return request(t, handler, "POST", "/api/v1/kernel/commands", map[string]any{
		"command_id": id, "idempotency_key": id, "workspace_id": workspaceID,
		"requested_actor": map[string]any{"type": "user", "id": "dev"},
		"command_type":    commandType, "target": map[string]any{"aggregate_type": aggregateType, "aggregate_id": nil},
		"preconditions": []any{}, "payload": payload,
		"causation": map[string]any{"source": "api", "attempt_id": nil, "event_id": nil},
	})
}
