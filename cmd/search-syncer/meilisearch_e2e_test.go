package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/meilisearch/meilisearch-go"
	"github.com/mrchypark/compos/internal/server"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

// TestMeilisearchE2EProjectionSync exercises the deployed Meilisearch engine.
// It is intentionally opt-in: the supplied key must be restricted to compos-tg900-e2e-*.
func TestMeilisearchE2EProjectionSync(t *testing.T) {
	meiliURL, meiliKey := os.Getenv("COMPOS_E2E_MEILISEARCH_URL"), os.Getenv("COMPOS_E2E_MEILISEARCH_KEY")
	if meiliURL == "" || meiliKey == "" {
		t.Skip("set COMPOS_E2E_MEILISEARCH_URL and COMPOS_E2E_MEILISEARCH_KEY to run against Meilisearch")
	}
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "meilisearch-e2e", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := server.New(db)
	defer handler.Close()
	compos := httptest.NewServer(handler)
	defer compos.Close()

	index := "compos-tg900-e2e-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg := Config{ComposEndpoint: compos.URL, MeilisearchURL: meiliURL, MeilisearchKey: meiliKey, IndexUID: index, PrimaryKey: "id", BatchSize: 100}
	meili := newMeilisearchClient(cfg)
	defer func() {
		task, err := meili.DeleteIndex(index)
		if err != nil {
			var meiliErr *meilisearch.Error
			if !errors.As(err, &meiliErr) || meiliErr.StatusCode != http.StatusNotFound {
				t.Errorf("cleanup index %s: %v", index, err)
			}
			return
		}
		if err := waitTask(meili.WaitForTask, task.TaskUID); err != nil {
			t.Errorf("wait for cleanup index %s: %v", index, err)
		}
	}()
	if err := handler.EnableSearch(meiliURL, index, meiliKey); err != nil {
		t.Fatal(err)
	}

	workspaceA := e2eWorkspace(t, compos.URL, "Search A", "search-a")
	workspaceB := e2eWorkspace(t, compos.URL, "Search B", "search-b")
	e2eCommand(t, compos.URL, workspaceA, "define-notes-a", "object_type.define", "object_type", "", nil, map[string]any{"key": "notes", "singular": "Note", "plural": "Notes", "field_schema": map[string]any{"type": "object"}})
	e2eCommand(t, compos.URL, workspaceB, "define-notes-b", "object_type.define", "object_type", "", nil, map[string]any{"key": "notes", "singular": "Note", "plural": "Notes", "field_schema": map[string]any{"type": "object"}})
	objectA := e2eCommand(t, compos.URL, workspaceA, "create-a", "object.create", "object_instance", "", nil, map[string]any{"object_type_key": "notes", "title": "가나다 A", "values": map[string]any{}})
	e2eCommand(t, compos.URL, workspaceA, "create-b", "object.create", "object_instance", "", nil, map[string]any{"object_type_key": "notes", "title": "가나다 B", "values": map[string]any{}})
	e2eCommand(t, compos.URL, workspaceB, "create-external", "object.create", "object_instance", "", nil, map[string]any{"object_type_key": "notes", "title": "가나다 외부", "values": map[string]any{}})

	stateA, stateB := SyncState{}, SyncState{}
	t.Log("scenario: snapshot")
	cfg.WorkspaceID = workspaceA
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("workspace A snapshot: %v", err)
	}
	cfg.WorkspaceID = workspaceB
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateB); err != nil {
		t.Fatalf("workspace B snapshot: %v", err)
	}

	// Full syllable, prefix, and chosung all use the real index settings and fields.
	t.Log("scenario: Korean search and workspace filter")
	for _, query := range []string{"가나다", "가나", "ㄱㄴㄷ", "가ㄴ", "ㄱㅏㄴㅏ"} {
		hits := e2eSearch(t, compos.URL, workspaceA, query, nil, 20, 0)
		if !e2eHasTitle(hits, "가나다 A") {
			t.Fatalf("%q did not find Korean document: %#v", query, hits)
		}
		if e2eHasTitle(hits, "가나다 외부") {
			t.Fatalf("workspace filter leaked another workspace for %q: %#v", query, hits)
		}
	}
	page := e2eSearch(t, compos.URL, workspaceA, "가나다", []string{"title:asc"}, 1, 1)
	if len(page) != 1 || page[0] != "가나다 B" {
		t.Fatalf("sort/pagination page=%#v, want [가나다 B]", page)
	}

	// Event path starts with a newly-created object, not just mutations of snapshot data.
	t.Log("scenario: event create/update/delete")
	e2eCommand(t, compos.URL, workspaceA, "create-c", "object.create", "object_instance", "", nil, map[string]any{"object_type_key": "notes", "title": "가나다 C", "values": map[string]any{}})
	cfg.WorkspaceID = workspaceA
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("workspace A create event: %v", err)
	}
	if !e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "가나다 C", nil, 20, 0), "가나다 C") {
		t.Fatal("event-created object was not indexed")
	}

	// Update, then archive, advance the cursor only after Meilisearch accepts each task.
	e2eCommand(t, compos.URL, workspaceA, "update-a", "object.update", "object_instance", objectA, []any{e2eVersion(objectA, 1)}, map[string]any{"title": "수정 가나다 A"})
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("workspace A update events: %v", err)
	}
	if !e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "수정", nil, 20, 0), "수정 가나다 A") {
		t.Fatal("updated object was not indexed through events")
	}
	e2eCommand(t, compos.URL, workspaceA, "delete-a", "object.delete", "object_instance", objectA, []any{e2eVersion(objectA, 2)}, map[string]any{"hard": false})
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("workspace A delete events: %v", err)
	}
	if e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "수정", nil, 20, 0), "수정 가나다 A") {
		t.Fatal("archived object remains indexed")
	}

	// Knowledge commands are production kernel commands; same bundle/concept IDs must remain workspace-scoped.
	t.Log("scenario: knowledge isolation")
	e2eCommand(t, compos.URL, workspaceA, "accept-knowledge", "knowledge.accept", "knowledge_document", "", nil, map[string]any{"bundle_id": "core", "path": "search.md", "source_markdown": "---\ntype: Ontology Concept\ntitle: 지식 가나다\n---\n검색 지식"})
	e2eCommand(t, compos.URL, workspaceB, "accept-knowledge-b", "knowledge.accept", "knowledge_document", "", nil, map[string]any{"bundle_id": "core", "path": "search.md", "source_markdown": "---\ntype: Ontology Concept\ntitle: 외부 지식 가나다\n---\n검색 지식"})
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("knowledge accept event: %v", err)
	}
	cfg.WorkspaceID = workspaceB
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateB); err != nil {
		t.Fatalf("workspace B knowledge accept event: %v", err)
	}
	cfg.WorkspaceID = workspaceA
	if !e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "지식", nil, 20, 0), "지식 가나다") {
		t.Fatal("accepted knowledge was not indexed")
	}
	if hits := e2eSearchFiltered(t, compos.URL, workspaceA, "", `type = "knowledge"`, nil, 20, 0); len(hits) != 1 || hits[0] != "지식 가나다" {
		t.Fatalf("type filter leaked or missed knowledge: %#v", hits)
	}
	e2eCommand(t, compos.URL, workspaceA, "remove-knowledge", "knowledge.remove", "knowledge_document", "", nil, map[string]any{"bundle_id": "core", "concept_id": "search"})
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("knowledge remove event: %v", err)
	}
	if e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "지식", nil, 20, 0), "지식 가나다") {
		t.Fatal("removed knowledge remains indexed")
	}
	if !e2eHasTitle(e2eSearch(t, compos.URL, workspaceB, "외부 지식", nil, 20, 0), "외부 지식 가나다") {
		t.Fatal("workspace A knowledge removal deleted workspace B document")
	}

	// A lost index and a fresh process state both rebuild from the authoritative snapshot.
	t.Log("scenario: index rebuild and restart")
	task, err := meili.DeleteIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitTask(meili.WaitForTask, task.TaskUID); err != nil {
		t.Fatal(err)
	}
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &stateA); err != nil {
		t.Fatalf("rebuild after index deletion: %v", err)
	}
	if !e2eHasTitle(e2eSearch(t, compos.URL, workspaceA, "가나다", nil, 20, 0), "가나다 B") {
		t.Fatal("index deletion did not rebuild snapshot")
	}
	if err := syncOnce(ctx, compos.Client(), meili, cfg, &SyncState{}); err != nil {
		t.Fatalf("restart snapshot: %v", err)
	}
}

func e2eWorkspace(t *testing.T, endpoint, name, key string) string {
	t.Helper()
	body := e2ePost(t, endpoint, "/api/v1/workspaces", map[string]any{"name": name, "key": key, "idempotency_key": key})
	data := body["data"].(map[string]any)
	return data["id"].(string)
}

func e2eCommand(t *testing.T, endpoint, workspace, id, commandType, aggregateType, aggregateID string, preconditions []any, payload map[string]any) string {
	t.Helper()
	if preconditions == nil {
		preconditions = []any{}
	}
	var targetID any
	if aggregateID != "" {
		targetID = aggregateID
	}
	body := e2ePost(t, endpoint, "/api/v1/kernel/commands", map[string]any{
		"command_id": id, "idempotency_key": id, "workspace_id": workspace,
		"requested_actor": map[string]any{"type": "user", "id": "dev"}, "command_type": commandType,
		"target": map[string]any{"aggregate_type": aggregateType, "aggregate_id": targetID}, "preconditions": preconditions, "payload": payload,
		"causation": map[string]any{"source": "api", "attempt_id": nil, "event_id": nil},
	})
	if body["outcome"] != "applied" {
		t.Fatalf("%s outcome=%#v", id, body)
	}
	aggregate, _ := body["aggregate"].(map[string]any)
	result, _ := aggregate["id"].(string)
	return result
}

func e2eVersion(id string, version int) map[string]any {
	return map[string]any{"aggregate_type": "object_instance", "aggregate_id": id, "expected_version": version}
}

func e2ePost(t *testing.T, endpoint, path string, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(endpoint+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("POST %s status=%d body=%#v err=%v", path, response.StatusCode, body, err)
	}
	return body
}

func e2eSearch(t *testing.T, endpoint, workspace, query string, sort []string, limit, offset int) []string {
	return e2eSearchFiltered(t, endpoint, workspace, query, "", sort, limit, offset)
}

func e2eSearchFiltered(t *testing.T, endpoint, workspace, query, filter string, sort []string, limit, offset int) []string {
	t.Helper()
	body := e2ePost(t, endpoint, fmt.Sprintf("/api/v1/workspaces/%s/search", workspace), map[string]any{"q": query, "filter": filter, "sort": sort, "limit": limit, "offset": offset})
	data := body["data"].(map[string]any)
	rawHits, _ := data["hits"].([]any)
	titles := make([]string, 0, len(rawHits))
	for _, raw := range rawHits {
		if hit, ok := raw.(map[string]any); ok {
			if title, ok := hit["title"].(string); ok {
				titles = append(titles, title)
			}
		}
	}
	return titles
}

func e2eHasTitle(titles []string, want string) bool {
	for _, title := range titles {
		if title == want {
			return true
		}
	}
	return false
}
