package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestGraphProjectionReconcileCannotHideANewerCommittedHead(t *testing.T) {
	handler := &Server{graphProjectionHeads: map[string]int64{"w": 3}, graphProjectionCursors: map[string]int64{"w": 1}}
	handler.finishGraphProjectionReconcile(map[string]int64{"w": 2}, map[string]int64{"w": 2}, nil)
	if handler.graphProjectionHeads["w"] != 3 || handler.graphProjectionCursors["w"] != 2 {
		t.Fatalf("heads=%v cursors=%v", handler.graphProjectionHeads, handler.graphProjectionCursors)
	}
}

func TestHTTPWorkspaceRegistryAndCommandCompatibility(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	workspace := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "One", "key": "one", "idempotency_key": "idem_workspace"})
	if workspace.Code != http.StatusCreated {
		t.Fatalf("workspace status=%d body=%s", workspace.Code, workspace.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(workspace.Body.Bytes(), &created)
	createdData := created["data"].(map[string]any)
	workspaceID := createdData["id"].(string)
	if createdData["workspace_id"] != workspaceID || createdData["created"] != true || createdData["status"] != "created" || createdData["created_at"] == "" {
		t.Fatalf("incomplete workspace response: %#v", createdData)
	}
	duplicate := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "One", "key": "one", "idempotency_key": "idem_workspace"})
	if duplicate.Code != http.StatusOK || !bytes.Contains(duplicate.Body.Bytes(), []byte(`"created":false`)) || !bytes.Contains(duplicate.Body.Bytes(), []byte(`"status":"duplicate"`)) {
		t.Fatalf("duplicate status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	conflict := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Two", "key": "ONE", "idempotency_key": "idem_other"})
	if conflict.Code != http.StatusConflict || !bytes.Contains(conflict.Body.Bytes(), []byte(`"code":"workspace_key_conflict"`)) {
		t.Fatalf("key conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	adapterPath := "/api/v1/workspaces/" + workspaceID + "/adapter/inbound-events"
	adapterBody := map[string]any{"adapter_id": " slack ", "inbound_event_id": " evt_1 ", "payload": map[string]any{"value": 1}}
	firstAdapter := request(t, handler, "POST", adapterPath, adapterBody)
	secondAdapter := request(t, handler, "POST", adapterPath, adapterBody)
	conflictingAdapter := request(t, handler, "POST", adapterPath, map[string]any{"adapter_id": "slack", "inbound_event_id": "evt_1", "payload": map[string]any{"value": 2}})
	invalidAdapter := request(t, handler, "POST", adapterPath, map[string]any{"adapter_id": "", "inbound_event_id": "evt_2", "payload": map[string]any{}})
	if firstAdapter.Code != http.StatusAccepted || secondAdapter.Code != http.StatusOK || conflictingAdapter.Code != http.StatusConflict || invalidAdapter.Code != http.StatusBadRequest {
		t.Fatalf("adapter statuses=%d,%d,%d,%d", firstAdapter.Code, secondAdapter.Code, conflictingAdapter.Code, invalidAdapter.Code)
	}
	if !bytes.Equal(firstAdapter.Body.Bytes(), secondAdapter.Body.Bytes()) {
		t.Fatalf("idempotent adapter response changed: first=%s second=%s", firstAdapter.Body.String(), secondAdapter.Body.String())
	}
	var builtins int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM object_types WHERE workspace_id=? AND key IN ('people','companies')`, workspaceID).Scan(&builtins); err != nil {
		t.Fatal(err)
	}
	if builtins != 2 {
		t.Fatalf("built-in object types=%d", builtins)
	}
	var worksFor int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM relation_definitions WHERE workspace_id=? AND key='works_for'`, workspaceID).Scan(&worksFor); err != nil {
		t.Fatal(err)
	}
	if worksFor != 1 {
		t.Fatal("works_for was not provisioned")
	}
	registry := request(t, handler, "GET", "/api/v1/kernel/command-registry", nil)
	if registry.Code != 200 {
		t.Fatal(registry.Body.String())
	}
	if bytes.Contains(registry.Body.Bytes(), []byte(`task.create`)) || bytes.Contains(registry.Body.Bytes(), []byte(`task_submission`)) {
		t.Fatalf("legacy task command remains: %s", registry.Body.String())
	}
	command := map[string]any{"command_id": "cmd_1", "idempotency_key": "idem_1", "workspace_id": workspaceID, "requested_actor": map[string]any{"type": "user", "id": "dev"}, "command_type": "object_type.define", "target": map[string]any{"aggregate_type": "object_type", "aggregate_id": nil}, "preconditions": []any{}, "payload": map[string]any{"key": "orders", "singular": "Order", "plural": "Orders", "field_schema": map[string]any{"type": "object"}}, "causation": map[string]any{"source": "api", "attempt_id": nil, "event_id": nil}}
	response := request(t, handler, "POST", "/api/v1/kernel/commands", command)
	if response.Code != 200 {
		t.Fatalf("command status=%d body=%s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"outcome":"applied"`)) {
		t.Fatal(response.Body.String())
	}
	command["command_id"], command["idempotency_key"] = "cmd_2", "idem_2"
	command["payload"] = map[string]any{"key": "invoices", "singular": "Invoice", "plural": "Invoices", "field_schema": map[string]any{"type": "object"}}
	if second := request(t, handler, "POST", "/api/v1/kernel/commands", command); second.Code != http.StatusOK {
		t.Fatalf("second command status=%d body=%s", second.Code, second.Body.String())
	}
	events := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/projection/events?after=0&limit=1", nil)
	if events.Code != http.StatusOK || !bytes.Contains(events.Body.Bytes(), []byte(`"workspace_id":"`+workspaceID+`"`)) || !bytes.Contains(events.Body.Bytes(), []byte(`"event_type":"object_type.defined"`)) {
		t.Fatalf("projection events status=%d body=%s", events.Code, events.Body.String())
	}
	var eventPage struct {
		Events []any `json:"events"`
	}
	if err := json.Unmarshal(events.Body.Bytes(), &eventPage); err != nil || len(eventPage.Events) != 1 {
		t.Fatalf("projection limit was not honored: err=%v body=%s", err, events.Body.String())
	}
	surface := request(t, handler, "GET", "/api/v1/workspaces/"+workspaceID+"/command-surface", nil)
	if bytes.Contains(surface.Body.Bytes(), []byte(`"templates"`)) || bytes.Contains(surface.Body.Bytes(), []byte(`object_type.template_set`)) {
		t.Fatalf("retired presentation contract exposed: %s", surface.Body.String())
	}
	if surface.Code != 200 || !bytes.Contains(surface.Body.Bytes(), []byte(`"workspace_id":"`+workspaceID+`"`)) || !bytes.Contains(surface.Body.Bytes(), []byte(`"key":"orders"`)) || !bytes.Contains(surface.Body.Bytes(), []byte(`"kind":"object_type"`)) || !bytes.Contains(surface.Body.Bytes(), []byte(`"object_type_id":`)) || !bytes.Contains(surface.Body.Bytes(), []byte(`"commands":["create","get"]`)) || !bytes.Contains(surface.Body.Bytes(), []byte(`"relations":[]`)) {
		t.Fatalf("surface status=%d body=%s", surface.Code, surface.Body.String())
	}
}

func TestRhizaWorkspaceCreationIsAtomicIdempotentAndConcurrent(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "workspace-api-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	defer handler.Close()

	created := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "One", "key": "one", "idempotency_key": "idem_one"})
	duplicate := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "One", "key": "one", "idempotency_key": "idem_one"})
	idempotencyConflict := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Changed", "key": "changed", "idempotency_key": "idem_one"})
	keyConflict := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Other", "key": "one", "idempotency_key": "idem_other"})
	if created.Code != http.StatusCreated || duplicate.Code != http.StatusOK || idempotencyConflict.Code != http.StatusConflict || keyConflict.Code != http.StatusConflict {
		t.Fatalf("workspace statuses=%d,%d,%d,%d bodies=%s | %s", created.Code, duplicate.Code, idempotencyConflict.Code, keyConflict.Code, idempotencyConflict.Body.String(), keyConflict.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	workspaceID := body["data"].(map[string]any)["id"].(string)
	var memberships, types, revisions, definitions, objects, bindings, relations, slots, indexes, requests int
	err = s.ReadRow(ctx, store.ReadLinearizable, `SELECT
		(SELECT COUNT(*) FROM workspace_memberships WHERE workspace_id=?),
		(SELECT COUNT(*) FROM object_types WHERE workspace_id=?),
		(SELECT COUNT(*) FROM object_type_schema_revisions WHERE workspace_id=?),
		(SELECT COUNT(*) FROM relation_definitions WHERE workspace_id=?),
		(SELECT COUNT(*) FROM object_instances WHERE workspace_id=?),
		(SELECT COUNT(*) FROM principal_person_bindings WHERE workspace_id=?),
		(SELECT COUNT(*) FROM object_relations WHERE workspace_id=?),
		(SELECT COUNT(*) FROM relation_cardinality_slots WHERE workspace_id=?),
		(SELECT COUNT(*) FROM object_field_index WHERE workspace_id=?),
		(SELECT COUNT(*) FROM workspace_creation_requests WHERE workspace_id=?)`, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID, workspaceID).Scan(&memberships, &types, &revisions, &definitions, &objects, &bindings, &relations, &slots, &indexes, &requests)
	if err != nil || memberships != 1 || types != 2 || revisions != 2 || definitions != 1 || objects != 2 || bindings != 1 || relations != 1 || slots != 1 || indexes != 2 || requests != 1 {
		t.Fatalf("builtins=%d,%d,%d,%d,%d,%d,%d,%d,%d,%d err=%v", memberships, types, revisions, definitions, objects, bindings, relations, slots, indexes, requests, err)
	}
	commandResponse := request(t, handler, "POST", "/api/v1/kernel/commands", map[string]any{
		"command_id": "cmd_workspace_rhiza", "idempotency_key": "idem_workspace_rhiza", "workspace_id": workspaceID,
		"requested_actor": map[string]any{"type": "user", "id": "dev"}, "command_type": "object_type.define",
		"target": map[string]any{"aggregate_type": "object_type", "aggregate_id": nil}, "preconditions": []any{},
		"payload":   map[string]any{"key": "orders", "singular": "Order", "plural": "Orders", "field_schema": map[string]any{"type": "object"}},
		"causation": map[string]any{"source": "api", "attempt_id": nil, "event_id": nil},
	})
	if commandResponse.Code != http.StatusOK || !bytes.Contains(commandResponse.Body.Bytes(), []byte(`"outcome":"applied"`)) {
		t.Fatalf("command after workspace creation status=%d body=%s", commandResponse.Code, commandResponse.Body.String())
	}

	statuses := make(chan int, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Concurrent", "key": "concurrent", "idempotency_key": fmt.Sprintf("concurrent-%d", i)})
			statuses <- response.Code
		}(i)
	}
	wg.Wait()
	close(statuses)
	winners, conflicts := 0, 0
	for status := range statuses {
		if status == http.StatusCreated {
			winners++
		} else if status == http.StatusConflict {
			conflicts++
		}
	}
	var concurrentWorkspaces int
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM workspaces WHERE key='concurrent'`).Scan(&concurrentWorkspaces); err != nil || winners != 1 || conflicts != 7 || concurrentWorkspaces != 1 {
		t.Fatalf("concurrent winners=%d conflicts=%d workspaces=%d err=%v", winners, conflicts, concurrentWorkspaces, err)
	}
}

func TestInternalObjectReadRouteIsNotExposed(t *testing.T) {
	db, handler, workspace := authTestServer(t)
	defer db.Close()
	defer handler.Close()

	body := map[string]any{"namespace": workspace, "objectType": "orders", "schemaDigest": "ignored", "resourceId": "obj_1"}
	if response := request(t, handler, http.MethodPost, "/internal/v1/objects:read", body); response.Code != http.StatusNotFound {
		t.Fatalf("unauthenticated internal object read status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(t, handler, http.MethodPost, "/internal/v1/objects:read", map[string]any{"namespace": "another-workspace", "objectType": "orders", "schemaDigest": "ignored", "resourceId": "obj_1"}); response.Code != http.StatusNotFound {
		t.Fatalf("cross-workspace internal object read status=%d body=%s", response.Code, response.Body.String())
	}
}

func request(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		encoded, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
