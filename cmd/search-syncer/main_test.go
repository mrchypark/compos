package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

func TestWaitTaskRejectsFailedMeilisearchTask(t *testing.T) {
	wait := func(uid int64, interval time.Duration) (*meilisearch.Task, error) {
		if uid != 7 || interval != 100*time.Millisecond {
			t.Fatalf("uid=%d interval=%s", uid, interval)
		}
		return &meilisearch.Task{Status: meilisearch.TaskStatusFailed}, nil
	}
	if err := waitTask(wait, 7); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("failed task error=%v", err)
	}
	if err := waitTask(func(int64, time.Duration) (*meilisearch.Task, error) {
		return &meilisearch.Task{Status: meilisearch.TaskStatusSucceeded}, nil
	}, 8); err != nil {
		t.Fatalf("successful task error=%v", err)
	}
	want := errors.New("transport")
	if err := waitTask(func(int64, time.Duration) (*meilisearch.Task, error) { return nil, want }, 9); !errors.Is(err, want) {
		t.Fatalf("transport error=%v", err)
	}
}

func TestSyncOnceRebuildsSnapshotWhenIndexWasRecreated(t *testing.T) {
	meili := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/indexes/objects":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"missing","code":"index_not_found","type":"invalid_request","link":""}`))
		case strings.HasPrefix(r.URL.Path, "/tasks/"):
			_, _ = w.Write([]byte(`{"uid":1,"status":"succeeded","type":"indexCreation","enqueuedAt":"2026-01-01T00:00:00Z","startedAt":"2026-01-01T00:00:00Z","finishedAt":"2026-01-01T00:00:00Z"}`))
		default:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"taskUid":1}`))
		}
	}))
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/workspaces/wks_1/projection/snapshot" {
			t.Fatalf("unexpected Compos path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"snapshot":{"workspace_id":"wks_1","projection_sequence":4,"schema_version":3,"objects":[]}}`))
	}))
	defer compos.Close()

	state := SyncState{LastSequence: 9, SnapshotReady: true}
	cfg := Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects", PrimaryKey: "id"}
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err != nil {
		t.Fatal(err)
	}
	if state.LastSequence != 4 {
		t.Fatalf("last sequence=%d, want rebuilt snapshot sequence 4", state.LastSequence)
	}
	if !state.SnapshotReady {
		t.Fatal("snapshot was not marked ready")
	}
}

func TestSyncOnceUsesEventsAfterZeroSequenceSnapshot(t *testing.T) {
	var snapshots, events int
	meili := syncerMeilisearch(t, false)
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/workspaces/wks_1/projection/snapshot":
			snapshots++
			_, _ = w.Write([]byte(`{"snapshot":{"workspace_id":"wks_1","projection_sequence":0,"objects":[]}}`))
		case "/api/v1/workspaces/wks_1/projection/events":
			events++
			_, _ = w.Write([]byte(`{"events":[{"id":"evt_1","workspace_id":"wks_1","projection_sequence":1,"aggregate_type":"ignored"}]}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer compos.Close()

	state := SyncState{}
	cfg := Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects"}
	for range 2 {
		if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err != nil {
			t.Fatal(err)
		}
	}
	if snapshots != 1 || events != 1 || !state.SnapshotReady || state.LastSequence != 1 {
		t.Fatalf("snapshots=%d events=%d state=%+v", snapshots, events, state)
	}
}

func TestSyncOnceDoesNotMarkFailedSnapshotReady(t *testing.T) {
	meili := syncerMeilisearch(t, false)
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer compos.Close()
	state := SyncState{LastSequence: 7}
	err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects"}, &state)
	if err == nil || state.SnapshotReady || state.LastSequence != 7 {
		t.Fatalf("err=%v state=%+v", err, state)
	}
}

func TestSyncOnceDiscoversWorkspaceAndResetsOnRotation(t *testing.T) {
	workspace := "wks_one"
	var snapshots []string
	meili := syncerMeilisearch(t, false)
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/search-syncer/workspace" {
			if got := r.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("authorization=%q", got)
			}
			_, _ = w.Write([]byte(`{"workspace_id":"` + workspace + `"}`))
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/projection/snapshot") {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		snapshots = append(snapshots, r.URL.Path)
		_, _ = w.Write([]byte(`{"snapshot":{"workspace_id":"` + workspace + `","projection_sequence":3,"objects":[]}}`))
	}))
	defer compos.Close()
	state := SyncState{}
	cfg := Config{ComposEndpoint: compos.URL, APIToken: "token", IndexUID: "objects"}
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err != nil {
		t.Fatal(err)
	}
	workspace = "wks_two"
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err != nil {
		t.Fatal(err)
	}
	if state.WorkspaceID != "wks_two" || state.LastSequence != 3 || len(snapshots) != 2 || !strings.Contains(snapshots[1], "wks_two") {
		t.Fatalf("snapshots=%v state=%+v", snapshots, state)
	}
}

func TestSyncOnceWorkspaceRotationClearsOldWorkspaceBeforeStateChange(t *testing.T) {
	workspace := "wks_two"
	failNextClear := true
	var filters []string
	var snapshots []string
	var task int
	meili := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/indexes/objects":
			_, _ = w.Write([]byte(`{"uid":"objects","primaryKey":"id"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/indexes/objects/documents/delete":
			var body struct {
				Filter string `json:"filter"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			filters = append(filters, body.Filter)
			task++
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"taskUid":%d}`, task)))
		case r.Method == http.MethodPatch && r.URL.Path == "/indexes/objects/settings":
			task++
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"taskUid":%d}`, task)))
		case strings.HasPrefix(r.URL.Path, "/tasks/"):
			status := "succeeded"
			if failNextClear {
				status, failNextClear = "failed", false
			}
			_, _ = w.Write([]byte(`{"uid":1,"status":"` + status + `","type":"documentDeletion","enqueuedAt":"2026-01-01T00:00:00Z"}`))
		default:
			t.Fatalf("unexpected Meilisearch request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/search-syncer/workspace" {
			_, _ = w.Write([]byte(`{"workspace_id":"` + workspace + `"}`))
			return
		}
		snapshots = append(snapshots, r.URL.Path)
		_, _ = w.Write([]byte(`{"snapshot":{"workspace_id":"wks_two","projection_sequence":3,"objects":[]}}`))
	}))
	defer compos.Close()

	state := SyncState{WorkspaceID: "wks_one", LastSequence: 7, SnapshotReady: true}
	cfg := Config{ComposEndpoint: compos.URL, APIToken: "token", IndexUID: "objects"}
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err == nil {
		t.Fatal("expected old workspace clear failure")
	}
	if state.WorkspaceID != "wks_one" || state.LastSequence != 7 || !state.SnapshotReady || len(snapshots) != 0 {
		t.Fatalf("state changed before cleanup: %+v snapshots=%v", state, snapshots)
	}
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), cfg, &state); err != nil {
		t.Fatal(err)
	}
	if state.WorkspaceID != "wks_two" || state.LastSequence != 3 || !state.SnapshotReady || len(snapshots) != 1 || !strings.Contains(snapshots[0], "wks_two") {
		t.Fatalf("state=%+v snapshots=%v", state, snapshots)
	}
	if strings.Join(filters, "\n") != "workspace_id = \"wks_one\"\nworkspace_id = \"wks_one\"\nworkspace_id = \"wks_two\"" {
		t.Fatalf("filters=%v", filters)
	}
}

func TestSyncOnceDoesNotAdvanceCursorWhenEventFails(t *testing.T) {
	meili := syncerMeilisearch(t, true)
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"events":[{"id":"evt_1","workspace_id":"wks_1","projection_sequence":3,"aggregate_type":"object_instance","aggregate_id":"obj_1","aggregate_version":1,"event_type":"object.created","payload":{"title":"broken"}}]}`))
	}))
	defer compos.Close()
	state := SyncState{LastSequence: 2, SnapshotReady: true}
	err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects"}, &state)
	if err == nil || state.LastSequence != 2 {
		t.Fatalf("err=%v state=%+v", err, state)
	}
}

func TestObjectSearchIDIsStableAndWorkspaceScoped(t *testing.T) {
	canonicalID := "obj_backoffice/workerp/samplecustomer/v1"
	first := objectSearchID("wks_1", canonicalID)
	if first != objectSearchID("wks_1", canonicalID) || first == objectSearchID("wks_2", canonicalID) {
		t.Fatalf("unexpected IDs: %q %q", first, objectSearchID("wks_2", canonicalID))
	}
	if !strings.HasPrefix(first, "obj_") || strings.Contains(first, "/") {
		t.Fatalf("invalid Meilisearch ID %q", first)
	}
}

func syncerMeilisearch(t *testing.T, failTasks bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/indexes/objects":
			_, _ = w.Write([]byte(`{"uid":"objects","primaryKey":"id"}`))
		case strings.HasPrefix(r.URL.Path, "/tasks/"):
			status := "succeeded"
			if failTasks {
				status = "failed"
			}
			_, _ = w.Write([]byte(`{"uid":1,"status":"` + status + `","type":"documentAdditionOrUpdate","enqueuedAt":"2026-01-01T00:00:00Z"}`))
		default:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"taskUid":1}`))
		}
	}))
}

func TestProcessObjectEventsUsesProjectionEventTypeAndCanonicalPayload(t *testing.T) {
	requests := make(chan struct {
		method string
		path   string
		body   string
	}, 10)
	meili := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := ""
		if r.Body != nil {
			var raw json.RawMessage
			_ = json.NewDecoder(r.Body).Decode(&raw)
			body = string(raw)
		}
		requests <- struct {
			method string
			path   string
			body   string
		}{r.Method, r.URL.Path, body}
		if strings.Contains(r.URL.Path, "/tasks/") {
			_, _ = w.Write([]byte(`{"uid":1,"status":"succeeded","type":"documentAdditionOrUpdate","enqueuedAt":"2026-01-01T00:00:00Z","startedAt":"2026-01-01T00:00:00Z","finishedAt":"2026-01-01T00:00:00Z"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"taskUid":1}`))
	}))
	defer meili.Close()
	client := meilisearch.New(meili.URL)
	cfg := Config{IndexUID: "objects"}
	canonicalID := "obj_backoffice/workerp/samplecustomer/v1"
	searchID := objectSearchID("wks_1", canonicalID)
	if err := indexDocuments(context.Background(), client, cfg, []Object{{ID: canonicalID, WorkspaceID: "wks_1", ObjectTypeKey: "orders", Title: "Snapshot"}}); err != nil {
		t.Fatal(err)
	}

	events := []ProjectionEvent{
		{WorkspaceID: "wks_1", AggregateType: "object_instance", AggregateID: canonicalID, AggregateVersion: 1, EventType: "object.created", CreatedAt: "2026-01-01T00:00:00Z", Payload: map[string]any{"object_type_key": "orders", "title": "Order", "values": map[string]any{"amount": float64(7)}, "created_at": "2025-12-31T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}},
		{WorkspaceID: "wks_1", AggregateType: "object_instance", AggregateID: canonicalID, AggregateVersion: 2, EventType: "object.updated", CreatedAt: "2026-01-02T00:00:00Z", Payload: map[string]any{"object_type_key": "orders", "title": "Updated", "values": map[string]any{"amount": float64(8)}}},
		{WorkspaceID: "wks_1", AggregateType: "object_instance", AggregateID: canonicalID, AggregateVersion: 3, EventType: "object.moved", CreatedAt: "2026-01-03T00:00:00Z", Payload: map[string]any{"state_key": "ready"}},
		{WorkspaceID: "wks_1", AggregateType: "object_instance", AggregateID: canonicalID, AggregateVersion: 4, EventType: "object.deleted", CreatedAt: "2026-01-04T00:00:00Z", Payload: map[string]any{}},
	}
	for _, event := range events {
		if err := processObjectEvent(context.Background(), client, cfg, event); err != nil {
			t.Fatal(err)
		}
	}
	close(requests)
	var calls []string
	var writes int
	for request := range requests {
		if strings.Contains(request.path, "/tasks/") {
			continue
		}
		calls = append(calls, request.method+" "+request.path+" "+request.body)
		if request.method == http.MethodPost || request.method == http.MethodPut || request.method == http.MethodPatch {
			writes++
			if !strings.Contains(request.body, `"id":"`+searchID+`"`) {
				t.Fatalf("object write used a noncanonical search ID: %s %s", request.method, request.body)
			}
		}
		if request.method == http.MethodDelete && request.path != "/indexes/objects/documents/"+searchID {
			t.Fatalf("object delete used a noncanonical search ID: %s", request.path)
		}
	}
	if writes != 4 {
		t.Fatalf("object writes=%d, want snapshot/create/update/move", writes)
	}
	joined := strings.Join(calls, "\n")
	for _, want := range []string{"orders", `"id":"` + searchID + `"`, `"object_id":"` + canonicalID + `"`, `"workspace_id":"wks_1"`, `"payload_amount":7`, `"created_at":"2025-12-31T00:00:00Z"`, `"state":"ready"`, `"version":2`, "DELETE /indexes/objects/documents/" + searchID} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in requests:\n%s", want, joined)
		}
	}
}

func TestLegacyKnowledgeEventsAdvanceCursorWithoutIndexing(t *testing.T) {
	meili := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("legacy event must not touch search: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"events":[{"id":"legacy","workspace_id":"wks_1","projection_sequence":8,"aggregate_type":"knowledge_document","aggregate_id":"core:old","payload":{"operation":"accepted"}}]}`))
	}))
	defer compos.Close()
	state := SyncState{LastSequence: 7, SnapshotReady: true}
	if err := syncFromEvents(context.Background(), compos.Client(), meilisearch.New(meili.URL), Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects"}, &state); err != nil || state.LastSequence != 8 {
		t.Fatalf("err=%v state=%+v", err, state)
	}
}

func TestRestartRebuildClearsLegacyWorkspaceDocuments(t *testing.T) {
	var cleared bool
	meili := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/indexes/objects/documents/delete" {
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `workspace_id = \"wks_1\"`) {
				t.Errorf("unbounded clear: %s", body)
			}
			cleared = true
		}
		if strings.HasPrefix(r.URL.Path, "/tasks/") {
			_, _ = w.Write([]byte(`{"uid":1,"status":"succeeded"}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/indexes/objects" {
			_, _ = w.Write([]byte(`{"uid":"objects","primaryKey":"id"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"taskUid":1}`))
	}))
	defer meili.Close()
	compos := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"snapshot":{"workspace_id":"wks_1","projection_sequence":9,"schema_version":3,"objects":[]}}`))
	}))
	defer compos.Close()
	state := SyncState{}
	if err := syncOnce(context.Background(), compos.Client(), meilisearch.New(meili.URL), Config{ComposEndpoint: compos.URL, WorkspaceID: "wks_1", IndexUID: "objects"}, &state); err != nil {
		t.Fatal(err)
	}
	if !cleared || !state.SnapshotReady || state.LastSequence != 9 {
		t.Fatalf("cleared=%v state=%+v", cleared, state)
	}
}
