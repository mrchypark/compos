package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
)

func TestParseRecordScope(t *testing.T) {
	valid := `(id = "obj_a" || id = "obj_b")`
	ids, err := parseRecordScope(valid)
	if err != nil || len(ids) != 2 || ids[0] != "obj_a" || ids[1] != "obj_b" {
		t.Fatalf("parseRecordScope(%q) = %v, %v", valid, ids, err)
	}
	ids, err = parseRecordScope(`(1 = 0)`)
	if err != nil || len(ids) != 0 {
		t.Fatalf("empty scope = %v, %v", ids, err)
	}
	ids, err = parseRecordScope(`(id = "obj_a" || id = "obj_b") && (id = "obj_b" || id = "obj_c")`)
	if err != nil || len(ids) != 1 || ids[0] != "obj_b" {
		t.Fatalf("composed scope intersection = %v, %v", ids, err)
	}
	ids, err = parseRecordScope(`(1 = 0) && (id = "obj_a")`)
	if err != nil || len(ids) != 0 {
		t.Fatalf("empty composed scope = %v, %v", ids, err)
	}
	ids, err = parseRecordScope(`(id = "a&&b" || id = "c||d")`)
	if err != nil || len(ids) != 2 || ids[0] != "a&&b" || ids[1] != "c||d" {
		t.Fatalf("delimiters inside IDs = %v, %v", ids, err)
	}
	for _, filter := range []string{"", `id = "obj_a"`, `(id = "obj_a" || 1 = 1)`, `(id = "obj_a") && (title = "x")`, `(id = "obj_a") && (id != "obj_b")`, `(id = "")`, `(id = "a") && (id = "b") && (id = "c")`, `(1 = 0) || (id = "obj_a")`} {
		if _, err := parseRecordScope(filter); err == nil {
			t.Errorf("parseRecordScope(%q) accepted unsupported filter", filter)
		}
	}
	tooMany := make([]string, maxRecordScopeIDs+1)
	for i := range tooMany {
		tooMany[i] = `id = "obj_x"`
	}
	if _, err := parseRecordScope("(" + strings.Join(tooMany, " || ") + ")"); err == nil {
		t.Fatal("accepted more IDs than the scope limit")
	}
	if _, err := parseRecordScope("(" + strings.Repeat(" ", maxRecordScopeBytes) + ")"); err == nil {
		t.Fatal("accepted a scope larger than the byte limit")
	}
}

func TestTypedObjectCatalogScopesBeforePagingAndPinsWorkspaceAndType(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	typeReceipt := execute("cmd_scope_type", "object_type.define", "object_type", nil, map[string]any{"key": "scope_items", "singular": "Scope Item", "plural": "Scope Items", "field_schema": map[string]any{"type": "object"}})
	otherType := execute("cmd_scope_other_type", "object_type.define", "object_type", nil, map[string]any{"key": "other_items", "singular": "Other Item", "plural": "Other Items", "field_schema": map[string]any{"type": "object"}})
	first := execute("cmd_scope_first", "object.create", "object_instance", nil, map[string]any{"object_type_key": "scope_items", "title": "Unselected first"})
	allowed := execute("cmd_scope_allowed", "object.create", "object_instance", nil, map[string]any{"object_type_key": "scope_items", "title": "Authorized"})
	token := typedObjectAuthorityFixture(t, handler)
	if _, err := db.DB().Exec(`UPDATE object_instances SET updated_at=? WHERE id=?`, "2999-01-01T00:00:00Z", first.Aggregate.ID); err != nil {
		t.Fatal(err)
	}
	// This is Exposure's exact shape: authorized IDs, ANDed with the caller's id filter.
	composedFilter := `(id = "` + first.Aggregate.ID + `" || id = "` + allowed.Aggregate.ID + `") && (id = "` + allowed.Aggregate.ID + `")`
	path := "/api/v1/workspaces/wks_1/object-types/" + typeReceipt.Aggregate.ID + "/objects?filter=" + url.QueryEscape(composedFilter) + "&limit=1"
	listed := authRequest(t, handler, http.MethodGet, path, token, nil)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"id":"`+allowed.Aggregate.ID+`"`) || strings.Contains(listed.Body.String(), first.Aggregate.ID) || !strings.Contains(listed.Body.String(), `"next_cursor":null`) {
		t.Fatalf("scoped first page status=%d body=%s", listed.Code, listed.Body.String())
	}
	empty := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/object-types/"+typeReceipt.Aggregate.ID+"/objects?filter="+url.QueryEscape(`(1 = 0) && (id = "`+allowed.Aggregate.ID+`")`), token, nil)
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"data":[]`) {
		t.Fatalf("empty scope status=%d body=%s", empty.Code, empty.Body.String())
	}
	wrongType := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/object-types/"+otherType.Aggregate.ID+"/objects?filter="+url.QueryEscape(`(id = "`+allowed.Aggregate.ID+`")`), token, nil)
	if wrongType.Code != http.StatusOK || !strings.Contains(wrongType.Body.String(), `"data":[]`) {
		t.Fatalf("wrong type status=%d body=%s", wrongType.Code, wrongType.Body.String())
	}
	_, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_other','other','Other',1,?,?)`, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	wrongWorkspace := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_other/object-types/"+typeReceipt.Aggregate.ID+"/objects?filter="+url.QueryEscape(`(id = "`+allowed.Aggregate.ID+`")`), token, nil)
	if wrongWorkspace.Code != http.StatusOK || !strings.Contains(wrongWorkspace.Body.String(), `"data":[]`) {
		t.Fatalf("wrong workspace status=%d body=%s", wrongWorkspace.Code, wrongWorkspace.Body.String())
	}
	invalid := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/object-types/"+typeReceipt.Aggregate.ID+"/objects?filter="+url.QueryEscape(`(id = "`+allowed.Aggregate.ID+`" || 1 = 1)`), token, nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("unsupported filter status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	missing := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/object-types/"+typeReceipt.Aggregate.ID+"/objects", token, nil)
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("missing filter status=%d body=%s", missing.Code, missing.Body.String())
	}
}

func TestTypedObjectCommandsPinIdentityAndRequireVersion(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	typeReceipt := execute("cmd_typed_type", "object_type.define", "object_type", nil, map[string]any{"key": "typed_items", "singular": "Typed Item", "plural": "Typed Items", "field_schema": map[string]any{"type": "object"}})
	otherType := execute("cmd_typed_other_type", "object_type.define", "object_type", nil, map[string]any{"key": "typed_other", "singular": "Typed Other", "plural": "Typed Others", "field_schema": map[string]any{"type": "object"}})
	token := typedObjectAuthorityFixture(t, handler)
	collection := "/api/v1/workspaces/wks_1/object-types/" + typeReceipt.Aggregate.ID + "/objects"
	duplicateCreate := authRequestRaw(t, handler, http.MethodPost, collection, token, `{"title":"First","TITLE":"Second","idempotency_key":"typed_duplicate_title"}`)
	if duplicateCreate.Code != http.StatusBadRequest {
		t.Fatalf("case-colliding create fields status=%d body=%s", duplicateCreate.Code, duplicateCreate.Body.String())
	}
	spoofed := authRequest(t, handler, http.MethodPost, collection, token, map[string]any{"title": "Spoof", "idempotency_key": "typed_spoof", "command_type": "object.delete"})
	if spoofed.Code != http.StatusBadRequest {
		t.Fatalf("spoofed command status=%d body=%s", spoofed.Code, spoofed.Body.String())
	}
	created := authRequest(t, handler, http.MethodPost, collection, token, map[string]any{"title": "Created", "values": map[string]any{}, "idempotency_key": "typed_create"})
	if created.Code != http.StatusOK || !strings.Contains(created.Body.String(), `"outcome":"applied"`) {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var objectID string
	if err := db.DB().QueryRow(`SELECT id FROM object_instances WHERE workspace_id='wks_1' AND title='Created'`).Scan(&objectID); err != nil {
		t.Fatal(err)
	}
	row := "/" + objectID
	for _, body := range []string{
		`{"values":{},"idempotency_key":"typed_duplicate_version","expected_version":1,"EXPECTED_VERSION":2}`,
		`{"values":{},"idempotency_key":"typed_once","IDEMPOTENCY_KEY":"typed_twice","expected_version":1}`,
		`{"values":{"field":1,"field":2},"idempotency_key":"typed_nested_duplicate","expected_version":1}`,
	} {
		duplicate := authRequestRaw(t, handler, http.MethodPatch, collection+row, token, body)
		if duplicate.Code != http.StatusBadRequest {
			t.Errorf("duplicate JSON keys status=%d body=%s for %s", duplicate.Code, duplicate.Body.String(), body)
		}
	}
	wrongType := authRequest(t, handler, http.MethodPatch, "/api/v1/workspaces/wks_1/object-types/"+otherType.Aggregate.ID+"/objects"+row, token, map[string]any{"title": "Wrong type", "idempotency_key": "typed_wrong_type", "expected_version": 1})
	if wrongType.Code != http.StatusNotFound {
		t.Fatalf("wrong type update status=%d body=%s", wrongType.Code, wrongType.Body.String())
	}
	stale := authRequest(t, handler, http.MethodPatch, collection+row, token, map[string]any{"title": "Stale", "idempotency_key": "typed_stale", "expected_version": 9})
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale update status=%d body=%s", stale.Code, stale.Body.String())
	}
	updated := authRequest(t, handler, http.MethodPatch, collection+row, token, map[string]any{"values": map[string]any{"ok": true}, "idempotency_key": "typed_update", "expected_version": 1})
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"version":2`) {
		t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
	}

	unauthorizedArchive := authRequest(t, handler, http.MethodDelete, collection+row, token, map[string]any{"idempotency_key": "typed_archive_denied", "expected_version": 2})
	if unauthorizedArchive.Code != http.StatusForbidden {
		t.Fatalf("unauthorized archive status=%d body=%s", unauthorizedArchive.Code, unauthorizedArchive.Body.String())
	}
	var archived sql.NullString
	if err := db.DB().QueryRow(`SELECT archived_at FROM object_instances WHERE id=?`, objectID).Scan(&archived); err != nil || archived.Valid {
		t.Fatalf("unauthorized archive changed row: archived=%v err=%v", archived, err)
	}
}

func authRequestRaw(t *testing.T, handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestTypedObjectCommandReplays(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	typ := execute("cmd_replay_type", "object_type.define", "object_type", nil, map[string]any{"key": "replay_items", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}})
	token := typedObjectAuthorityFixture(t, handler)
	collection := "/api/v1/workspaces/wks_1/object-types/" + typ.Aggregate.ID + "/objects"
	// Capture durable effects as well as the row: a duplicate must append nothing.
	snapshot := func() string {
		t.Helper()
		var rows string
		if err := db.DB().QueryRow(`SELECT COALESCE(json_group_array(json_object('id',id,'title',title,'values',payload_json,'version',version,'archived_at',archived_at,'updated_at',updated_at)),'[]') FROM object_instances`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"command_ledger", "activities", "outbox_events", "projection_events", "journal_records"} {
			var count int
			if err := db.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
				t.Fatal(err)
			}
			rows += fmt.Sprintf("/%s=%d", table, count)
		}
		return rows
	}
	var objectID string
	for _, step := range []struct {
		name, method string
		body         map[string]any
		version      int
	}{
		{"create", http.MethodPost, map[string]any{"title": "Created", "idempotency_key": "replay_create"}, 1},
		{"update", http.MethodPatch, map[string]any{"title": "Updated", "values": map[string]any{"ok": true}, "expected_version": 1, "idempotency_key": "replay_update"}, 2},
		{"archive", http.MethodDelete, map[string]any{"expected_version": 2, "idempotency_key": "replay_archive"}, 3},
	} {
		t.Run(step.name, func(t *testing.T) {
			path := collection
			if objectID != "" {
				path += "/" + objectID
			}
			first := authRequest(t, handler, step.method, path, token, step.body)
			if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"outcome":"applied"`) {
				t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
			}
			var receipt struct {
				CommandID string `json:"command_id"`
				Aggregate struct {
					ID      string `json:"id"`
					Version int    `json:"version"`
				} `json:"aggregate"`
				Projection json.RawMessage `json:"projection"`
			}
			if err := json.Unmarshal(first.Body.Bytes(), &receipt); err != nil {
				t.Fatal(err)
			}
			objectID = receipt.Aggregate.ID
			if receipt.Aggregate.Version != step.version {
				t.Fatalf("version=%d want=%d", receipt.Aggregate.Version, step.version)
			}
			if step.name == "create" {
				var payload string
				if err := db.DB().QueryRow(`SELECT payload_json FROM command_ledger WHERE command_id=?`, receipt.CommandID).Scan(&payload); err != nil {
					t.Fatal(err)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(payload), &fields); err != nil {
					t.Fatal(err)
				}
				if _, exists := fields["values"]; exists {
					t.Fatalf("omitted values present in command: %s", payload)
				}
			}
			before := snapshot()
			replay := authRequest(t, handler, step.method, path, token, step.body)
			if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"outcome":"duplicate"`) {
				t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
			}
			var duplicate map[string]json.RawMessage
			if err := json.Unmarshal(replay.Body.Bytes(), &duplicate); err != nil {
				t.Fatal(err)
			}
			if string(duplicate["command_id"]) != strconv.Quote(receipt.CommandID) || string(duplicate["projection"]) != string(receipt.Projection) {
				t.Fatalf("replay receipt changed: %s", replay.Body.String())
			}
			if after := snapshot(); after != before {
				t.Fatalf("replay mutated state: before=%s after=%s", before, after)
			}
			changed := make(map[string]any, len(step.body))
			for key, value := range step.body {
				changed[key] = value
			}
			if step.name == "archive" {
				changed["expected_version"] = 3
			} else {
				changed["title"] = "Different"
			}
			conflict := authRequest(t, handler, step.method, path, token, changed)
			if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), `"idempotency_conflict"`) {
				t.Fatalf("changed request status=%d body=%s", conflict.Code, conflict.Body.String())
			}
			if after := snapshot(); after != before {
				t.Fatalf("conflict mutated state: before=%s after=%s", before, after)
			}
		})
	}
	before := snapshot()
	fresh := authRequest(t, handler, http.MethodDelete, collection+"/"+objectID, token, map[string]any{"expected_version": 3, "idempotency_key": "new_archive"})
	if fresh.Code != http.StatusConflict {
		t.Fatalf("fresh archive of archived row status=%d body=%s", fresh.Code, fresh.Body.String())
	}

	handler.authority = testAuthority{request: func(*http.Request) (string, error) { return "", errTestAuthorityDenied }}
	denied := authRequest(t, handler, http.MethodDelete, collection+"/"+objectID, token, map[string]any{"expected_version": 2, "idempotency_key": "replay_archive"})
	if denied.Code != http.StatusForbidden {
		t.Fatalf("revoked membership replay status=%d body=%s", denied.Code, denied.Body.String())
	}
	if after := snapshot(); after != before {
		t.Fatalf("denied replay mutated state: before=%s after=%s", before, after)
	}
}

func TestTypedObjectCommandIDsIsolateLedgerScopes(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	execute("cmd_scope_replay_type", "object_type.define", "object_type", nil, map[string]any{"key": "id_scopes", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}})

	ids := map[string]bool{}
	for _, actor := range []string{"dev", "other"} {
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request = request.WithContext(context.WithValue(request.Context(), principalContextKey{}, actor))
		response := httptest.NewRecorder()
		handler.executeTypedObjectCommand(response, request, kernel.Command{WorkspaceID: "wks_1", IdempotencyKey: "shared-key", CommandType: "object.create", Target: kernel.Target{AggregateType: "object_instance"}, Preconditions: []kernel.Precondition{}, Payload: json.RawMessage(`{"object_type_key":"id_scopes","title":"Scoped"}`), Causation: kernel.Causation{Source: "api"}})
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"outcome":"applied"`) {
			t.Fatalf("principal=%s status=%d body=%s", actor, response.Code, response.Body.String())
		}
		var receipt struct {
			CommandID string `json:"command_id"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
			t.Fatal(err)
		}
		if receipt.CommandID == "" || ids[receipt.CommandID] {
			t.Fatalf("principal command ID collision: %q", receipt.CommandID)
		}
		ids[receipt.CommandID] = true
	}
	var count int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM object_instances WHERE title='Scoped'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("isolated objects=%d err=%v", count, err)
	}
}
