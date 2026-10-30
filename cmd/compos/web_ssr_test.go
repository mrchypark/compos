package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func TestWebGetJSONPreservesLargeInteger(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"version":9007199254740993}}`)
	}))
	defer backend.Close()
	got, err := webGetJSON(context.Background(), runtimeDeps{client: backend.Client()}, cliSession{}, backend.URL)
	if err != nil {
		t.Fatalf("webGetJSON: %v", err)
	}
	data := got["data"].(map[string]any)
	if version, ok := positiveJSONInt64(data["version"]); !ok || version != 9007199254740993 {
		t.Fatalf("large version = %#v, %t", version, ok)
	}
}

func TestPositiveJSONInt64RejectsInvalidVersions(t *testing.T) {
	for _, value := range []any{json.Number("0"), json.Number("-1"), json.Number("1.5"), json.Number("9223372036854775808"), float64(1), "1", nil} {
		if _, ok := positiveJSONInt64(value); ok {
			t.Fatalf("accepted invalid version %#v", value)
		}
	}
}

func TestValidateWebCommandReceipt(t *testing.T) {
	command := commandEnvelope{CommandID: "cmd_1"}
	valid := `{"command_id":"cmd_1","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":5}}`
	if receipt, err := validateWebCommandReceipt([]byte(valid), command, "obj_1", 5); err != nil || receipt.Aggregate.Version != 5 {
		t.Fatalf("valid receipt = %#v, %v", receipt, err)
	}
	duplicate := strings.Replace(valid, `"applied"`, `"duplicate"`, 1)
	if _, err := validateWebCommandReceipt([]byte(duplicate), command, "obj_1", 5); err != nil {
		t.Fatalf("duplicate receipt rejected: %v", err)
	}
	for name, body := range map[string]string{
		"bad outcome":  strings.Replace(valid, `"applied"`, `"rejected"`, 1),
		"command id":   strings.Replace(valid, `"cmd_1"`, `"cmd_2"`, 1),
		"type":         strings.Replace(valid, `"object_instance"`, `"object_type"`, 1),
		"empty id":     strings.Replace(valid, `"obj_1"`, `""`, 1),
		"wrong id":     strings.Replace(valid, `"obj_1"`, `"obj_2"`, 1),
		"zero version": strings.Replace(valid, `"version":5`, `"version":0`, 1),
		"bad version":  strings.Replace(valid, `"version":5`, `"version":1.5`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateWebCommandReceipt([]byte(body), command, "obj_1", 5); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestWebSSRNativeFormsRoundTripEscapedObjectPaths(t *testing.T) {
	const nounKey = "task/type"
	const objectID = "obj/1"
	var commands []commandEnvelope
	var postEvents []string
	currentVersion := int64(2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workspaces/wks_1/command-surface":
			_ = json.NewEncoder(w).Encode(map[string]any{"nouns": []any{map[string]any{
				"key": nounKey, "singular": "Task", "plural": "Tasks",
				"field_schema": map[string]any{"properties": map[string]any{"notes": map[string]any{"type": "string"}}, "required": []any{}},
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/workspaces/wks_1/objects":
			postEvents = append(postEvents, "GET list")
			if got := r.URL.Query().Get("object_type"); got != nounKey {
				t.Fatalf("object type query = %q, want %q", got, nounKey)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"id": objectID, "title": "One", "object_type_key": nounKey}}})
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/api/v1/workspaces/wks_1/objects/obj%2F1":
			postEvents = append(postEvents, "GET object")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": objectID, "title": "One", "object_type_key": nounKey, "version": currentVersion, "values": map[string]any{"notes": "old"}}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/kernel/commands":
			postEvents = append(postEvents, "POST command")
			var command commandEnvelope
			if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
				t.Fatalf("decode command: %v", err)
			}
			commands = append(commands, command)
			receiptVersion := int64(1)
			if command.CommandType == "object.update" {
				currentVersion++
				receiptVersion = currentVersion
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": command.CommandID, "outcome": "applied", "workspace_id": command.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": objectID, "version": receiptVersion}})
		default:
			t.Fatalf("unexpected backend request %s %s", r.Method, r.URL.String())
		}
	}))
	defer backend.Close()

	deps := runtimeDeps{
		client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(),
		stdout: io.Discard, stderr: io.Discard,
	}
	app := webSSR{deps: deps, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	frontend := httptest.NewServer(http.HandlerFunc(app.serveHTTP))
	defer frontend.Close()
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}

	get := func(path string) *http.Response {
		t.Helper()
		response, err := client.Get(frontend.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return response
	}
	assertPage := func(path, contains string) {
		t.Helper()
		response := get(path)
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(contains)) {
			t.Fatalf("GET %s = %d %s", path, response.StatusCode, body)
		}
	}
	assertPage("/", `/objects/task%2Ftype`)
	assertPage("/objects/task%2Ftype", `>One</a>`)
	assertPage("/objects/task%2Ftype/obj%2F1", `<dd>old</dd>`)
	assertPage("/objects/task%2Ftype/new", `name="_compos_form_mode" value="native"`)
	for _, path := range []string{"/objects/task%2Ftype?template=list_view", "/objects/task%2Ftype/obj%2F1?template=detail_view", "/objects/task%2Ftype/new?template=form_edit"} {
		response := get(path)
		response.Body.Close()
		if response.StatusCode != http.StatusGone {
			t.Fatalf("GET %s = %d, want 410", path, response.StatusCode)
		}
	}
	postEvents = nil

	post := func(path string, values url.Values) *http.Response {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, frontend.URL+path, strings.NewReader(values.Encode()))
		if err != nil {
			t.Fatalf("new POST request: %v", err)
		}
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		return response
	}
	for _, tc := range []struct {
		path string
		form url.Values
		want string
	}{
		{"/objects/task%2Ftype", url.Values{"_csrf": {"csrf"}, "_compos_form_mode": {"native"}, "/title": {"Created"}, "/notes": {"new"}}, "/objects/task%2Ftype"},
		{"/objects/task%2Ftype/obj%2F1", url.Values{"_csrf": {"csrf"}, "_compos_form_mode": {"native"}, "/title": {"Updated"}, "/notes": {"updated"}}, "/objects/task%2Ftype/obj%2F1"},
	} {
		response := post(tc.path, tc.form)
		response.Body.Close()
		if response.StatusCode != http.StatusSeeOther || response.Header.Get("Location") != tc.want {
			t.Fatalf("POST %s = %d Location %q", tc.path, response.StatusCode, response.Header.Get("Location"))
		}
	}
	if len(commands) != 2 || commands[0].CommandType != "object.create" || commands[1].CommandType != "object.update" {
		t.Fatalf("submitted commands = %#v", commands)
	}
	retired := post("/objects/task%2Ftype", url.Values{"_csrf": {"csrf"}, "template": {"form_edit"}, "/title": {"Rejected"}})
	retired.Body.Close()
	if retired.StatusCode != http.StatusBadRequest || len(commands) != 2 {
		t.Fatalf("retired template form = %d commands=%d", retired.StatusCode, len(commands))
	}
	if got, want := strings.Join(postEvents, ","), "POST command,GET object,GET object,POST command,GET object"; got != want {
		t.Fatalf("POST canonical revalidation sequence = %q, want %q", got, want)
	}

	for _, path := range []string{"/objects/%2e%2e", "/objects/task%2Ftype/%2e%2e"} {
		request := httptest.NewRequest(http.MethodGet, "http://example.test"+path, nil)
		response := httptest.NewRecorder()
		app.serveHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("traversal path %s = %d, want 404", path, response.Code)
		}
	}
}

func TestWebSSRReadOnlyRejectsWrites(t *testing.T) {
	schema := map[string]any{
		"properties": map[string]any{
			"notes":  map[string]any{"type": "string"},
			"active": map[string]any{"type": "boolean"},
		},
		"required": []any{},
	}
	if !formSchemaSupported(schema) {
		t.Fatal("an optional boolean must not invalidate unrelated supported fields")
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks"}]}`)
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client()}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1"}, writes: false}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/objects/tasks/new", nil),
		httptest.NewRequest(http.MethodGet, "/objects/tasks/obj_1/edit", nil),
		httptest.NewRequest(http.MethodPost, "/objects/tasks", strings.NewReader("_csrf=x")),
	} {
		response := httptest.NewRecorder()
		app.serveHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("read-only %s %s = %d, want 404", request.Method, request.URL.Path, response.Code)
		}
	}
}

func TestWebSSRRejectsObjectNounMismatchBeforeRender(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/command-surface") {
			_, _ = io.WriteString(w, `{"nouns":[{"key":"customers","singular":"Customer","plural":"Customers","field_schema":{"properties":{},"required":[]}}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"id":"obj_1","title":"Contract","object_type_key":"contracts","values":{}}}`)
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client()}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1"}, writes: true}
	for _, path := range []string{"/objects/customers/obj_1", "/objects/customers/obj_1/edit"} {
		response := httptest.NewRecorder()
		app.serveHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404", path, response.Code)
		}
	}
}

func TestWebSSRUpdateRejectsRequestedObjectIDMismatchBeforeCommand(t *testing.T) {
	var posts int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
		}
		if strings.HasSuffix(r.URL.Path, "/command-surface") {
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"id":"obj_other","title":"Other","object_type_key":"tasks","version":1,"values":{}}}`)
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), newID: fixedIDGenerator()}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_requested", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Updated"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusNotFound || posts != 0 {
		t.Fatalf("mismatched update = %d posts=%d", response.Code, posts)
	}
}

func TestWebSSRNativeEditUncheckedBooleanOverlaysFalseAndPreservesMissingFields(t *testing.T) {
	var received commandEnvelope
	var objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{"active":{"type":"boolean"},"notes":{"type":"string"}},"required":[]}}]}`)
		case r.Method == http.MethodGet:
			objectReads++
			version := 4
			if objectReads > 1 {
				version = 5
			}
			_, _ = fmt.Fprintf(w, `{"data":{"id":"obj_1","title":"Old","object_type_key":"tasks","version":%d,"values":{"active":true,"notes":"preserve"}}}`, version)
		case r.Method == http.MethodPost:
			decoder := json.NewDecoder(r.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&received); err != nil {
				t.Fatalf("decode command: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": received.CommandID, "outcome": "applied", "workspace_id": received.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": 5}})
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&_compos_present_%2Factive=1&%2Ftitle=New"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("native edit = %d: %s", response.Code, response.Body.String())
	}
	payload := received.Payload
	values, _ := payload["values"].(map[string]any)
	if values["active"] != false || values["notes"] != "preserve" {
		t.Fatalf("overlay values = %#v", values)
	}
	if received.Payload["title"] != "New" {
		t.Fatalf("native edit title = %#v, want New", received.Payload["title"])
	}
}

func TestWebSSREditPreservesLargeVersionExactly(t *testing.T) {
	const version int64 = 9007199254740993
	var received commandEnvelope
	var objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
		case r.Method == http.MethodGet:
			objectReads++
			readVersion := version
			if objectReads > 1 {
				readVersion++
			}
			_, _ = fmt.Fprintf(w, `{"data":{"id":"obj_1","title":"Old","object_type_key":"tasks","version":%d,"values":{}}}`, readVersion)
		case r.Method == http.MethodPost:
			decoder := json.NewDecoder(r.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&received); err != nil {
				t.Fatalf("decode command: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": received.CommandID, "outcome": "duplicate", "workspace_id": received.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": version + 1}})
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Updated"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("large-version edit = %d: %s", response.Code, response.Body.String())
	}
	if got, ok := positiveJSONInt64(received.Preconditions[0]["expected_version"]); !ok || got != version {
		t.Fatalf("expected version = %#v, %t, want %d", got, ok, version)
	}
}

func TestWebSSRUpdateRejectsMaxInt64VersionBeforeSubmit(t *testing.T) {
	var posts, objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
		case r.Method == http.MethodGet:
			objectReads++
			_, _ = io.WriteString(w, `{"data":{"id":"obj_1","title":"Old","object_type_key":"tasks","version":9223372036854775807,"values":{}}}`)
		case r.Method == http.MethodPost:
			posts++
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), newID: fixedIDGenerator()}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Updated"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusBadGateway || response.Header().Get("Location") != "" || posts != 0 || objectReads != 1 {
		t.Fatalf("MaxInt64 update = status %d location %q posts %d reads %d", response.Code, response.Header().Get("Location"), posts, objectReads)
	}
}

func TestWebSSRNativeEditWithoutTitlePreservesCurrentTitle(t *testing.T) {
	var received commandEnvelope
	var objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{"notes":{"type":"string"}},"required":[]}}]}`)
		case r.Method == http.MethodGet:
			objectReads++
			version := 5
			if objectReads > 1 {
				version = 6
			}
			_, _ = fmt.Fprintf(w, `{"data":{"id":"obj_1","title":"Keep me","object_type_key":"tasks","version":%d,"values":{"notes":"old"}}}`, version)
		case r.Method == http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
				t.Fatalf("decode command: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": received.CommandID, "outcome": "applied", "workspace_id": received.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": 6}})
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Fnotes=updated"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("native edit = %d: %s", response.Code, response.Body.String())
	}
	if received.Payload["title"] != "Keep me" {
		t.Fatalf("native edit title = %#v, want preserved title", received.Payload["title"])
	}
}

func TestWebSSRDoesNotRedirectWhenCanonicalRevalidationFails(t *testing.T) {
	var commands, objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
		case r.Method == http.MethodPost:
			commands++
			var command commandEnvelope
			_ = json.NewDecoder(r.Body).Decode(&command)
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": command.CommandID, "outcome": "applied", "workspace_id": command.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": 1}})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/objects/obj_1"):
			objectReads++
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			t.Fatalf("unexpected backend request %s %s", r.Method, r.URL.String())
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Created"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusBadGateway || response.Header().Get("Location") != "" {
		t.Fatalf("failed canonical revalidation = %d Location %q", response.Code, response.Header().Get("Location"))
	}
	if commands != 1 || objectReads != 1 {
		t.Fatalf("commands=%d object reads=%d, want one bounded attempt each", commands, objectReads)
	}
}

func TestWebSSRUpdateDoesNotRedirectWhenCanonicalRevalidationFails(t *testing.T) {
	var commands, objectReads int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/command-surface"):
			_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/objects/obj_1"):
			objectReads++
			if objectReads == 1 {
				_, _ = io.WriteString(w, `{"data":{"id":"obj_1","title":"Old","object_type_key":"tasks","version":4,"values":{}}}`)
				return
			}
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost:
			commands++
			var command commandEnvelope
			_ = json.NewDecoder(r.Body).Decode(&command)
			_ = json.NewEncoder(w).Encode(map[string]any{"command_id": command.CommandID, "outcome": "applied", "workspace_id": command.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": 5}})
		default:
			t.Fatalf("unexpected backend request %s %s", r.Method, r.URL.String())
		}
	}))
	defer backend.Close()
	app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
	request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Updated"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.serveHTTP(response, request)
	if response.Code != http.StatusBadGateway || response.Header().Get("Location") != "" {
		t.Fatalf("failed update canonical revalidation = %d Location %q", response.Code, response.Header().Get("Location"))
	}
	if commands != 1 || objectReads != 2 {
		t.Fatalf("commands=%d object reads=%d, want one submit plus initial and confirmation reads", commands, objectReads)
	}
}

func TestWebSSRUpdateRejectsWrongReceiptVersionWithoutRedirect(t *testing.T) {
	for _, receiptVersion := range []int64{4, 6} {
		t.Run(strconv.FormatInt(receiptVersion, 10), func(t *testing.T) {
			var objectReads int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/command-surface"):
					_, _ = io.WriteString(w, `{"nouns":[{"key":"tasks","singular":"Task","plural":"Tasks","field_schema":{"properties":{},"required":[]}}]}`)
				case r.Method == http.MethodGet:
					objectReads++
					_, _ = io.WriteString(w, `{"data":{"id":"obj_1","title":"Old","object_type_key":"tasks","version":4,"values":{}}}`)
				case r.Method == http.MethodPost:
					var command commandEnvelope
					_ = json.NewDecoder(r.Body).Decode(&command)
					_ = json.NewEncoder(w).Encode(map[string]any{"command_id": command.CommandID, "outcome": "applied", "workspace_id": command.WorkspaceID, "aggregate": map[string]any{"type": "object_instance", "id": "obj_1", "version": receiptVersion}})
				}
			}))
			defer backend.Close()
			app := webSSR{deps: runtimeDeps{client: backend.Client(), now: func() time.Time { return time.Unix(0, 0).UTC() }, newID: fixedIDGenerator(), stdout: io.Discard, stderr: io.Discard}, session: cliSession{Server: backend.URL, WorkspaceID: "wks_1", PrincipalID: "usr_1"}, journal: ledger.NewMemoryJournal(), writes: true, csrf: "csrf"}
			request := httptest.NewRequest(http.MethodPost, "/objects/tasks/obj_1", strings.NewReader("_csrf=csrf&_compos_form_mode=native&%2Ftitle=Updated"))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			app.serveHTTP(response, request)
			if response.Code != http.StatusBadGateway || response.Header().Get("Location") != "" || objectReads != 1 {
				t.Fatalf("receipt version %d = status %d location %q reads %d", receiptVersion, response.Code, response.Header().Get("Location"), objectReads)
			}
		})
	}
}
