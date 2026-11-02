package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "compos-cli-test-*")
	if err != nil {
		panic(err)
	}
	path := filepath.Join(dir, "auth.json")
	body := []byte(`{"server":"http://127.0.0.1:7878","workspace_id":"wks_1","principal_id":"usr_1","credential":"usr_1","created_at":"1970-01-01T00:00:00Z"}`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}
	if err := os.Setenv("COMPOS_SESSION_PATH", path); err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func testSessionPath(t *testing.T, principal string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auth.json")
	session := cliSession{
		Server:      "http://127.0.0.1:7878",
		WorkspaceID: "wks_1",
		PrincipalID: principal,
		Credential:  principal,
	}
	body, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("session should marshal: %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("session should write: %v", err)
	}
	return path
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe should open: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()
	if err := w.Close(); err != nil {
		t.Fatalf("stdout pipe should close: %v", err)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("stdout should read: %v", err)
	}
	return string(body)
}

func TestDefaultJournal_usesLocalBboltJournalByDefault(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "ledger.db")
	t.Setenv("COMPOS_LEDGER_PATH", journalPath)

	journal, err := defaultJournal()
	if err != nil {
		t.Fatalf("default journal should open local ledger journal: %v", err)
	}
	defer journal.Close()
	if _, err := os.Stat(journalPath); err != nil {
		t.Fatalf("request ledger should exist: %v", err)
	}
}

func TestRun_rejects_legacy_top_level_product_nouns(t *testing.T) {
	for _, command := range []string{
		"project",
		"projects",
		"issue",
		"issues",
		"work-item",
		"work-items",
		"work_item",
		"work_items",
		"record",
		"records",
		"template",
		"templates",
		"activity",
		"activities",
		"status",
		"statuses",
	} {
		t.Run(command, func(t *testing.T) {
			if err := runWithTestDeps([]string{command}, ledger.NewMemoryJournal(), runtimeDeps{}); err == nil {
				t.Fatalf("expected legacy command %q to be rejected", command)
			}
		})
	}
}

func TestRun_rejectsForwarderRun(t *testing.T) {
	err := runWithTestDeps([]string{"forwarder", "run"}, ledger.NewMemoryJournal(), runtimeDeps{})

	if err == nil || !strings.Contains(err.Error(), `unknown command "forwarder"`) {
		t.Fatalf("expected removed forwarder command to be rejected, got %v", err)
	}
}

func TestRun_rejectsRemovedTaskCommands(t *testing.T) {
	for _, command := range []string{"task", "submission"} {
		err := runWithTestDeps([]string{command, "create"}, ledger.NewMemoryJournal(), runtimeDeps{})
		if err == nil || !strings.Contains(err.Error(), `unknown command "`+command+`"`) {
			t.Fatalf("expected removed %q command to be rejected, got %v", command, err)
		}
	}
}

func TestObjectUsage_includesUpdateAndMoveCommands(t *testing.T) {
	err := runWithTestDeps([]string{"object"}, ledger.NewMemoryJournal(), runtimeDeps{})

	if err == nil || !strings.Contains(err.Error(), "usage: compos object <create|update|move|get>") {
		t.Fatalf("expected object usage with update and move, got %v", err)
	}
}

func TestWorkspaceUsage_includesCreateCommand(t *testing.T) {
	err := runWithTestDeps([]string{"workspace"}, ledger.NewMemoryJournal(), runtimeDeps{})

	if err == nil || !strings.Contains(err.Error(), "usage: compos workspace <create>") {
		t.Fatalf("expected workspace usage with create, got %v", err)
	}
}

func TestWorkspaceHelp_includesCreateCommand(t *testing.T) {
	var out bytes.Buffer

	if err := runWithTestDeps([]string{"help", "workspace"}, ledger.NewMemoryJournal(), runtimeDeps{stdout: &out}); err != nil {
		t.Fatalf("workspace help should succeed: %v", err)
	}
	if !strings.Contains(out.String(), "workspace create [--server URL] --name NAME [--key KEY] [--idempotency-key KEY]") {
		t.Fatalf("workspace help should document create, got %q", out.String())
	}
}

func TestVersionCommandUsesBuildInjectedVersion(t *testing.T) {
	original := version
	version = "9.9.9-test"
	t.Cleanup(func() { version = original })

	var out bytes.Buffer
	err := runWithTestDeps([]string{"version"}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: &out,
	})

	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "9.9.9-test" {
		t.Fatalf("expected injected version, got %q", got)
	}
}

func TestCommandNeedsLedger_onlyForCommandMutations(t *testing.T) {
	nonMutatingCommands := [][]string{
		{},
		{"version"},
		{"login"},
		{"logout"},
		{"whoami"},
		{"web"},
		{"help", "contracts"},
		{"help", "workspace"},
		{"workspace", "create"},
		{"objects", "list"},
		{"object", "get"},
		{"relation"},
		{"contracts", "help"},
		{"contracts", "get"},
		{"ledger", "retry", "--journal", "/tmp/compos-ledger.db"},
		{"audit", "retry"},
		{"ledger", "export"},
		{"ledger", "sync"},
	}
	for _, args := range nonMutatingCommands {
		if commandNeedsLedger(args) {
			t.Fatalf("non-mutating command should not open request ledger: %#v", args)
		}
	}

	mutationCommands := [][]string{
		{"define", "--key", "contracts", "--singular", "contract", "--plural", "contracts"},
		{"object-type", "define"},
		{"object", "create"},
		{"object", "update"},
		{"object", "move"},
		{"contracts", "create"},
		{"contracts", "move"},
		{"contracts", "link"},
		{"relation", "define"},
		{"relation", "link"},
		{"relation", "unlink"},
		{"ledger", "retry"},
	}
	for _, args := range mutationCommands {
		if !commandNeedsLedger(args) {
			t.Fatalf("mutation command should open request ledger: %#v", args)
		}
	}
}

func TestUsage_documents_operational_flags(t *testing.T) {
	text := usage()
	for _, want := range []string{
		"web [--listen 127.0.0.1:8787] [--no-open] [--server URL] [--workspace W]",
		"define [--workspace W] --key K --singular S --plural P",
		"PLURAL_KEY create",
		"PLURAL_KEY move",
		"PLURAL_KEY link",
		"workspace create",
		"object update",
		"--state-machine-id",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("usage should document %q:\n%s", want, text)
		}
	}
	for _, denied := range []string{
		"automation-rule define",
		"capability grant",
		"capability list",
		"task create",
		"submission create",
	} {
		if strings.Contains(text, denied) {
			t.Fatalf("usage must not expose non-user CLI command %q:\n%s", denied, text)
		}
	}
	if strings.Contains(text, "--token") {
		t.Fatalf("usage must not expose removed --token flag:\n%s", text)
	}
}

func TestWebDashboard_returnsInertiaPage(t *testing.T) {
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: `{"workspace_id":"wks_1","nouns":[{"key":"contract","kind":"object_type","object_type_id":"otp_1","version":1,"commands":["create"],"field_schema":{},"state_machine_id":null,"relations":[]}]}`},
			{status: http.StatusOK, body: `{"data":[{"id":"obj_1","workspace_id":"wks_1","object_type_key":"contract","title":"Acme","values":{},"state_key":null,"version":1}],"consistency":{"projection_sequence":1}}`},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Inertia", "true")
	rec := httptest.NewRecorder()

	renderDashboardPage(rec, req, runtimeDeps{client: client}, cliSession{
		Server:      "http://server",
		WorkspaceID: "wks_1",
		PrincipalID: "usr_1",
		Credential:  "csn_1",
	}, webManifestEntry{File: "assets/app.js"})

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if rec.Header().Get("X-Inertia") != "true" {
		t.Fatalf("expected Inertia response header")
	}
	var page inertiaPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("response should be JSON page: %v", err)
	}
	if page.Component != "Dashboard" {
		t.Fatalf("unexpected component: %q", page.Component)
	}
	surface := page.Props["commandSurface"].(map[string]any)
	if surface["workspace_id"] != "wks_1" {
		t.Fatalf("unexpected command surface: %#v", surface)
	}
}

func TestWebGetJSON_truncatesNonSuccessResponseBodyInError(t *testing.T) {
	body := strings.Repeat("x", webMaxErrorMessageBytes+200)
	_, err := webGetJSON(context.Background(), runtimeDeps{
		client: &fakeHTTPClient{
			status: http.StatusInternalServerError,
			body:   body,
		},
	}, cliSession{
		Server:      "http://server",
		WorkspaceID: "wks_1",
		PrincipalID: "usr_1",
		Credential:  "csn_1",
	}, "http://server/api/v1/workspaces/wks_1/objects")

	if err == nil {
		t.Fatal("expected server read error")
	}
	message := err.Error()
	if strings.Contains(message, strings.Repeat("x", webMaxErrorMessageBytes+1)) {
		t.Fatalf("error should not include the full response body, got %d bytes", len(message))
	}
	if !strings.Contains(message, "truncated") {
		t.Fatalf("error should indicate truncation, got %q", message)
	}
}

func TestWebHandler_rejectsMutationRoutes(t *testing.T) {
	handler := newWebHandler(runtimeDeps{}, cliSession{}, webManifestEntry{File: "assets/app.js"}, fstest.MapFS{})

	for _, path := range []string{"/objects", "/object-types"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Host = "127.0.0.1"
		req.RemoteAddr = "127.0.0.1:12345"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST %s should be unavailable in read-only web console, got %d", path, rec.Code)
		}
	}
}

func TestWebHandler_rejectsNonLocalHostHeader(t *testing.T) {
	handler := newWebHandler(runtimeDeps{}, cliSession{}, webManifestEntry{File: "assets/app.js"}, fstest.MapFS{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "attacker.example"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("unexpected status for non-local host: %d", rec.Code)
	}
}

func TestLocalHostOnlyHandler_rejectsNonLocalHostBeforeNextHandler(t *testing.T) {
	called := false
	handler := localHostOnlyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/callback", nil)
	req.Host = "attacker.example"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("unexpected status for non-local host: %d", rec.Code)
	}
	if called {
		t.Fatal("next handler should not be called for non-local host")
	}
}

func TestLocalHostOnlyHandler_rejectsNonLoopbackRemoteWithLocalHost(t *testing.T) {
	called := false
	handler := localHostOnlyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "127.0.0.1"
	req.RemoteAddr = "203.0.113.10:45678"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("unexpected status for non-loopback remote: %d", rec.Code)
	}
	if called {
		t.Fatal("next handler should not be called for non-loopback remote")
	}
}

func TestWebHandler_allowsLocalHostHeader(t *testing.T) {
	handler := newWebHandler(runtimeDeps{}, cliSession{}, webManifestEntry{File: "assets/app.js"}, fstest.MapFS{})
	req := httptest.NewRequest(http.MethodPost, "/objects", strings.NewReader(`{}`))
	req.Host = "LocalHost:8788"
	req.RemoteAddr = "[::1]:12345"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("local host should reach read-only route handling, got %d", rec.Code)
	}
}

func TestWebHandler_setsSecurityHeaders(t *testing.T) {
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: `{"workspace_id":"wks_1","nouns":[]}`},
			{status: http.StatusOK, body: `{"data":[],"consistency":{"projection_sequence":1}}`},
		},
	}
	handler := newWebHandler(runtimeDeps{client: client}, cliSession{
		Server:      "http://server",
		WorkspaceID: "wks_1",
		PrincipalID: "usr_1",
		Credential:  "csn_1",
	}, webManifestEntry{File: "assets/app.js"}, fstest.MapFS{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "localhost:8788"
	req.RemoteAddr = "127.0.0.1:12345"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("missing X-Frame-Options header")
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("missing X-Content-Type-Options header")
	}
}

func TestCredentiallessSession_omitsAuthorizationHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://server", nil)
	setBearerCredential(req, "")
	if got := req.Header.Get("Authorization"); got != "" {
		t.Fatalf("credential-less session must omit Authorization, got %q", got)
	}

	sessionPath := filepath.Join(t.TempDir(), "auth.json")
	if err := writeTestIdentity(sessionPath, cliSession{
		Server: "http://server", WorkspaceID: "wks_default", PrincipalID: "dev",
	}); err != nil {
		t.Fatalf("credential-less development session should save: %v", err)
	}
}

func TestRemovedTokenFlag_failsBeforeHttp(t *testing.T) {
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}

	err := runWithTestDeps([]string{
		"object", "create",
		"--workspace", "wks_1",
		"--token", "usr_1",
		"--type", "contract",
		"--title", "Acme",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
	})
	if err == nil || !strings.Contains(err.Error(), "--token has been removed") {
		t.Fatalf("expected removed token error, got %v", err)
	}
	if client.calls != 0 {
		t.Fatalf("removed token flag must fail before HTTP, got %d calls", client.calls)
	}
}

func TestCommandsRejectMissingRequiredFlagsBeforeSubmit(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "object create type",
			args: []string{"object", "create", "--title", "Acme"},
			want: "type is required",
		},
		{
			name: "object create title",
			args: []string{"object", "create", "--type", "contract"},
			want: "title is required",
		},
		{
			name: "workspace create name",
			args: []string{"workspace", "create"},
			want: "name is required",
		},
		{
			name: "object update id",
			args: []string{"object", "update", "--version", "1", "--title", "Acme"},
			want: "id is required",
		},
		{
			name: "object update version",
			args: []string{"object", "update", "--id", "obj_1", "--title", "Acme"},
			want: "version is required",
		},
		{
			name: "object update positive version",
			args: []string{"object", "update", "--id", "obj_1", "--version", "0", "--title", "Acme"},
			want: "version must be greater than zero",
		},
		{
			name: "object update mutation",
			args: []string{"object", "update", "--id", "obj_1", "--version", "1"},
			want: "at least one of title or values is required",
		},
		{
			name: "object update values",
			args: []string{"object", "update", "--id", "obj_1", "--version", "1", "--values", "[]"},
			want: "values must be JSON object",
		},
		{
			name: "object move id",
			args: []string{"object", "move", "--to-state", "ready"},
			want: "id is required",
		},
		{
			name: "object move to state",
			args: []string{"object", "move", "--id", "obj_1"},
			want: "to-state is required",
		},
		{
			name: "object get id",
			args: []string{"object", "get"},
			want: "id is required",
		},
		{
			name: "object type define key",
			args: []string{"object-type", "define"},
			want: "key is required",
		},
		{
			name: "object type define singular",
			args: []string{"object-type", "define", "--key", "contracts"},
			want: "singular is required",
		},
		{
			name: "object type define plural",
			args: []string{"object-type", "define", "--key", "contracts", "--singular", "contract"},
			want: "plural is required",
		},
		{
			name: "relation define key",
			args: []string{"relation", "define", "--from-type", "otp_customer", "--to-type", "otp_contract", "--cardinality", "one_to_many"},
			want: "key is required",
		},
		{
			name: "relation define from type",
			args: []string{"relation", "define", "--key", "customer_contracts", "--to-type", "otp_contract", "--cardinality", "one_to_many"},
			want: "from-type is required",
		},
		{
			name: "relation define to type",
			args: []string{"relation", "define", "--key", "customer_contracts", "--from-type", "otp_customer", "--cardinality", "one_to_many"},
			want: "to-type is required",
		},
		{
			name: "relation define cardinality",
			args: []string{"relation", "define", "--key", "customer_contracts", "--from-type", "otp_customer", "--to-type", "otp_contract"},
			want: "cardinality is required",
		},
		{
			name: "relation define cardinality enum",
			args: []string{"relation", "define", "--key", "customer_contracts", "--from-type", "otp_customer", "--to-type", "otp_contract", "--cardinality", "many"},
			want: `cardinality must be "one_to_one", "one_to_many", "many_to_one", or "many_to_many"`,
		},
		{
			name: "relation link definition",
			args: []string{"relation", "link", "--from", "obj_customer", "--to", "obj_contract"},
			want: "definition is required",
		},
		{
			name: "relation link from",
			args: []string{"relation", "link", "--definition", "rdf_customer_contracts", "--to", "obj_contract"},
			want: "from is required",
		},
		{
			name: "relation link to",
			args: []string{"relation", "link", "--definition", "rdf_customer_contracts", "--from", "obj_customer"},
			want: "to is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			journal := ledger.NewMemoryJournal()
			client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}
			err := runWithTestDeps(tc.args, journal, runtimeDeps{
				stdout: io.Discard,
				stderr: io.Discard,
				client: client,
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
			if client.calls != 0 {
				t.Fatalf("invalid command should not call HTTP, got %d calls", client.calls)
			}
			if len(journal.Events()) != 0 {
				t.Fatalf("invalid command should not append ledger events, got %d", len(journal.Events()))
			}
		})
	}
}

func TestWorkspaceCreate_postsControlPlaneRequestWithStableIdempotencyKey(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusCreated,
		body:   `{"data":{"id":"wks_new","workspace_id":"wks_new","key":"acme-research","name":"Acme Research","creator_principal_id":"usr_1","created":true,"status":"created"}}`,
	}
	var stdout bytes.Buffer

	err := runWithTestDeps([]string{
		"workspace", "create",
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", "acme-research",
	}, journal, runtimeDeps{
		stdout: &stdout,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID: func(prefix string) string {
			t.Fatalf("workspace create should not call newID(%q)", prefix)
			return ""
		},
	})
	if err != nil {
		t.Fatalf("workspace create should succeed: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != client.body {
		t.Fatalf("workspace create should print raw response, got %q", got)
	}
	if client.calls != 1 {
		t.Fatalf("expected one HTTP call, got %d", client.calls)
	}
	req := client.requests[0]
	if req.Method != http.MethodPost || req.URL.Path != "/api/v1/workspaces" {
		t.Fatalf("workspace create must POST control-plane endpoint, got %s %s", req.Method, req.URL.Path)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer usr_1" {
		t.Fatalf("workspace create should use bearer auth, got %q", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("workspace create should send JSON, got %q", got)
	}
	wantBody := `{"name":"Acme Research","key":"acme-research","idempotency_key":"idem_a75f4a5b1b12df1b8277fef4f568a7492f440d7e1e0af40d298220e838ea8254"}`
	if got := string(client.bodies[0]); got != wantBody {
		t.Fatalf("unexpected exact workspace create body:\ngot  %s\nwant %s", got, wantBody)
	}
	var payload map[string]any
	if err := json.Unmarshal(client.bodies[0], &payload); err != nil {
		t.Fatalf("workspace create body should be JSON: %v", err)
	}
	wantPayload := map[string]any{
		"name":            "Acme Research",
		"key":             "acme-research",
		"idempotency_key": "idem_a75f4a5b1b12df1b8277fef4f568a7492f440d7e1e0af40d298220e838ea8254",
	}
	if !reflect.DeepEqual(payload, wantPayload) {
		t.Fatalf("unexpected workspace create payload: %#v", payload)
	}
	if len(journal.Events()) != 0 {
		t.Fatalf("workspace create must not use command request ledger, got %d events", len(journal.Events()))
	}
}

func TestWorkspaceCreate_derivesSameIdempotencyKeyForSameCanonicalIntent(t *testing.T) {
	first := workspaceCreateIdempotencyKeyForArgs(t,
		"--server", "http://server",
		"--name", " Acme Research ",
		"--key", " Acme-Research ",
	)
	second := workspaceCreateIdempotencyKeyForArgs(t,
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", "acme-research",
	)

	if first != second {
		t.Fatalf("same canonical workspace create intent should reuse idempotency key: %q != %q", first, second)
	}
}

func TestWorkspaceCreate_caseOnlyKeyRetryConvergesToSameCanonicalRequest(t *testing.T) {
	requestBody := func(args ...string) []byte {
		t.Helper()

		client := &fakeHTTPClient{status: http.StatusCreated, body: `{}`}
		deps := cliRuntimeDeps(client)
		deps.newID = func(prefix string) string {
			t.Fatalf("workspace create should not call newID(%q)", prefix)
			return ""
		}
		commandArgs := append([]string{"workspace", "create"}, args...)
		if err := runWithTestDeps(commandArgs, ledger.NewMemoryJournal(), deps); err != nil {
			t.Fatalf("workspace create should succeed: %v", err)
		}
		if len(client.bodies) != 1 {
			t.Fatalf("expected one workspace create body, got %d", len(client.bodies))
		}
		return client.bodies[0]
	}

	first := requestBody(
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", " Acme-Research ",
	)
	second := requestBody(
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", "acme-research",
	)

	if string(first) != string(second) {
		t.Fatalf("case-only workspace key retry should converge:\nfirst  %s\nsecond %s", first, second)
	}
	var payload map[string]any
	if err := json.Unmarshal(first, &payload); err != nil {
		t.Fatalf("workspace create body should be JSON: %v", err)
	}
	if payload["key"] != "acme-research" {
		t.Fatalf("workspace key should be sent canonicalized, got %#v", payload)
	}
}

func TestWorkspaceCreate_derivesDifferentIdempotencyKeyForDifferentIntent(t *testing.T) {
	first := workspaceCreateIdempotencyKeyForArgs(t,
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", "acme-research",
	)
	second := workspaceCreateIdempotencyKeyForArgs(t,
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", "beta-research",
	)

	if first == second {
		t.Fatalf("different workspace create intents should not reuse idempotency key %q", first)
	}
}

func TestWorkspaceCreate_usesExplicitIdempotencyKeyAndOmitsEmptyKey(t *testing.T) {
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"data":{"id":"wks_new","workspace_id":"wks_new","key":"acme-research","name":"Acme Research","creator_principal_id":"usr_1","created":false,"status":"duplicate"}}`,
	}

	err := runWithTestDeps([]string{
		"workspace", "create",
		"--server", "http://server",
		"--name", "Acme Research",
		"--key", " ",
		"--idempotency-key", "idem-stable",
	}, ledger.NewMemoryJournal(), cliRuntimeDeps(client))
	if err != nil {
		t.Fatalf("workspace create should succeed: %v", err)
	}
	wantBody := `{"name":"Acme Research","idempotency_key":"idem-stable"}`
	if got := string(client.bodies[0]); got != wantBody {
		t.Fatalf("unexpected exact workspace create body:\ngot  %s\nwant %s", got, wantBody)
	}
	var payload map[string]any
	if err := json.Unmarshal(client.bodies[0], &payload); err != nil {
		t.Fatalf("workspace create body should be JSON: %v", err)
	}
	if payload["idempotency_key"] != "idem-stable" {
		t.Fatalf("expected explicit idempotency key, got %#v", payload)
	}
	if _, ok := payload["key"]; ok {
		t.Fatalf("empty optional key should be omitted, got %#v", payload)
	}
}

func TestWorkspaceCreate_requiresLoginSessionBeforeHttp(t *testing.T) {
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}

	err := runWithTestDeps([]string{
		"workspace", "create",
		"--name", "Acme Research",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout:           io.Discard,
		stderr:           io.Discard,
		client:           client,
		identityProvider: testIdentityProvider(filepath.Join(t.TempDir(), "missing-auth.json")),
		newID:            fixedIDGenerator(),
	})
	if err == nil || !strings.Contains(err.Error(), "external identity integration unavailable") {
		t.Fatalf("expected login error, got %v", err)
	}
	if client.calls != 0 {
		t.Fatalf("missing login should not call HTTP, got %d calls", client.calls)
	}
}

func TestWorkspaceCreate_explicitServerRequiresExternalIdentity(t *testing.T) {
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}
	err := runWithTestDeps([]string{
		"workspace", "create", "--server", "http://server", "--name", "Acme Research",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: io.Discard, stderr: io.Discard, client: client,
		identityProvider: testIdentityProvider(filepath.Join(t.TempDir(), "missing-auth.json")),
	})
	if err == nil {
		t.Fatal("explicit server bypassed external identity")
	}
	if client.calls != 0 {
		t.Fatalf("expected no HTTP calls, got %d", client.calls)
	}
}

func TestWorkspaceCreate_reportsServerErrorBody(t *testing.T) {
	client := &fakeHTTPClient{
		status: http.StatusConflict,
		body:   `{"error":{"code":"workspace_key_conflict","message":"workspace key is already in use"}}`,
	}

	err := runWithTestDeps([]string{
		"workspace", "create",
		"--server", "http://server",
		"--name", "Acme Research",
		"--idempotency-key", "idem-stable",
	}, ledger.NewMemoryJournal(), cliRuntimeDeps(client))
	if err == nil {
		t.Fatal("expected workspace create error")
	}
	want := `workspace create failed: HTTP 409: {"error":{"code":"workspace_key_conflict","message":"workspace key is already in use"}}`
	if err.Error() != want {
		t.Fatalf("unexpected error:\ngot  %q\nwant %q", err.Error(), want)
	}
}

func TestObjectUpdate_submitsCanonicalCommandWithOnlySuppliedMutations(t *testing.T) {
	tests := []struct {
		name        string
		mutationArg []string
		wantPayload map[string]any
	}{
		{
			name:        "title only",
			mutationArg: []string{"--title", "Renamed"},
			wantPayload: map[string]any{"title": "Renamed"},
		},
		{
			name:        "values only",
			mutationArg: []string{"--values", `{"amount":200}`},
			wantPayload: map[string]any{"values": map[string]any{"amount": float64(200)}},
		},
		{
			name:        "title and values",
			mutationArg: []string{"--title", "Renamed", "--values", `{"amount":200,"status":"active"}`},
			wantPayload: map[string]any{
				"title":  "Renamed",
				"values": map[string]any{"amount": float64(200), "status": "active"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			journal := ledger.NewMemoryJournal()
			client := &fakeHTTPClient{
				status: http.StatusOK,
				body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":4},"projection":{"workspace_id":"wks_1","projection_sequence":4}}`,
			}
			args := []string{
				"object", "update",
				"--server", "http://server",
				"--workspace", "wks_1",
				"--id", "obj_1",
				"--version", "3",
			}
			args = append(args, tt.mutationArg...)

			if err := runWithTestDeps(args, journal, cliRuntimeDeps(client)); err != nil {
				t.Fatalf("object update should succeed: %v", err)
			}
			command := ingestedCommandFromBody(t, client.bodies[0])
			if command["command_type"] != "object.update" {
				t.Fatalf("unexpected command type: %#v", command["command_type"])
			}
			target := command["target"].(map[string]any)
			if target["aggregate_type"] != "object_instance" || target["aggregate_id"] != "obj_1" {
				t.Fatalf("unexpected target: %#v", target)
			}
			preconditions := command["preconditions"].([]any)
			if len(preconditions) != 1 {
				t.Fatalf("unexpected preconditions: %#v", preconditions)
			}
			precondition := preconditions[0].(map[string]any)
			if precondition["aggregate_type"] != "object_instance" || precondition["aggregate_id"] != "obj_1" || precondition["expected_version"] != float64(3) {
				t.Fatalf("unexpected precondition: %#v", precondition)
			}
			payload := command["payload"].(map[string]any)
			if !reflect.DeepEqual(payload, tt.wantPayload) {
				t.Fatalf("unexpected payload: %#v", payload)
			}
		})
	}
}

func TestObjectCreate_requiresLoginSessionBeforeLedgerOrHttp(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}

	err := runWithTestDeps([]string{
		"object", "create",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout:           io.Discard,
		stderr:           io.Discard,
		client:           client,
		identityProvider: testIdentityProvider(filepath.Join(t.TempDir(), "missing-auth.json")),
	})
	if err == nil || !strings.Contains(err.Error(), "external identity integration unavailable") {
		t.Fatalf("expected login error, got %v", err)
	}
	if len(journal.Events()) != 0 {
		t.Fatalf("missing login should not append ledger events, got %d", len(journal.Events()))
	}
	if client.calls != 0 {
		t.Fatalf("missing login should not call HTTP, got %d calls", client.calls)
	}
}

func TestObjectCreate_appends_ledger_before_http_submit(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}
	deps := cliRuntimeDeps(client)
	identityPath := filepath.Join(t.TempDir(), "auth.json")
	deps.identityProvider = testIdentityProvider(identityPath)
	if err := writeTestIdentity(identityPath, cliSession{Server: "HTTP://SERVER/", WorkspaceID: "wks_1", PrincipalID: "usr_1", Credential: "usr_1"}); err != nil {
		t.Fatal(err)
	}
	deps.sleep = noSleep
	err := runWithTestDeps([]string{
		"object", "create",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
		"--values", `{"amount":100}`,
	}, journal, deps)
	if err != nil {
		t.Fatalf("object create should succeed: %v", err)
	}

	events := journal.Events()
	if len(events) != 3 {
		t.Fatalf("expected appended, submitted, committed events, got %d", len(events))
	}
	if events[0].Kind != ledger.EventAttemptAppended {
		t.Fatalf("first event should be attempt_appended, got %q", events[0].Kind)
	}
	if events[0].Appended == nil {
		t.Fatal("first event should include append payload")
	}
	if events[0].Appended.Server != "http://server" || events[0].Appended.PrincipalID != "usr_1" {
		t.Fatalf("ledger attempt scope = server %q principal %q", events[0].Appended.Server, events[0].Appended.PrincipalID)
	}
	if len(events[0].Appended.CommandIntent) == 0 {
		t.Fatal("first local ledger event should include command intent for server submit")
	}
	if got := events[0].Appended.ActorContextHash; !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("actor context should be stored as a sha256 hash, got %q", got)
	}
	if events[0].Appended.ActorContextHash == "usr_1" || strings.Contains(events[0].Appended.ActorContextHash, "usr_1") {
		t.Fatalf("actor context hash must not store the raw token: %q", events[0].Appended.ActorContextHash)
	}
	if client.calls != 1 {
		t.Fatalf("expected one HTTP call, got %d", client.calls)
	}
	if got := client.requests[0].URL.Path; got != "/api/v1/kernel/commands" {
		t.Fatalf("command submit must use Kernel endpoint, got %q", got)
	}
	var command map[string]any
	if err := json.Unmarshal(client.bodies[0], &command); err != nil {
		t.Fatalf("Kernel body should be JSON: %v", err)
	}
	expectedActorContextHash, err := jsonHash(map[string]any{
		"workspace_id":               "wks_1",
		"authenticated_principal_id": "usr_1",
		"requested_actor":            command["requested_actor"],
		"causation_source":           "cli",
	})
	if err != nil {
		t.Fatalf("expected actor context hash should compute: %v", err)
	}
	if events[0].Appended.ActorContextHash != expectedActorContextHash {
		t.Fatalf("actor context hash should match server contract, got %q want %q", events[0].Appended.ActorContextHash, expectedActorContextHash)
	}
	if command["command_type"] != "object.create" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
}

func TestObjectCreate_rejectsDifferentUserActorBeforeLedgerOrKernelSubmit(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_1","outcome":"applied","projection":{"workspace_id":"wks_2","projection_sequence":1}}`,
	}
	deps := cliRuntimeDeps(client)
	deps.identityProvider = testIdentityProvider(testSessionPath(t, "usr_1"))

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--actor", "usr_2",
		"--type", "contract",
		"--title", "Acme",
	}, journal, deps)
	if err == nil || !strings.Contains(err.Error(), `requested user actor "usr_2" cannot be submitted with session principal "usr_1"`) {
		t.Fatalf("expected actor mismatch error, got %v", err)
	}
	if len(journal.Events()) != 0 {
		t.Fatalf("actor mismatch should not append ledger events, got %d", len(journal.Events()))
	}
	if client.calls != 0 {
		t.Fatalf("actor mismatch must not submit to Kernel, got %d calls", client.calls)
	}
}

func TestObjectCreate_submitsKernelCommandAndRecordsLocalAttempt(t *testing.T) {
	journal := &capturingJournal{}
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}
	deps := cliRuntimeDeps(client)

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, deps)
	if err != nil {
		t.Fatalf("object create should succeed: %v", err)
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "object.create" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
}

func TestObjectCreate_sendsExplicitIdempotencyKeyForServerDeduping(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: `{"command_id":"cmd_1","outcome":"applied","projection":{"workspace_id":"wks_1"}}`},
			{status: http.StatusOK, body: `{"command_id":"cmd_3","outcome":"duplicate","projection":{"workspace_id":"wks_1"}}`},
		},
	}
	idGenerator := fixedIDGenerator()

	for i := 0; i < 2; i++ {
		err := runWithTestDeps([]string{
			"object", "create",
			"--server", "http://server",
			"--workspace", "wks_1",
			"--idempotency-key", "idem-stable",
			"--type", "contract",
			"--title", "Acme",
		}, journal, runtimeDeps{
			stdout: io.Discard,
			stderr: io.Discard,
			client: client,
			now:    func() time.Time { return time.Unix(10, 0).UTC() },
			newID:  idGenerator,
		})
		if err != nil {
			t.Fatalf("object create attempt %d should succeed: %v", i+1, err)
		}
	}
	if client.calls != 2 {
		t.Fatalf("expected two Kernel submits, got %d", client.calls)
	}
	first := ingestedCommandFromBody(t, client.bodies[0])
	second := ingestedCommandFromBody(t, client.bodies[1])
	if first["idempotency_key"] != "idem-stable" || second["idempotency_key"] != "idem-stable" {
		t.Fatalf("expected explicit idempotency key in both requests: %#v %#v", first["idempotency_key"], second["idempotency_key"])
	}
	if first["command_id"] == "" || second["command_id"] == "" || first["command_id"] == second["command_id"] {
		t.Fatalf("command_id should be present and independent from idempotency dedupe: %#v %#v", first["command_id"], second["command_id"])
	}
}
func TestObjectCreate_appends_rejected_when_response_body_read_fails(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status:  http.StatusOK,
		readErr: errors.New("read failed"),
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})

	if err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("expected read failure, got %v", err)
	}
	events := journal.Events()
	if len(events) != 5 {
		t.Fatalf("expected appended and two submitted/rejected attempts, got %d", len(events))
	}
	if events[2].Kind != ledger.EventAttemptRejected || events[4].Kind != ledger.EventAttemptRejected {
		t.Fatalf("expected both submit attempts to be rejected, got %q and %q", events[2].Kind, events[4].Kind)
	}
	if events[4].Rejected == nil || events[4].Rejected.Code != "read_error" {
		t.Fatalf("expected read_error rejection, got %#v", events[4].Rejected)
	}
	if client.calls != 2 {
		t.Fatalf("expected retry after read failure, got %d calls", client.calls)
	}
	if got := client.requests[1].Header.Get(idempotencyRetryHeader); got != string(events[0].AttemptID) {
		t.Fatalf("second request should carry retry attempt id %q, got %q", events[0].AttemptID, got)
	}
}

func TestObjectCreate_appendsRejectedWhenHTTPSubmitFails(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{err: errors.New("dial failed")}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
		sleep:  noSleep,
	})

	if err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("expected HTTP submit failure, got %v", err)
	}
	events := journal.Events()
	if len(events) != 5 {
		t.Fatalf("expected appended and two submitted/rejected attempts, got %d", len(events))
	}
	if events[4].Rejected == nil || events[4].Rejected.Code != "network_error" {
		t.Fatalf("expected network_error rejection, got %#v", events[4].Rejected)
	}
	if client.calls != 2 {
		t.Fatalf("expected retry after network failure, got %d calls", client.calls)
	}
	if got := client.requests[1].Header.Get(idempotencyRetryHeader); got != string(events[0].AttemptID) {
		t.Fatalf("second request should carry retry attempt id %q, got %q", events[0].AttemptID, got)
	}
}

func TestObjectCreate_retriesTransientServerErrorWithSameIntent(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusBadGateway, body: `temporary`},
			{status: http.StatusOK, body: `{"command_id":"cmd_2","outcome":"duplicate","projection":{"workspace_id":"wks_1","projection_sequence":1}}`},
		},
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
		sleep:  noSleep,
	})
	if err != nil {
		t.Fatalf("object create should retry and succeed: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("expected two HTTP calls, got %d", client.calls)
	}
	if !bytes.Equal(client.bodies[0], client.bodies[1]) {
		t.Fatalf("retry should resend the exact same command intent")
	}
	events := journal.Events()
	if len(events) != 5 {
		t.Fatalf("expected appended, rejected first attempt, and committed retry, got %d", len(events))
	}
	if events[2].Rejected == nil || events[2].Rejected.Code != "502" {
		t.Fatalf("expected first attempt to record 502, got %#v", events[2].Rejected)
	}
	if events[4].Committed == nil {
		t.Fatalf("expected retry to commit, got %#v", events[4])
	}
	if got := client.requests[1].Header.Get(idempotencyRetryHeader); got != string(events[0].AttemptID) {
		t.Fatalf("second request should carry retry attempt id %q, got %q", events[0].AttemptID, got)
	}
}

func TestObjectCreate_retriesRetryableConflictWithSameIntent(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusConflict, body: `{"error":{"retryable":true}}`},
			{status: http.StatusOK, body: `{"command_id":"cmd_2","outcome":"duplicate","projection":{"workspace_id":"wks_1","projection_sequence":1}}`},
		},
	}
	deps := cliRuntimeDeps(client)
	deps.sleep = noSleep

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, deps)
	if err != nil {
		t.Fatalf("object create should retry retryable conflict and succeed: %v", err)
	}
	if client.calls != 2 {
		t.Fatalf("expected two HTTP calls, got %d", client.calls)
	}
	if !bytes.Equal(client.bodies[0], client.bodies[1]) {
		t.Fatalf("retry should resend the exact same command intent")
	}
	events := journal.Events()
	if len(events) != 5 {
		t.Fatalf("expected appended, rejected first attempt, and committed retry, got %d", len(events))
	}
	if events[2].Rejected == nil || events[2].Rejected.Code != "409" {
		t.Fatalf("expected first attempt to record 409, got %#v", events[2].Rejected)
	}
	if events[4].Committed == nil {
		t.Fatalf("expected retry to commit, got %#v", events[4])
	}
	if got := client.requests[1].Header.Get(idempotencyRetryHeader); got != string(events[0].AttemptID) {
		t.Fatalf("second request should carry retry attempt id %q, got %q", events[0].AttemptID, got)
	}
}

func TestObjectCreate_doesNotRetryStaleConflict(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusConflict,
		body:   `{"error":{"code":"precondition_failed","retryable":false,"message":"stale"}}`,
	}
	deps := cliRuntimeDeps(client)
	deps.sleep = noSleep

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, deps)
	if err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("expected stale conflict failure, got %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("stale conflict should not retry, got %d calls", client.calls)
	}
	events := journal.Events()
	if len(events) != 3 {
		t.Fatalf("expected appended, submitted, rejected, got %d events", len(events))
	}
	if events[2].Rejected == nil || events[2].Rejected.Code != "409" {
		t.Fatalf("expected 409 rejection, got %#v", events[2].Rejected)
	}
}

func TestObjectCreate_truncatesRejectedResponseBodyInErrorAndLedger(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	body := strings.Repeat("x", maxHTTPErrorMessageBytes+200)
	client := &fakeHTTPClient{
		status: http.StatusBadGateway,
		body:   body,
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
		sleep:  noSleep,
	})

	if err == nil {
		t.Fatal("expected server rejection")
	}
	if strings.Contains(err.Error(), strings.Repeat("x", maxHTTPErrorMessageBytes+1)) {
		t.Fatalf("error should not include the full response body, got %d bytes", len(err.Error()))
	}
	events := journal.Events()
	if len(events) != 5 {
		t.Fatalf("expected appended and two submitted/rejected attempts, got %d", len(events))
	}
	rejected := events[4].Rejected
	if rejected == nil {
		t.Fatal("expected rejected ledger event")
	}
	if strings.Contains(rejected.Message, strings.Repeat("x", maxHTTPErrorMessageBytes+1)) {
		t.Fatalf("ledger rejection should not include the full response body, got %d bytes", len(rejected.Message))
	}
	if !strings.Contains(rejected.Message, "truncated") {
		t.Fatalf("ledger rejection should indicate truncation, got %q", rejected.Message)
	}
}

func TestObjectCreate_marksReceiptInvalidWhenSuccessfulReceiptCannotParse(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `not-json`,
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})

	if err == nil || !strings.Contains(err.Error(), "server receipt is invalid") {
		t.Fatalf("expected receipt parse failure, got %v", err)
	}
	events := journal.Events()
	if len(events) != 3 {
		t.Fatalf("expected appended, submitted, receipt-invalid events, got %d", len(events))
	}
	if events[2].Kind != ledger.EventAttemptRejected || events[2].Rejected == nil || events[2].Rejected.Code != "receipt_invalid" {
		t.Fatalf("third event should be receipt-invalid rejection, got %#v", events[2])
	}
	if client.calls != 1 {
		t.Fatalf("invalid receipt must not trigger automatic retry, got %d calls", client.calls)
	}
}

func TestObjectCreate_returnsReceiptWhenCommitLedgerAppendFailsAfterKernelSuccess(t *testing.T) {
	journal := &commitFailingJournal{err: errors.New("ledger disk full")}
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}
	var stdout strings.Builder
	var stderr strings.Builder

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: &stdout,
		stderr: &stderr,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})

	if err != nil {
		t.Fatalf("Kernel success should not become CLI failure after commit ledger append fails: %v", err)
	}
	if !strings.Contains(stdout.String(), `"command_id":"cmd_2"`) {
		t.Fatalf("expected server receipt on stdout, got %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "local ledger commit logging failed") {
		t.Fatalf("expected local ledger commit warning, got %s", stderr.String())
	}
}

func TestObjectCreate_does_not_submit_when_ledger_append_fails(t *testing.T) {
	journal := failingJournal{err: errors.New("disk full")}
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{}`,
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})

	if err == nil {
		t.Fatal("expected ledger append error")
	}
	if client.calls != 0 {
		t.Fatalf("HTTP must not be called when ledger append fails, got %d calls", client.calls)
	}
}

func TestObjectCreate_doesNotSubmitWhenLedgerSubmittedAppendFails(t *testing.T) {
	journal := &submittedFailingJournal{err: errors.New("submitted write failed")}
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{}`,
	}

	err := runWithTestDeps([]string{
		"object", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--type", "contract",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})

	if err == nil || !strings.Contains(err.Error(), "submitted write failed") {
		t.Fatalf("expected submitted ledger append error, got %v", err)
	}
	if client.calls != 0 {
		t.Fatalf("HTTP must not be called when submitted ledger append fails, got %d calls", client.calls)
	}
	if len(journal.events) != 1 || journal.events[0].Kind != ledger.EventAttemptAppended {
		t.Fatalf("expected only appended event before submitted append failure, got %#v", journal.events)
	}
}

func TestNewID_adds_random_suffix_for_non_command_ids(t *testing.T) {
	first := newID("idem")
	second := newID("idem")

	if first == second {
		t.Fatalf("expected generated IDs to differ, got %q", first)
	}
	if !strings.HasPrefix(first, "idem_") || len(strings.Split(first, "_")) != 3 {
		t.Fatalf("expected prefix, timestamp, and random suffix, got %q", first)
	}
}

func TestRemovedLedgerCommandsAndOldCLIAreUnknownWithoutWrites(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}
	for _, args := range [][]string{{"audit", "retry"}, {"ledger", "export"}, {"ledger", "sync"}, {"ledger", "retry", "--audit", "attempt_1"}} {
		err := runWithTestDeps(args, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client})
		if err == nil {
			t.Fatalf("%v should be unknown", args)
		}
		if len(journal.Events()) != 0 || client.calls != 0 {
			t.Fatalf("unknown command %v must not write: events=%d http=%d", args, len(journal.Events()), client.calls)
		}
	}
}

func TestLedgerRetryReplaysExplicitAttempt(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	commandIntent := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"dev"},"command_type":"object.create","target":{"object_type":"contract"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	appended := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "http://server", PrincipalID: "dev", WorkspaceID: "wks_1", CommandType: "object.create", CommandIntent: commandIntent, CreatedAt: time.Unix(10, 0).UTC()}
	fillLedgerIntentHashes(t, &appended)
	if _, err := journal.AppendAttempt(context.Background(), appended); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendSubmitted(context.Background(), ledger.AttemptSubmitted{AttemptID: "attempt_1", CommandID: "cmd_1", SubmittedAt: time.Unix(11, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendRejected(context.Background(), ledger.AttemptRejected{AttemptID: "attempt_1", Code: "network_error", Message: "dial failed", RejectedAt: time.Unix(12, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	client := &fakeHTTPClient{status: http.StatusOK, body: `{"command_id":"cmd_1","outcome":"duplicate","projection":{"workspace_id":"wks_1","projection_sequence":1}}`}
	var retry bytes.Buffer
	sessionPath := filepath.Join(t.TempDir(), "auth.json")
	if err := writeTestIdentity(sessionPath, cliSession{Server: "http://server", WorkspaceID: "wks_1", PrincipalID: "dev", Credential: "dev"}); err != nil {
		t.Fatal(err)
	}
	err := runWithTestDeps([]string{"ledger", "retry", "--attempt", "attempt_1", "--server", "http://server"}, journal, runtimeDeps{stdout: &retry, stderr: io.Discard, client: client, identityProvider: testIdentityProvider(sessionPath), now: func() time.Time { return time.Unix(13, 0).UTC() }, newID: fixedIDGenerator(), sleep: noSleep})
	if err != nil {
		t.Fatalf("ledger retry: %v", err)
	}
	if client.calls != 1 || client.requests[0].Header.Get(idempotencyRetryHeader) != "attempt_1" || !strings.Contains(retry.String(), `"retried":1`) {
		t.Fatalf("retry calls=%d header=%q output=%s", client.calls, client.requests[0].Header.Get(idempotencyRetryHeader), retry.String())
	}
	gotCommand := ingestedCommandFromBody(t, client.bodies[0])
	var wantCommand map[string]any
	if err := json.Unmarshal(commandIntent, &wantCommand); err != nil || !reflect.DeepEqual(gotCommand, wantCommand) {
		t.Fatalf("retry command=%#v want=%#v err=%v", gotCommand, wantCommand, err)
	}
}

func TestLedgerRetryReadsExplicitBboltLedger(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "ledger.db")
	journal, err := ledger.NewBboltJournal(journalPath)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	commandIntent := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"dev"},"command_type":"object.create","target":{"object_type":"contract"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	appended := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "http://server", PrincipalID: "dev", WorkspaceID: "wks_1", CommandType: "object.create", CommandIntent: commandIntent, CreatedAt: time.Unix(10, 0).UTC()}
	fillLedgerIntentHashes(t, &appended)
	if _, err := journal.AppendAttempt(context.Background(), appended); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendSubmitted(context.Background(), ledger.AttemptSubmitted{AttemptID: "attempt_1", CommandID: "cmd_1", SubmittedAt: time.Unix(11, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendRejected(context.Background(), ledger.AttemptRejected{AttemptID: "attempt_1", Code: "network_error", RejectedAt: time.Unix(12, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	client := &fakeHTTPClient{status: http.StatusOK, body: `{"command_id":"cmd_1","outcome":"duplicate","projection":{"workspace_id":"wks_1","projection_sequence":1}}`}
	sessionPath := filepath.Join(t.TempDir(), "auth.json")
	if err := writeTestIdentity(sessionPath, cliSession{Server: "http://server", WorkspaceID: "wks_1", PrincipalID: "dev", Credential: "dev"}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTestDeps([]string{"ledger", "retry", "--journal", journalPath, "--attempt", "attempt_1", "--server", "http://server"}, ledger.NewMemoryJournal(), runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client, identityProvider: testIdentityProvider(sessionPath), now: func() time.Time { return time.Unix(13, 0).UTC() }, newID: fixedIDGenerator(), sleep: noSleep}); err != nil {
		t.Fatalf("ledger retry explicit bbolt: %v", err)
	}
	if client.calls != 1 || client.requests[0].Header.Get(idempotencyRetryHeader) != "attempt_1" {
		t.Fatalf("retry calls=%d header=%q", client.calls, client.requests[0].Header.Get(idempotencyRetryHeader))
	}
}

func TestLedgerAttemptStatesPrefersRejectedOverSubmittedAtSameTime(t *testing.T) {
	at := time.Unix(10, 0).UTC()
	states := ledgerAttemptStates([]ledger.EventRecord{
		{Event: ledger.Event{Kind: ledger.EventAttemptRejected, AttemptID: "attempt_1", Rejected: &ledger.AttemptRejected{AttemptID: "attempt_1", Code: "400", RejectedAt: at}}},
		{Event: ledger.Event{Kind: ledger.EventAttemptSubmitted, AttemptID: "attempt_1", Submitted: &ledger.AttemptSubmitted{AttemptID: "attempt_1", CommandID: "cmd_1", SubmittedAt: at}}},
	})
	if got := states["attempt_1"].status; got != ledger.AttemptStatusRejected {
		t.Fatalf("same-time rejection should remain terminal over submitted, got %q", got)
	}
}

func TestCommittedEventFromReceiptParsesProjectionWorkspace(t *testing.T) {
	committed, err := committedEventFromReceipt("attempt_1", "cmd_1", "wks_1", []byte(`{"command_id":"cmd_1","outcome":"duplicate","aggregate":{"type":"object_instance","id":"obj_1","version":3},"projection":{"workspace_id":"wks_1","projection_sequence":7}}`), time.Unix(11, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if committed.WorkspaceID != "wks_1" || committed.ProjectionSequence == nil || *committed.ProjectionSequence != 7 || committed.AggregateType != "object_instance" || committed.AggregateID != "obj_1" || committed.AggregateVersion == nil || *committed.AggregateVersion != 3 {
		t.Fatalf("committed=%#v", committed)
	}
}

func TestObjectTypeDefine_syncs_ingested_command_intent_with_schema_payload(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_type","id":"otp_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}

	err := runWithTestDeps([]string{
		"object-type", "define",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--key", "contracts",
		"--singular", "contract",
		"--plural", "contracts",
		"--field-schema", `{"required":["amount"]}`,
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type define should succeed: %v", err)
	}
	if len(journal.Events()) != 3 {
		t.Fatalf("expected ledger lifecycle events, got %d", len(journal.Events()))
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "object_type.define" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "object_type" || target["aggregate_id"] != nil {
		t.Fatalf("unexpected target: %#v", target)
	}
	payload := command["payload"].(map[string]any)
	if payload["key"] != "contracts" || payload["singular"] != "contract" || payload["plural"] != "contracts" {
		t.Fatalf("unexpected noun payload: %#v", payload)
	}
	fieldSchema := payload["field_schema"].(map[string]any)
	if len(fieldSchema["required"].([]any)) != 1 {
		t.Fatalf("unexpected field schema: %#v", fieldSchema)
	}
}

func TestObjectTypeSchemaEvolveSubmitsExactCASCommand(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{status: http.StatusOK, body: `{"command_id":"cmd_1","outcome":"applied","aggregate":{"type":"object_type","id":"otp_1","version":2},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`}
	err := runWithTestDeps([]string{"object-type", "schema-evolve", "--server", "http://server", "--workspace", "wks_1", "--id", "otp_1", "--version", "1", "--field-schema", `{"type":"object"}`, "--idempotency-key", "stable"}, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client, now: func() time.Time { return time.Unix(10, 0).UTC() }, newID: fixedIDGenerator()})
	if err != nil {
		t.Fatalf("schema evolve: %v", err)
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "object_type.schema_evolve" {
		t.Fatalf("command=%#v", command)
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "object_type" || target["aggregate_id"] != "otp_1" {
		t.Fatalf("target=%#v", target)
	}
	preconditions := command["preconditions"].([]any)
	if len(preconditions) != 1 || preconditions[0].(map[string]any)["expected_version"] != float64(1) {
		t.Fatalf("preconditions=%#v", preconditions)
	}
	payload := command["payload"].(map[string]any)
	if len(payload) != 1 {
		t.Fatalf("payload=%#v", payload)
	}
}

func TestObjectTypeWordDefine_requires_and_emits_explicit_noun_fields(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_type","id":"otp_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}

	err := runWithTestDeps([]string{
		"define",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--key", "contracts",
		"--singular", "contract",
		"--plural", "contracts",
		"--field-schema", `{"required":["amount"]}`,
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type word define should succeed: %v", err)
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "object_type.define" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	payload := command["payload"].(map[string]any)
	if payload["key"] != "contracts" || payload["singular"] != "contract" || payload["plural"] != "contracts" {
		t.Fatalf("explicit noun fields should be preserved: %#v", payload)
	}
}

func TestObjectTypeWordCreate_submits_object_create_with_implicit_type(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
			{status: http.StatusOK, body: `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`},
		},
	}

	err := runWithTestDeps([]string{
		"contracts", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--title", "Acme",
		"--values", `{"amount":100}`,
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type word create should succeed: %v", err)
	}
	if client.requests[0].Method != http.MethodGet || client.requests[0].URL.Path != "/api/v1/workspaces/wks_1/command-surface" {
		t.Fatalf("expected command surface lookup first, got %s %s", client.requests[0].Method, client.requests[0].URL.Path)
	}
	command := ingestedCommandFromBody(t, client.bodies[1])
	if command["command_type"] != "object.create" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	payload := command["payload"].(map[string]any)
	if payload["object_type_key"] != "contracts" || payload["title"] != "Acme" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestObjectTypeWordMove_submits_object_move_command(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
			{status: http.StatusOK, body: `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_instance","id":"obj_1","version":2},"projection":{"workspace_id":"wks_1","projection_sequence":2}}`},
		},
	}

	err := runWithTestDeps([]string{
		"contracts", "move",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--id", "obj_1",
		"--version", "1",
		"--to-state", "review",
		"--force",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type word move should succeed: %v", err)
	}
	command := ingestedCommandFromBody(t, client.bodies[1])
	if command["command_type"] != "object.move" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "object_instance" || target["aggregate_id"] != "obj_1" {
		t.Fatalf("unexpected target: %#v", target)
	}
	payload := command["payload"].(map[string]any)
	if payload["to_state"] != "review" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	if payload["force"] != true {
		t.Fatalf("expected force payload, got %#v", payload)
	}
}

func TestObjectTypeWordLink_submits_relation_link_command(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
			{status: http.StatusOK, body: `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_relation","id":"rel_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`},
		},
	}

	err := runWithTestDeps([]string{
		"contracts", "link",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--definition", "rdf_customer_contracts",
		"--definition-version", "1",
		"--from", "obj_customer",
		"--from-version", "2",
		"--to", "obj_contract",
		"--to-version", "4",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type word link should succeed: %v", err)
	}
	command := ingestedCommandFromBody(t, client.bodies[1])
	if command["command_type"] != "relation.link" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	payload := command["payload"].(map[string]any)
	if payload["relation_definition_id"] != "rdf_customer_contracts" ||
		payload["from_object_id"] != "obj_customer" ||
		payload["to_object_id"] != "obj_contract" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestObjectTypeWordCreate_does_not_dispatch_by_singular_name(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
		},
	}

	err := runWithTestDeps([]string{
		"contract", "create",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--title", "Acme",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err == nil || !strings.Contains(err.Error(), `object type word "contract" is not defined`) {
		t.Fatalf("expected singular noun to be rejected, got %v", err)
	}
	if len(journal.Events()) != 0 {
		t.Fatalf("undefined word should not append ledger events, got %d", len(journal.Events()))
	}
	if client.calls != 1 {
		t.Fatalf("undefined word should only fetch surface, got %d calls", client.calls)
	}
}

func TestObjectTypeWordHelp_fetches_command_surface(t *testing.T) {
	var stdout bytes.Buffer
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
		},
	}

	err := runWithTestDeps([]string{
		"help", "contracts",
		"--server", "http://server",
		"--workspace", "wks_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: &stdout,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("object type word help should succeed: %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"contracts commands:", "contracts create", "create one contract", "singular: contract", "plural: contracts", "field_schema:", "customer_contracts (rdf_customer_contracts)"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output should contain %q, got:\n%s", want, output)
		}
	}
	if client.requests[0].URL.Path != "/api/v1/workspaces/wks_1/command-surface" {
		t.Fatalf("unexpected surface path: %s", client.requests[0].URL.Path)
	}
}

func TestObjectTypeWordHelp_requires_explicit_surface_noun_names(t *testing.T) {
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: `{"workspace_id":"wks_1","nouns":[{"key":"contracts","kind":"object_type"}]}`},
		},
	}

	err := runWithTestDeps([]string{
		"help", "contracts",
		"--server", "http://server",
		"--workspace", "wks_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
	})
	if err == nil || !strings.Contains(err.Error(), `command surface noun "contracts" singular is required`) {
		t.Fatalf("expected missing explicit noun name to fail without fallback, got %v", err)
	}
}

func TestObjectTypeWordHelp_works_as_word_subcommand(t *testing.T) {
	var stdout bytes.Buffer
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
		},
	}

	err := runWithTestDeps([]string{
		"contracts", "help",
		"--server", "http://server",
		"--workspace", "wks_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: &stdout,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("word help should succeed: %v", err)
	}
	if !strings.Contains(stdout.String(), "contracts commands:") {
		t.Fatalf("unexpected help output:\n%s", stdout.String())
	}
}

func TestObjectTypeWordSubcommandHelp_uses_command_surface(t *testing.T) {
	var stdout bytes.Buffer
	client := &fakeHTTPClient{
		responses: []fakeHTTPResponse{
			{status: http.StatusOK, body: fakeCommandSurfaceBody()},
		},
	}

	err := runWithTestDeps([]string{
		"contracts", "create", "--help",
		"--server", "http://server",
		"--workspace", "wks_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: &stdout,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("subcommand help should succeed: %v", err)
	}
	if !strings.Contains(stdout.String(), "contracts create") {
		t.Fatalf("unexpected help output:\n%s", stdout.String())
	}
	if client.calls != 1 {
		t.Fatalf("help should only fetch command surface, got %d calls", client.calls)
	}
}

func TestRelationDefine_syncs_ingested_command_intent_with_object_type_preconditions(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"relation_definition","id":"rdf_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}

	err := runWithTestDeps([]string{
		"relation", "define",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--key", "customer_contracts",
		"--from-type", "otp_customer",
		"--from-type-version", "2",
		"--to-type", "otp_contract",
		"--to-type-version", "3",
		"--cardinality", "one_to_many",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("relation define should succeed: %v", err)
	}
	if len(journal.Events()) != 3 {
		t.Fatalf("expected ledger lifecycle events, got %d", len(journal.Events()))
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "relation_definition.define" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "relation_definition" || target["aggregate_id"] != nil {
		t.Fatalf("unexpected target: %#v", target)
	}
	payload := command["payload"].(map[string]any)
	if payload["key"] != "customer_contracts" ||
		payload["from_object_type_id"] != "otp_customer" ||
		payload["to_object_type_id"] != "otp_contract" ||
		payload["cardinality"] != "one_to_many" ||
		payload["required"] != false {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	preconditions := command["preconditions"].([]any)
	if len(preconditions) != 2 {
		t.Fatalf("expected object type preconditions, got %#v", preconditions)
	}
}

func TestRelationLink_syncs_ingested_command_intent_with_endpoint_preconditions(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_relation","id":"rel_1","version":1},"projection":{"workspace_id":"wks_1","projection_sequence":1}}`,
	}

	err := runWithTestDeps([]string{
		"relation", "link",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--definition", "rdf_customer_contracts",
		"--definition-version", "1",
		"--from", "obj_customer",
		"--from-version", "2",
		"--to", "obj_contract",
		"--to-version", "4",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("relation link should succeed: %v", err)
	}
	if len(journal.Events()) != 3 {
		t.Fatalf("expected ledger lifecycle events, got %d", len(journal.Events()))
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "relation.link" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "object_relation" || target["aggregate_id"] != nil {
		t.Fatalf("unexpected target: %#v", target)
	}
	payload := command["payload"].(map[string]any)
	if payload["relation_definition_id"] != "rdf_customer_contracts" ||
		payload["from_object_id"] != "obj_customer" ||
		payload["to_object_id"] != "obj_contract" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	preconditions := command["preconditions"].([]any)
	if len(preconditions) != 3 {
		t.Fatalf("expected relation definition and endpoint preconditions, got %#v", preconditions)
	}
}

func TestRelationUnlink_syncs_ingested_command_intent_with_relation_precondition(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"command_id":"cmd_2","outcome":"applied","aggregate":{"type":"object_relation","id":"rel_1","version":2},"projection":{"workspace_id":"wks_1","projection_sequence":2}}`,
	}

	err := runWithTestDeps([]string{
		"relation", "unlink",
		"--server", "http://server",
		"--workspace", "wks_1",
		"--id", "rel_1",
		"--version", "1",
		"--definition", "rdf_customer_contracts",
	}, journal, runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	})
	if err != nil {
		t.Fatalf("relation unlink should succeed: %v", err)
	}
	if len(journal.Events()) != 3 {
		t.Fatalf("expected ledger lifecycle events, got %d", len(journal.Events()))
	}
	command := ingestedCommandFromBody(t, client.bodies[0])
	if command["command_type"] != "relation.unlink" {
		t.Fatalf("unexpected command type: %#v", command["command_type"])
	}
	target := command["target"].(map[string]any)
	if target["aggregate_type"] != "object_relation" || target["aggregate_id"] != "rel_1" {
		t.Fatalf("unexpected target: %#v", target)
	}
	payload := command["payload"].(map[string]any)
	if payload["relation_definition_id"] != "rdf_customer_contracts" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	preconditions := command["preconditions"].([]any)
	if len(preconditions) != 1 {
		t.Fatalf("expected relation precondition, got %#v", preconditions)
	}
}

func TestObjectsList_withoutWorkspaceKeepsLoginError(t *testing.T) {
	err := runWithTestDeps([]string{
		"objects", "list",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout:           io.Discard,
		stderr:           io.Discard,
		client:           &fakeHTTPClient{status: http.StatusOK, body: `{}`},
		identityProvider: testIdentityProvider(filepath.Join(t.TempDir(), "missing-auth.json")),
	})
	if err == nil || !strings.Contains(err.Error(), "external identity integration unavailable") {
		t.Fatalf("expected wrapped login error, got %v", err)
	}
}

func TestObjectGet_canonicalReadSendsBearerToken(t *testing.T) {
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"data":{"id":"obj_1"},"consistency":{"source":"kernel"}}`,
	}
	var stdout strings.Builder

	err := runWithTestDeps([]string{
		"object", "get",
		"--server", "http://kernel",
		"--workspace", "wks_1",
		"--id", "obj_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: &stdout,
		stderr: io.Discard,
		client: client,
	})
	if err != nil {
		t.Fatalf("canonical object get should succeed: %v", err)
	}
	if got := client.requests[0].Header.Get("Authorization"); got != "Bearer usr_1" {
		t.Fatalf("unexpected Authorization header: %q", got)
	}
	if !strings.Contains(stdout.String(), `"source":"kernel"`) {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func TestObjectGet_canonicalReadEscapesPathSegments(t *testing.T) {
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{"data":{"id":"obj/1 %"},"consistency":{"source":"kernel"}}`,
	}

	err := runWithTestDeps([]string{
		"object", "get",
		"--server", "http://kernel",
		"--workspace", "wks/a b%",
		"--id", "obj/1 %",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout:           io.Discard,
		stderr:           io.Discard,
		client:           client,
		identityProvider: testIdentityProvider(testSessionPath(t, "usr_1")),
	})
	if err != nil {
		t.Fatalf("canonical object get should succeed: %v", err)
	}
	if got := client.requests[0].URL.EscapedPath(); got != "/api/v1/workspaces/wks%2Fa%20b%25/objects/obj%2F1%20%25" {
		t.Fatalf("unexpected escaped path: %s", got)
	}
}

func TestObjectsList_explicitServerRequiresExternalIdentity(t *testing.T) {
	client := &fakeHTTPClient{
		status: http.StatusOK,
		body:   `{}`,
	}

	err := runWithTestDeps([]string{
		"objects", "list",
		"--server", "http://kernel",
		"--workspace", "wks_1",
	}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout:           io.Discard,
		stderr:           io.Discard,
		client:           client,
		identityProvider: testIdentityProvider(filepath.Join(t.TempDir(), "missing-auth.json")),
	})
	if err == nil {
		t.Fatal("explicit server bypassed external identity")
	}
	if client.calls != 0 {
		t.Fatalf("expected no canonical reads, got %d", client.calls)
	}
}

type fakeHTTPResponse struct {
	status int
	body   string
}

func fakeCommandSurfaceBody() string {
	return `{
		"workspace_id": "wks_1",
		"nouns": [
			{
				"key": "contracts",
				"singular": "contract",
				"plural": "contracts",
				"kind": "object_type",
				"object_type_id": "otp_contract",
				"version": 3,
				"commands": ["create", "get", "move", "link"],
				"field_schema": {"required": ["amount"]},
				"state_machine_id": "stm_contract",
				"relations": [
					{
						"id": "rdf_customer_contracts",
						"key": "customer_contracts",
						"version": 1,
						"from_object_type_id": "otp_customer",
						"to_object_type_id": "otp_contract",
						"cardinality": "one_to_many"
					}
				]
			}
		]
	}`
}

type fakeHTTPClient struct {
	status    int
	body      string
	err       error
	readErr   error
	responses []fakeHTTPResponse
	calls     int
	requests  []*http.Request
	bodies    [][]byte
}

func (c *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.requests = append(c.requests, req.Clone(req.Context()))
	if req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		c.bodies = append(c.bodies, body)
	} else {
		c.bodies = append(c.bodies, nil)
	}
	if c.err != nil {
		return nil, c.err
	}
	status := c.status
	body := c.body
	if len(c.responses) >= c.calls {
		response := c.responses[c.calls-1]
		status = response.status
		body = response.body
	}
	var responseBody io.ReadCloser = io.NopCloser(strings.NewReader(body))
	if c.readErr != nil {
		responseBody = errorReadCloser{err: c.readErr}
	}
	return &http.Response{
		StatusCode: status,
		Body:       responseBody,
		Header:     make(http.Header),
	}, nil
}

type errorReadCloser struct {
	err error
}

func (r errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r errorReadCloser) Close() error {
	return nil
}

func ingestedCommandFromBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("request body should be JSON: %v", err)
	}
	if _, ok := envelope["command_type"].(string); ok {
		return envelope
	}
	command, ok := envelope["command"].(map[string]any)
	if !ok {
		t.Fatalf("request body should include command intent, got %#v", envelope)
	}
	return command
}

func fillLedgerIntentHashes(t *testing.T, event *ledger.AttemptAppended) {
	t.Helper()
	var command commandEnvelope
	if err := json.Unmarshal(event.CommandIntent, &command); err != nil {
		t.Fatalf("ledger command intent: %v", err)
	}
	var err error
	if event.PayloadHash, err = jsonHash(command.Payload); err != nil {
		t.Fatal(err)
	}
	if event.TargetHash, err = jsonHash(command.Target); err != nil {
		t.Fatal(err)
	}
	event.IdempotencyKey = command.IdempotencyKey
	event.CommandHash, err = jsonHash(command)
	if err != nil {
		t.Fatal(err)
	}
	event.ActorContextHash, err = jsonHash(map[string]any{
		"workspace_id":               command.WorkspaceID,
		"authenticated_principal_id": event.PrincipalID,
		"requested_actor":            command.RequestedActor,
		"causation_source":           command.Causation["source"],
	})
	if err != nil {
		t.Fatal(err)
	}
}

type failingJournal struct {
	err error
}

type commitFailingJournal struct {
	err      error
	appended ledger.AttemptAppended
}

func cliRuntimeDeps(client *fakeHTTPClient) runtimeDeps {
	return runtimeDeps{
		stdout: io.Discard,
		stderr: io.Discard,
		client: client,
		now:    func() time.Time { return time.Unix(10, 0).UTC() },
		newID:  fixedIDGenerator(),
	}
}

func workspaceCreateIdempotencyKeyForArgs(t *testing.T, args ...string) string {
	t.Helper()

	client := &fakeHTTPClient{status: http.StatusCreated, body: `{}`}
	deps := cliRuntimeDeps(client)
	deps.newID = func(prefix string) string {
		t.Fatalf("workspace create should not call newID(%q)", prefix)
		return ""
	}
	commandArgs := append([]string{"workspace", "create"}, args...)
	if err := runWithTestDeps(commandArgs, ledger.NewMemoryJournal(), deps); err != nil {
		t.Fatalf("workspace create should succeed: %v", err)
	}
	if len(client.bodies) != 1 {
		t.Fatalf("expected one workspace create body, got %d", len(client.bodies))
	}
	var payload struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := json.Unmarshal(client.bodies[0], &payload); err != nil {
		t.Fatalf("workspace create body should be JSON: %v", err)
	}
	if payload.IdempotencyKey == "" {
		t.Fatalf("workspace create body should include idempotency_key: %s", client.bodies[0])
	}
	return payload.IdempotencyKey
}

func noSleep(context.Context, time.Duration) error {
	return nil
}

type submittedFailingJournal struct {
	capturingJournal
	err error
}

type capturingJournal struct {
	appended ledger.AttemptAppended
	events   []ledger.Event
}

func (j *capturingJournal) AppendAttempt(_ context.Context, event ledger.AttemptAppended) (ledger.AttemptID, error) {
	j.appended = event
	j.events = append(j.events, ledger.Event{Kind: ledger.EventAttemptAppended, AttemptID: event.AttemptID, Appended: &event})
	return event.AttemptID, nil
}
func (j *capturingJournal) AppendSubmitted(_ context.Context, event ledger.AttemptSubmitted) error {
	j.events = append(j.events, ledger.Event{Kind: ledger.EventAttemptSubmitted, AttemptID: event.AttemptID, Submitted: &event})
	return nil
}
func (j *capturingJournal) AppendRejected(_ context.Context, event ledger.AttemptRejected) error {
	j.events = append(j.events, ledger.Event{Kind: ledger.EventAttemptRejected, AttemptID: event.AttemptID, Rejected: &event})
	return nil
}
func (j *capturingJournal) AppendCommitted(_ context.Context, event ledger.AttemptCommitted) error {
	j.events = append(j.events, ledger.Event{Kind: ledger.EventAttemptCommitted, AttemptID: event.AttemptID, Committed: &event})
	return nil
}
func (j *submittedFailingJournal) AppendSubmitted(context.Context, ledger.AttemptSubmitted) error {
	return j.err
}

func (j failingJournal) AppendAttempt(context.Context, ledger.AttemptAppended) (ledger.AttemptID, error) {
	return "", j.err
}
func (j failingJournal) AppendSubmitted(context.Context, ledger.AttemptSubmitted) error {
	return nil
}
func (j failingJournal) AppendRejected(context.Context, ledger.AttemptRejected) error {
	return nil
}
func (j failingJournal) AppendCommitted(context.Context, ledger.AttemptCommitted) error {
	return nil
}
func (j *commitFailingJournal) AppendAttempt(_ context.Context, event ledger.AttemptAppended) (ledger.AttemptID, error) {
	j.appended = event
	return event.AttemptID, nil
}
func (j *commitFailingJournal) AppendSubmitted(context.Context, ledger.AttemptSubmitted) error {
	return nil
}
func (j *commitFailingJournal) AppendRejected(context.Context, ledger.AttemptRejected) error {
	return nil
}
func (j *commitFailingJournal) AppendCommitted(context.Context, ledger.AttemptCommitted) error {
	return j.err
}
func fixedIDGenerator() func(prefix string) string {
	sequence := 0
	return func(prefix string) string {
		sequence++
		return prefix + "_" + string(rune('0'+sequence))
	}
}
