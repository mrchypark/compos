package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func TestLedgerListFiltersMetadataAndDoesNotWrite(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	appendAttempt := func(id, workspace string, at time.Time) {
		t.Helper()
		if _, err := journal.AppendAttempt(context.Background(), ledger.AttemptAppended{AttemptID: ledger.AttemptID(id), Server: "https://api.example.test/prefix", PrincipalID: "usr_1", WorkspaceID: workspace, CommandType: "object.create", CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Unix(10, 0).UTC()
	appendAttempt("attempt_unsent", "wks_1", base)
	appendAttempt("attempt_unknown", "wks_1", base.Add(100*time.Millisecond))
	if err := journal.AppendSubmitted(context.Background(), ledger.AttemptSubmitted{AttemptID: "attempt_unknown", CommandID: "cmd_1", SubmittedAt: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	appendAttempt("attempt_rejected", "wks_1", base.Add(time.Second))
	if err := journal.AppendRejected(context.Background(), ledger.AttemptRejected{AttemptID: "attempt_rejected", Code: "400", Message: "secret detail", RejectedAt: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	appendAttempt("attempt_other", "wks_2", base.Add(3*time.Second))
	before := len(journal.Events())
	var output bytes.Buffer
	if err := runWithDeps([]string{"ledger", "list", "--workspace", "wks_1", "--limit", "10"}, journal, runtimeDeps{stdout: &output, stderr: io.Discard}); err != nil {
		t.Fatalf("ledger list: %v", err)
	}
	if len(journal.Events()) != before {
		t.Fatalf("ledger list must not append events: before=%d after=%d", before, len(journal.Events()))
	}
	if strings.Contains(output.String(), "secret detail") {
		t.Fatalf("ledger list leaked rejection message: %s", output.String())
	}
	var result struct {
		Attempts []ledgerListItem `json:"attempts"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Attempts) != 3 || result.Attempts[0].AttemptID != "attempt_rejected" || result.Attempts[0].Status != "rejected" || result.Attempts[0].Reason != "400" || result.Attempts[0].Retryable || result.Attempts[1].Status != "unknown" || !result.Attempts[1].Retryable || result.Attempts[2].Status != "unsent" || !result.Attempts[2].Retryable {
		t.Fatalf("attempts=%#v", result.Attempts)
	}
	output.Reset()
	if err := runWithDeps([]string{"ledger", "list", "--status", "unknown", "--limit", "1"}, journal, runtimeDeps{stdout: &output, stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || len(result.Attempts) != 1 || result.Attempts[0].AttemptID != "attempt_unknown" {
		t.Fatalf("filtered attempts=%#v err=%v", result.Attempts, err)
	}
	if err := runWithDeps([]string{"ledger", "list", "--limit", "0"}, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard}); err == nil {
		t.Fatal("non-positive list limit should fail")
	}
}

func TestLedgerListMarksUnpinnedAttemptNotRetryable(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	if _, err := journal.AppendAttempt(context.Background(), ledger.AttemptAppended{AttemptID: "attempt_1", WorkspaceID: "wks_1", CommandType: "object.create", CreatedAt: time.Unix(10, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runWithDeps([]string{"ledger", "list"}, journal, runtimeDeps{stdout: &output, stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Attempts []ledgerListItem `json:"attempts"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Retryable || result.Attempts[0].Reason != "missing_scope" {
		t.Fatalf("attempts=%#v", result.Attempts)
	}
}

func TestLedgerListCommittedIsAbsorbingAfterLateFailure(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	at := time.Unix(10, 0).UTC()
	if _, err := journal.AppendAttempt(context.Background(), ledger.AttemptAppended{AttemptID: "attempt_1", Server: "https://api.example.test", PrincipalID: "usr_1", WorkspaceID: "wks_1", CommandType: "object.create", CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendCommitted(context.Background(), ledger.AttemptCommitted{AttemptID: "attempt_1", CommandID: "cmd_1", CommittedAt: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendRejected(context.Background(), ledger.AttemptRejected{AttemptID: "attempt_1", Code: "network_error", RejectedAt: at.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runWithDeps([]string{"ledger", "list"}, journal, runtimeDeps{stdout: &output, stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Attempts []ledgerListItem `json:"attempts"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].Status != "committed" || result.Attempts[0].Retryable {
		t.Fatalf("attempts=%#v", result.Attempts)
	}
}

func TestLedgerRetryRejectsChangedScopeBeforeHTTP(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	command := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"object_type":"contract"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	appended := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "https://api.example.test/prefix", PrincipalID: "usr_1", WorkspaceID: "wks_1", CommandType: "object.create", IdempotencyKey: "idem_1", CommandIntent: command, CreatedAt: time.Unix(10, 0).UTC()}
	fillLedgerIntentHashes(t, &appended)
	if _, err := journal.AppendAttempt(context.Background(), appended); err != nil {
		t.Fatal(err)
	}
	client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}
	path := testSessionPath(t, "usr_1")
	if err := saveSession(runtimeDeps{sessionPath: path}, cliSession{Server: "https://api.example.test/other", WorkspaceID: "wks_1", PrincipalID: "usr_1", Credential: "token"}); err != nil {
		t.Fatal(err)
	}
	err := runWithDeps([]string{"ledger", "retry", "--attempt", "attempt_1"}, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client, sessionPath: path})
	if err == nil || !strings.Contains(err.Error(), "does not match the active session") || client.calls != 0 {
		t.Fatalf("retry err=%v calls=%d", err, client.calls)
	}
}

func TestLedgerRetryAppendedAttemptCommits(t *testing.T) {
	journal := ledger.NewMemoryJournal()
	command := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"object_type":"contract"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	appended := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "https://api.example.test/prefix", PrincipalID: "usr_1", WorkspaceID: "wks_1", CommandType: "object.create", IdempotencyKey: "idem_1", CommandIntent: command, CreatedAt: time.Unix(10, 0).UTC()}
	fillLedgerIntentHashes(t, &appended)
	if _, err := journal.AppendAttempt(context.Background(), appended); err != nil {
		t.Fatal(err)
	}
	path := testSessionPath(t, "usr_1")
	if err := saveSession(runtimeDeps{sessionPath: path}, cliSession{Server: "https://api.example.test/prefix", WorkspaceID: "wks_1", PrincipalID: "usr_1", Credential: "token"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeHTTPClient{status: http.StatusOK, body: `{"command_id":"cmd_1","outcome":"applied","projection":{"workspace_id":"wks_1","projection_sequence":1}}`}
	if err := runWithDeps([]string{"ledger", "retry", "--attempt", "attempt_1"}, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client, sessionPath: path, now: func() time.Time { return time.Unix(11, 0).UTC() }, newID: fixedIDGenerator(), sleep: noSleep}); err != nil {
		t.Fatalf("retry appended attempt: %v", err)
	}
	records, err := journal.EventRecords(context.Background(), ledger.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	states := ledgerAttemptStates(records)
	if client.calls != 1 || states["attempt_1"].committed.AttemptID != "attempt_1" {
		t.Fatalf("calls=%d state=%#v", client.calls, states["attempt_1"])
	}
}

func TestLedgerRetryScopePinsBlockHTTP(t *testing.T) {
	command := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"object_type":"contract"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	for _, tc := range []struct {
		name              string
		server, principal string
		sessionPrincipal  string
		sessionWorkspace  string
	}{
		{name: "missing server", principal: "usr_1", sessionPrincipal: "usr_1", sessionWorkspace: "wks_1"},
		{name: "missing principal", server: "https://api.example.test", sessionPrincipal: "usr_1", sessionWorkspace: "wks_1"},
		{name: "different principal", server: "https://api.example.test", principal: "usr_1", sessionPrincipal: "usr_2", sessionWorkspace: "wks_1"},
		{name: "different workspace", server: "https://api.example.test", principal: "usr_1", sessionPrincipal: "usr_1", sessionWorkspace: "wks_2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journal := ledger.NewMemoryJournal()
			appended := ledger.AttemptAppended{AttemptID: "attempt_1", Server: tc.server, PrincipalID: tc.principal, WorkspaceID: "wks_1", CommandType: "object.create", IdempotencyKey: "idem_1", CommandIntent: command, CreatedAt: time.Unix(10, 0).UTC()}
			if tc.server != "" && tc.principal != "" {
				fillLedgerIntentHashes(t, &appended)
			}
			if _, err := journal.AppendAttempt(context.Background(), appended); err != nil {
				t.Fatal(err)
			}
			path := testSessionPath(t, tc.sessionPrincipal)
			if err := saveSession(runtimeDeps{sessionPath: path}, cliSession{Server: "https://api.example.test", WorkspaceID: tc.sessionWorkspace, PrincipalID: tc.sessionPrincipal, Credential: "token"}); err != nil {
				t.Fatal(err)
			}
			client := &fakeHTTPClient{status: http.StatusOK, body: `{}`}
			err := runWithDeps([]string{"ledger", "retry", "--attempt", "attempt_1"}, journal, runtimeDeps{stdout: io.Discard, stderr: io.Discard, client: client, sessionPath: path})
			if err == nil || client.calls != 0 {
				t.Fatalf("retry err=%v calls=%d", err, client.calls)
			}
		})
	}
}

func TestCanonicalLedgerServerPreservesEscapedPathPrefix(t *testing.T) {
	got, err := canonicalLedgerServer("HTTPS://API.EXAMPLE.TEST/a%2Fb/")
	if err != nil || got != "https://api.example.test/a%2Fb" {
		t.Fatalf("server=%q err=%v", got, err)
	}
	for _, raw := range []string{"https://user@example.test", "https://example.test/?x=1", "https://example.test/#x"} {
		if _, err := canonicalLedgerServer(raw); err == nil {
			t.Fatalf("%q should be rejected", raw)
		}
	}
}
