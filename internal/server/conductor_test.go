package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestConductorOutboxDeliveryPublishesOnlyExecutionRequests(t *testing.T) {
	received := make(chan map[string]any, 1)
	conductor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("request path=%s authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		received <- body
		w.WriteHeader(http.StatusAccepted)
		encoded, _ := json.Marshal(body["events"])
		var events []conductorOutboxEvent
		if err := json.Unmarshal(encoded, &events); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, events, "queued"))
	}))
	defer conductor.Close()

	ctx := context.Background()
	db, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	hash := "sha256:" + strings.Repeat("0", 64)
	for _, commandID := range []string{"cmd_external", "cmd_generic"} {
		if _, err := db.DB().Exec(`INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, commandID, "wks_1", "dev", "null", `{"type":"user","id":"dev"}`, hash, commandID, "function.invoke", `{"aggregate_type":"work_stream","aggregate_id":"wrk_1"}`, `[]`, `{"source":"api","attempt_id":null,"event_id":null}`, hash, `{}`, "applied", `{"command_id":"`+commandID+`","outcome":"applied"}`, now, now); err != nil {
			t.Fatal(err)
		}
	}
	executionPayload := `{"recipient":"ops@example.com","_compos":{"workspace_id":"wks_1","execution_request_id":"inv_1","handler_key":"connector.invoke","handler_version":"1","operation":"gmail.messages.send","actor":{"type":"user","id":"dev"}}}`
	if _, err := db.DB().Exec(`INSERT INTO outbox_events VALUES('evt_external','wks_1','cmd_external','connector.invoke',?,'pending',?,?)`, executionPayload, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO outbox_events VALUES('evt_generic','wks_1','cmd_generic','object.updated','{}','pending',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	server := newTestServer(db)
	if err := server.EnableConductor(conductor.URL, "secret", "callback-secret"); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.wakeConductor()
	select {
	case body := <-received:
		events, _ := body["events"].([]any)
		if len(events) != 1 {
			t.Fatalf("events=%v", body["events"])
		}
		event, _ := events[0].(map[string]any)
		if event["id"] != "evt_external" || event["name"] != "connector.invoke" || event["idempotency_key"] != "cmd_external" {
			t.Fatalf("event=%v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("conductor request was not delivered")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		var external, generic string
		if err := db.DB().QueryRow(`SELECT status FROM outbox_events WHERE id='evt_external'`).Scan(&external); err != nil {
			t.Fatal(err)
		}
		if err := db.DB().QueryRow(`SELECT status FROM outbox_events WHERE id='evt_generic'`).Scan(&generic); err != nil {
			t.Fatal(err)
		}
		if external == "published" {
			if generic != "pending" {
				t.Fatalf("generic outbox status=%s", generic)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("external outbox status=%s", external)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConductorOutboxRequiresMatchingAcknowledgements(t *testing.T) {
	for _, mode := range []string{"malformed", "missing", "foreign", "wrong-event", "empty-event", "null-event", "missing-execution", "rejected", "duplicate", "unknown", "trailing", "junk", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var requests []string
			responseMode := mode
			conductor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Events []conductorOutboxEvent `json:"events"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				var data struct {
					Compos struct {
						ExecutionRequestID string `json:"execution_request_id"`
					} `json:"_compos"`
				}
				if len(payload.Events) != 1 || json.Unmarshal(payload.Events[0].Data, &data) != nil {
					t.Fatalf("events=%v", payload.Events)
				}
				requests = append(requests, data.Compos.ExecutionRequestID)
				w.WriteHeader(http.StatusAccepted)
				switch responseMode {
				case "malformed":
					_, _ = w.Write([]byte(`{"accepted":`))
				case "missing":
					_, _ = w.Write([]byte(`{"accepted":true,"events":[]}`))
				case "foreign":
					_, _ = w.Write([]byte(`{"accepted":true,"events":[{"execution_id":"exec_1","execution_request_id":"inv_other","status":"queued"}]}`))
				case "empty-event":
					_, _ = w.Write([]byte(`{"accepted":true,"events":[{"event_id":"","execution_id":"exec_1","execution_request_id":"inv_1","status":"queued"}]}`))
				case "null-event":
					_, _ = w.Write([]byte(`{"accepted":true,"events":[{"event_id":null,"execution_id":"exec_1","execution_request_id":"inv_1","status":"queued"}]}`))
				case "wrong-event", "missing-execution", "rejected":
					ack := conductorAcceptedResponse(t, payload.Events, "queued")
					if responseMode == "wrong-event" {
						ack["events"].([]map[string]string)[0]["event_id"] = "other"
					} else if responseMode == "missing-execution" {
						delete(ack["events"].([]map[string]string)[0], "execution_id")
					} else {
						ack["accepted"] = false
					}
					_ = json.NewEncoder(w).Encode(ack)
				case "duplicate":
					ack := conductorAcceptedResponse(t, payload.Events, "queued")
					ack["events"] = append(ack["events"].([]map[string]string), ack["events"].([]map[string]string)[0])
					_ = json.NewEncoder(w).Encode(ack)
				case "unknown":
					_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, "unknown"))
				case "trailing":
					_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, "queued"))
					_, _ = w.Write([]byte(`{}`))
				case "junk":
					_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, "queued"))
					_, _ = w.Write([]byte(`junk`))
				case "oversized":
					_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, "queued"))
					_, _ = w.Write([]byte(strings.Repeat(" ", 1<<20)))
				default:
					_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, responseMode))
				}
			}))
			defer conductor.Close()
			db, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			now := "2026-01-01T00:00:00Z"
			if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
				t.Fatal(err)
			}
			hash := "sha256:" + strings.Repeat("0", 64)
			if _, err := db.DB().Exec(`INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "cmd_1", "wks_1", "dev", "null", `{"type":"user","id":"dev"}`, hash, "idem_1", "function.invoke", `{"aggregate_type":"work_stream","aggregate_id":"wrk_1"}`, `[]`, `{"source":"api"}`, hash, `{}`, "applied", `{"command_id":"cmd_1","outcome":"applied"}`, now, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.DB().Exec(`INSERT INTO outbox_events VALUES('evt_1','wks_1','cmd_1','connector.invoke','{"_compos":{"execution_request_id":"inv_1"}}','pending',?,?)`, now, now); err != nil {
				t.Fatal(err)
			}
			server := newTestServer(db)
			defer server.Close()
			server.conductorURL, server.conductorClient = conductor.URL, conductor.Client()
			if err := server.dispatchConductorOutbox(ctx); err == nil {
				t.Fatal("expected acknowledgment error")
			}
			var status string
			if err := db.DB().QueryRow(`SELECT status FROM outbox_events WHERE id='evt_1'`).Scan(&status); err != nil || status != "pending" {
				t.Fatalf("status=%q err=%v", status, err)
			}
			responseMode = "queued"
			if mode == "unknown" {
				responseMode = "succeeded"
			}
			if err := server.dispatchConductorOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.DB().QueryRow(`SELECT status FROM outbox_events WHERE id='evt_1'`).Scan(&status); err != nil || status != "published" {
				t.Fatalf("status=%q err=%v", status, err)
			}
			if len(requests) != 2 || requests[0] != "inv_1" || requests[1] != "inv_1" {
				t.Fatalf("requests=%v", requests)
			}
		})
	}
}

func conductorAcceptedResponse(t *testing.T, events []conductorOutboxEvent, status string) map[string]any {
	t.Helper()
	acknowledgements := make([]map[string]string, len(events))
	for i, event := range events {
		var data struct {
			Compos struct {
				ExecutionRequestID string `json:"execution_request_id"`
			} `json:"_compos"`
		}
		if err := json.Unmarshal(event.Data, &data); err != nil || data.Compos.ExecutionRequestID == "" {
			t.Fatalf("event=%s data=%s err=%v", event.ID, event.Data, err)
		}
		acknowledgements[i] = map[string]string{"event_id": event.ID, "execution_id": "exec_" + data.Compos.ExecutionRequestID, "execution_request_id": data.Compos.ExecutionRequestID, "status": status}
	}
	return map[string]any{"accepted": true, "events": acknowledgements}
}

func TestConductorAcknowledgementAcceptsQueuedAndTerminalExecutionStates(t *testing.T) {
	events := []conductorOutboxEvent{{ID: "evt_1", Data: json.RawMessage(`{"_compos":{"execution_request_id":"inv_1"}}`)}}
	for _, status := range []string{"queued", "running", "retry_wait", "succeeded", "failed", "timed_out", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			body, err := json.Marshal(conductorAcceptedResponse(t, events, status))
			if err != nil {
				t.Fatal(err)
			}
			if err := validateConductorAcknowledgement(strings.NewReader(string(body)), events); err != nil {
				t.Fatal(err)
			}
			ack := conductorAcceptedResponse(t, events, status)
			delete(ack["events"].([]map[string]string)[0], "event_id")
			body, _ = json.Marshal(ack)
			if err := validateConductorAcknowledgement(strings.NewReader(string(body)), events); err != nil {
				t.Fatalf("optional event ID: %v", err)
			}
		})
	}
}

func TestRhizaConductorOutboxDispatchIsBoundedAndDrains(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "conductor-outbox-batch", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	one := int64(1)
	now := "2026-01-01T00:00:00Z"
	fixture := []rhiza.SQLStatement{{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, Args: []any{now, now}, ExpectedRowsAffected: &one}}
	apply := func(requestID string) {
		if err := db.ApplyStatements(ctx, requestID, fixture); err != nil {
			t.Fatal(err)
		}
		fixture = nil
	}
	apply("conductor-outbox-workspace")
	for i := range 65 {
		id := fmt.Sprintf("%03d", i)
		commandID, eventID := "cmd_"+id, "evt_"+id
		fixture = append(fixture,
			rhiza.SQLStatement{SQL: `INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, Args: []any{commandID, "wks_1", "dev", "null", `{"type":"user","id":"dev"}`, "sha256:" + strings.Repeat("0", 64), commandID, "function.invoke", `{"aggregate_type":"work_stream","aggregate_id":"wrk_1"}`, `[]`, `{"source":"api"}`, "sha256:" + strings.Repeat("1", 64), `{}`, "applied", `{"command_id":"` + commandID + `","outcome":"applied"}`, now, now}, ExpectedRowsAffected: &one},
			rhiza.SQLStatement{SQL: `INSERT INTO outbox_events VALUES(?,?,?,?,?,'pending',?,?)`, Args: []any{eventID, "wks_1", commandID, "connector.invoke", `{"_compos":{"execution_request_id":"inv_` + id + `"}}`, now, now}, ExpectedRowsAffected: &one},
		)
		if len(fixture) == 64 {
			apply("conductor-outbox-fixture-" + id)
		}
	}
	if len(fixture) > 0 {
		apply("conductor-outbox-fixture-final")
	}

	var batches [][]string
	conductor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Events []conductorOutboxEvent `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if len(payload.Events) == 0 || len(payload.Events) > 64 {
			t.Errorf("event batch size=%d", len(payload.Events))
		}
		ids := make([]string, len(payload.Events))
		for i, event := range payload.Events {
			ids[i] = event.ID
		}
		batches = append(batches, ids)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(conductorAcceptedResponse(t, payload.Events, "queued"))
	}))
	defer conductor.Close()

	server := newTestServer(db)
	defer server.Close()
	server.conductorURL = conductor.URL
	server.conductorClient = conductor.Client()
	if err := server.dispatchConductorOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.dispatchConductorOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.dispatchConductorOutbox(ctx); err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 || len(batches[0]) != 64 || len(batches[1]) != 1 {
		t.Fatalf("batches=%v", batches)
	}
	seen := map[string]bool{}
	for _, batch := range batches {
		for _, id := range batch {
			if seen[id] {
				t.Fatalf("event resent: %s", id)
			}
			seen[id] = true
		}
	}
	if len(seen) != 65 {
		t.Fatalf("published events=%d", len(seen))
	}
	var pending int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM outbox_events WHERE status='pending'`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("pending=%d err=%v", pending, err)
	}
}

func TestConductorCallbackIsAuthenticatedWorkspaceScopedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := "2026-01-01T00:00:00Z"
	hash := "sha256:" + strings.Repeat("0", 64)
	if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	if _, err := db.DB().Exec(`INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, "cmd_invoke", "wks_1", "dev", "null", `{"type":"user","id":"dev"}`, hash, "idem_invoke", "function.invoke", `{"aggregate_type":"work_stream","aggregate_id":"wrk_1"}`, `[]`, `{"source":"api","attempt_id":null,"event_id":null}`, hash, `{}`, "applied", `{"command_id":"cmd_invoke","outcome":"applied"}`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO spec_revisions VALUES('spc_function','wks_1','test.function',1,'function','{}',?,?, 'dev')`, hash, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO journal_records(id,workspace_id,stream_id,stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,command_id,payload_json,created_at) VALUES('jnl_1','wks_1','wrk_1',1,'function.requested','spc_function','human','dev','dev','inv_1','cmd_invoke','{"binding_kind":"native","handler_key":"native.invoke","handler_version":"1"}',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO outbox_events VALUES('evt_invoke','wks_1','cmd_invoke','native.invoke','{"_compos":{"execution_request_id":"inv_1","handler_key":"native.invoke","handler_version":"1"}}','published',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO journal_records(id,workspace_id,stream_id,stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,command_id,payload_json,created_at) VALUES('jnl_recover','wks_1','wrk_1',2,'function.requested','spc_function','human','dev','dev','inv_recover','cmd_invoke','{"binding_kind":"native","handler_key":"native.invoke","handler_version":"1"}',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(`INSERT INTO outbox_events VALUES('evt_recover','wks_1','cmd_invoke','native.invoke','{"_compos":{"execution_request_id":"inv_recover","handler_key":"native.invoke","handler_version":"1"}}','published',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	recoverCallback := conductorCallback{CallbackID: "cb_recover", WorkspaceID: "wks_1", ExecutionRequestID: "inv_recover", ExecutionID: "exec_recover", ProviderKey: "native.invoke", ProviderRunRef: "exec_recover", HandlerVersion: "1", RuntimeStatus: "failed", AttemptCount: 1, Result: json.RawMessage(`{}`), OccurredAt: now}
	recoverPayload := canonical(recoverCallback)
	if _, err := db.DB().Exec(`INSERT INTO adapter_inbound_events VALUES('ain_recover','wks_1','conductor','cb_recover',?,?,'appended',?)`, sha([]byte(recoverPayload)), recoverPayload, now); err != nil {
		t.Fatal(err)
	}

	outbound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer outbound.Close()
	server := newTestServer(db)
	if err := server.EnableConductor(outbound.URL, "service-secret", "callback-secret"); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var status string
		if err := db.DB().QueryRow(`SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_recover'`).Scan(&status); err == nil && status == "reduced" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending Conductor callback was not recovered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	callback := map[string]any{
		"callback_id": "cb_exec_1", "workspace_id": "wks_1", "execution_request_id": "inv_1", "execution_id": "exec_1",
		"provider_key": "native.invoke", "provider_run_ref": "exec_1", "handler_version": "1", "runtime_status": "failed", "attempt_count": 1,
		"result": json.RawMessage(`{"nested":{"b":2,"a":9007199254740993}}`), "error_class": "provider_4xx", "error_detail": "rejected", "occurred_at": now,
	}
	call := func(token string, body map[string]any) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/conductor/callbacks", strings.NewReader(string(payload)))
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, req)
		return recorder
	}
	if response := call("wrong", callback); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d body=%s", response.Code, response.Body.String())
	}
	wrongIdentity := make(map[string]any, len(callback))
	for key, value := range callback {
		wrongIdentity[key] = value
	}
	wrongIdentity["callback_id"], wrongIdentity["provider_key"] = "cb_wrong_identity", "other.handler"
	if response := call("callback-secret", wrongIdentity); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "execution_identity_conflict") {
		t.Fatalf("wrong identity status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := db.DB().Exec(`CREATE TRIGGER fail_conductor_inbound BEFORE INSERT ON adapter_inbound_events BEGIN SELECT RAISE(ABORT, 'forced insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	storageFailure := make(map[string]any, len(callback))
	for key, value := range callback {
		storageFailure[key] = value
	}
	storageFailure["callback_id"] = "cb_storage_failure"
	if response := call("callback-secret", storageFailure); response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "idempotency_conflict") {
		t.Fatalf("storage failure status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := db.DB().Exec(`DROP TRIGGER fail_conductor_inbound`); err != nil {
		t.Fatal(err)
	}
	if response := call("callback-secret", callback); response.Code != http.StatusAccepted {
		t.Fatalf("callback status=%d body=%s", response.Code, response.Body.String())
	}
	if response := call("callback-secret", callback); response.Code != http.StatusOK {
		t.Fatalf("duplicate callback status=%d body=%s", response.Code, response.Body.String())
	}
	converged := make(map[string]any, len(callback))
	for key, value := range callback {
		converged[key] = value
	}
	converged["callback_id"] = "cb_exec_same"
	if response := call("callback-secret", converged); response.Code != http.StatusAccepted {
		t.Fatalf("equivalent terminal callback status=%d body=%s", response.Code, response.Body.String())
	}
	conflict := make(map[string]any, len(callback))
	for key, value := range callback {
		conflict[key] = value
	}
	conflict["callback_id"], conflict["error_detail"] = "cb_exec_conflict", "different"
	if response := call("callback-secret", conflict); response.Code != http.StatusConflict {
		t.Fatalf("conflicting terminal callback status=%d body=%s", response.Code, response.Body.String())
	}
	changed := make(map[string]any, len(callback))
	for key, value := range callback {
		changed[key] = value
	}
	changed["attempt_count"] = 2
	if response := call("callback-secret", changed); response.Code != http.StatusConflict {
		t.Fatalf("changed callback status=%d body=%s", response.Code, response.Body.String())
	}
	unknown := make(map[string]any, len(callback))
	for key, value := range callback {
		unknown[key] = value
	}
	unknown["callback_id"], unknown["execution_request_id"] = "cb_unknown", "missing"
	if response := call("callback-secret", unknown); response.Code != http.StatusNotFound {
		t.Fatalf("unknown invocation status=%d body=%s", response.Code, response.Body.String())
	}

	var inbound, ledger, activities, outbox, projections, terminal int
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM adapter_inbound_events WHERE workspace_id='wks_1' AND adapter_id='conductor'`: &inbound,
		`SELECT COUNT(*) FROM command_ledger`:    &ledger,
		`SELECT COUNT(*) FROM activities`:        &activities,
		`SELECT COUNT(*) FROM outbox_events`:     &outbox,
		`SELECT COUNT(*) FROM projection_events`: &projections,
		`SELECT COUNT(*) FROM journal_records WHERE kind='function.failed' AND correlation_id='inv_1'`: &terminal,
	} {
		if err := db.DB().QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if inbound != 4 || ledger != 3 || activities != 2 || outbox != 4 || projections != 2 || terminal != 1 {
		t.Fatalf("inbound=%d ledger=%d activities=%d outbox=%d projections=%d terminal=%d", inbound, ledger, activities, outbox, projections, terminal)
	}
	var inboundStatus string
	if err := db.DB().QueryRow(`SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_exec_1'`).Scan(&inboundStatus); err != nil || inboundStatus != "reduced" {
		t.Fatalf("inbound status=%q err=%v", inboundStatus, err)
	}
}

func TestRhizaConductorCallbackRejectsChangedReducedCallback(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "conductor-callback-idempotency", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := "2026-01-01T00:00:00Z"
	one := int64(1)
	hash := "sha256:" + strings.Repeat("0", 64)
	if err := db.ApplyStatements(ctx, "conductor-callback-fixture", []rhiza.SQLStatement{
		{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, Args: []any{now, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, Args: []any{"cmd_invoke", "wks_1", "dev", "null", `{"type":"user","id":"dev"}`, hash, "idem_invoke", "function.invoke", `{"aggregate_type":"work_stream","aggregate_id":"wrk_1"}`, `[]`, `{"source":"api","attempt_id":null,"event_id":null}`, hash, `{}`, "applied", `{"command_id":"cmd_invoke","outcome":"applied"}`, now, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO spec_revisions VALUES('spc_function','wks_1','test.function',1,'function','{}',?,?, 'dev')`, Args: []any{hash, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO journal_records(id,workspace_id,stream_id,stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,command_id,payload_json,created_at) VALUES('jnl_1','wks_1','wrk_1',1,'function.requested','spc_function','human','dev','dev','inv_1','cmd_invoke','{"binding_kind":"native","handler_key":"native.invoke","handler_version":"1"}',?)`, Args: []any{now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO outbox_events VALUES('evt_invoke','wks_1','cmd_invoke','native.invoke','{"_compos":{"execution_request_id":"inv_1","handler_key":"native.invoke","handler_version":"1"}}','published',?,?)`, Args: []any{now, now}, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}

	server := newTestServer(db)
	defer server.Close()
	server.conductorCallbackToken = "callback-secret"
	callback := map[string]any{
		"callback_id": "cb_exec_1", "workspace_id": "wks_1", "execution_request_id": "inv_1", "execution_id": "exec_1",
		"provider_key": "native.invoke", "provider_run_ref": "exec_1", "handler_version": "1", "runtime_status": "failed", "attempt_count": 1,
		"result": json.RawMessage(`{"nested":{"b":2,"a":9007199254740993}}`), "error_class": "provider_4xx", "error_detail": "rejected", "occurred_at": now,
	}
	call := func(body map[string]any) *httptest.ResponseRecorder {
		payload, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/conductor/callbacks", strings.NewReader(string(payload)))
		req.Header.Set("Authorization", "Bearer callback-secret")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, req)
		return response
	}
	savedAuthorizer := server.kernel.AuthorizeCommand
	server.kernel.AuthorizeCommand = func(context.Context, string, kernel.Command) (kernel.Actor, error) {
		return kernel.Actor{}, kernel.ErrAuthorizationDenied
	}
	deniedCallback := make(map[string]any, len(callback))
	for key, value := range callback {
		deniedCallback[key] = value
	}
	deniedCallback["callback_id"] = "cb_explicitly_denied"
	if response := call(deniedCallback); response.Code != http.StatusForbidden {
		t.Fatalf("explicit denial status=%d body=%s", response.Code, response.Body.String())
	}
	var deniedStatus string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_explicitly_denied'`).Scan(&deniedStatus); err != nil || deniedStatus != "rejected" {
		t.Fatalf("explicit denial status=%s err=%v", deniedStatus, err)
	}
	server.kernel.AuthorizeCommand = func(context.Context, string, kernel.Command) (kernel.Actor, error) {
		return kernel.Actor{}, context.DeadlineExceeded
	}
	if response := call(callback); response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"retryable":true`) {
		t.Fatalf("authority outage status=%d body=%s", response.Code, response.Body.String())
	}
	var pendingStatus string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_exec_1'`).Scan(&pendingStatus); err != nil || pendingStatus != "appended" {
		t.Fatalf("outage callback status=%s err=%v", pendingStatus, err)
	}
	var pendingErr *kernel.CommandError
	if err := server.reducePendingConductorCallbacks(ctx); !errors.As(err, &pendingErr) || !pendingErr.Retryable {
		t.Fatalf("pending recovery error=%v", err)
	}
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE inbound_event_id='cb_exec_1'`).Scan(&pendingStatus); err != nil || pendingStatus != "appended" {
		t.Fatalf("recovery outage status=%s err=%v", pendingStatus, err)
	}
	server.kernel.AuthorizeCommand = savedAuthorizer
	if response := call(callback); response.Code != http.StatusOK {
		t.Fatalf("callback status=%d body=%s", response.Code, response.Body.String())
	}
	reordered := make(map[string]any, len(callback))
	for key, value := range callback {
		reordered[key] = value
	}
	reordered["result"] = json.RawMessage(`{"nested":{"a":9007199254740993,"b":2}}`)
	if response := call(reordered); response.Code != http.StatusOK {
		t.Fatalf("reordered callback status=%d body=%s", response.Code, response.Body.String())
	}
	var storedPayload string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM adapter_inbound_events WHERE inbound_event_id='cb_exec_1'`).Scan(&storedPayload); err != nil || !strings.Contains(storedPayload, `9007199254740993`) || !strings.Contains(storedPayload, `"a":9007199254740993,"b":2`) {
		t.Fatalf("stored payload=%s err=%v", storedPayload, err)
	}
	changedResult := make(map[string]any, len(callback))
	for key, value := range callback {
		changedResult[key] = value
	}
	changedResult["result"] = json.RawMessage(`{"nested":{"a":9007199254740994,"b":2}}`)
	if response := call(changedResult); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "idempotency_conflict") {
		t.Fatalf("changed result status=%d body=%s", response.Code, response.Body.String())
	}
	changed := make(map[string]any, len(callback))
	for key, value := range callback {
		changed[key] = value
	}
	changed["attempt_count"] = 2
	if response := call(changed); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "idempotency_conflict") {
		t.Fatalf("changed callback status=%d body=%s", response.Code, response.Body.String())
	}
	var inbound, terminal int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM adapter_inbound_events WHERE workspace_id='wks_1' AND adapter_id='conductor' AND inbound_event_id='cb_exec_1'`).Scan(&inbound); err != nil {
		t.Fatal(err)
	}
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE kind='function.failed' AND correlation_id='inv_1'`).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if inbound != 1 || terminal != 1 {
		t.Fatalf("inbound=%d terminal=%d", inbound, terminal)
	}
}

func TestConductorEquivalentInvalidResultConvergesAfterKernelNormalization(t *testing.T) {
	s, handler, execute := newWorkflowTestRuntime(t)
	outbound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer outbound.Close()
	if err := handler.EnableConductor(outbound.URL, "service-secret", "callback-secret"); err != nil {
		t.Fatal(err)
	}
	function := execute("cmd_normalized_function", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "normalized-result", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Normalized result", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object", "required": []any{"value"}, "properties": map[string]any{"value": map[string]any{"type": "string"}}},
			"binding":       map[string]any{"kind": "native", "handler_key": "native.normalize", "handler_version": "1"},
		},
	})
	task := execute("cmd_normalized_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "normalized-task", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Normalize", "goal": "Normalize", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object"}, "completion": map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_normalized_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}})
	workID := work.Aggregate.ID
	execute("cmd_normalized_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_normalized_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{}})
	var invocationID string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='function.requested' AND spec_revision_id=?`, workID, function.Aggregate.ID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	callback := map[string]any{
		"callback_id": "cb_normalized_1", "workspace_id": "wks_1", "execution_request_id": invocationID,
		"execution_id": "exec_normalized", "provider_key": "native.normalize", "provider_run_ref": "run_normalized", "handler_version": "1",
		"runtime_status": "succeeded", "attempt_count": 1, "result": map[string]any{"value": 7}, "occurred_at": now,
	}
	call := func(body map[string]any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/conductor/callbacks", strings.NewReader(string(raw)))
		req.Header.Set("Authorization", "Bearer callback-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	if response := call(callback); response.Code != http.StatusAccepted {
		t.Fatalf("first callback status=%d body=%s", response.Code, response.Body.String())
	}
	equivalent := map[string]any{}
	for key, value := range callback {
		equivalent[key] = value
	}
	equivalent["callback_id"] = "cb_normalized_2"
	if response := call(equivalent); response.Code != http.StatusAccepted {
		t.Fatalf("equivalent callback status=%d body=%s", response.Code, response.Body.String())
	}
	conflict := map[string]any{}
	for key, value := range callback {
		conflict[key] = value
	}
	conflict["callback_id"], conflict["result"] = "cb_normalized_3", map[string]any{"value": 8}
	if response := call(conflict); response.Code != http.StatusConflict {
		t.Fatalf("conflicting callback status=%d body=%s", response.Code, response.Body.String())
	}
	var terminal string
	if err := s.DB().QueryRow(`SELECT payload_json FROM journal_records WHERE stream_id=? AND correlation_id=? AND kind='function.failed'`, workID, invocationID).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(terminal, `"runtime_status":"failed"`) || !strings.Contains(terminal, `"result_digest":"sha256:`) {
		t.Fatalf("terminal=%s", terminal)
	}
}

func TestConductorComposConnectorReadsAreAuthenticatedAndWorkspaceScoped(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	server := newTestServer(db)
	typeReceipt, err := server.kernel.Execute(ctx, "dev", connectorCommand("cmd_type", "object_type.define", "object_type", map[string]any{
		"key": "orders", "singular": "Order", "plural": "Orders", "field_schema": map[string]any{"type": "object"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	_ = typeReceipt
	objectReceipt, err := server.kernel.Execute(ctx, "dev", connectorCommand("cmd_object", "object.create", "object_instance", map[string]any{
		"object_type_key": "orders", "title": "Order 1", "values": map[string]any{"status": "ready"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	outbound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer outbound.Close()
	if err := server.EnableConductor(outbound.URL, "service-secret", "callback-secret"); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	call := func(path, token, workspace string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Conductor-Workspace-Id", workspace)
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, req)
		return recorder
	}
	path := "/v1/connector/objects/orders/records/" + objectReceipt.Aggregate.ID
	if response := call(path, "wrong", "wks_1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token status=%d", response.Code)
	}
	if response := call(path, "service-secret", "wks_other"); response.Code != http.StatusNotFound {
		t.Fatalf("wrong workspace status=%d body=%s", response.Code, response.Body.String())
	}
	if response := call(path, "service-secret", "wks_1"); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"ready"`) {
		t.Fatalf("record status=%d body=%s", response.Code, response.Body.String())
	}
	if response := call("/v1/connector/objects/orders/schema", "service-secret", "wks_1"); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"key":"orders"`) {
		t.Fatalf("schema status=%d body=%s", response.Code, response.Body.String())
	}
}

func connectorCommand(id, commandType, aggregateType string, payload any) kernel.Command {
	raw, _ := json.Marshal(payload)
	return kernel.Command{
		CommandID: id, IdempotencyKey: id, WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"},
		CommandType: commandType, Target: kernel.Target{AggregateType: aggregateType}, Preconditions: []kernel.Precondition{}, Payload: raw,
		Causation: kernel.Causation{Source: "api"},
	}
}
