package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestAgentDefinitionRouteAndResultSubmission(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	tool := execute("cmd_agent_tool", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "agent.lookup", "revision": 1, "kind": "function",
		"spec": map[string]any{"name": "Lookup", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "binding": map[string]any{"kind": "native", "handler_key": "lookup", "handler_version": "1"}},
	})
	agent := execute("cmd_agent_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "research-agent", "revision": 1, "kind": "participant",
		"spec": map[string]any{
			"name": "Research agent", "instructions": "Answer the question.", "model_policy": map[string]any{"model": "test"},
			"tools": []any{tool.Aggregate.ID}, "permissions": map[string]any{"network": false},
			"execution_policy": map[string]any{"handler_key": "agent.run", "handler_version": "1", "timeout_seconds": 30, "retry_policy": map[string]any{"max_attempts": 2}},
		},
	})
	task := execute("cmd_agent_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "agent-task", "revision": 1, "kind": "task",
		"spec": map[string]any{
			"name": "Answer", "goal": "Return an answer", "input_schema": map[string]any{"type": "object", "required": []any{"question"}, "properties": map[string]any{"question": map[string]any{"type": "string"}}},
			"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "string"}}},
			"completion":    map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_agent_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"question": "Why?"}})
	workID := work.Aggregate.ID
	execute("cmd_agent_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "agent", "id": agent.Aggregate.ID}})

	var invocationID, eventType, outbox string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='agent.requested'`, workID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT event_type,payload_json FROM outbox_events WHERE command_id='cmd_agent_route'`).Scan(&eventType, &outbox); err != nil || eventType != "agent.run" || !strings.Contains(outbox, `"agent_definition"`) {
		t.Fatalf("agent outbox type=%q payload=%s err=%v", eventType, outbox, err)
	}
	callback := conductorCallback{CallbackID: "cb_agent", WorkspaceID: "wks_1", ExecutionRequestID: invocationID, ExecutionID: "exec_agent", ProviderKey: "agent.run", ProviderRunRef: "exec_agent", HandlerVersion: "1", RuntimeStatus: "succeeded", AttemptCount: 1, Result: json.RawMessage(`{"answer":"Because."}`), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	payload := canonical(callback)
	if _, err := s.DB().Exec(`INSERT INTO adapter_inbound_events VALUES('ain_agent','wks_1','conductor','cb_agent',?,?,'appended',?)`, sha([]byte(payload)), payload, callback.OccurredAt); err != nil {
		t.Fatal(err)
	}
	streamID, principalID, requestKind, err := handler.conductorInvocation(ctx, callback)
	if err != nil || streamID != workID || principalID != "dev" || requestKind != "agent.requested" {
		t.Fatalf("invocation stream=%q principal=%q kind=%q err=%v", streamID, principalID, requestKind, err)
	}
	if status, err := handler.reduceConductorCallbackCore(ctx, callback, streamID, principalID, requestKind); err != nil || status != "reduced" {
		t.Fatalf("reduce status=%q err=%v", status, err)
	}

	response := request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work/"+workID, nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"completed"`) || !strings.Contains(response.Body.String(), `"kind":"agent"`) || !strings.Contains(response.Body.String(), `"answer":"Because."`) {
		t.Fatalf("agent projection status=%d body=%s", response.Code, response.Body.String())
	}
	for kind, want := range map[string]int{"work.routed": 1, "agent.requested": 1, "agent.succeeded": 1, "work.submitted": 1, "work.satisfied": 1} {
		var got int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind=?`, workID, kind).Scan(&got); err != nil || got != want {
			t.Fatalf("%s=%d want=%d err=%v", kind, got, want, err)
		}
	}
	if status, err := handler.reduceConductorCallbackCore(ctx, callback, workID, "dev", "agent.requested"); err != nil || status != "reduced" {
		t.Fatalf("same callback replay after completion status=%q err=%v", status, err)
	}
	postCompletion := callback
	postCompletion.CallbackID = "cb_agent_after_completion"
	postCompletionPayload := canonical(postCompletion)
	if _, err := s.DB().Exec(`INSERT INTO adapter_inbound_events VALUES('ain_agent_after_completion','wks_1','conductor','cb_agent_after_completion',?,?,'appended',?)`, sha([]byte(postCompletionPayload)), postCompletionPayload, postCompletion.OccurredAt); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.reduceConductorCallbackCore(ctx, postCompletion, workID, "dev", "agent.requested"); err == nil || !strings.Contains(err.Error(), "already terminal") {
		t.Fatalf("new callback after completed Work was accepted: %v", err)
	}

	lateWork := execute("cmd_late_agent_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"question": "Too late?"}})
	lateWorkID := lateWork.Aggregate.ID
	execute("cmd_late_agent_route", "work.route", "work_stream", &lateWorkID, map[string]any{"participant": map[string]any{"kind": "agent", "id": agent.Aggregate.ID}})
	execute("cmd_late_agent_cancel", "work.cancel", "work_stream", &lateWorkID, map[string]any{"reason": "no longer needed"})
	var lateInvocationID string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='agent.requested'`, lateWorkID).Scan(&lateInvocationID); err != nil {
		t.Fatal(err)
	}
	lateCallback := conductorCallback{CallbackID: "cb_late_agent", WorkspaceID: "wks_1", ExecutionRequestID: lateInvocationID, ExecutionID: "exec_late_agent", ProviderKey: "agent.run", ProviderRunRef: "exec_late_agent", HandlerVersion: "1", RuntimeStatus: "succeeded", AttemptCount: 1, Result: json.RawMessage(`{"answer":"Too late."}`), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	latePayload := canonical(lateCallback)
	if _, err := s.DB().Exec(`INSERT INTO adapter_inbound_events VALUES('ain_late_agent','wks_1','conductor','cb_late_agent',?,?,'appended',?)`, sha([]byte(latePayload)), latePayload, lateCallback.OccurredAt); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.reduceConductorCallbackCore(ctx, lateCallback, lateWorkID, "dev", "agent.requested"); err == nil || !strings.Contains(err.Error(), "already terminal") {
		t.Fatalf("late Agent callback was accepted: %v", err)
	}
	var lateTerminal int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind IN ('agent.succeeded','agent.failed')`, lateWorkID).Scan(&lateTerminal); err != nil || lateTerminal != 0 {
		t.Fatalf("late Agent terminal facts=%d err=%v", lateTerminal, err)
	}

	invalidWork := execute("cmd_invalid_agent_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"question": "Invalid?"}})
	invalidWorkID := invalidWork.Aggregate.ID
	execute("cmd_invalid_agent_route", "work.route", "work_stream", &invalidWorkID, map[string]any{"participant": map[string]any{"kind": "agent", "id": agent.Aggregate.ID}})
	var invalidInvocationID string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='agent.requested'`, invalidWorkID).Scan(&invalidInvocationID); err != nil {
		t.Fatal(err)
	}
	invalidCallback := conductorCallback{CallbackID: "cb_invalid_agent_1", WorkspaceID: "wks_1", ExecutionRequestID: invalidInvocationID, ExecutionID: "exec_invalid_agent", ProviderKey: "agent.run", ProviderRunRef: "exec_invalid_agent", HandlerVersion: "1", RuntimeStatus: "succeeded", AttemptCount: 1, Result: json.RawMessage(`{"answer":7}`), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
	appendCallback := func(id string, callback conductorCallback) {
		t.Helper()
		payload := canonical(callback)
		if _, err := s.DB().Exec(`INSERT INTO adapter_inbound_events VALUES(?,'wks_1','conductor',?,?,?,'appended',?)`, id, callback.CallbackID, sha([]byte(payload)), payload, callback.OccurredAt); err != nil {
			t.Fatal(err)
		}
	}
	appendCallback("ain_invalid_agent_1", invalidCallback)
	if status, err := handler.reduceConductorCallbackCore(ctx, invalidCallback, invalidWorkID, "dev", "agent.requested"); err != nil || status != "reduced" {
		t.Fatalf("invalid Agent result status=%q err=%v", status, err)
	}
	equivalent := invalidCallback
	equivalent.CallbackID = "cb_invalid_agent_2"
	appendCallback("ain_invalid_agent_2", equivalent)
	if status, err := handler.reduceConductorCallbackCore(ctx, equivalent, invalidWorkID, "dev", "agent.requested"); err != nil || status != "reduced" {
		t.Fatalf("equivalent invalid Agent result status=%q err=%v", status, err)
	}
	var invalidTerminal, reducedCallbacks int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='agent.failed'`, invalidWorkID).Scan(&invalidTerminal); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM adapter_inbound_events WHERE inbound_event_id IN ('cb_invalid_agent_1','cb_invalid_agent_2') AND status='reduced'`).Scan(&reducedCallbacks); err != nil {
		t.Fatal(err)
	}
	if invalidTerminal != 1 || reducedCallbacks != 2 {
		t.Fatalf("invalid Agent terminal=%d reduced callbacks=%d", invalidTerminal, reducedCallbacks)
	}
}
