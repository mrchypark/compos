package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/kernel"
)

func TestTaskRunProjectionAndSubmissionCompletion(t *testing.T) {
	_, handler, execute := newWorkflowTestRuntime(t)
	badSpec, _ := json.Marshal(map[string]any{"family_id": "invalid", "revision": 1, "kind": "task", "spec": map[string]any{"name": "Invalid"}})
	if _, err := handler.kernel.Execute(context.Background(), "dev", kernel.Command{CommandID: "cmd_taskrun_bad_spec", IdempotencyKey: "idem_taskrun_bad_spec", WorkspaceID: "wks_1", CommandType: "spec.publish", Target: kernel.Target{AggregateType: "spec_revision"}, Preconditions: []kernel.Precondition{}, Payload: badSpec, Causation: kernel.Causation{Source: "api"}}); err == nil {
		t.Fatal("Task spec without contracts was accepted")
	}
	function := execute("cmd_taskrun_function", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "research.answer", "revision": 1, "kind": "function",
		"spec": map[string]any{"name": "Answer", "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"question"}, "properties": map[string]any{"question": map[string]any{"type": "string"}}}, "output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "string"}}}, "binding": map[string]any{"kind": "native", "handler_key": "research.answer", "handler_version": "1"}},
	})
	emptyPolicy, _ := json.Marshal(map[string]any{"family_id": "research.empty", "revision": 1, "kind": "task", "spec": map[string]any{
		"name": "Empty policy", "goal": "Reject ambiguous authority",
		"input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"},
		"completion": map[string]any{"kind": "submission_valid"}, "capability_policy": map[string]any{"allowed_function_revision_ids": []any{}},
	}})
	if _, err := handler.kernel.Execute(context.Background(), "dev", kernel.Command{CommandID: "cmd_taskrun_empty_policy", IdempotencyKey: "idem_taskrun_empty_policy", WorkspaceID: "wks_1", CommandType: "spec.publish", Target: kernel.Target{AggregateType: "spec_revision"}, Preconditions: []kernel.Precondition{}, Payload: emptyPolicy, Causation: kernel.Causation{Source: "api"}}); err == nil {
		t.Fatal("empty Task capability allowlist was accepted")
	}
	task := execute("cmd_taskrun_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "research", "revision": 1, "kind": "task",
		"spec": map[string]any{
			"name": "Research", "goal": "Return one supported answer",
			"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"question"}, "properties": map[string]any{"question": map[string]any{"type": "string"}}},
			"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "string"}}},
			"constraints":   []any{"cite evidence"}, "capability_policy": map[string]any{"allowed_function_revision_ids": []any{function.Aggregate.ID}},
			"ontology_context": map[string]any{"concept_refs": []any{"Research"}}, "completion": map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_taskrun_open", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"question": "Why?"}})
	workID := work.Aggregate.ID
	execute("cmd_taskrun_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}, "reason": "review"})
	execute("cmd_taskrun_handoff", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "reviewer"}, "reason": "specialist review"})
	inbox := request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work", nil)
	if inbox.Code != http.StatusOK || strings.Contains(inbox.Body.String(), `"id":"`+workID+`"`) {
		t.Fatalf("work routed to another human remained in inbox: status=%d body=%s", inbox.Code, inbox.Body.String())
	}
	execute("cmd_taskrun_return", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}, "reason": "return"})
	inbox = request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work", nil)
	if inbox.Code != http.StatusOK || !strings.Contains(inbox.Body.String(), `"id":"`+workID+`"`) {
		t.Fatalf("human inbox status=%d body=%s", inbox.Code, inbox.Body.String())
	}
	execute("cmd_taskrun_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"question": "Why?"}})
	inbox = request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work", nil)
	detail := request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work/"+workID, nil)
	if !strings.Contains(inbox.Body.String(), `"status":"running"`) || !strings.Contains(detail.Body.String(), `"status":"running"`) {
		t.Fatalf("running status diverged: inbox=%s detail=%s", inbox.Body.String(), detail.Body.String())
	}
	invalidSubmission, _ := json.Marshal(map[string]any{"submission": map[string]any{"answer": 7}})
	if _, err := handler.kernel.Execute(context.Background(), "dev", kernel.Command{CommandID: "cmd_taskrun_bad_submit", IdempotencyKey: "idem_taskrun_bad_submit", WorkspaceID: "wks_1", CommandType: "work.submit", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: invalidSubmission, Causation: kernel.Causation{Source: "api"}}); err == nil {
		t.Fatal("submission outside the Task output contract was accepted")
	}
	submit := kernel.Command{
		CommandID: "cmd_taskrun_submit", IdempotencyKey: "idem_taskrun_submit", WorkspaceID: "wks_1", CommandType: "work.submit",
		Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{},
		Payload: json.RawMessage(`{"submission":{"answer":"Because."}}`), Causation: kernel.Causation{Source: "api"},
	}
	response := request(t, handler, http.MethodPost, "/api/v1/kernel/commands", submit)
	if response.Code != http.StatusOK {
		t.Fatalf("human submission status=%d body=%s", response.Code, response.Body.String())
	}
	inbox = request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work", nil)
	if inbox.Code != http.StatusOK || strings.Contains(inbox.Body.String(), `"id":"`+workID+`"`) {
		t.Fatalf("submitted work remained in human inbox: status=%d body=%s", inbox.Code, inbox.Body.String())
	}
	var invocationID string
	if err := handler.store.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND command_id='cmd_taskrun_invoke' AND kind='function.requested'`, workID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	result, _ := json.Marshal(map[string]any{"invocation_id": invocationID, "callback_id": "cb_taskrun", "adapter_inbound_event_id": "cb_taskrun", "execution_id": "exec_taskrun", "provider_key": "research.answer", "provider_run_ref": "exec_taskrun", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"answer": "Because."}})
	if _, err := handler.store.DB().Exec(`INSERT INTO adapter_inbound_events VALUES('ain_taskrun','wks_1','conductor','cb_taskrun',?,?,'appended','2026-01-01T00:00:00Z')`, "sha256:"+strings.Repeat("0", 64), `{}`); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.kernel.Execute(context.Background(), "dev", kernel.Command{CommandID: "cmd_taskrun_result", IdempotencyKey: "conductor:cb_taskrun", WorkspaceID: "wks_1", CommandType: "function.record_result", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: result, Causation: kernel.Causation{Source: "adapter_reducer"}}); err == nil || !strings.Contains(err.Error(), "already terminal") {
		t.Fatalf("late Function callback was accepted: %v", err)
	}

	response = request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work/"+workID, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	data := body["data"].(map[string]any)
	if data["status"] != "completed" || data["owner_principal_id"] != "dev" || data["version"] != float64(7) {
		t.Fatalf("task run projection=%v", data)
	}
	if data["task"].(map[string]any)["revision_id"] != task.Aggregate.ID || len(data["submissions"].([]any)) != 1 || len(data["assignments"].([]any)) != 3 || len(data["handoffs"].([]any)) != 2 || len(data["executions"].([]any)) != 1 || data["completion"] == nil {
		t.Fatalf("task/submissions=%v/%v", data["task"], data["submissions"])
	}
	if data["executions"].([]any)[0].(map[string]any)["status"] != "running" {
		t.Fatalf("late Function callback changed execution=%v", data["executions"])
	}
	if _, err := handler.kernel.Execute(context.Background(), "dev", kernel.Command{
		CommandID: "cmd_taskrun_cancel", IdempotencyKey: "idem_taskrun_cancel", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"},
		CommandType: "work.cancel", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: json.RawMessage(`{"reason":"late"}`), Causation: kernel.Causation{Source: "api"},
	}); err == nil {
		t.Fatal("completed TaskRun accepted cancellation")
	}

	cancelled := execute("cmd_taskrun_cancel_open", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"question": "Stop?"}})
	cancelledID := cancelled.Aggregate.ID
	execute("cmd_taskrun_cancel_valid", "work.cancel", "work_stream", &cancelledID, map[string]any{"reason": "no longer needed"})
	response = request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/work/"+cancelledID, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("cancelled status=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["data"].(map[string]any)["status"] != "cancelled" {
		t.Fatalf("cancelled TaskRun projection=%v err=%v", body, err)
	}
}
