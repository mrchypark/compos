package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
)

func TestCompositeWorkflowReplaysToOneCompletion(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)

	execute("cmd_type", "object_type.define", "object_type", nil, map[string]any{
		"key": "requests", "singular": "Request", "plural": "Requests",
		"field_schema": map[string]any{"type": "object", "additionalProperties": true, "state_machine": map[string]any{"initial": "requested", "transitions": map[string]any{"requested": []any{"approved"}, "approved": []any{}}}},
	})
	object := execute("cmd_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "requests", "title": "Request", "values": map[string]any{}})
	task := execute("cmd_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "approve", "revision": 1, "kind": "task", "spec": workflowTaskSpec("Approve", "Approve the request", "approved")})
	event := execute("cmd_event", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "request.approved", "revision": 1, "kind": "event", "spec": map[string]any{"name": "Request approved", "payload_schema": map[string]any{"type": "object", "additionalProperties": true, "required": []any{"state_key"}, "properties": map[string]any{"state_key": map[string]any{"type": "string"}}}}})
	compositeEvent := execute("cmd_composite_event", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "request.approval.completed", "revision": 1, "kind": "event", "spec": map[string]any{"name": "Request approval completed", "payload_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"state"}, "properties": map[string]any{"state": map[string]any{"type": "string"}}}}})
	child := execute("cmd_child", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "request.approve", "revision": 1, "kind": "function",
		"spec": map[string]any{"name": "Approve request", "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}}}, "binding": map[string]any{"kind": "kernel_command", "command_type": "object.move", "to_state": "approved"}, "emits": []any{event.Aggregate.ID}},
	})
	workflow := execute("cmd_workflow", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "approve-flow", "revision": 1, "kind": "workflow", "spec": map[string]any{"name": "Approve flow", "steps": []any{map[string]any{
		"id": "decision", "kind": "branch", "condition": map[string]any{"left": "$.root.approve", "operator": "eq", "right": true},
		"then": []any{map[string]any{"id": "approve", "kind": "invoke", "function_revision_id": child.Aggregate.ID, "input_mapping": map[string]any{"object_id": "$.root.object_id", "expected_version": "$.root.expected_version"}, "output_mapping": map[string]any{"state": "$.output.state_key"}}}, "else": []any{},
	}}}})
	composite := execute("cmd_composite", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "request.approve-composite", "revision": 1, "kind": "function",
		"spec": map[string]any{"name": "Approve request workflow", "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version", "approve"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}, "approve": map[string]any{"type": "boolean"}}}, "output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"state"}, "properties": map[string]any{"state": map[string]any{"type": "string"}}}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID}, "emits": []any{compositeEvent.Aggregate.ID}},
	})
	work := execute("cmd_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}})
	workID := work.Aggregate.ID
	execute("cmd_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID, "expected_version": 1, "approve": true}})
	handler.wakeWorkflow()

	deadline := time.Now().Add(30 * time.Second)
	for {
		var completed int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='workflow.completed'`, workID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("workflow did not complete")
		}
		time.Sleep(10 * time.Millisecond)
	}
	handler.wakeWorkflow()
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}

	var state string
	if err := s.DB().QueryRow(`SELECT state_key FROM object_instances WHERE id=?`, object.Aggregate.ID).Scan(&state); err != nil || state != "approved" {
		t.Fatalf("state=%q err=%v", state, err)
	}
	for kind, want := range map[string]int{"workflow.started": 1, "workflow.completed": 1, "function.requested": 2, "function.succeeded": 2, "domain.event.emitted": 2, "work.satisfied": 1} {
		var got int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind=?`, workID, kind).Scan(&got); err != nil || got != want {
			t.Fatalf("%s=%d want=%d err=%v", kind, got, want, err)
		}
	}
	var terminal string
	if err := s.DB().QueryRow(`SELECT payload_json FROM journal_records WHERE stream_id=? AND kind='function.succeeded' AND spec_revision_id=?`, workID, composite.Aggregate.ID).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(terminal), &payload)
	if payload["output"].(map[string]any)["state"] != "approved" {
		t.Fatalf("mapped workflow output=%v", payload["output"])
	}
}

func TestParallelWorkflowFansOutAndJoinsMappedOutputs(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	execute("cmd_parallel_type", "object_type.define", "object_type", nil, map[string]any{"key": "jobs", "singular": "Job", "plural": "Jobs", "field_schema": map[string]any{"type": "object"}})
	object := execute("cmd_parallel_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "jobs", "title": "Job", "values": map[string]any{}})
	task := execute("cmd_parallel_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "parallel", "revision": 1, "kind": "task", "spec": workflowTaskSpec("Parallel", "Run both branches", "done")})
	function := func(id, label string) kernel.Receipt {
		return execute(id, "spec.publish", "spec_revision", nil, map[string]any{"family_id": "parallel." + label, "revision": 1, "kind": "function", "spec": map[string]any{
			"name": label, "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"label"}, "properties": map[string]any{"label": map[string]any{"type": "string"}}},
			"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"value"}, "properties": map[string]any{"value": map[string]any{"type": "string"}}},
			"binding":       map[string]any{"kind": "native", "handler_key": "test.echo", "handler_version": "1"},
		}})
	}
	left, right := function("cmd_parallel_left", "left"), function("cmd_parallel_right", "right")
	workflow := execute("cmd_parallel_workflow", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "parallel-flow", "revision": 1, "kind": "workflow", "spec": map[string]any{"name": "Parallel flow", "steps": []any{map[string]any{
		"id": "fanout", "kind": "parallel", "branches": []any{
			map[string]any{"id": "left", "steps": []any{map[string]any{"id": "call-left", "kind": "invoke", "function_revision_id": left.Aggregate.ID, "input_mapping": map[string]any{"label": "left"}}}},
			map[string]any{"id": "right", "steps": []any{map[string]any{"id": "call-right", "kind": "invoke", "function_revision_id": right.Aggregate.ID, "input_mapping": map[string]any{"label": "right"}}}},
		}, "output_mapping": map[string]any{"first": "$.output.left.value", "second": "$.output.right.value"},
	}}}})
	composite := execute("cmd_parallel_composite", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "parallel-composite", "revision": 1, "kind": "function", "spec": map[string]any{"name": "Parallel composite", "input_schema": map[string]any{"type": "object", "additionalProperties": false}, "output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"first", "second"}, "properties": map[string]any{"first": map[string]any{"type": "string"}, "second": map[string]any{"type": "string"}}}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID}}})
	work := execute("cmd_parallel_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}})
	workID := work.Aggregate.ID
	execute("cmd_parallel_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_parallel_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}})
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := s.DB().Query(`SELECT spec_revision_id,correlation_id FROM journal_records WHERE stream_id=? AND kind='function.requested' AND spec_revision_id IN (?,?)`, workID, left.Aggregate.ID, right.Aggregate.ID)
	if err != nil {
		t.Fatal(err)
	}
	invocations := map[string]string{}
	for rows.Next() {
		var specID, invocation string
		if err := rows.Scan(&specID, &invocation); err != nil {
			t.Fatal(err)
		}
		invocations[specID] = invocation
	}
	rows.Close()
	if len(invocations) != 2 {
		t.Fatalf("parallel fan-out requests=%v", invocations)
	}
	for specID, value := range map[string]string{left.Aggregate.ID: "L", right.Aggregate.ID: "R"} {
		if _, err := s.DB().Exec(`INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, "ain_"+value, "wks_1", "conductor", "cb_"+value, "sha256:"+strings.Repeat("0", 64), `{}`, "2026-01-01T00:00:00Z"); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(map[string]any{"invocation_id": invocations[specID], "callback_id": "cb_" + value, "adapter_inbound_event_id": "cb_" + value, "execution_id": "exec_" + value, "provider_key": "test.echo", "provider_run_ref": "exec_" + value, "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"value": value}})
		if _, err := handler.kernel.Execute(ctx, "dev", kernel.Command{CommandID: "cmd_cb_" + value, IdempotencyKey: "cb:" + value, WorkspaceID: "wks_1", CommandType: "function.record_result", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: raw, Causation: kernel.Causation{Source: "adapter_reducer"}}); err != nil {
			t.Fatal(err)
		}
	}
	handler.wakeWorkflow()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var terminal string
		err := s.DB().QueryRow(`SELECT payload_json FROM journal_records WHERE stream_id=? AND kind='function.succeeded' AND spec_revision_id=?`, workID, composite.Aggregate.ID).Scan(&terminal)
		if err == nil {
			var payload map[string]any
			_ = json.Unmarshal([]byte(terminal), &payload)
			output := payload["output"].(map[string]any)
			if output["first"] != "L" || output["second"] != "R" {
				t.Fatalf("parallel output=%v", output)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parallel workflow did not join: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWorkflowRouteWaitsForSubmissionWithoutAwaitNode(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	workflow := execute("cmd_route_workflow", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "human-route", "revision": 1, "kind": "workflow", "spec": map[string]any{
			"name": "Human route", "steps": []any{
				map[string]any{"id": "review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}},
			},
		},
	})
	composite := execute("cmd_route_composite", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "human-route-function", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Human route Function", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"},
			"binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID},
		},
	})
	task := execute("cmd_route_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "human-review", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Review", "goal": "Submit review", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object", "required": []any{"decision"}, "properties": map[string]any{"decision": map[string]any{"type": "string"}}},
			"completion":    map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_route_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}})
	workID := work.Aggregate.ID
	execute("cmd_route_owner", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_route_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}})
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	var completed int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='workflow.completed'`, workID).Scan(&completed)
	if completed != 0 {
		t.Fatal("human route completed before submission")
	}
	var routeStepID, routeID string
	if err := s.DB().QueryRow(`SELECT json_extract(payload_json,'$.workflow_step_id'),json_extract(payload_json,'$.workflow_route_id') FROM journal_records WHERE stream_id=? AND kind='work.routed' AND json_extract(payload_json,'$.workflow_route_id') IS NOT NULL ORDER BY stream_sequence DESC LIMIT 1`, workID).Scan(&routeStepID, &routeID); err != nil {
		t.Fatal(err)
	}
	execute("cmd_route_submit", "work.submit", "work_stream", &workID, map[string]any{"workflow_step_id": routeStepID, "workflow_route_id": routeID, "submission": map[string]any{"decision": "approved"}})
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='workflow.completed'`, workID).Scan(&completed)
	if completed != 1 {
		t.Fatalf("workflow completions=%d", completed)
	}
}

func TestWorkflowRouteUsesPinnedSchemaAndSubmittingHumanForNextInvoke(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)

	quoteType := execute("cmd_handoff_quote_type", "object_type.define", "object_type", nil, map[string]any{
		"key": "quotes", "singular": "Quote", "plural": "Quotes",
		"field_schema": map[string]any{"type": "object", "properties": map[string]any{"amount": map[string]any{"type": "number"}}},
	})
	quote := execute("cmd_handoff_quote", "object.create", "object_instance", nil, map[string]any{"object_type_key": "quotes", "title": "Draft quote", "values": map[string]any{"amount": 10}})
	child := execute("cmd_handoff_child", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "handoff-child", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Use review", "input_schema": map[string]any{"type": "object", "required": []any{"review"}, "properties": map[string]any{"review": map[string]any{"type": "string"}}},
			"binding": map[string]any{"kind": "native", "handler_key": "test.echo", "handler_version": "1"},
		},
	})
	workflow := execute("cmd_handoff_workflow", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "handoff-flow", "revision": 1, "kind": "workflow", "spec": map[string]any{
			"name": "Handoff", "steps": []any{
				map[string]any{"id": "review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "reviewer"}, "output_schema": map[string]any{"type": "object", "required": []any{"decision"}, "properties": map[string]any{"decision": map[string]any{"type": "string"}}}, "output_mapping": map[string]any{"review": "$.output.decision"}, "data_binding_ids": []any{"quote"}},
				map[string]any{"id": "use-review", "kind": "invoke", "function_revision_id": child.Aggregate.ID, "input_mapping": map[string]any{"review": "$.current.review"}},
			},
		},
	})
	composite := execute("cmd_handoff_composite", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "handoff-composite", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Handoff workflow", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"},
			"binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID},
		},
	})
	task := execute("cmd_handoff_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "handoff-task", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Handoff task", "goal": "Collect review", "input_schema": map[string]any{"type": "object", "required": []any{"object_id"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}}},
			"output_schema": map[string]any{"type": "object", "required": []any{"task_value"}, "properties": map[string]any{"task_value": map[string]any{"type": "string"}}}, "completion": map[string]any{"kind": "submission_valid"},
			"capability_policy": map[string]any{"allowed_function_revision_ids": []any{composite.Aggregate.ID, child.Aggregate.ID}},
			"data_bindings":     []any{map[string]any{"id": "quote", "object_type_id": quoteType.Aggregate.ID, "schema_version": 1, "context_key": "object_id", "writable_fields": []any{"amount"}, "allow_title": true, "allowed_operations": []any{"object.update"}}},
		},
	})
	work := execute("cmd_handoff_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": quote.Aggregate.ID}})
	workID := work.Aggregate.ID
	execute("cmd_handoff_owner", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_handoff_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}})
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	var stepID, routeID string
	if err := s.DB().QueryRow(`SELECT json_extract(payload_json,'$.workflow_step_id'),json_extract(payload_json,'$.workflow_route_id') FROM journal_records WHERE stream_id=? AND kind='work.routed' ORDER BY stream_sequence DESC LIMIT 1`, workID).Scan(&stepID, &routeID); err != nil {
		t.Fatal(err)
	}
	submit := func(id string, principal string, submission map[string]any, changes []any) error {
		payload := map[string]any{"workflow_step_id": stepID, "workflow_route_id": routeID, "submission": submission}
		if changes != nil {
			payload["changes"] = changes
		}
		raw, _ := json.Marshal(payload)
		_, err := handler.kernel.Execute(ctx, principal, kernel.Command{CommandID: id, IdempotencyKey: "idem_" + id, WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: principal}, CommandType: "work.submit", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: raw, Causation: kernel.Causation{Source: "api"}})
		return err
	}
	if err := submit("cmd_handoff_wrong_actor", "dev", map[string]any{"decision": "approved"}, nil); err == nil || !strings.Contains(err.Error(), "current work participant") {
		t.Fatalf("wrong actor submission error=%v", err)
	}
	misboundRouteID := routeID
	routeID = "cmd_wf_misbound"
	if err := submit("cmd_handoff_misbound", "reviewer", map[string]any{"decision": "approved"}, nil); err == nil || !strings.Contains(err.Error(), "no current participant") {
		t.Fatalf("misbound route submission error=%v", err)
	}
	routeID = misboundRouteID
	if err := submit("cmd_handoff_wrong_schema", "reviewer", map[string]any{"task_value": "wrong"}, nil); err == nil || !strings.Contains(err.Error(), "decision") {
		t.Fatalf("task schema bypassed route schema: %v", err)
	}
	changes := []any{map[string]any{
		"binding_id": "quote", "command_type": "object.update", "target": map[string]any{"aggregate_type": "object_instance", "aggregate_id": quote.Aggregate.ID},
		"preconditions": []any{map[string]any{"aggregate_type": "object_instance", "aggregate_id": quote.Aggregate.ID, "expected_version": 1}},
		"payload":       map[string]any{"title": "Approved quote", "values": map[string]any{"amount": 20}},
	}}
	if err := submit("cmd_handoff_submit", "reviewer", map[string]any{"decision": "approved"}, changes); err != nil {
		t.Fatal(err)
	}
	if err := submit("cmd_handoff_replay", "reviewer", map[string]any{"decision": "approved"}, nil); err == nil || !strings.Contains(err.Error(), "no current participant") {
		t.Fatalf("replayed route submission error=%v", err)
	}
	var quoteTitle, quoteValues string
	var quoteVersion int
	if err := s.DB().QueryRow(`SELECT title,payload_json,version FROM object_instances WHERE id=?`, quote.Aggregate.ID).Scan(&quoteTitle, &quoteValues, &quoteVersion); err != nil {
		t.Fatal(err)
	}
	var updatedValues map[string]any
	if err := json.Unmarshal([]byte(quoteValues), &updatedValues); err != nil {
		t.Fatal(err)
	}
	if quoteTitle != "Approved quote" || updatedValues["amount"] != float64(20) || quoteVersion != 2 {
		t.Fatalf("atomic quote update title=%q values=%v version=%d", quoteTitle, updatedValues, quoteVersion)
	}
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	var inputRaw string
	if err := s.DB().QueryRow(`SELECT payload_json FROM journal_records WHERE stream_id=? AND kind='function.requested' AND spec_revision_id=?`, workID, child.Aggregate.ID).Scan(&inputRaw); err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(inputRaw), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["input"].(map[string]any)["review"] != "approved" {
		t.Fatalf("next input=%v", payload["input"])
	}
	var invocationID, invokePrincipal string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='workflow.started'`, workID).Scan(&invocationID); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT principal_id FROM command_ledger WHERE command_id=?`, workflowCommandID(invocationID, "use-review", "invoke")).Scan(&invokePrincipal); err != nil || invokePrincipal != "reviewer" {
		t.Fatalf("next invoke principal=%q err=%v", invokePrincipal, err)
	}
}

func TestParallelWorkflowRoutesConvergeByStepIdentity(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	workflow := execute("cmd_parallel_route_workflow", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "parallel-human-route", "revision": 1, "kind": "workflow", "spec": map[string]any{
			"name": "Parallel review", "steps": []any{map[string]any{
				"id": "reviews", "kind": "parallel", "branches": []any{
					map[string]any{"id": "left", "steps": []any{map[string]any{"id": "left_review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}},
					map[string]any{"id": "right", "steps": []any{map[string]any{"id": "right_review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}},
				},
			}},
		},
	})
	composite := execute("cmd_parallel_route_composite", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "parallel-human-function", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Parallel human Function", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"},
			"binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID},
		},
	})
	task := execute("cmd_parallel_route_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "parallel-human-task", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Parallel human", "goal": "Collect both reviews", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object", "required": []any{"decision"}, "properties": map[string]any{"decision": map[string]any{"type": "string"}}},
			"completion":    map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_parallel_route_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}})
	workID := work.Aggregate.ID
	execute("cmd_parallel_route_owner", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	execute("cmd_parallel_route_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}})
	if err := handler.advanceWorkflows(ctx); err != nil {
		t.Fatal(err)
	}
	var routes int
	if err := s.DB().QueryRow(`SELECT COUNT(DISTINCT json_extract(payload_json,'$.workflow_step_id')) FROM journal_records WHERE stream_id=? AND kind='work.routed' AND json_extract(payload_json,'$.workflow_step_id') IS NOT NULL`, workID).Scan(&routes); err != nil || routes != 2 {
		t.Fatalf("workflow routes=%d err=%v", routes, err)
	}
	ambiguousPayload, _ := json.Marshal(map[string]any{"submission": map[string]any{"decision": "ambiguous"}})
	if _, err := handler.kernel.Execute(ctx, "dev", kernel.Command{CommandID: "cmd_parallel_route_ambiguous", IdempotencyKey: "idem_parallel_route_ambiguous", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: "work.submit", Target: kernel.Target{AggregateType: "work_stream", AggregateID: &workID}, Preconditions: []kernel.Precondition{}, Payload: ambiguousPayload, Causation: kernel.Causation{Source: "api"}}); err == nil || !strings.Contains(err.Error(), "workflow_route_id") {
		t.Fatalf("ambiguous Workflow submission error=%v", err)
	}
	for i, stepID := range []string{"left_review", "right_review"} {
		var routeID string
		if err := s.DB().QueryRow(`SELECT json_extract(payload_json,'$.workflow_route_id') FROM journal_records WHERE stream_id=? AND kind='work.routed' AND json_extract(payload_json,'$.workflow_step_id')=?`, workID, stepID).Scan(&routeID); err != nil {
			t.Fatal(err)
		}
		execute(fmt.Sprintf("cmd_parallel_route_submit_%d", i), "work.submit", "work_stream", &workID, map[string]any{"workflow_step_id": stepID, "workflow_route_id": routeID, "submission": map[string]any{"decision": stepID}})
		if err := handler.advanceWorkflows(ctx); err != nil {
			t.Fatal(err)
		}
		var completed int
		_ = s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='workflow.completed'`, workID).Scan(&completed)
		if completed != i {
			t.Fatalf("after submission %d workflow completions=%d", i+1, completed)
		}
	}
	var submittedSteps int
	if err := s.DB().QueryRow(`SELECT COUNT(DISTINCT json_extract(payload_json,'$.workflow_step_id')) FROM journal_records WHERE stream_id=? AND kind='work.submitted'`, workID).Scan(&submittedSteps); err != nil || submittedSteps != 2 {
		t.Fatalf("submitted step identities=%d err=%v", submittedSteps, err)
	}
}

func TestRepeatedWorkflowInvocationsKeepRouteSubmissionsIsolated(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	handler.workflowCancel()
	<-handler.workflowDone
	workflow := execute("cmd_repeated_route_workflow", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "repeated-route", "revision": 1, "kind": "workflow", "spec": map[string]any{
			"name": "Repeated review", "steps": []any{map[string]any{"id": "review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}},
		},
	})
	composite := execute("cmd_repeated_route_composite", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "repeated-route-function", "revision": 1, "kind": "function", "spec": map[string]any{
			"name": "Repeated route Function", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"},
			"binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID},
		},
	})
	task := execute("cmd_repeated_route_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "repeated-route-task", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Repeated routes", "goal": "Review twice", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object", "required": []any{"decision"}, "properties": map[string]any{"decision": map[string]any{"type": "string"}}},
			"completion":    map[string]any{"kind": "submission_valid"},
		},
	})
	work := execute("cmd_repeated_route_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}})
	workID := work.Aggregate.ID
	execute("cmd_repeated_route_owner", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	for i := 0; i < 2; i++ {
		execute(fmt.Sprintf("cmd_repeated_route_invoke_%d", i), "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{"run": i}})
		if err := handler.advanceWorkflows(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.DB().Query(`SELECT json_extract(payload_json,'$.workflow_route_id') FROM journal_records WHERE stream_id=? AND kind='work.routed' AND json_extract(payload_json,'$.workflow_step_id')='review' ORDER BY stream_sequence`, workID)
	if err != nil {
		t.Fatal(err)
	}
	var routeIDs []string
	for rows.Next() {
		var routeID string
		if err := rows.Scan(&routeID); err != nil {
			t.Fatal(err)
		}
		routeIDs = append(routeIDs, routeID)
	}
	rows.Close()
	if len(routeIDs) != 2 || routeIDs[0] == routeIDs[1] {
		t.Fatalf("workflow route IDs=%v", routeIDs)
	}
	invocationRows, err := s.DB().Query(`SELECT correlation_id FROM journal_records WHERE stream_id=? AND kind='workflow.started' ORDER BY stream_sequence`, workID)
	if err != nil {
		t.Fatal(err)
	}
	var invocations []string
	for invocationRows.Next() {
		var invocation string
		if err := invocationRows.Scan(&invocation); err != nil {
			t.Fatal(err)
		}
		invocations = append(invocations, invocation)
	}
	invocationRows.Close()
	if len(invocations) != 2 {
		t.Fatalf("workflow invocations=%v", invocations)
	}
	for i, routeID := range routeIDs {
		execute(fmt.Sprintf("cmd_repeated_route_submit_%d", i), "work.submit", "work_stream", &workID, map[string]any{"workflow_step_id": "review", "workflow_route_id": routeID, "submission": map[string]any{"decision": routeID}})
		for j, invocation := range invocations {
			ready, err := handler.workflowRouteSubmitted(ctx, workflowRun{workspaceID: "wks_1", streamID: workID, invocationID: invocation}, "review")
			if err != nil {
				t.Fatal(err)
			}
			if ready != (j <= i) {
				t.Fatalf("after route %d invocation %d ready=%v", i, j, ready)
			}
		}
	}
}

func TestSpecPublishRejectsReachableWorkflowFunctionCycle(t *testing.T) {
	ctx := context.Background()
	s, handler, _ := newWorkflowTestRuntime(t)
	workflowRaw, _ := json.Marshal(map[string]any{"name": "Cycle", "steps": []any{map[string]any{"id": "again", "kind": "invoke", "function_revision_id": "spc_cycle_function"}}})
	functionRaw, _ := json.Marshal(map[string]any{"name": "Cycle", "input_schema": map[string]any{"type": "object"}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": "spc_cycle_workflow"}})
	for _, values := range [][]any{
		{"spc_cycle_workflow", "cycle-workflow", "workflow", string(workflowRaw)},
		{"spc_cycle_function", "cycle-function", "function", string(functionRaw)},
	} {
		if _, err := s.DB().Exec(`INSERT INTO spec_revisions(id,workspace_id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id) VALUES(?,'wks_1',?,1,?,?,'sha256:test','2026-01-01T00:00:00Z','dev')`, values...); err != nil {
			t.Fatal(err)
		}
	}
	payload, _ := json.Marshal(map[string]any{"family_id": "cycle-entry", "revision": 1, "kind": "function", "spec": map[string]any{
		"name": "Cycle entry", "input_schema": map[string]any{"type": "object"}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": "spc_cycle_workflow"},
	}})
	_, err := handler.kernel.Execute(ctx, "dev", kernel.Command{CommandID: "cmd_cycle_entry", IdempotencyKey: "idem_cycle_entry", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: "spec.publish", Target: kernel.Target{AggregateType: "spec_revision"}, Preconditions: []kernel.Precondition{}, Payload: payload, Causation: kernel.Causation{Source: "api"}})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle publish error=%v", err)
	}
}

func TestWorkDeadlineSchedulerAppendsOneReactiveFact(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	task := execute("cmd_deadline_task", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "deadline", "revision": 1, "kind": "task", "spec": map[string]any{
			"name": "Deadline", "goal": "Finish before deadline", "input_schema": map[string]any{"type": "object"},
			"output_schema": map[string]any{"type": "object"}, "completion": map[string]any{"kind": "submission_valid"},
		},
	})
	deadline := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	work := execute("cmd_deadline_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}, "deadline_at": deadline})
	workID := work.Aggregate.ID
	execute("cmd_deadline_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}})
	if err := handler.advanceWorkDeadlines(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handler.advanceWorkDeadlines(ctx); err != nil {
		t.Fatal(err)
	}
	var facts, events int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='work.deadline.reached'`, workID).Scan(&facts)
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM activities WHERE aggregate_id=? AND event_type='work.deadline.reached'`, workID).Scan(&events)
	if facts != 1 || events != 1 {
		t.Fatalf("deadline facts=%d events=%d", facts, events)
	}
	view := request(t, handler, "GET", "/api/v1/workspaces/wks_1/work/"+workID, nil)
	if view.Code != 200 || !strings.Contains(view.Body.String(), `"deadline_reached":true`) || !strings.Contains(view.Body.String(), deadline) {
		t.Fatalf("view status=%d body=%s", view.Code, view.Body.String())
	}
}

func TestWorkflowSchedulerStopsWhenStoreCloses(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "closed.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	handler := newTestServer(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	handler.wakeWorkflow()
	select {
	case <-handler.workflowDone:
	case <-time.After(30 * time.Second):
		t.Fatal("workflow scheduler did not stop after its Store closed")
	}
}

type workflowTestExecute func(string, string, string, *string, any) kernel.Receipt

func workflowTaskSpec(name, goal, state string) map[string]any {
	return map[string]any{
		"name": name, "goal": goal,
		"input_schema":  map[string]any{"type": "object", "additionalProperties": true, "required": []any{"object_id"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}}},
		"output_schema": map[string]any{"type": "object", "additionalProperties": true},
		"completion":    map[string]any{"kind": "object_state_equals", "state": state},
	}
}

func newWorkflowTestRuntime(t *testing.T) (*store.Store, *Server, workflowTestExecute) {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "workflow.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	handler := newTestServer(s)
	t.Cleanup(func() { _ = handler.Close() })
	execute := func(id, commandType, aggregateType string, aggregateID *string, payload any) kernel.Receipt {
		t.Helper()
		raw, _ := json.Marshal(payload)
		receipt, err := handler.kernel.Execute(ctx, "dev", kernel.Command{CommandID: id, IdempotencyKey: "idem_" + id, WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: commandType, Target: kernel.Target{AggregateType: aggregateType, AggregateID: aggregateID}, Preconditions: []kernel.Precondition{}, Payload: raw, Causation: kernel.Causation{Source: "api"}})
		if err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	return s, handler, execute
}
