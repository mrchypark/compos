package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestWorldSpecJournalPurchaseApproval(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(s.DB())
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

	typeReceipt := execute(t, executor, "dev", command("cmd_type", "idem_type", "object_type.define", "object_type", nil, map[string]any{
		"key": "purchase_requests", "singular": "Purchase request", "plural": "Purchase requests",
		"field_schema": map[string]any{
			"type": "object", "additionalProperties": true,
			"state_machine": map[string]any{"initial": "requested", "transitions": map[string]any{"requested": []any{"approved"}, "approved": []any{}}},
		},
	}))
	_ = typeReceipt
	objectReceipt := execute(t, executor, "dev", command("cmd_object", "idem_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "purchase_requests", "title": "PR-481", "values": map[string]any{}}))
	objectID := objectReceipt.Aggregate.ID

	taskReceipt := execute(t, executor, "dev", command("cmd_task_spec", "idem_task_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "purchase-approval", "revision": 1, "kind": "task",
		"spec": taskSpec("Purchase approval", "Approve the purchase request", "approved"),
	}))
	functionReceipt := execute(t, executor, "dev", command("cmd_function_spec", "idem_function_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "purchase-request.approve", "revision": 1, "kind": "function",
		"spec": map[string]any{
			"name":          "Approve purchase request",
			"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}}},
			"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"id", "state_key", "version"}, "properties": map[string]any{"id": map[string]any{"type": "string"}, "state_key": map[string]any{"type": "string"}, "version": map[string]any{"type": "integer"}}},
			"binding":       map[string]any{"kind": "kernel_command", "command_type": "object.move", "to_state": "approved"},
		},
	}))
	mismatchedFunction := execute(t, executor, "dev", command("cmd_function_bad_output", "idem_function_bad_output", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "purchase-request.bad-output", "revision": 1, "kind": "function",
		"spec": map[string]any{
			"name": "Bad output", "input_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}}},
			"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"missing"}, "properties": map[string]any{"missing": map[string]any{"type": "string"}}},
			"binding":       map[string]any{"kind": "kernel_command", "command_type": "object.move", "to_state": "approved"},
		},
	}))

	workReceipt := execute(t, executor, "dev", command("cmd_work", "idem_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": taskReceipt.Aggregate.ID, "context": map[string]any{"object_id": objectID}}))
	workID := workReceipt.Aggregate.ID
	execute(t, executor, "dev", command("cmd_route_human", "idem_route_human", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	execute(t, executor, "dev", command("cmd_submit", "idem_submit", "work.submit", "work_stream", &workID, map[string]any{"submission": map[string]any{"decision": "approved"}}))
	execute(t, executor, "dev", command("cmd_route_auto", "idem_route_auto", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "automation", "id": "auto-approve"}}))

	invoke := command("cmd_invoke", "auto:invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": functionReceipt.Aggregate.ID, "input": map[string]any{"object_id": objectID, "expected_version": 1}})
	invoke.RequestedActor = &Actor{Type: "automation", ID: "auto-approve"}
	invoke.Causation.Source = "automation"
	invoke.Causation.Automation = json.RawMessage(`{"recursion_depth":0}`)
	if _, err := executor.Execute(ctx, "dev", invoke); err == nil || err.(*CommandError).Outcome != "forbidden" {
		t.Fatalf("route unexpectedly granted mutation authority: %v", err)
	}
	if _, err := s.DB().Exec(`INSERT INTO actor_capabilities(id,workspace_id,principal_id,actor_type,actor_id,command_type,causation_source,relation_definition_id,idempotency_key_prefix,max_automation_recursion_depth,created_at,updated_at) VALUES('cap_1','wks_1','dev','automation','auto-approve','function.invoke','automation','','auto:',2,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	badOutputInvoke := command("cmd_invoke_bad_output", "auto:invoke-bad-output", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": mismatchedFunction.Aggregate.ID, "input": map[string]any{"object_id": objectID, "expected_version": 1}})
	badOutputInvoke.RequestedActor, badOutputInvoke.Causation.Source, badOutputInvoke.Causation.Automation = &Actor{Type: "automation", ID: "auto-approve"}, "automation", json.RawMessage(`{"recursion_depth":0}`)
	if _, err := executor.Execute(ctx, "dev", badOutputInvoke); err == nil {
		t.Fatal("kernel Function output outside output_schema was committed")
	}
	invokeReceipt := execute(t, executor, "dev", invoke)
	if invokeReceipt.Aggregate.ID != objectID || invokeReceipt.Aggregate.Version != 2 {
		t.Fatalf("invoke aggregate=%+v", invokeReceipt.Aggregate)
	}
	duplicate := execute(t, executor, "dev", invoke)
	if duplicate.Outcome != "duplicate" {
		t.Fatalf("duplicate outcome=%s", duplicate.Outcome)
	}

	var state string
	var version int64
	if err := s.DB().QueryRow(`SELECT state_key,version FROM object_instances WHERE id=?`, objectID).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != "approved" || version != 2 {
		t.Fatalf("world state=%s version=%d", state, version)
	}
	rows, err := s.DB().Query(`SELECT stream_sequence,kind FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? ORDER BY stream_sequence`, workID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []string{"work.opened", "work.routed", "work.submitted", "work.routed", "function.requested", "function.succeeded", "work.satisfied"}
	var got []string
	for rows.Next() {
		var sequence int
		var kind string
		if err := rows.Scan(&sequence, &kind); err != nil {
			t.Fatal(err)
		}
		if sequence != len(got)+1 {
			t.Fatalf("journal sequence=%d want=%d", sequence, len(got)+1)
		}
		got = append(got, kind)
	}
	if len(got) != len(want) {
		t.Fatalf("journal kinds=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("journal kinds=%v", got)
		}
	}
	if _, err := s.DB().Exec(`UPDATE journal_records SET kind='work.cancelled' WHERE workspace_id='wks_1' AND stream_id=? AND stream_sequence=1`, workID); err == nil {
		t.Fatal("journal record update unexpectedly succeeded")
	}
	if _, err := s.DB().Exec(`UPDATE spec_revisions SET revision=2 WHERE id=?`, taskReceipt.Aggregate.ID); err == nil {
		t.Fatal("spec revision update unexpectedly succeeded")
	}

	unsatisfiedTask := execute(t, executor, "dev", command("cmd_task_unsatisfied", "idem_task_unsatisfied", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "purchase-remains-requested", "revision": 1, "kind": "task",
		"spec": taskSpec("Remain requested", "Keep the purchase request requested", "requested"),
	}))
	secondObject := execute(t, executor, "dev", command("cmd_object_2", "idem_object_2", "object.create", "object_instance", nil, map[string]any{"object_type_key": "purchase_requests", "title": "PR-482", "values": map[string]any{}}))
	secondWork := execute(t, executor, "dev", command("cmd_work_2", "idem_work_2", "work.open", "work_stream", nil, map[string]any{"task_revision_id": unsatisfiedTask.Aggregate.ID, "context": map[string]any{"object_id": secondObject.Aggregate.ID}}))
	secondWorkID := secondWork.Aggregate.ID
	execute(t, executor, "dev", command("cmd_route_auto_2", "idem_route_auto_2", "work.route", "work_stream", &secondWorkID, map[string]any{"participant": map[string]any{"kind": "automation", "id": "auto-approve"}}))
	invoke2 := command("cmd_invoke_2", "auto:invoke-2", "function.invoke", "work_stream", &secondWorkID, map[string]any{"function_revision_id": functionReceipt.Aggregate.ID, "input": map[string]any{"object_id": secondObject.Aggregate.ID, "expected_version": 1}})
	invoke2.RequestedActor = &Actor{Type: "automation", ID: "auto-approve"}
	invoke2.Causation.Source = "automation"
	invoke2.Causation.Automation = json.RawMessage(`{"recursion_depth":0}`)
	execute(t, executor, "dev", invoke2)
	var successes, satisfied int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='function.succeeded'`, secondWorkID).Scan(&successes); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='work.satisfied'`, secondWorkID).Scan(&satisfied); err != nil {
		t.Fatal(err)
	}
	if successes != 1 || satisfied != 0 {
		t.Fatalf("successful function must not imply task completion: succeeded=%d satisfied=%d", successes, satisfied)
	}
}

func TestKernelFunctionObjectUpdateUsesPinnedWorkObject(t *testing.T) {
	ctx := context.Background()
	backends := []struct {
		name string
		open func(*testing.T) *store.Store
	}{
		{name: "sqlite", open: func(t *testing.T) *store.Store {
			s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			now := "2026-01-01T00:00:00Z"
			if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
				t.Fatal(err)
			}
			if _, err := s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now); err != nil {
				t.Fatal(err)
			}
			return s
		}},
		{name: "rhiza", open: func(t *testing.T) *store.Store { return openRhizaKernelStore(t, ctx, "kernel-function-update") }},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.open(t)
			defer s.Close()
			executor := NewStoreExecutor(s)
			executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
			execute(t, executor, "dev", command("cmd_update_type", "idem_update_type", "object_type.define", "object_type", nil, map[string]any{
				"key": "requests", "singular": "Request", "plural": "Requests",
				"field_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"name"}, "properties": map[string]any{"name": map[string]any{"type": "string"}}, "state_machine": map[string]any{"initial": "requested", "transitions": map[string]any{"requested": []any{"approved"}, "approved": []any{}}}},
			}))
			object := execute(t, executor, "dev", command("cmd_update_object", "idem_update_object", "object.create", "object_instance", nil, map[string]any{"object_type_key": "requests", "title": "Before", "values": map[string]any{"name": "before"}}))
			other := execute(t, executor, "dev", command("cmd_update_other", "idem_update_other", "object.create", "object_instance", nil, map[string]any{"object_type_key": "requests", "title": "Other", "values": map[string]any{"name": "other"}}))
			function := execute(t, executor, "dev", command("cmd_update_function", "idem_update_function", "spec.publish", "spec_revision", nil, map[string]any{
				"family_id": "request.update", "revision": 1, "kind": "function",
				"spec": map[string]any{
					"name":          "Update request",
					"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id", "expected_version"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}, "expected_version": map[string]any{"type": "integer"}, "title": map[string]any{"type": "string"}, "values": map[string]any{"type": "object", "additionalProperties": true}}},
					"output_schema": map[string]any{"type": "object", "additionalProperties": true, "required": []any{"id", "title", "values", "version"}, "properties": map[string]any{"id": map[string]any{"type": "string"}, "title": map[string]any{"type": "string"}, "values": map[string]any{"type": "object", "additionalProperties": true}, "version": map[string]any{"type": "integer"}}},
					"binding":       map[string]any{"kind": "kernel_command", "command_type": "object.update"},
				},
			}))
			task := taskSpec("Update request", "Update the request", "approved")
			task["capability_policy"] = map[string]any{"allowed_function_revision_ids": []any{function.Aggregate.ID}}
			taskReceipt := execute(t, executor, "dev", command("cmd_update_task", "idem_update_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "request-update", "revision": 1, "kind": "task", "spec": task}))
			work := execute(t, executor, "dev", command("cmd_update_work", "idem_update_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": taskReceipt.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID}}))
			workID := work.Aggregate.ID
			execute(t, executor, "dev", command("cmd_update_route", "idem_update_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))

			wrong := command("cmd_update_wrong", "idem_update_wrong", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": other.Aggregate.ID, "expected_version": 1, "title": "Wrong"}})
			assertCommandOutcome(t, executor, wrong, "precondition_failed")
			invalid := command("cmd_update_invalid", "idem_update_invalid", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID, "expected_version": 1, "values": map[string]any{"name": 7}}})
			assertCommandOutcome(t, executor, invalid, "schema_mismatch")

			receipt := execute(t, executor, "dev", command("cmd_update_invoke", "idem_update_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID, "expected_version": 1, "title": "After", "values": map[string]any{"name": "after"}}}))
			if receipt.Aggregate.ID != object.Aggregate.ID || receipt.Aggregate.Version != 2 {
				t.Fatalf("update receipt=%+v", receipt.Aggregate)
			}
			stale := command("cmd_update_stale", "idem_update_stale", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{"object_id": object.Aggregate.ID, "expected_version": 1, "title": "Stale"}})
			assertCommandOutcome(t, executor, stale, "precondition_failed")
			var title, values string
			var version int64
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT title,payload_json,version FROM object_instances WHERE id=?`, object.Aggregate.ID).Scan(&title, &values, &version); err != nil {
				t.Fatal(err)
			}
			if title != "After" || values != `{"name":"after"}` || version != 2 {
				t.Fatalf("object title=%q values=%s version=%d", title, values, version)
			}
		})
	}
}

func TestExternalFunctionBindingsEmitPinnedConductorRequests(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now); err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(s.DB())
	executor.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

	execute(t, executor, "dev", command("cmd_type_external", "idem_type_external", "object_type.define", "object_type", nil, map[string]any{
		"key": "messages", "singular": "Message", "plural": "Messages", "field_schema": map[string]any{"type": "object"},
	}))
	object := execute(t, executor, "dev", command("cmd_object_external", "idem_object_external", "object.create", "object_instance", nil, map[string]any{
		"object_type_key": "messages", "title": "Welcome", "values": map[string]any{},
	}))
	externalTask := taskSpec("Send message", "Send one message", "sent")
	externalTask["failure_policy"] = map[string]any{"on_function_failed": map[string]any{
		"action": "route", "participant": map[string]any{"kind": "human", "id": "ops-reviewer"}, "reason": "Review final Function failure",
	}}
	task := execute(t, executor, "dev", command("cmd_task_external", "idem_task_external", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "send-message", "revision": 1, "kind": "task",
		"spec": externalTask,
	}))
	work := execute(t, executor, "dev", command("cmd_work_external", "idem_work_external", "work.open", "work_stream", nil, map[string]any{
		"task_revision_id": task.Aggregate.ID, "context": map[string]any{"object_id": object.Aggregate.ID},
	}))
	workID := work.Aggregate.ID
	execute(t, executor, "dev", command("cmd_route_external", "idem_route_external", "work.route", "work_stream", &workID, map[string]any{
		"participant": map[string]any{"kind": "human", "id": "dev"},
	}))

	inputSchema := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"recipient"}, "properties": map[string]any{"recipient": map[string]any{"type": "string"}}}
	event := execute(t, executor, "dev", command("cmd_message_event", "idem_message_event", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "message.sent", "revision": 1, "kind": "event",
		"spec": map[string]any{"name": "Message sent", "payload_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"message_id"}, "properties": map[string]any{"message_id": map[string]any{"type": "string"}}}},
	}))
	connector := execute(t, executor, "dev", command("cmd_connector_spec", "idem_connector_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "message.send", "revision": 1, "kind": "function",
		"spec": map[string]any{
			"name": "Send Gmail message", "input_schema": inputSchema, "output_schema": map[string]any{"type": "object"},
			"binding": map[string]any{"kind": "connector", "handler_version": "1", "provider": "gmail", "operation": "gmail.messages.send", "connection_id": "conn_1", "timeout_seconds": 30, "retry_policy": map[string]any{"max_attempts": 3, "backoff": "exponential"}}, "emits": []any{event.Aggregate.ID},
		},
	}))
	connectorInvoke := command("cmd_connector_invoke", "idem_connector_invoke", "function.invoke", "work_stream", &workID, map[string]any{
		"function_revision_id": connector.Aggregate.ID, "input": map[string]any{"recipient": "ops@example.com"},
	})
	connectorReceipt := execute(t, executor, "dev", connectorInvoke)
	if connectorReceipt.Aggregate.ID != workID || connectorReceipt.Aggregate.Version != 3 {
		t.Fatalf("connector receipt=%+v", connectorReceipt.Aggregate)
	}
	assertConductorOutbox(t, s.DB(), connectorInvoke.CommandID, "connector.invoke", "connector", "connector.invoke", "1", "gmail.messages.send")

	native := execute(t, executor, "dev", command("cmd_native_spec", "idem_native_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "message.render", "revision": 1, "kind": "function",
		"spec": map[string]any{
			"name": "Render message", "input_schema": inputSchema,
			"binding": map[string]any{"kind": "native", "handler_key": "message.render", "handler_version": "2", "operation": "render", "timeout_seconds": 10, "retry_policy": map[string]any{"max_attempts": 1}},
		},
	}))
	nativeInvoke := command("cmd_native_invoke", "idem_native_invoke", "function.invoke", "work_stream", &workID, map[string]any{
		"function_revision_id": native.Aggregate.ID, "input": map[string]any{"recipient": "ops@example.com"},
	})
	nativeReceipt := execute(t, executor, "dev", nativeInvoke)
	if nativeReceipt.Aggregate.ID != workID || nativeReceipt.Aggregate.Version != 4 {
		t.Fatalf("native receipt=%+v", nativeReceipt.Aggregate)
	}
	assertConductorOutbox(t, s.DB(), nativeInvoke.CommandID, "message.render", "native", "message.render", "2", "render")
	var connectorInvocation string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='function.requested'`, connectorInvoke.CommandID).Scan(&connectorInvocation); err != nil {
		t.Fatal(err)
	}
	result := command("cmd_connector_result", "conductor:cb_connector", "function.record_result", "work_stream", &workID, map[string]any{
		"invocation_id": connectorInvocation, "callback_id": "cb_connector", "adapter_inbound_event_id": "cb_connector", "execution_id": "exec_connector", "provider_key": "connector.invoke", "provider_run_ref": "exec_connector", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"message_id": "msg_1"},
	})
	insertConductorEvidence(t, s.DB(), "cb_connector", now)
	result.Causation.Source = "adapter_reducer"
	resultReceipt := execute(t, executor, "dev", result)
	if resultReceipt.Aggregate.Version != 6 || execute(t, executor, "dev", result).Outcome != "duplicate" {
		t.Fatalf("connector result receipt=%+v", resultReceipt)
	}

	var requested, terminal int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='function.requested'`, workID).Scan(&requested); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind IN ('function.succeeded','function.failed')`, workID).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if requested != 2 || terminal != 1 {
		t.Fatalf("external journal requested=%d terminal=%d", requested, terminal)
	}
	var emitted int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND kind='domain.event.emitted' AND spec_revision_id=?`, workID, event.Aggregate.ID).Scan(&emitted); err != nil || emitted != 1 {
		t.Fatalf("automatic domain events=%d err=%v", emitted, err)
	}
	badEventInvoke := command("cmd_connector_bad_event", "idem_connector_bad_event", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": connector.Aggregate.ID, "input": map[string]any{"recipient": "bad@example.com"}})
	execute(t, executor, "dev", badEventInvoke)
	var badInvocation string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='function.requested'`, badEventInvoke.CommandID).Scan(&badInvocation); err != nil {
		t.Fatal(err)
	}
	badResult := command("cmd_connector_bad_result", "conductor:cb_bad_event", "function.record_result", "work_stream", &workID, map[string]any{"invocation_id": badInvocation, "callback_id": "cb_bad_event", "adapter_inbound_event_id": "cb_bad_event", "execution_id": "exec_bad_event", "provider_key": "connector.invoke", "provider_run_ref": "exec_bad_event", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"message_id": 7}})
	insertConductorEvidence(t, s.DB(), "cb_bad_event", now)
	badResult.Causation.Source = "adapter_reducer"
	execute(t, executor, "dev", badResult)
	var failed int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND correlation_id=? AND kind='function.failed' AND json_extract(payload_json,'$.error_class')='event_schema_mismatch'`, workID, badInvocation).Scan(&failed); err != nil || failed != 1 {
		t.Fatalf("event mismatch failures=%d err=%v", failed, err)
	}
	var routedTo, reason string
	if err := s.DB().QueryRow(`SELECT participant_id,json_extract(payload_json,'$.reason') FROM journal_records WHERE stream_id=? AND kind='work.routed' ORDER BY stream_sequence DESC LIMIT 1`, workID).Scan(&routedTo, &reason); err != nil || routedTo != "ops-reviewer" || reason != "Review final Function failure" {
		t.Fatalf("failure policy route=%q reason=%q err=%v", routedTo, reason, err)
	}

	execute(t, executor, "dev", command("cmd_route_before_late_function", "idem_route_before_late_function", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
	lateInvoke := command("cmd_connector_late", "idem_connector_late", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": connector.Aggregate.ID, "input": map[string]any{"recipient": "late@example.com"}})
	execute(t, executor, "dev", lateInvoke)
	var lateInvocation string
	if err := s.DB().QueryRow(`SELECT correlation_id FROM journal_records WHERE command_id=? AND kind='function.requested'`, lateInvoke.CommandID).Scan(&lateInvocation); err != nil {
		t.Fatal(err)
	}
	execute(t, executor, "dev", command("cmd_cancel_before_function_result", "idem_cancel_before_function_result", "work.cancel", "work_stream", &workID, map[string]any{"reason": "no longer needed"}))
	lateResult := command("cmd_connector_late_result", "conductor:cb_late_function", "function.record_result", "work_stream", &workID, map[string]any{"invocation_id": lateInvocation, "callback_id": "cb_late_function", "adapter_inbound_event_id": "cb_late_function", "execution_id": "exec_late_function", "provider_key": "connector.invoke", "provider_run_ref": "exec_late_function", "handler_version": "1", "attempt_count": 1, "runtime_status": "succeeded", "output": map[string]any{"message_id": "too_late"}})
	insertConductorEvidence(t, s.DB(), "cb_late_function", now)
	lateResult.Causation.Source = "adapter_reducer"
	if _, err := executor.Execute(ctx, "dev", lateResult); err == nil || !strings.Contains(err.Error(), "already terminal") {
		t.Fatalf("late Function callback was accepted: %v", err)
	}
	var lateTerminal int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM journal_records WHERE stream_id=? AND correlation_id=? AND kind IN ('function.succeeded','function.failed')`, workID, lateInvocation).Scan(&lateTerminal); err != nil || lateTerminal != 0 {
		t.Fatalf("late Function terminal facts=%d err=%v", lateTerminal, err)
	}

	badComposite := command("cmd_composite_spec", "idem_composite_spec", "spec.publish", "spec_revision", nil, map[string]any{
		"family_id": "message.pipeline", "revision": 1, "kind": "function",
		"spec": map[string]any{"name": "Pipeline", "input_schema": inputSchema, "binding": map[string]any{"kind": "composite"}},
	})
	if _, err := executor.Execute(ctx, "dev", badComposite); err == nil || !strings.Contains(err.Error(), "workflow_revision_id") {
		t.Fatalf("composite binding was accepted without a pinned workflow: %v", err)
	}
}

func taskSpec(name, goal, state string) map[string]any {
	return map[string]any{
		"name": name, "goal": goal,
		"input_schema":  map[string]any{"type": "object", "additionalProperties": false, "required": []any{"object_id"}, "properties": map[string]any{"object_id": map[string]any{"type": "string"}}},
		"output_schema": map[string]any{"type": "object", "additionalProperties": true},
		"completion":    map[string]any{"kind": "object_state_equals", "state": state},
	}
}

func assertConductorOutbox(t *testing.T, db *sql.DB, commandID, eventType, bindingKind, handler, version, operation string) {
	t.Helper()
	var gotEvent, payload string
	if err := db.QueryRow(`SELECT event_type,payload_json FROM outbox_events WHERE command_id=?`, commandID).Scan(&gotEvent, &payload); err != nil {
		t.Fatal(err)
	}
	if gotEvent != eventType {
		t.Fatalf("outbox event=%s want=%s", gotEvent, eventType)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		t.Fatal(err)
	}
	compos, _ := data["_compos"].(map[string]any)
	if compos["handler_key"] != handler || compos["handler_version"] != version || compos["operation"] != operation {
		t.Fatalf("conductor context=%v", compos)
	}
	actor, _ := compos["actor"].(map[string]any)
	if actor["type"] != "user" || actor["id"] != "dev" {
		t.Fatalf("conductor actor=%v", actor)
	}
	var journalPayload string
	if err := db.QueryRow(`SELECT payload_json FROM journal_records WHERE command_id=? AND kind='function.requested'`, commandID).Scan(&journalPayload); err != nil {
		t.Fatal(err)
	}
	var journal map[string]any
	if err := json.Unmarshal([]byte(journalPayload), &journal); err != nil {
		t.Fatal(err)
	}
	if journal["binding_kind"] != bindingKind {
		t.Fatalf("journal binding=%v", journal["binding_kind"])
	}
}

func insertConductorEvidence(t *testing.T, db *sql.DB, callbackID, now string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, "ain_"+callbackID, "wks_1", "conductor", callbackID, "sha256:"+strings.Repeat("0", 64), `{}`, now); err != nil {
		t.Fatal(err)
	}
}

func execute(t *testing.T, executor *Executor, principal string, c Command) Receipt {
	t.Helper()
	receipt, err := executor.Execute(context.Background(), principal, c)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
