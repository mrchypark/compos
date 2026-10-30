package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (e *Executor) functionEventFactsRhiza(ctx context.Context, workspace, streamID, invocationID, objectID string, participant *Actor, functionSpec, output map[string]any, statements *[]rhiza.SQLStatement) ([]journalFact, error) {
	emits, _ := functionSpec["emits"].([]any)
	facts := make([]journalFact, 0, len(emits))
	for _, raw := range emits {
		eventID := raw.(string)
		spec, err := e.loadSpecRhiza(ctx, workspace, eventID, "event", statements)
		if err != nil {
			return nil, err
		}
		payloadSchema, _ := spec["payload_schema"].(map[string]any)
		if err := validateValues(output, payloadSchema, "function output event payload"); err != nil {
			return nil, err
		}
		subject := map[string]any{"work_stream_id": streamID}
		if objectID != "" {
			subject["object_id"] = objectID
		}
		payload := map[string]any{"event_revision_id": eventID, "data": output, "subject": subject}
		facts = append(facts, journalFact{streamID: streamID, kind: "domain.event.emitted", specRevision: eventID, participant: participant, correlationID: invocationID, causationID: invocationID, payload: payload})
	}
	return facts, nil
}

func (e *Executor) invokeFunctionRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	now := e.now().UTC()
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	workVersion, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	participant := commandParticipant(c, principal)
	routeID, _ := payload["workflow_route_id"].(string)
	if routeID != "" && c.Causation.Source != "workflow" {
		return Receipt{}, reject("forbidden", "workflow_route_id is reserved for Workflow invocations")
	}
	if _, _, err := e.requireCurrentParticipantRhiza(ctx, c.WorkspaceID, streamID, participant, "", routeID, false, &statements); err != nil {
		return Receipt{}, err
	}
	functionID, _ := payload["function_revision_id"].(string)
	functionSpec, err := e.loadSpecRhiza(ctx, c.WorkspaceID, functionID, "function", &statements)
	if err != nil {
		return Receipt{}, err
	}
	input, _ := payload["input"].(map[string]any)
	if err := validateValues(input, functionSpec["input_schema"].(map[string]any), "payload.input"); err != nil {
		return Receipt{}, err
	}
	taskID, contextValue, err := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	taskSpec, err := e.loadSpecRhiza(ctx, c.WorkspaceID, taskID, "task", &statements)
	if err != nil {
		return Receipt{}, err
	}
	if policy, ok := taskSpec["capability_policy"].(map[string]any); ok {
		permitted := false
		for _, raw := range anySlice(policy["allowed_function_revision_ids"]) {
			permitted = permitted || raw == functionID
		}
		if !permitted {
			return Receipt{}, reject("forbidden", "function is not allowed by the pinned task capability policy")
		}
	}
	objectID, hasObject := input["object_id"].(string)
	if hasObject && contextValue["object_id"] != objectID {
		return Receipt{}, reject("precondition_failed", "function input object does not match work context")
	}
	binding, _ := functionSpec["binding"].(map[string]any)
	kind, _ := binding["kind"].(string)
	if kind == "composite" {
		workflowID, _ := binding["workflow_revision_id"].(string)
		if _, err := e.loadSpecRhiza(ctx, c.WorkspaceID, workflowID, "workflow", &statements); err != nil {
			return Receipt{}, err
		}
		invocationID := aggregateID("inv", c.CommandID)
		eventPayload := map[string]any{"function_revision_id": functionID, "workflow_revision_id": workflowID, "input": input, "status": "started"}
		effect := makeEffect(streamID, "work_stream", workVersion+1, "workflow.started", eventPayload)
		effect.journal = []journalFact{
			{streamID: streamID, kind: "function.requested", specRevision: functionID, participant: participant, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "input": input, "binding_kind": kind}},
			{streamID: streamID, kind: "workflow.started", specRevision: workflowID, participant: participant, correlationID: invocationID, payload: eventPayload},
		}
		return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(now))
	}
	if kind == "connector" || kind == "native" {
		effect, err := requestExternalFunction(streamID, workVersion, taskID, functionID, kind, binding, input, contextValue, participant, c)
		if err != nil {
			return Receipt{}, err
		}
		return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(now))
	}
	expectedVersion, ok := integer(input["expected_version"])
	if strings.TrimSpace(objectID) == "" || !ok || expectedVersion < 1 {
		return Receipt{}, reject("schema_mismatch", "kernel function input requires object_id and positive expected_version")
	}
	commandType, _ := binding["command_type"].(string)
	innerPayload := map[string]any{}
	switch commandType {
	case "object.move":
		innerPayload["to_state"] = binding["to_state"]
		if force, ok := binding["force"].(bool); ok {
			innerPayload["force"] = force
		}
	case "object.update":
		if title, exists := input["title"]; exists {
			value, ok := title.(string)
			if !ok || strings.TrimSpace(value) == "" {
				return Receipt{}, reject("schema_mismatch", "kernel object.update input.title must be a non-empty string")
			}
			innerPayload["title"] = value
		}
		if values, exists := input["values"]; exists {
			value, ok := values.(map[string]any)
			if !ok {
				return Receipt{}, reject("schema_mismatch", "kernel object.update input.values must be an object")
			}
			innerPayload["values"] = value
		}
		if len(innerPayload) == 0 {
			return Receipt{}, reject("schema_mismatch", "kernel object.update input requires title or values")
		}
	default:
		return Receipt{}, reject("schema_mismatch", "unsupported kernel function binding")
	}
	innerRaw, _ := json.Marshal(innerPayload)
	inner := Command{WorkspaceID: c.WorkspaceID, CommandType: commandType, Target: Target{AggregateType: "object_instance", AggregateID: &objectID}, Preconditions: []Precondition{{AggregateType: "object_instance", AggregateID: objectID, ExpectedVersion: expectedVersion}}, Payload: innerRaw}
	var worldEffect effect
	if commandType == "object.move" {
		worldEffect, statements, err = e.planMoveObjectRhiza(ctx, principal, inner, innerPayload, statements, now)
	} else {
		worldEffect, statements, err = e.planUpdateObjectRhiza(ctx, principal, inner, innerPayload, statements, now)
	}
	if err != nil {
		return Receipt{}, err
	}
	if worldEffect.aggregate.Version-1 != expectedVersion {
		return Receipt{}, reject("precondition_failed", "precondition version does not match")
	}
	invocationID := aggregateID("inv", c.CommandID)
	functionOutput := make(map[string]any, len(worldEffect.payload))
	for key, value := range worldEffect.payload {
		functionOutput[key] = value
	}
	if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
		if err := validateValues(functionOutput, outputSchema, "function output"); err != nil {
			return Receipt{}, err
		}
	}
	facts := []journalFact{
		{streamID: streamID, kind: "function.requested", specRevision: functionID, participant: participant, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "input": input}},
		{streamID: streamID, kind: "function.succeeded", specRevision: functionID, participant: participant, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "output": functionOutput}},
	}
	eventFacts, err := e.functionEventFactsRhiza(ctx, c.WorkspaceID, streamID, invocationID, objectID, participant, functionSpec, functionOutput, &statements)
	if err != nil {
		return Receipt{}, err
	}
	facts = append(facts, eventFacts...)
	if c.Causation.Source != "workflow" {
		completion, _ := taskSpec["completion"].(map[string]any)
		if completion["kind"] == "object_state_equals" && completion["state"] == worldEffect.payload["state_key"] {
			facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: invocationID, payload: map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "object_id": objectID, "object_version": worldEffect.aggregate.Version}})
		}
	}
	worldEffect.payload["work_stream_id"], worldEffect.payload["function_revision_id"], worldEffect.payload["invocation_id"] = streamID, functionID, invocationID
	worldEffect.journal = facts
	return e.commitRhiza(ctx, principal, actor, c, statements, worldEffect, timestamp(now))
}

type externalResult struct {
	invocationID, callbackID, adapterEventID, executionID string
	providerKey, providerRunRef, handlerVersion           string
	status, errorClass, errorDetail                       string
	attemptCount                                          int64
	output                                                map[string]any
}

func parseExternalResult(payload map[string]any) (externalResult, error) {
	result := externalResult{}
	result.invocationID, _ = payload["invocation_id"].(string)
	result.callbackID, _ = payload["callback_id"].(string)
	result.adapterEventID, _ = payload["adapter_inbound_event_id"].(string)
	result.executionID, _ = payload["execution_id"].(string)
	result.providerKey, _ = payload["provider_key"].(string)
	result.providerRunRef, _ = payload["provider_run_ref"].(string)
	result.handlerVersion, _ = payload["handler_version"].(string)
	result.attemptCount, _ = integer(payload["attempt_count"])
	result.status, _ = payload["runtime_status"].(string)
	result.errorClass, _ = payload["error_class"].(string)
	result.errorDetail, _ = payload["error_detail"].(string)
	result.output, _ = payload["output"].(map[string]any)
	if result.output == nil {
		result.output = map[string]any{}
	}
	if strings.TrimSpace(result.invocationID) == "" || strings.TrimSpace(result.callbackID) == "" || result.adapterEventID != result.callbackID || strings.TrimSpace(result.executionID) == "" || strings.TrimSpace(result.providerKey) == "" || strings.TrimSpace(result.providerRunRef) == "" || strings.TrimSpace(result.handlerVersion) == "" || result.attemptCount < 1 {
		return externalResult{}, reject("schema_mismatch", "external result requires bound callback, execution, provider, and positive attempt")
	}
	return result, nil
}

func (e *Executor) failurePolicyFactsRhiza(ctx context.Context, workspace, streamID, taskID, policyName, invocationID, causationID string, statements *[]rhiza.SQLStatement) ([]journalFact, error) {
	zero := int64(0)
	var resolved int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, workspace, streamID).Scan(&resolved); err != nil {
		return nil, err
	}
	if resolved != 0 {
		return nil, nil
	}
	*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, Args: []any{workspace, streamID}, WantRows: true, ExpectedReturnedRows: &zero})
	task, err := e.loadSpecRhiza(ctx, workspace, taskID, "task", statements)
	if err != nil {
		return nil, err
	}
	policy, _ := task["failure_policy"].(map[string]any)
	action, _ := policy[policyName].(map[string]any)
	if action == nil {
		return nil, nil
	}
	reason, _ := action["reason"].(string)
	if strings.TrimSpace(reason) == "" {
		reason = "Execution reached a final failure"
	}
	payload := map[string]any{"reason": reason, "failed_invocation_id": invocationID}
	if action["action"] == "cancel" {
		return []journalFact{{streamID: streamID, kind: "work.cancelled", specRevision: taskID, causationID: causationID, payload: payload}}, nil
	}
	participant, err := participantFrom(action["participant"])
	if err != nil {
		return nil, err
	}
	payload["participant"] = map[string]any{"kind": participant.Type, "id": participant.ID}
	return []journalFact{{streamID: streamID, kind: "work.routed", specRevision: taskID, participant: participant, causationID: causationID, payload: payload}}, nil
}

func (e *Executor) taskSatisfiedRhiza(ctx context.Context, workspace, streamID, taskID, objectID string, statements *[]rhiza.SQLStatement) (bool, error) {
	task, err := e.loadSpecRhiza(ctx, workspace, taskID, "task", statements)
	if err != nil {
		return false, err
	}
	completion, _ := task["completion"].(map[string]any)
	switch completion["kind"] {
	case "object_state_equals":
		var state sql.NullString
		var version int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT state_key,version FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, objectID).Scan(&state, &version); err != nil {
			return false, err
		}
		one := int64(1)
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL AND state_key IS ? AND version=?`, Args: []any{workspace, objectID, nullable(state.String), version}, WantRows: true, ExpectedReturnedRows: &one})
		return state.String == completion["state"], nil
	case "submission_valid":
		rows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.submitted'`, workspace, streamID)
		if err != nil {
			return false, err
		}
		count := int64(0)
		for rows.Next() {
			count++
		}
		_ = rows.Close()
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.submitted'`, Args: []any{workspace, streamID}, WantRows: true, ExpectedReturnedRows: &count})
		return count != 0, nil
	}
	return false, reject("schema_mismatch", "unsupported task completion policy")
}

func (e *Executor) recordFunctionResultRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	result, err := parseExternalResult(payload)
	if err != nil {
		return Receipt{}, err
	}
	var requestID, requestCommandID, functionID, participantKind, participantID, requestCausation, handlerKey, pinnedHandlerVersion string
	var requestSequence int64
	err = e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT j.id,j.command_id,j.stream_sequence,COALESCE(j.spec_revision_id,''),COALESCE(j.participant_kind,''),COALESCE(j.participant_id,''),COALESCE(json_extract(c.causation_json,'$.source'),''),COALESCE(json_extract(j.payload_json,'$.handler_key'),''),COALESCE(json_extract(j.payload_json,'$.handler_version'),'') FROM journal_records j JOIN command_ledger c ON c.workspace_id=j.workspace_id AND c.command_id=j.command_id WHERE j.workspace_id=? AND j.stream_id=? AND j.correlation_id=? AND j.kind='function.requested' ORDER BY j.stream_sequence LIMIT 1`, c.WorkspaceID, streamID, result.invocationID).Scan(&requestID, &requestCommandID, &requestSequence, &functionID, &participantKind, &participantID, &requestCausation, &handlerKey, &pinnedHandlerVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, reject("precondition_failed", "function invocation does not exist")
	}
	if err != nil {
		return Receipt{}, err
	}
	if result.providerKey != handlerKey || result.handlerVersion != pinnedHandlerVersion {
		return Receipt{}, reject("precondition_failed", "external result does not match the pinned handler identity")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records j JOIN command_ledger c ON c.workspace_id=j.workspace_id AND c.command_id=j.command_id WHERE j.workspace_id=? AND j.id=? AND j.command_id=? AND j.stream_id=? AND j.stream_sequence=? AND j.correlation_id=? AND j.kind='function.requested' AND COALESCE(j.spec_revision_id,'')=? AND COALESCE(j.participant_kind,'')=? AND COALESCE(j.participant_id,'')=? AND COALESCE(json_extract(c.causation_json,'$.source'),'')=? AND COALESCE(json_extract(j.payload_json,'$.handler_key'),'')=? AND COALESCE(json_extract(j.payload_json,'$.handler_version'),'')=?`, Args: []any{c.WorkspaceID, requestID, requestCommandID, streamID, requestSequence, result.invocationID, functionID, participantKind, participantID, requestCausation, handlerKey, pinnedHandlerVersion}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('function.succeeded','function.failed')`, Args: []any{c.WorkspaceID, streamID, result.invocationID}, WantRows: true, ExpectedReturnedRows: &zero},
		rhiza.SQLStatement{SQL: `UPDATE adapter_inbound_events SET status='reduced' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, Args: []any{c.WorkspaceID, result.adapterEventID}, ExpectedRowsAffected: &one},
	)
	resultDigest := externalResultDigest(result.status, result.output, result.errorClass, result.errorDetail)
	var taskID, objectID string
	var functionSpec map[string]any
	if result.status == "succeeded" {
		var contextValue map[string]any
		taskID, contextValue, err = e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
		if err != nil {
			return Receipt{}, err
		}
		objectID, _ = contextValue["object_id"].(string)
		functionSpec, err = e.loadSpecRhiza(ctx, c.WorkspaceID, functionID, "function", &statements)
		if err != nil {
			return Receipt{}, err
		}
		if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
			if err := validateValues(result.output, outputSchema, "payload.output"); err != nil {
				result.status, result.errorClass, result.errorDetail = "failed", "output_schema_mismatch", err.Error()
			}
		}
	}
	participant := &Actor{Type: participantKind, ID: participantID}
	var eventFacts []journalFact
	if result.status == "succeeded" {
		eventFacts, err = e.functionEventFactsRhiza(ctx, c.WorkspaceID, streamID, result.invocationID, objectID, participant, functionSpec, result.output, &statements)
		if err != nil {
			result.status, result.errorClass, result.errorDetail, eventFacts = "failed", "event_schema_mismatch", err.Error(), nil
		}
	}
	kind := "function.succeeded"
	eventPayload := map[string]any{"function_revision_id": functionID, "invocation_id": result.invocationID, "callback_id": result.callbackID, "runtime_status": result.status, "output": result.output, "execution_id": result.executionID, "provider_key": result.providerKey, "provider_run_ref": result.providerRunRef, "handler_version": result.handlerVersion, "attempt_count": result.attemptCount, "result_digest": resultDigest}
	if result.status != "succeeded" {
		kind = "function.failed"
		eventPayload["error_class"], eventPayload["error_detail"] = result.errorClass, result.errorDetail
	}
	facts := []journalFact{{streamID: streamID, kind: kind, specRevision: functionID, participant: participant, correlationID: result.invocationID, causationID: result.callbackID, payload: eventPayload}}
	facts = append(facts, eventFacts...)
	if result.status != "succeeded" {
		if failedTaskID, _, taskErr := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements); taskErr == nil {
			policy, err := e.failurePolicyFactsRhiza(ctx, c.WorkspaceID, streamID, failedTaskID, "on_function_failed", result.invocationID, result.callbackID, &statements)
			if err != nil {
				return Receipt{}, err
			}
			facts = append(facts, policy...)
		}
	}
	if result.status == "succeeded" && requestCausation != "workflow" {
		var resolved int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, c.WorkspaceID, streamID).Scan(&resolved); err != nil {
			return Receipt{}, err
		}
		if resolved == 0 {
			statements = append(statements, rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, Args: []any{c.WorkspaceID, streamID}, WantRows: true, ExpectedReturnedRows: &zero})
			satisfied, err := e.taskSatisfiedRhiza(ctx, c.WorkspaceID, streamID, taskID, objectID, &statements)
			if err != nil {
				return Receipt{}, err
			}
			if satisfied {
				proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID}
				if objectID != "" {
					var objectVersion int64
					if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_instances WHERE workspace_id=? AND id=?`, c.WorkspaceID, objectID).Scan(&objectVersion); err != nil {
						return Receipt{}, err
					}
					proof["object_id"], proof["object_version"] = objectID, objectVersion
				}
				facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: result.callbackID, payload: proof})
			}
		}
	}
	effect := makeEffect(streamID, "work_stream", version+int64(len(facts))-1, kind, eventPayload)
	effect.journal = facts
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) recordAgentResultRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	result, err := parseExternalResult(payload)
	if err != nil {
		return Receipt{}, err
	}
	var requestID, agentID, participantKind, participantID, handlerKey, pinnedHandlerVersion, stepID, routeID string
	var requestSequence int64
	err = e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,stream_sequence,COALESCE(spec_revision_id,''),COALESCE(participant_kind,''),COALESCE(participant_id,''),COALESCE(json_extract(payload_json,'$.handler_key'),''),COALESCE(json_extract(payload_json,'$.handler_version'),''),COALESCE(json_extract(payload_json,'$.workflow_step_id'),''),COALESCE(json_extract(payload_json,'$.workflow_route_id'),'') FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind='agent.requested' ORDER BY stream_sequence LIMIT 1`, c.WorkspaceID, streamID, result.invocationID).Scan(&requestID, &requestSequence, &agentID, &participantKind, &participantID, &handlerKey, &pinnedHandlerVersion, &stepID, &routeID)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, reject("precondition_failed", "agent invocation does not exist")
	}
	if err != nil {
		return Receipt{}, err
	}
	if result.providerKey != handlerKey || result.handlerVersion != pinnedHandlerVersion {
		return Receipt{}, reject("precondition_failed", "external result does not match the pinned handler identity")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND correlation_id=? AND kind='agent.requested' AND COALESCE(spec_revision_id,'')=? AND COALESCE(participant_kind,'')=? AND COALESCE(participant_id,'')=? AND COALESCE(json_extract(payload_json,'$.handler_key'),'')=? AND COALESCE(json_extract(payload_json,'$.handler_version'),'')=? AND COALESCE(json_extract(payload_json,'$.workflow_step_id'),'')=? AND COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')=?`, Args: []any{c.WorkspaceID, requestID, streamID, requestSequence, result.invocationID, agentID, participantKind, participantID, handlerKey, pinnedHandlerVersion, stepID, routeID}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('agent.succeeded','agent.failed')`, Args: []any{c.WorkspaceID, streamID, result.invocationID}, WantRows: true, ExpectedReturnedRows: &zero},
		rhiza.SQLStatement{SQL: `UPDATE adapter_inbound_events SET status='reduced' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, Args: []any{c.WorkspaceID, result.adapterEventID}, ExpectedRowsAffected: &one},
	)
	resultDigest := externalResultDigest(result.status, result.output, result.errorClass, result.errorDetail)
	taskID, contextValue, err := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	task, err := e.loadSpecRhiza(ctx, c.WorkspaceID, taskID, "task", &statements)
	if err != nil {
		return Receipt{}, err
	}
	if result.status == "succeeded" {
		outputSchema, err := e.workflowSubmissionSchemaRhiza(ctx, c.WorkspaceID, streamID, task, stepID, routeID, &statements)
		if err != nil {
			return Receipt{}, err
		}
		if err := validateValues(result.output, outputSchema, "payload.output"); err != nil {
			result.status, result.errorClass, result.errorDetail = "failed", "output_schema_mismatch", err.Error()
		}
	}
	participant := &Actor{Type: participantKind, ID: participantID}
	kind := "agent.succeeded"
	eventPayload := map[string]any{"agent_revision_id": agentID, "invocation_id": result.invocationID, "callback_id": result.callbackID, "runtime_status": result.status, "output": result.output, "execution_id": result.executionID, "provider_key": result.providerKey, "provider_run_ref": result.providerRunRef, "handler_version": result.handlerVersion, "attempt_count": result.attemptCount, "result_digest": resultDigest}
	facts := []journalFact{}
	if result.status == "succeeded" {
		submission := map[string]any{"submission": result.output}
		if stepID != "" {
			submission["workflow_step_id"], submission["workflow_route_id"] = stepID, routeID
		}
		facts = append(facts,
			journalFact{streamID: streamID, kind: kind, specRevision: agentID, participant: participant, correlationID: result.invocationID, causationID: result.callbackID, payload: eventPayload},
			journalFact{streamID: streamID, kind: "work.submitted", specRevision: taskID, participant: participant, correlationID: result.invocationID, causationID: result.callbackID, payload: submission},
		)
		if task["completion"].(map[string]any)["kind"] == "submission_valid" && stepID == "" {
			proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "submission_sequence": version + 1}
			if objectID, _ := contextValue["object_id"].(string); objectID != "" {
				proof["object_id"] = objectID
			}
			facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, participant: participant, correlationID: result.invocationID, causationID: result.callbackID, payload: proof})
		}
	} else {
		kind = "agent.failed"
		eventPayload["error_class"], eventPayload["error_detail"] = result.errorClass, result.errorDetail
		facts = append(facts, journalFact{streamID: streamID, kind: kind, specRevision: agentID, participant: participant, correlationID: result.invocationID, causationID: result.callbackID, payload: eventPayload})
		policy, err := e.failurePolicyFactsRhiza(ctx, c.WorkspaceID, streamID, taskID, "on_agent_failed", result.invocationID, result.callbackID, &statements)
		if err != nil {
			return Receipt{}, err
		}
		facts = append(facts, policy...)
	}
	effect := makeEffect(streamID, "work_stream", version+int64(len(facts))-1, kind, eventPayload)
	effect.journal = facts
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) completeWorkflowRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	if c.Causation.Source != "workflow" {
		return Receipt{}, reject("forbidden", "workflow completion requires workflow causation")
	}
	streamID := *c.Target.AggregateID
	invocationID, _ := payload["invocation_id"].(string)
	status, _ := payload["runtime_status"].(string)
	output, _ := payload["output"].(map[string]any)
	if strings.TrimSpace(invocationID) == "" || output == nil {
		return Receipt{}, reject("schema_mismatch", "workflow completion requires invocation_id and output")
	}
	var functionRecordID, workflowRecordID, functionID, workflowID, participantKind, participantID string
	var functionSequence, workflowSequence int64
	err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT f.id,w.id,f.stream_sequence,w.stream_sequence,f.spec_revision_id,w.spec_revision_id,COALESCE(f.participant_kind,''),COALESCE(f.participant_id,'') FROM journal_records f JOIN journal_records w ON w.workspace_id=f.workspace_id AND w.stream_id=f.stream_id AND w.correlation_id=f.correlation_id AND w.kind='workflow.started' WHERE f.workspace_id=? AND f.stream_id=? AND f.correlation_id=? AND f.kind='function.requested'`, c.WorkspaceID, streamID, invocationID).Scan(&functionRecordID, &workflowRecordID, &functionSequence, &workflowSequence, &functionID, &workflowID, &participantKind, &participantID)
	if err != nil {
		return Receipt{}, reject("precondition_failed", "workflow invocation does not exist")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records f JOIN journal_records w ON w.workspace_id=f.workspace_id AND w.stream_id=f.stream_id AND w.correlation_id=f.correlation_id AND w.kind='workflow.started' WHERE f.workspace_id=? AND f.id=? AND w.id=? AND f.stream_id=? AND f.correlation_id=? AND f.kind='function.requested' AND f.stream_sequence=? AND w.stream_sequence=? AND f.spec_revision_id=? AND w.spec_revision_id=? AND COALESCE(f.participant_kind,'')=? AND COALESCE(f.participant_id,'')=?`, Args: []any{c.WorkspaceID, functionRecordID, workflowRecordID, streamID, invocationID, functionSequence, workflowSequence, functionID, workflowID, participantKind, participantID}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('workflow.completed','workflow.failed')`, Args: []any{c.WorkspaceID, streamID, invocationID}, WantRows: true, ExpectedReturnedRows: &zero},
	)
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	participant := &Actor{Type: participantKind, ID: participantID}
	taskID, contextValue, err := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	objectID, _ := contextValue["object_id"].(string)
	var eventFacts []journalFact
	if status == "succeeded" {
		functionSpec, err := e.loadSpecRhiza(ctx, c.WorkspaceID, functionID, "function", &statements)
		if err != nil {
			return Receipt{}, err
		}
		if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
			if err := validateValues(output, outputSchema, "function output"); err != nil {
				status, payload["error_detail"] = "failed", err.Error()
			}
		}
		if status == "succeeded" {
			eventFacts, err = e.functionEventFactsRhiza(ctx, c.WorkspaceID, streamID, invocationID, objectID, participant, functionSpec, output, &statements)
			if err != nil {
				status, payload["error_detail"], eventFacts = "failed", err.Error(), nil
			}
		}
	}
	workflowKind, functionKind := "workflow.completed", "function.succeeded"
	if status != "succeeded" {
		workflowKind, functionKind = "workflow.failed", "function.failed"
	}
	eventPayload := map[string]any{"workflow_revision_id": workflowID, "function_revision_id": functionID, "invocation_id": invocationID, "runtime_status": status, "output": output}
	if detail, _ := payload["error_detail"].(string); detail != "" {
		eventPayload["error_detail"] = detail
	}
	facts := []journalFact{
		{streamID: streamID, kind: workflowKind, specRevision: workflowID, participant: participant, correlationID: invocationID, payload: eventPayload},
		{streamID: streamID, kind: functionKind, specRevision: functionID, participant: participant, correlationID: invocationID, payload: eventPayload},
	}
	facts = append(facts, eventFacts...)
	if status != "succeeded" {
		policy, err := e.failurePolicyFactsRhiza(ctx, c.WorkspaceID, streamID, taskID, "on_function_failed", invocationID, c.CommandID, &statements)
		if err != nil {
			return Receipt{}, err
		}
		facts = append(facts, policy...)
	}
	if status == "succeeded" {
		var resolved int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, c.WorkspaceID, streamID).Scan(&resolved); err != nil {
			return Receipt{}, err
		}
		if resolved == 0 {
			statements = append(statements, rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, Args: []any{c.WorkspaceID, streamID}, WantRows: true, ExpectedReturnedRows: &zero})
			satisfied, err := e.taskSatisfiedRhiza(ctx, c.WorkspaceID, streamID, taskID, objectID, &statements)
			if err != nil {
				return Receipt{}, err
			}
			if satisfied {
				proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID}
				if objectID != "" {
					var objectVersion int64
					if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_instances WHERE workspace_id=? AND id=?`, c.WorkspaceID, objectID).Scan(&objectVersion); err != nil {
						return Receipt{}, err
					}
					proof["object_id"], proof["object_version"] = objectID, objectVersion
				}
				facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: invocationID, payload: proof})
			}
		}
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	effect := makeEffect(streamID, "work_stream", version+int64(len(facts))-1, workflowKind, eventPayload)
	effect.journal = facts
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}
