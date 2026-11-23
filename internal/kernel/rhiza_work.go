package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (e *Executor) ensureWorkOpenRhiza(ctx context.Context, workspace, stream string, statements *[]rhiza.SQLStatement) error {
	rows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=?`, workspace, stream)
	if err != nil {
		return err
	}
	count := int64(0)
	for rows.Next() {
		count++
	}
	_ = rows.Close()
	if count == 0 {
		return reject("precondition_failed", "work stream does not exist")
	}
	zero := int64(0)
	*statements = append(*statements,
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=?`, Args: []any{workspace, stream}, WantRows: true, ExpectedReturnedRows: &count},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, Args: []any{workspace, stream}, WantRows: true, ExpectedReturnedRows: &zero},
	)
	return nil
}

func (e *Executor) nextWorkVersionRhiza(ctx context.Context, workspace, stream string) (int64, error) {
	var sequence int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COALESCE(MAX(stream_sequence),0) FROM journal_records WHERE workspace_id=? AND stream_id=?`, workspace, stream).Scan(&sequence); err != nil {
		return 0, err
	}
	if sequence == 0 {
		return 0, reject("precondition_failed", "work stream does not exist")
	}
	return sequence + 1, nil
}

func (e *Executor) workTaskRhiza(ctx context.Context, workspace, stream string, statements *[]rhiza.SQLStatement) (string, map[string]any, error) {
	var id, taskID, payload string
	var sequence int64
	err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,spec_revision_id,stream_sequence,payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.opened' ORDER BY stream_sequence LIMIT 1`, workspace, stream).Scan(&id, &taskID, &sequence, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, reject("precondition_failed", "work stream does not exist")
	}
	if err != nil {
		return "", nil, err
	}
	one := int64(1)
	*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND kind='work.opened' AND spec_revision_id=? AND payload_json=?`, Args: []any{workspace, id, stream, sequence, taskID, payload}, WantRows: true, ExpectedReturnedRows: &one})
	var opened map[string]any
	if err := json.Unmarshal([]byte(payload), &opened); err != nil {
		return "", nil, err
	}
	contextValue, _ := opened["context"].(map[string]any)
	return taskID, contextValue, nil
}

func (e *Executor) currentWorkRouteRhiza(ctx context.Context, workspace, stream string, actor *Actor, requestedStepID, requestedRouteID string, unresolved bool, statements *[]rhiza.SQLStatement) (string, string, error) {
	var recordID, kind, id, stepID, routeID string
	var sequence int64
	err := e.store.ReadRow(ctx, store.ReadLinearizable, `
		SELECT id,stream_sequence,participant_kind,participant_id,COALESCE(json_extract(payload_json,'$.workflow_step_id'),''),COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')
		FROM journal_records route
		WHERE workspace_id=? AND stream_id=? AND kind='work.routed'
		  AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_step_id'),'')=?)
		  AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')=?)
		  AND (?=0 OR NOT EXISTS (
		    SELECT 1 FROM journal_records submitted
		    WHERE submitted.workspace_id=route.workspace_id AND submitted.stream_id=route.stream_id
		      AND submitted.kind='work.submitted' AND submitted.stream_sequence>route.stream_sequence
		      AND COALESCE(json_extract(submitted.payload_json,'$.workflow_route_id'),'')=COALESCE(json_extract(route.payload_json,'$.workflow_route_id'),'')))
		ORDER BY stream_sequence DESC LIMIT 1`, workspace, stream, requestedStepID, requestedStepID, requestedRouteID, requestedRouteID, unresolved).Scan(&recordID, &sequence, &kind, &id, &stepID, &routeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", reject("precondition_failed", "work stream has no current participant")
	}
	if err != nil {
		return "", "", err
	}
	if actor.Type != kind || actor.ID != id {
		return "", "", reject("forbidden", "requested actor is not the current work participant")
	}
	one, zero := int64(1), int64(0)
	*statements = append(*statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND kind='work.routed' AND participant_kind=? AND participant_id=?`, Args: []any{workspace, recordID, stream, sequence, kind, id}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.routed' AND stream_sequence>? AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_step_id'),'')=?) AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')=?)`, Args: []any{workspace, stream, sequence, requestedStepID, requestedStepID, requestedRouteID, requestedRouteID}, WantRows: true, ExpectedReturnedRows: &zero},
	)
	if unresolved {
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.submitted' AND stream_sequence>? AND COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')=?`, Args: []any{workspace, stream, sequence, routeID}, WantRows: true, ExpectedReturnedRows: &zero})
	}
	return stepID, routeID, nil
}

func (e *Executor) openWorkRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	taskID, _ := payload["task_revision_id"].(string)
	contextValue, _ := payload["context"].(map[string]any)
	task, err := e.loadSpecRhiza(ctx, c.WorkspaceID, taskID, "task", &statements)
	if err != nil {
		return Receipt{}, err
	}
	if err := validateValues(contextValue, task["input_schema"].(map[string]any), "payload.context"); err != nil {
		return Receipt{}, err
	}
	one := int64(1)
	if err := validateWorkBindingContexts(func(query string, args ...any) workContextRow {
		return e.store.ReadRow(ctx, store.ReadLinearizable, query, args...)
	}, func(query string, args ...any) {
		statements = append(statements, rhiza.SQLStatement{SQL: query, Args: args, WantRows: true, ExpectedReturnedRows: &one})
	}, c.WorkspaceID, task, contextValue); err != nil {
		return Receipt{}, err
	}
	if objectID, ok := contextValue["object_id"].(string); ok {
		var version int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, objectID).Scan(&version); err != nil {
			return Receipt{}, reject("precondition_failed", "work context object does not exist")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=?`, Args: []any{c.WorkspaceID, objectID, version}, WantRows: true, ExpectedReturnedRows: &one})
	}
	streamID := aggregateID("wrk", c.CommandID)
	eventPayload := map[string]any{"task_revision_id": taskID, "context": contextValue, "owner_principal_id": principal}
	if deadline, _ := payload["deadline_at"].(string); strings.TrimSpace(deadline) != "" {
		parsed, err := time.Parse(time.RFC3339, deadline)
		if err != nil {
			return Receipt{}, reject("schema_mismatch", "payload.deadline_at must be RFC3339")
		}
		eventPayload["deadline_at"] = parsed.UTC().Format(time.RFC3339)
	}
	effect := makeEffect(streamID, "work_stream", 1, "work.opened", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "work.opened", specRevision: taskID, payload: eventPayload}}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) routeWorkRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	participant, err := participantFrom(payload["participant"])
	if err != nil {
		return Receipt{}, err
	}
	eventPayload := map[string]any{"participant": map[string]any{"kind": participant.Type, "id": participant.ID}}
	stepID, _ := payload["workflow_step_id"].(string)
	routeID, _ := payload["workflow_route_id"].(string)
	if stepID != "" {
		if c.Causation.Source != "workflow" || !workflowIdentifier(stepID) || routeID != c.CommandID {
			return Receipt{}, reject("forbidden", "Workflow route identity must match its command")
		}
		eventPayload["workflow_step_id"], eventPayload["workflow_route_id"] = stepID, routeID
	} else if routeID != "" {
		return Receipt{}, reject("schema_mismatch", "workflow_route_id requires workflow_step_id")
	}
	if reason, _ := payload["reason"].(string); strings.TrimSpace(reason) != "" {
		eventPayload["reason"] = reason
	}
	effect := makeEffect(streamID, "work_stream", version, "work.routed", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "work.routed", participant: participant, payload: eventPayload}}
	if participant.Type == "agent" {
		agent, err := e.loadSpecRhiza(ctx, c.WorkspaceID, participant.ID, "participant", &statements)
		if err != nil {
			return Receipt{}, err
		}
		taskID, contextValue, err := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
		if err != nil {
			return Receipt{}, err
		}
		task, err := e.loadSpecRhiza(ctx, c.WorkspaceID, taskID, "task", &statements)
		if err != nil {
			return Receipt{}, err
		}
		invocationID := aggregateID("agn", c.CommandID)
		execution := agent["execution_policy"].(map[string]any)
		handlerKey := execution["handler_key"].(string)
		compos := map[string]any{"workspace_id": c.WorkspaceID, "execution_request_id": invocationID, "handler_key": handlerKey, "handler_version": execution["handler_version"], "workflow_instance_id": streamID, "task_id": streamID, "actor": map[string]any{"type": "agent", "id": participant.ID}}
		for _, name := range []string{"connection_id", "credential_revision", "timeout_seconds", "retry_policy"} {
			if value, exists := execution[name]; exists {
				compos[name] = value
			}
		}
		effect.aggregate.Version++
		requestPayload := map[string]any{"agent_revision_id": participant.ID, "task_revision_id": taskID, "input": contextValue, "handler_key": handlerKey, "handler_version": execution["handler_version"]}
		for _, name := range []string{"connection_id", "credential_revision"} {
			if value, exists := execution[name]; exists {
				requestPayload[name] = value
			}
		}
		if stepID != "" {
			requestPayload["workflow_step_id"], requestPayload["workflow_route_id"], compos["workflow_step_id"], compos["workflow_route_id"] = stepID, routeID, stepID, routeID
		}
		effect.journal = append(effect.journal, journalFact{streamID: streamID, kind: "agent.requested", specRevision: participant.ID, participant: participant, correlationID: invocationID, payload: requestPayload})
		effect.outboxEvent = handlerKey
		effect.outboxPayload = map[string]any{"agent_definition": agent, "task_definition": task, "input": contextValue, "subject_ref": contextValue, "_compos": compos}
	}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) submitWorkRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	participant := commandParticipant(c, principal)
	requestedStepID, _ := payload["workflow_step_id"].(string)
	requestedRouteID, _ := payload["workflow_route_id"].(string)
	if requestedStepID != "" && !workflowIdentifier(requestedStepID) {
		return Receipt{}, reject("schema_mismatch", "workflow_step_id must be a valid Workflow step ID")
	}
	if (requestedStepID == "") != (requestedRouteID == "") {
		return Receipt{}, reject("schema_mismatch", "workflow_step_id and workflow_route_id must be provided together")
	}
	stepID, routeID, err := e.currentWorkRouteRhiza(ctx, c.WorkspaceID, streamID, participant, requestedStepID, requestedRouteID, true, &statements)
	if err != nil {
		return Receipt{}, err
	}
	if requestedRouteID == "" && routeID != "" {
		return Receipt{}, reject("schema_mismatch", "Workflow submissions require workflow_step_id and workflow_route_id")
	}
	submission, _ := payload["submission"].(map[string]any)
	taskID, contextValue, err := e.workTaskRhiza(ctx, c.WorkspaceID, streamID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	task, err := e.loadSpecRhiza(ctx, c.WorkspaceID, taskID, "task", &statements)
	if err != nil {
		return Receipt{}, err
	}
	submissionSchema, err := e.workflowSubmissionSchemaRhiza(ctx, c.WorkspaceID, streamID, task, stepID, routeID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	if err := validateValues(submission, submissionSchema, "payload.submission"); err != nil {
		return Receipt{}, err
	}
	eventPayload := map[string]any{"submission": submission}
	if stepID != "" {
		eventPayload["workflow_step_id"], eventPayload["workflow_route_id"] = stepID, routeID
	}
	effect := makeEffect(streamID, "work_stream", version, "work.submitted", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "work.submitted", participant: participant, payload: eventPayload}}
	if task["completion"].(map[string]any)["kind"] == "submission_valid" && stepID == "" {
		objectID, _ := contextValue["object_id"].(string)
		effect.aggregate.Version++
		effect.journal = append(effect.journal, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, participant: participant, payload: map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "object_id": objectID, "submission_sequence": version}})
	}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) workflowSubmissionSchemaRhiza(ctx context.Context, workspace, stream string, task map[string]any, stepID, routeID string, statements *[]rhiza.SQLStatement) (map[string]any, error) {
	fallback, _ := task["output_schema"].(map[string]any)
	if routeID == "" {
		return fallback, nil
	}
	rows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id,stream_sequence,correlation_id,spec_revision_id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='workflow.started' ORDER BY stream_sequence`, workspace, stream)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var recordID, invocationID, workflowID string
		var sequence int64
		if err := rows.Scan(&recordID, &sequence, &invocationID, &workflowID); err != nil {
			return nil, err
		}
		workflow, err := e.loadSpecRhiza(ctx, workspace, workflowID, "workflow", statements)
		if err != nil {
			return nil, err
		}
		contract, found := WorkflowRouteContractForSpec(workflow, invocationID, stepID, routeID)
		if !found {
			continue
		}
		one := int64(1)
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND correlation_id=? AND spec_revision_id=? AND kind='workflow.started'`, Args: []any{workspace, recordID, stream, sequence, invocationID, workflowID}, WantRows: true, ExpectedReturnedRows: &one})
		if contract.OutputSchema != nil {
			return contract.OutputSchema, nil
		}
		return fallback, nil
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nil, reject("precondition_failed", "workflow route definition is unavailable")
}

func (e *Executor) cancelWorkRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	reason, _ := payload["reason"].(string)
	if strings.TrimSpace(reason) == "" {
		return Receipt{}, reject("schema_mismatch", "work cancellation requires reason")
	}
	eventPayload := map[string]any{"reason": reason}
	effect := makeEffect(streamID, "work_stream", version, "work.cancelled", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "work.cancelled", participant: commandParticipant(c, principal), payload: eventPayload}}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) recordWorkDeadlineRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	if c.Causation.Source != "scheduler" {
		return Receipt{}, reject("forbidden", "Work deadline facts require scheduler causation")
	}
	streamID := *c.Target.AggregateID
	if err := e.ensureWorkOpenRhiza(ctx, c.WorkspaceID, streamID, &statements); err != nil {
		return Receipt{}, err
	}
	deadline, _ := payload["deadline_at"].(string)
	parsed, err := time.Parse(time.RFC3339, deadline)
	if err != nil {
		return Receipt{}, reject("schema_mismatch", "payload.deadline_at must be RFC3339")
	}
	deadline = parsed.UTC().Format(time.RFC3339)
	var openedID, openedDeadline string
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,COALESCE(json_extract(payload_json,'$.deadline_at'),'') FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.opened'`, c.WorkspaceID, streamID).Scan(&openedID, &openedDeadline); err != nil || openedDeadline != deadline {
		return Receipt{}, reject("precondition_failed", "Work deadline does not match the opened Work")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND kind='work.opened' AND COALESCE(json_extract(payload_json,'$.deadline_at'),'')=?`, Args: []any{c.WorkspaceID, openedID, streamID, deadline}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.deadline.reached'`, Args: []any{c.WorkspaceID, streamID}, WantRows: true, ExpectedReturnedRows: &zero},
	)
	version, err := e.nextWorkVersionRhiza(ctx, c.WorkspaceID, streamID)
	if err != nil {
		return Receipt{}, err
	}
	eventPayload := map[string]any{"deadline_at": deadline}
	effect := makeEffect(streamID, "work_stream", version, "work.deadline.reached", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "work.deadline.reached", payload: eventPayload}}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}
