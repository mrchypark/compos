package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
)

type workflowRun struct {
	workspaceID, streamID, invocationID, workflowID, principalID string
	input                                                        map[string]any
	startedSequence                                              int64
}

type workflowState struct {
	root, current map[string]any
	steps         map[string]any
	routeStepID   string
	routeID       string
	principalID   string
}

type workflowSubmission struct {
	output                       map[string]any
	principalID, participantKind string
}

func (s *Server) enableWorkflowScheduler() {
	s.workflowWake = make(chan struct{}, 1)
	s.workflowDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s.workflowCancel = cancel
	go s.runWorkflowScheduler(ctx)
}

func (s *Server) runWorkflowScheduler(ctx context.Context) {
	defer close(s.workflowDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.advanceWorkDeadlines(ctx); err != nil {
			if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "database is closed") {
				return
			}
			log.Printf("Work deadline scheduler: %v", err)
		}
		if err := s.advanceReactiveRules(ctx); err != nil {
			if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "database is closed") {
				return
			}
			log.Printf("reactive rule scheduler: %v", err)
		}
		if err := s.advanceWorkflows(ctx); err != nil {
			if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "database is closed") {
				return
			}
			log.Printf("workflow scheduler: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-s.workflowWake:
		case <-ticker.C:
		}
	}
}

func (s *Server) advanceWorkDeadlines(ctx context.Context) error {
	now := time.Now().UTC().Format(time.RFC3339)
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `
		SELECT opened.workspace_id,opened.stream_id,opened.principal_id,json_extract(opened.payload_json,'$.deadline_at')
		FROM journal_records opened
		WHERE opened.kind='work.opened'
		  AND json_extract(opened.payload_json,'$.deadline_at') IS NOT NULL
		  AND json_extract(opened.payload_json,'$.deadline_at')<=?
		  AND NOT EXISTS (SELECT 1 FROM journal_records terminal WHERE terminal.workspace_id=opened.workspace_id AND terminal.stream_id=opened.stream_id AND terminal.kind IN ('work.satisfied','work.cancelled','work.deadline.reached'))
		ORDER BY json_extract(opened.payload_json,'$.deadline_at'),opened.workspace_id,opened.stream_id LIMIT 100`, now)
	if err != nil {
		return err
	}
	type dueWork struct{ workspace, stream, principal, deadline string }
	values := []dueWork{}
	for rows.Next() {
		var value dueWork
		if err := rows.Scan(&value.workspace, &value.stream, &value.principal, &value.deadline); err != nil {
			rows.Close()
			return err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, value := range values {
		id := workflowCommandID(value.stream, "deadline", value.deadline)
		payload, _ := json.Marshal(map[string]any{"deadline_at": value.deadline})
		_, err := s.kernel.Execute(ctx, value.principal, kernel.Command{
			CommandID: id, IdempotencyKey: "deadline:" + value.stream + ":" + value.deadline,
			WorkspaceID: value.workspace, CommandType: "work.record_deadline",
			Target: kernel.Target{AggregateType: "work_stream", AggregateID: &value.stream}, Preconditions: []kernel.Precondition{},
			Payload: payload, Causation: kernel.Causation{Source: "scheduler"},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) advanceWorkflows(ctx context.Context) error {
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `SELECT w.workspace_id,w.stream_id,w.correlation_id,w.spec_revision_id,w.principal_id,w.payload_json,w.stream_sequence FROM journal_records w WHERE w.kind='workflow.started' AND NOT EXISTS (SELECT 1 FROM journal_records t WHERE t.workspace_id=w.workspace_id AND t.stream_id=w.stream_id AND t.correlation_id=w.correlation_id AND t.kind IN ('workflow.completed','workflow.failed')) AND NOT EXISTS (SELECT 1 FROM journal_records terminal WHERE terminal.workspace_id=w.workspace_id AND terminal.stream_id=w.stream_id AND terminal.kind IN ('work.satisfied','work.cancelled')) ORDER BY w.created_at LIMIT 100`)
	if err != nil {
		return err
	}
	var runs []workflowRun
	for rows.Next() {
		var run workflowRun
		var payload string
		if err := rows.Scan(&run.workspaceID, &run.streamID, &run.invocationID, &run.workflowID, &run.principalID, &payload, &run.startedSequence); err != nil {
			rows.Close()
			return err
		}
		var value map[string]any
		if err := json.Unmarshal([]byte(payload), &value); err != nil {
			rows.Close()
			return err
		}
		run.input, _ = value["input"].(map[string]any)
		runs = append(runs, run)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, run := range runs {
		if err := s.advanceWorkflow(ctx, run); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) advanceWorkflow(ctx context.Context, run workflowRun) error {
	var raw string
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM spec_revisions WHERE workspace_id=? AND id=? AND kind='workflow'`, run.workspaceID, run.workflowID).Scan(&raw); err != nil {
		return s.completeWorkflow(ctx, run, run.principalID, "failed", map[string]any{}, "workflow revision is unavailable")
	}
	var spec map[string]any
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return err
	}
	steps, _ := spec["steps"].([]any)
	state := &workflowState{root: run.input, current: run.input, steps: map[string]any{}, principalID: run.principalID}
	ready, err := s.advanceWorkflowSteps(ctx, run, steps, state)
	if err != nil {
		return s.completeWorkflow(ctx, run, state.principalID, "failed", state.current, err.Error())
	}
	if !ready {
		return nil
	}
	return s.completeWorkflow(ctx, run, state.principalID, "succeeded", state.current, "")
}

func (s *Server) advanceWorkflowSteps(ctx context.Context, run workflowRun, steps []any, state *workflowState) (bool, error) {
	for _, rawStep := range steps {
		step := rawStep.(map[string]any)
		stepID := step["id"].(string)
		switch step["kind"] {
		case "route":
			routeID := workflowCommandID(run.invocationID, stepID, "route")
			payload := map[string]any{"participant": step["participant"], "workflow_step_id": stepID, "workflow_route_id": routeID}
			if reason, ok := step["reason"]; ok {
				payload["reason"] = reason
			}
			if err := s.executeWorkflowCommand(ctx, run, state.principalID, stepID, "route", "work.route", payload, nil); err != nil {
				return false, err
			}
			state.routeStepID, state.routeID = stepID, routeID
			participant := step["participant"].(map[string]any)
			if participant["kind"] == "human" || participant["kind"] == "agent" {
				submission, err := s.workflowRouteSubmission(ctx, run, stepID)
				if err != nil {
					return false, err
				}
				if submission == nil {
					return false, nil
				}
				state.steps[stepID], state.current = submission.output, submission.output
				if mapping, ok := step["output_mapping"].(map[string]any); ok {
					state.current, err = workflowMapping(mapping, state, submission.output)
					if err != nil {
						return false, err
					}
				}
				if submission.participantKind == "human" {
					state.principalID = submission.principalID
				}
			}
		case "invoke":
			commandID := workflowCommandID(run.invocationID, stepID, "invoke")
			childInvocation := "inv_" + strings.ReplaceAll(commandID, "-", "")
			kind, childOutput, terminal, err := s.functionTerminal(ctx, run.workspaceID, run.streamID, childInvocation)
			if err != nil {
				return false, err
			}
			if !terminal {
				actor, ready, err := s.workflowActor(ctx, run, state.principalID, state.routeID)
				if err != nil || !ready {
					return false, err
				}
				input := state.current
				if mapping, ok := step["input_mapping"].(map[string]any); ok {
					input, err = workflowMapping(mapping, state, nil)
					if err != nil {
						return false, err
					}
				}
				payload := map[string]any{"function_revision_id": step["function_revision_id"], "input": input}
				if state.routeID != "" {
					payload["workflow_route_id"] = state.routeID
				}
				if err := s.executeWorkflowCommand(ctx, run, state.principalID, stepID, "invoke", "function.invoke", payload, actor); err != nil {
					return false, err
				}
				s.wakeConductor()
				kind, childOutput, terminal, err = s.functionTerminal(ctx, run.workspaceID, run.streamID, childInvocation)
				if err != nil {
					return false, err
				}
				if !terminal {
					return false, nil
				}
			}
			if kind == "function.failed" {
				return false, fmt.Errorf("workflow child Function %s failed", stepID)
			}
			state.steps[stepID] = childOutput
			state.current = childOutput
			if mapping, ok := step["output_mapping"].(map[string]any); ok {
				state.current, err = workflowMapping(mapping, state, childOutput)
				if err != nil {
					return false, err
				}
			}
		case "branch":
			condition, err := workflowCondition(step["condition"].(map[string]any), state)
			if err != nil {
				return false, err
			}
			selected := step["else"].([]any)
			if condition {
				selected = step["then"].([]any)
			}
			ready, err := s.advanceWorkflowSteps(ctx, run, selected, state)
			if err != nil || !ready {
				return ready, err
			}
			output := state.current
			state.steps[stepID] = output
			if mapping, ok := step["output_mapping"].(map[string]any); ok {
				state.current, err = workflowMapping(mapping, state, output)
				if err != nil {
					return false, err
				}
			}
		case "parallel":
			base := state.current
			outputs := map[string]any{}
			branchStates := []*workflowState{}
			pending := false
			for _, rawBranch := range step["branches"].([]any) {
				branch := rawBranch.(map[string]any)
				branchState := &workflowState{root: state.root, current: base, steps: cloneWorkflowValues(state.steps), routeStepID: state.routeStepID, routeID: state.routeID, principalID: state.principalID}
				ready, err := s.advanceWorkflowSteps(ctx, run, branch["steps"].([]any), branchState)
				if err != nil {
					return false, err
				}
				pending = pending || !ready
				outputs[branch["id"].(string)] = branchState.current
				branchStates = append(branchStates, branchState)
			}
			if pending {
				return false, nil
			}
			for _, branchState := range branchStates {
				for id, value := range branchState.steps {
					state.steps[id] = value
				}
			}
			state.steps[stepID] = outputs
			state.current = outputs
			if mapping, ok := step["output_mapping"].(map[string]any); ok {
				mapped, err := workflowMapping(mapping, state, outputs)
				if err != nil {
					return false, err
				}
				state.current = mapped
			}
		}
	}
	return true, nil
}

func workflowMapping(mapping map[string]any, state *workflowState, output map[string]any) (map[string]any, error) {
	context := map[string]any{"root": state.root, "current": state.current, "steps": state.steps, "output": output}
	result := make(map[string]any, len(mapping))
	for key, raw := range mapping {
		selector, selected := raw.(string)
		if !selected || !strings.HasPrefix(selector, "$") {
			result[key] = raw
			continue
		}
		value, ok := workflowSelect(context, selector)
		if !ok {
			return nil, fmt.Errorf("workflow selector %s did not resolve", selector)
		}
		result[key] = value
	}
	return result, nil
}

func workflowCondition(condition map[string]any, state *workflowState) (bool, error) {
	value, exists := workflowSelect(map[string]any{"root": state.root, "current": state.current, "steps": state.steps}, condition["left"].(string))
	switch condition["operator"] {
	case "exists":
		return exists, nil
	case "not_exists":
		return !exists, nil
	case "eq":
		return exists && reflect.DeepEqual(value, condition["right"]), nil
	case "not_eq":
		return !exists || !reflect.DeepEqual(value, condition["right"]), nil
	default:
		return false, fmt.Errorf("unsupported workflow condition operator")
	}
}

func workflowSelect(context map[string]any, selector string) (any, bool) {
	var current any = context
	for _, part := range strings.Split(strings.TrimPrefix(selector, "$."), ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func cloneWorkflowValues(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func (s *Server) executeWorkflowCommand(ctx context.Context, run workflowRun, principalID, stepID, action, commandType string, payload map[string]any, actor *kernel.Actor) error {
	id := workflowCommandID(run.invocationID, stepID, action)
	raw, _ := json.Marshal(payload)
	eventID := run.invocationID
	_, err := s.kernel.Execute(ctx, principalID, kernel.Command{
		CommandID: id, IdempotencyKey: "workflow:" + run.invocationID + ":" + stepID + ":" + action,
		WorkspaceID: run.workspaceID, RequestedActor: actor, CommandType: commandType,
		Target: kernel.Target{AggregateType: "work_stream", AggregateID: &run.streamID}, Preconditions: []kernel.Precondition{}, Payload: raw,
		Causation: kernel.Causation{Source: "workflow", EventID: &eventID},
	})
	return err
}

func (s *Server) workflowActor(ctx context.Context, run workflowRun, principalID, routeID string) (*kernel.Actor, bool, error) {
	var kind, id string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT participant_kind,participant_id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.routed' AND (?='' OR json_extract(payload_json,'$.workflow_route_id')=?) ORDER BY stream_sequence DESC LIMIT 1`, run.workspaceID, run.streamID, routeID, routeID).Scan(&kind, &id)
	if err != nil {
		return nil, false, err
	}
	switch kind {
	case "human":
		return nil, id == principalID, nil
	case "agent":
		return &kernel.Actor{Type: "service", ID: id}, true, nil
	case "automation":
		return &kernel.Actor{Type: "automation", ID: id}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported workflow participant %q", kind)
	}
}

func (s *Server) functionTerminal(ctx context.Context, workspace, stream, invocation string) (string, map[string]any, bool, error) {
	var kind, raw string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT kind,payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('function.succeeded','function.failed') ORDER BY stream_sequence DESC LIMIT 1`, workspace, stream, invocation).Scan(&kind, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "", nil, false, err
	}
	output, _ := payload["output"].(map[string]any)
	return kind, output, true, nil
}

func (s *Server) workflowRouteSubmission(ctx context.Context, run workflowRun, stepID string) (*workflowSubmission, error) {
	routeID := workflowCommandID(run.invocationID, stepID, "route")
	var principalID, kind, raw string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT principal_id,COALESCE(participant_kind,''),payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.submitted' AND json_extract(payload_json,'$.workflow_step_id')=? AND json_extract(payload_json,'$.workflow_route_id')=? ORDER BY stream_sequence DESC LIMIT 1`, run.workspaceID, run.streamID, stepID, routeID).Scan(&principalID, &kind, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, err
	}
	output, ok := payload["submission"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("workflow route submission payload is invalid")
	}
	return &workflowSubmission{output: output, principalID: principalID, participantKind: kind}, nil
}

func (s *Server) workflowRouteSubmitted(ctx context.Context, run workflowRun, stepID string) (bool, error) {
	submission, err := s.workflowRouteSubmission(ctx, run, stepID)
	return submission != nil, err
}

func (s *Server) completeWorkflow(ctx context.Context, run workflowRun, principalID, status string, output map[string]any, detail string) error {
	payload := map[string]any{"invocation_id": run.invocationID, "runtime_status": status, "output": output}
	if detail != "" {
		payload["error_detail"] = detail
	}
	return s.executeWorkflowCommand(ctx, run, principalID, "complete", status, "workflow.complete", payload, nil)
}

func workflowCommandID(invocation, step, action string) string {
	sum := sha256.Sum256([]byte(invocation + "\x00" + step + "\x00" + action))
	return "cmd_wf_" + hex.EncodeToString(sum[:16])
}

func (s *Server) wakeWorkflow() {
	if s.workflowWake == nil {
		return
	}
	select {
	case s.workflowWake <- struct{}{}:
	default:
	}
}
