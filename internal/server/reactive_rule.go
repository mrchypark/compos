package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/reactiverule"
	"github.com/mrchypark/compos/internal/store"
)

type reactiveRuleCandidate struct {
	workspaceID, ruleID, revisionID, automationID, principalID string
	trigger, predicate, action, limits                         map[string]any
	eventID, eventType, aggregateType, aggregateID             string
	aggregateVersion, sequence                                 int64
	payload, causation                                         map[string]any
}

func (s *Server) advanceReactiveRules(ctx context.Context) error {
	candidates, err := s.reactiveRuleCandidates(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := s.evaluateReactiveRule(ctx, candidate); err != nil {
			return err
		}
	}
	return s.dispatchReactiveRules(ctx)
}

func (s *Server) reactiveRuleCandidates(ctx context.Context) ([]reactiveRuleCandidate, error) {
	// ponytail: bounded rule×event scan; add a per-rule cursor only after this query is measured.
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `
		SELECT r.workspace_id,r.id,v.id,r.automation_id,v.created_by_principal_id,
		       v.trigger_json,v.predicates_json,v.action_json,v.limits_json,
		       e.id,a.event_type,e.aggregate_type,e.aggregate_id,e.aggregate_version,e.projection_sequence,e.payload_json,c.causation_json
		FROM reactive_rules r
		JOIN reactive_rule_revisions v ON v.workspace_id=r.workspace_id AND v.id=r.current_revision_id
		JOIN projection_events e ON e.workspace_id=r.workspace_id AND e.projection_sequence>CAST(COALESCE(NULLIF(r.activation_event_cursor,''),'0') AS INTEGER)
		JOIN activities a ON a.workspace_id=e.workspace_id AND a.command_id=e.command_id
			AND a.aggregate_type=e.aggregate_type AND a.aggregate_id=e.aggregate_id
		JOIN command_ledger c ON c.workspace_id=e.workspace_id AND c.command_id=e.command_id
		WHERE r.status='active' AND r.archived_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM reactive_rule_evaluations x WHERE x.workspace_id=r.workspace_id AND x.rule_revision_id=v.id AND x.triggering_event_id=e.id)
		ORDER BY e.projection_sequence,r.id LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []reactiveRuleCandidate
	for rows.Next() {
		var candidate reactiveRuleCandidate
		var trigger, predicate, action, limits, payload, causation string
		if err := rows.Scan(&candidate.workspaceID, &candidate.ruleID, &candidate.revisionID, &candidate.automationID, &candidate.principalID,
			&trigger, &predicate, &action, &limits, &candidate.eventID, &candidate.eventType, &candidate.aggregateType, &candidate.aggregateID,
			&candidate.aggregateVersion, &candidate.sequence, &payload, &causation); err != nil {
			return nil, err
		}
		for _, item := range []struct {
			raw    string
			target *map[string]any
		}{{trigger, &candidate.trigger}, {predicate, &candidate.predicate}, {action, &candidate.action}, {limits, &candidate.limits}, {payload, &candidate.payload}, {causation, &candidate.causation}} {
			if err := json.Unmarshal([]byte(item.raw), item.target); err != nil {
				return nil, err
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (s *Server) evaluateReactiveRule(ctx context.Context, candidate reactiveRuleCandidate) error {
	actionID, _ := candidate.action["action_id"].(string)
	data := map[string]any{
		"payload":  candidate.payload,
		"target":   map[string]any{"aggregate_type": candidate.aggregateType, "aggregate_id": candidate.aggregateID, "aggregate_version": candidate.aggregateVersion, "projection_sequence": candidate.sequence},
		"metadata": map[string]any{"event_type": candidate.eventType, "causation_source": candidate.causation["source"], "causation_automation_id": automationID(candidate.causation)},
	}
	data["event"] = map[string]any{"id": candidate.eventID, "workspace_id": candidate.workspaceID, "event_type": candidate.eventType, "payload": candidate.payload, "target": data["target"], "metadata": data["metadata"]}
	depth := automationDepth(candidate.causation) + 1
	matched := kernel.ReactiveRuleTriggerEventAllowed(candidate.eventType) && triggerMatches(candidate, data)
	if matched {
		matched, _ = reactiverule.Match(candidate.predicate, data)
	}
	if max, ok := intValue(candidate.limits["max_recursion_depth"]); !ok || depth > max {
		matched = false
	}
	if matched {
		limited, err := s.reactiveRuleLimited(ctx, candidate)
		if err != nil {
			return err
		}
		matched = !limited
	}
	evaluationID := "aev_" + strings.TrimPrefix(sha([]byte(candidate.workspaceID+":"+candidate.revisionID+":"+candidate.eventID+":"+actionID)), "sha256:")[:32]
	now := time.Now().UTC().Format(time.RFC3339Nano)
	predicateResult, dispatchStatus := "skipped", "applied"
	var commandID, namespace, idempotencyKey, renderedHash any
	evaluation := map[string]any{"event": data["event"], "matched": matched}
	if matched {
		target, err := renderReactiveRuleTemplate(candidate.action["target_template"], data)
		if err != nil {
			return s.insertReactiveRuleEvaluation(ctx, candidate, evaluationID, actionID, depth, "failed", "rejected", nil, nil, nil, nil, map[string]any{"error": err.Error()}, now)
		}
		payload, err := renderReactiveRuleTemplate(candidate.action["payload_template"], data)
		if err != nil {
			return s.insertReactiveRuleEvaluation(ctx, candidate, evaluationID, actionID, depth, "failed", "rejected", nil, nil, nil, nil, map[string]any{"error": err.Error()}, now)
		}
		preconditionTemplate := candidate.action["preconditions_template"]
		if preconditionTemplate == nil {
			preconditionTemplate = []any{}
		}
		preconditions, err := renderReactiveRuleTemplate(preconditionTemplate, data)
		if err != nil {
			return s.insertReactiveRuleEvaluation(ctx, candidate, evaluationID, actionID, depth, "failed", "rejected", nil, nil, nil, nil, map[string]any{"error": err.Error()}, now)
		}
		commandID = "cmd_automation_" + strings.TrimPrefix(sha([]byte(evaluationID)), "sha256:")[:24]
		namespace = candidate.workspaceID + "/" + candidate.automationID + "/" + candidate.revisionID + "/" + actionID
		idempotencyKey = "automation:" + candidate.automationID + ":" + candidate.revisionID + ":" + candidate.eventID + ":" + actionID
		command := map[string]any{
			"command_id": commandID, "idempotency_key": idempotencyKey, "workspace_id": candidate.workspaceID,
			"requested_actor": map[string]any{"type": "automation", "id": candidate.automationID}, "command_type": candidate.action["command_type"],
			"target": target, "preconditions": preconditions, "payload": payload,
			"causation": map[string]any{"source": "automation", "event_id": candidate.eventID, "automation": map[string]any{"automation_id": candidate.automationID, "rule_revision_id": candidate.revisionID, "triggering_event_id": candidate.eventID, "recursion_depth": depth}},
		}
		evaluation["principal_id"], evaluation["command"] = candidate.principalID, command
		renderedHash, predicateResult, dispatchStatus = sha([]byte(canonical(command))), "matched", "pending"
	}
	return s.insertReactiveRuleEvaluation(ctx, candidate, evaluationID, actionID, depth, predicateResult, dispatchStatus, commandID, renderedHash, namespace, idempotencyKey, evaluation, now)
}

func (s *Server) insertReactiveRuleEvaluation(ctx context.Context, candidate reactiveRuleCandidate, id, actionID string, depth int, predicate, status string, commandID, renderedHash, namespace, idempotencyKey any, evaluation map[string]any, now string) error {
	payload := canonical(evaluation)
	return s.store.Exec(ctx, "reactive-rule-evaluation:"+id, `INSERT OR IGNORE INTO reactive_rule_evaluations(id,workspace_id,automation_id,rule_id,rule_revision_id,triggering_event_id,action_id,recursion_depth,predicate_result,emitted_command_id,rendered_command_hash,idempotency_namespace,idempotency_key,dispatch_status,payload_hash,evaluation_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, candidate.workspaceID, candidate.automationID, candidate.ruleID, candidate.revisionID, candidate.eventID, actionID, depth, predicate, commandID, renderedHash, namespace, idempotencyKey, status, sha([]byte(payload)), payload, now, now)
}

func (s *Server) dispatchReactiveRules(ctx context.Context) error {
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `
		SELECT e.id,e.workspace_id,e.rule_id,e.rule_revision_id,e.dispatch_attempt_count,e.evaluation_json,v.limits_json
		FROM reactive_rule_evaluations e
		JOIN reactive_rules r ON r.workspace_id=e.workspace_id AND r.id=e.rule_id
		JOIN reactive_rule_revisions v ON v.workspace_id=e.workspace_id AND v.id=e.rule_revision_id
		WHERE e.dispatch_status IN ('pending','retryable_failed') AND r.status='active' AND r.circuit_breaker_status='closed'
		ORDER BY e.created_at,e.id LIMIT 100`)
	if err != nil {
		return err
	}
	type pending struct {
		id, workspaceID, ruleID, revisionID, raw string
		attempt, failureThreshold                int
	}
	var evaluations []pending
	for rows.Next() {
		var item pending
		var limitsRaw string
		if err := rows.Scan(&item.id, &item.workspaceID, &item.ruleID, &item.revisionID, &item.attempt, &item.raw, &limitsRaw); err != nil {
			rows.Close()
			return err
		}
		var limits map[string]any
		if err := json.Unmarshal([]byte(limitsRaw), &limits); err != nil {
			rows.Close()
			return err
		}
		item.failureThreshold, _ = intValue(limits["failure_threshold"])
		evaluations = append(evaluations, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range evaluations {
		var evaluation struct {
			PrincipalID string         `json:"principal_id"`
			Command     kernel.Command `json:"command"`
		}
		if err := json.Unmarshal([]byte(item.raw), &evaluation); err != nil {
			return err
		}
		_, commandErr := s.kernel.Execute(ctx, evaluation.PrincipalID, evaluation.Command)
		status, code, detail := "applied", any(nil), any(nil)
		if commandErr != nil {
			var rejected *kernel.CommandError
			if errors.As(commandErr, &rejected) {
				status, code = "rejected", rejected.Outcome
				if rejected.Retryable {
					status = "retryable_failed"
				}
			} else {
				status = "retryable_failed"
			}
			detail = commandErr.Error()
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		nextAttempt := item.attempt + 1
		requestID := "automation-dispatch:" + item.id + ":" + strconv.Itoa(nextAttempt) + ":" + status
		if err := s.store.Exec(ctx, requestID, `UPDATE reactive_rule_evaluations SET dispatch_status=?,dispatch_attempt_count=?,last_dispatch_error_code=?,last_dispatch_error_message=?,updated_at=? WHERE id=? AND dispatch_attempt_count=? AND dispatch_status IN ('pending','retryable_failed')`, status, nextAttempt, code, detail, now, item.id, item.attempt); err != nil {
			return err
		}
		if commandErr != nil {
			if err := s.openAutomationCircuitIfThreshold(ctx, item.workspaceID, item.ruleID, item.revisionID, item.failureThreshold); err != nil {
				return err
			}
		}
		if commandErr == nil {
			if evaluation.Command.CommandType == "function.invoke" {
				s.wakeConductor()
			}
			s.wakeWorkflow()
		}
	}
	return nil
}

func (s *Server) openAutomationCircuitIfThreshold(ctx context.Context, workspaceID, ruleID, revisionID string, threshold int) error {
	if threshold < 1 {
		return nil
	}
	var failures int
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COALESCE(SUM(dispatch_attempt_count),0) FROM reactive_rule_evaluations WHERE workspace_id=? AND rule_revision_id=? AND dispatch_status IN ('rejected','retryable_failed','dead_lettered')`, workspaceID, revisionID).Scan(&failures); err != nil {
		return err
	}
	if failures < threshold {
		return nil
	}
	var principal string
	var version int64
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT created_by_principal_id,version FROM reactive_rules WHERE workspace_id=? AND id=? AND current_revision_id=? AND status='active' AND circuit_breaker_status='closed'`, workspaceID, ruleID, revisionID).Scan(&principal, &version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	payload, _ := json.Marshal(map[string]any{"reason": "automation dispatch failure threshold reached", "circuit_breaker": true})
	commandID := "cmd_automation_circuit_" + strings.TrimPrefix(sha([]byte(workspaceID+":"+ruleID+":"+revisionID)), "sha256:")[:24]
	_, err := s.kernel.Execute(ctx, principal, kernel.Command{
		CommandID: commandID, IdempotencyKey: "automation:circuit:" + revisionID, WorkspaceID: workspaceID,
		RequestedActor: &kernel.Actor{Type: "user", ID: principal}, CommandType: "reactive_rule.disable",
		Target:        kernel.Target{AggregateType: "reactive_rule", AggregateID: &ruleID},
		Preconditions: []kernel.Precondition{{AggregateType: "reactive_rule", AggregateID: ruleID, ExpectedVersion: version}},
		Payload:       payload, Causation: kernel.Causation{Source: "automation"},
	})
	return err
}

func triggerMatches(candidate reactiveRuleCandidate, data map[string]any) bool {
	trigger := candidate.trigger
	if trigger["source"] != "kernel_canonical_event" || trigger["event_type"] != candidate.eventType {
		return false
	}
	if aggregate, _ := trigger["aggregate_type"].(string); aggregate != "" && aggregate != candidate.aggregateType {
		return false
	}
	source, _ := candidate.causation["source"].(string)
	if allowed, ok := trigger["allowed_causation_sources"].([]any); ok && len(allowed) != 0 && !containsString(allowed, source) {
		return false
	}
	excluded, _ := trigger["excluded_automation_ids"].([]any)
	id := automationID(candidate.causation)
	for _, raw := range excluded {
		value, _ := raw.(string)
		if value == id || value == "self" && id == candidate.automationID {
			return false
		}
	}
	return true
}

func (s *Server) reactiveRuleLimited(ctx context.Context, candidate reactiveRuleCandidate) (bool, error) {
	checks := []struct {
		status, since string
		limit         any
	}{
		{"emitted", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano), candidate.limits["max_emits_per_minute"]},
		{"emitted", time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano), candidate.limits["max_emits_per_hour"]},
		{"pending", "", candidate.limits["max_pending_dispatches"]},
	}
	for _, check := range checks {
		limit, ok := intValue(check.limit)
		if !ok {
			return true, nil
		}
		var count int
		var err error
		if check.status == "pending" {
			err = s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM reactive_rule_evaluations WHERE workspace_id=? AND rule_revision_id=? AND dispatch_status IN ('pending','retryable_failed')`, candidate.workspaceID, candidate.revisionID).Scan(&count)
		} else {
			err = s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM reactive_rule_evaluations WHERE workspace_id=? AND rule_revision_id=? AND emitted_command_id IS NOT NULL AND created_at>=?`, candidate.workspaceID, candidate.revisionID, check.since).Scan(&count)
		}
		if err != nil {
			return false, err
		}
		if count >= limit {
			return true, nil
		}
	}
	return false, nil
}

func renderReactiveRuleTemplate(value any, data map[string]any) (any, error) {
	switch value := value.(type) {
	case nil:
		return map[string]any{}, nil
	case []any:
		result := make([]any, len(value))
		for i := range value {
			rendered, err := renderReactiveRuleTemplate(value[i], data)
			if err != nil {
				return nil, err
			}
			result[i] = rendered
		}
		return result, nil
	case map[string]any:
		if len(value) == 1 {
			if raw, ok := value["$from"].(map[string]any); ok {
				source, _ := raw["source"].(string)
				path, _ := raw["path"].(string)
				root := map[string]any{"event.payload": data["payload"], "event.target": data["target"], "event.metadata": data["metadata"]}[source]
				if root == nil {
					return nil, fmt.Errorf("unsupported automation template source %q", source)
				}
				return jsonPointer(root, path)
			}
		}
		result := make(map[string]any, len(value))
		for key, item := range value {
			rendered, err := renderReactiveRuleTemplate(item, data)
			if err != nil {
				return nil, err
			}
			result[key] = rendered
		}
		return result, nil
	default:
		return value, nil
	}
}

func jsonPointer(value any, pointer string) (any, error) {
	if pointer == "" {
		return value, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("automation template path must be a JSON Pointer")
	}
	current := value
	for _, token := range strings.Split(pointer[1:], "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch value := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = value[token]
			if !ok {
				return nil, fmt.Errorf("automation template path %q is missing", pointer)
			}
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(value) {
				return nil, fmt.Errorf("automation template path %q is missing", pointer)
			}
			current = value[index]
		default:
			return nil, fmt.Errorf("automation template path %q is missing", pointer)
		}
	}
	return current, nil
}

func automationID(causation map[string]any) string {
	automation, _ := causation["automation"].(map[string]any)
	id, _ := automation["automation_id"].(string)
	return id
}

func automationDepth(causation map[string]any) int {
	automation, _ := causation["automation"].(map[string]any)
	value, _ := intValue(automation["recursion_depth"])
	return value
}

func intValue(value any) (int, bool) {
	switch value := value.(type) {
	case float64:
		return int(value), value == float64(int(value))
	case int:
		return value, true
	case int64:
		return int(value), true
	default:
		return 0, false
	}
}

func containsString(values []any, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
