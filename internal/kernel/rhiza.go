package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/mrchypark/compos/internal/reactiverule"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (e *Executor) executeRhiza(ctx context.Context, principal string, c Command) (receipt Receipt, err error) {
	defer func() { err = rhizaAvailabilityError(err) }()
	if !map[string]bool{
		"object_type.define": true, "object_type.schema_evolve": true,
		"relation_definition.define": true, "object_type.delete": true, "relation_definition.delete": true,
		"reactive_rule.define": true, "reactive_rule.publish_revision": true, "reactive_rule.disable": true,
		"reactive_rule.retire": true, "reactive_rule.delete": true, "object.delete": true,
		"event.emit": true, "knowledge.propose": true, "knowledge.review": true,
		"knowledge.accept": true, "knowledge.remove": true,
		"work.open": true, "work.route": true, "work.submit": true,
		"work.cancel": true, "work.record_deadline": true,
		"object.create": true, "object.update": true, "object.move": true,
		"relation.link": true, "relation.unlink": true,
		"spec.publish": true, "function.invoke": true, "function.record_result": true,
		"agent.record_result": true, "workflow.complete": true,
	}[c.CommandType] {
		return Receipt{}, reject("unsupported_storage", c.CommandType+" has not been migrated to Rhiza atomic statements")
	}
	if duplicate, found, err := duplicateReceiptRhiza(ctx, e.store, principal, c); err != nil || found {
		return duplicate, err
	}
	var payload map[string]any
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return Receipt{}, err
	}
	actor, guards, err := e.prepareRhiza(ctx, principal, c, payload)
	if err != nil {
		return Receipt{}, err
	}
	switch c.CommandType {
	case "object_type.define":
		return e.defineObjectTypeRhiza(ctx, principal, actor, c, payload, guards)
	case "object_type.schema_evolve":
		return e.evolveObjectTypeRhiza(ctx, principal, actor, c, payload, guards)
	case "relation_definition.define":
		return e.defineRelationRhiza(ctx, principal, actor, c, payload, guards)
	case "object_type.delete":
		return e.archiveObjectTypeRhiza(ctx, principal, actor, c, guards)
	case "relation_definition.delete":
		return e.archiveRelationRhiza(ctx, principal, actor, c, guards)
	case "reactive_rule.define":
		return e.defineReactiveRuleRhiza(ctx, principal, actor, c, payload, guards)
	case "reactive_rule.publish_revision", "reactive_rule.disable", "reactive_rule.retire":
		return e.changeReactiveRuleRhiza(ctx, principal, actor, c, payload, guards)
	case "reactive_rule.delete":
		return e.archiveSimpleRhiza(ctx, principal, actor, c, guards, "reactive_rules", "reactive_rule", "reactive_rule.deleted")
	case "object.delete":
		return e.archiveSimpleRhiza(ctx, principal, actor, c, guards, "object_instances", "object_instance", "object.deleted")
	case "event.emit":
		return e.emitDomainEventRhiza(ctx, principal, actor, c, payload, guards)
	case "knowledge.propose":
		return e.proposeKnowledgeRhiza(ctx, principal, actor, c, payload, guards)
	case "knowledge.review":
		return e.reviewKnowledgeRhiza(ctx, principal, actor, c, payload, guards)
	case "knowledge.accept":
		return e.acceptKnowledgeRhiza(ctx, principal, actor, c, payload, guards)
	case "knowledge.remove":
		return e.removeKnowledgeRhiza(ctx, principal, actor, c, payload, guards)
	case "work.open":
		return e.openWorkRhiza(ctx, principal, actor, c, payload, guards)
	case "work.route":
		return e.routeWorkRhiza(ctx, principal, actor, c, payload, guards)
	case "work.submit":
		if _, hasChanges := payload["changes"]; hasChanges {
			return e.submitWorkChangesRhiza(ctx, principal, actor, c, payload, guards)
		}
		return e.submitWorkRhiza(ctx, principal, actor, c, payload, guards)
	case "work.cancel":
		return e.cancelWorkRhiza(ctx, principal, actor, c, payload, guards)
	case "work.record_deadline":
		return e.recordWorkDeadlineRhiza(ctx, principal, actor, c, payload, guards)
	case "object.create":
		return e.createObjectRhiza(ctx, principal, actor, c, payload, guards)
	case "object.update":
		return e.updateObjectRhiza(ctx, principal, actor, c, payload, guards)
	case "object.move":
		return e.moveObjectRhiza(ctx, principal, actor, c, payload, guards)
	case "relation.link":
		return e.linkObjectRelationRhiza(ctx, principal, actor, c, payload, guards)
	case "relation.unlink":
		return e.unlinkObjectRelationRhiza(ctx, principal, actor, c, payload, guards)
	case "spec.publish":
		return e.publishSpecRhiza(ctx, principal, actor, c, payload, guards)
	case "function.invoke":
		return e.invokeFunctionRhiza(ctx, principal, actor, c, payload, guards)
	case "function.record_result":
		return e.recordFunctionResultRhiza(ctx, principal, actor, c, payload, guards)
	case "agent.record_result":
		return e.recordAgentResultRhiza(ctx, principal, actor, c, payload, guards)
	case "workflow.complete":
		return e.completeWorkflowRhiza(ctx, principal, actor, c, payload, guards)
	}
	panic("unreachable Rhiza command")
}

func (e *Executor) defineReactiveRuleRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	automationID, _ := payload["automation_id"].(string)
	for _, name := range []string{"automation_id", "rule_key", "name"} {
		if value, _ := payload[name].(string); strings.TrimSpace(value) == "" {
			return Receipt{}, reject("schema_mismatch", "ReactiveRule "+name+" must not be empty")
		}
	}
	requested, _ := payload["requested_actor"].(map[string]any)
	if err := onlyKeys("ReactiveRule requested_actor", requested, "type", "id"); err != nil {
		return Receipt{}, err
	}
	if requested["type"] != "automation" || requested["id"] != automationID {
		return Receipt{}, reject("validation_failed", "ReactiveRule requested_actor must match automation_id")
	}
	action, _ := payload["action"].(map[string]any)
	if err := onlyKeys("ReactiveRule action", action, "action_id", "kind", "command_type", "target_template", "payload_template", "preconditions_template"); err != nil {
		return Receipt{}, err
	}
	actionType, _ := action["command_type"].(string)
	actionDefinition, ok := definition(actionType)
	if !ok || !actionDefinition.AutomationActionAllowed {
		return Receipt{}, reject("forbidden", "automation action command_type is not allowlisted")
	}
	trigger, _ := payload["trigger"].(map[string]any)
	if err := onlyKeys("automation trigger", trigger, "source", "event_type", "event_schema_version", "aggregate_type", "allowed_causation_sources", "excluded_automation_ids"); err != nil {
		return Receipt{}, err
	}
	if trigger["source"] != "kernel_canonical_event" {
		return Receipt{}, reject("schema_mismatch", "automation trigger source must be kernel_canonical_event")
	}
	if version, ok := integer(trigger["event_schema_version"]); !ok || version != 1 {
		return Receipt{}, reject("schema_mismatch", "automation trigger event_schema_version must be 1")
	}
	if eventType, _ := trigger["event_type"].(string); strings.TrimSpace(eventType) == "" {
		return Receipt{}, reject("schema_mismatch", "automation trigger event_type is required")
	} else if !ReactiveRuleTriggerEventAllowed(eventType) {
		return Receipt{}, reject("forbidden", "automation trigger event_type is not a public Kernel event")
	}
	if aggregate, exists := trigger["aggregate_type"]; exists {
		if value, ok := aggregate.(string); !ok || strings.TrimSpace(value) == "" {
			return Receipt{}, reject("schema_mismatch", "automation trigger aggregate_type must be a non-empty string")
		}
	}
	for _, name := range []string{"allowed_causation_sources", "excluded_automation_ids"} {
		if err := validateAutomationStringList(name, trigger[name]); err != nil {
			return Receipt{}, err
		}
	}
	predicates, _ := payload["predicates"].(map[string]any)
	if err := reactiverule.ValidatePredicate(predicates); err != nil {
		return Receipt{}, reject("schema_mismatch", "automation predicates: "+err.Error())
	}
	if action["kind"] != "emit_command" {
		return Receipt{}, reject("schema_mismatch", "automation action kind must be emit_command")
	}
	if actionID, _ := action["action_id"].(string); strings.TrimSpace(actionID) == "" {
		return Receipt{}, reject("schema_mismatch", "automation action_id is required")
	}
	if _, ok := action["target_template"].(map[string]any); !ok {
		return Receipt{}, reject("schema_mismatch", "automation action target_template must be an object")
	}
	if _, ok := action["payload_template"].(map[string]any); !ok {
		return Receipt{}, reject("schema_mismatch", "automation action payload_template must be an object")
	}
	if _, ok := action["preconditions_template"].([]any); !ok {
		return Receipt{}, reject("schema_mismatch", "automation action preconditions_template must be an array")
	}
	for _, name := range []string{"target_template", "payload_template", "preconditions_template"} {
		if err := reactiverule.ValidateTemplate(action[name]); err != nil {
			return Receipt{}, reject("schema_mismatch", "automation action "+name+": "+err.Error())
		}
	}
	idempotency, _ := payload["idempotency"].(map[string]any)
	if len(idempotency) != 0 {
		return Receipt{}, reject("schema_mismatch", "automation idempotency is system-owned in v1")
	}
	limits, _ := payload["limits"].(map[string]any)
	if err := onlyKeys("automation limits", limits, "max_recursion_depth", "max_emits_per_minute", "max_emits_per_hour", "max_pending_dispatches", "failure_threshold"); err != nil {
		return Receipt{}, err
	}
	for _, name := range []string{"max_recursion_depth", "max_emits_per_minute", "max_emits_per_hour", "max_pending_dispatches", "failure_threshold"} {
		value, ok := integer(limits[name])
		if !ok || value < 0 || name != "max_recursion_depth" && value < 1 {
			return Receipt{}, reject("schema_mismatch", "automation limits."+name+" is invalid")
		}
	}
	id, revision, ts := aggregateID("arl", c.CommandID), aggregateID("arv", c.CommandID), timestamp(e.now().UTC())
	definitionJSON, actorJSON := canonical(payload), canonical(payload["requested_actor"])
	hash, one := digestJSON([]byte(definitionJSON)), int64(1)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `INSERT INTO reactive_rules(id,workspace_id,automation_id,rule_key,name,status,current_revision_id,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,'draft',NULL,1,?,?,?,?)`, Args: []any{id, c.WorkspaceID, payload["automation_id"], payload["rule_key"], payload["name"], ts, ts, principal, principal}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO reactive_rule_revisions(id,workspace_id,rule_id,automation_id,definition_schema_version,definition_hash,requested_actor_json,permission_policy,trigger_json,predicates_json,action_json,idempotency_json,limits_json,created_at,created_by_principal_id) VALUES(?,?,?,?,1,?,?,'owner_principal',?,?,?,?,?,?,?)`, Args: []any{revision, c.WorkspaceID, id, payload["automation_id"], hash, actorJSON, canonical(payload["trigger"]), canonical(payload["predicates"]), canonical(payload["action"]), canonical(payload["idempotency"]), canonical(payload["limits"]), ts, principal}, ExpectedRowsAffected: &one},
	)
	effect := makeEffect(id, "reactive_rule", 1, "reactive_rule.defined", map[string]any{"id": id, "revision_id": revision, "definition_hash": hash, "version": 1})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) changeReactiveRuleRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	id := *c.Target.AggregateID
	ts := timestamp(e.now().UTC())
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM reactive_rules WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&version); err != nil {
		return Receipt{}, reject("precondition_failed", "ReactiveRule does not exist")
	}
	next, status, event := version+1, "active", "reactive_rule.revision_published"
	var revision any
	one := int64(1)
	if c.CommandType == "reactive_rule.publish_revision" {
		revision = payload["revision_id"]
		cursor, _ := payload["activation_event_cursor"].(string)
		if parsed, err := strconv.ParseInt(cursor, 10, 64); err != nil || parsed < 0 {
			return Receipt{}, reject("validation_failed", "activation_event_cursor must be a projection sequence")
		}
		var exists int
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT 1 FROM reactive_rule_revisions WHERE workspace_id=? AND rule_id=? AND id=?`, c.WorkspaceID, id, revision).Scan(&exists); err != nil {
			return Receipt{}, reject("precondition_failed", "ReactiveRule revision does not exist")
		}
		statements = append(statements,
			rhiza.SQLStatement{SQL: `SELECT 1 FROM reactive_rule_revisions WHERE workspace_id=? AND rule_id=? AND id=?`, Args: []any{c.WorkspaceID, id, revision}, WantRows: true, ExpectedReturnedRows: &one},
			rhiza.SQLStatement{SQL: `UPDATE reactive_rule_revisions SET published_at=COALESCE(published_at,?) WHERE workspace_id=? AND rule_id=? AND id=?`, Args: []any{ts, c.WorkspaceID, id, revision}, ExpectedRowsAffected: &one},
		)
	} else if c.CommandType == "reactive_rule.disable" {
		status, event = "disabled", "reactive_rule.disabled"
	} else {
		status, event = "retired", "reactive_rule.retired"
	}
	reason, _ := payload["reason"].(string)
	circuitBreaker, _ := payload["circuit_breaker"].(bool)
	cursor, _ := payload["activation_event_cursor"].(string)
	statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE reactive_rules SET status=?,current_revision_id=COALESCE(?,current_revision_id),version=?,updated_at=?,updated_by_principal_id=?,enabled_at=CASE WHEN ?='active' THEN ? ELSE enabled_at END,disabled_at=CASE WHEN ?='disabled' THEN ? ELSE disabled_at END,activation_event_cursor=CASE WHEN ?='active' THEN ? ELSE activation_event_cursor END,last_disabled_reason=CASE WHEN ?='disabled' THEN NULLIF(?,'') ELSE last_disabled_reason END,circuit_breaker_status=CASE WHEN ?='disabled' AND ? THEN 'open' WHEN ?='active' THEN 'closed' ELSE circuit_breaker_status END WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=?`, Args: []any{status, revision, next, ts, principal, status, ts, status, ts, status, cursor, status, reason, status, circuitBreaker, status, c.WorkspaceID, id, version}, ExpectedRowsAffected: &one})
	effect := makeEffect(id, "reactive_rule", next, event, map[string]any{"id": id, "status": status, "version": next})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) archiveSimpleRhiza(ctx context.Context, principal string, actor Actor, c Command, statements []rhiza.SQLStatement, table, kind, event string) (Receipt, error) {
	id := *c.Target.AggregateID
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM `+table+` WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&version); err != nil {
		return Receipt{}, reject("precondition_failed", kind+" does not exist")
	}
	next, ts, one := version+1, timestamp(e.now().UTC()), int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE ` + table + ` SET archived_at=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=?`, Args: []any{ts, next, ts, principal, c.WorkspaceID, id, version}, ExpectedRowsAffected: &one})
	effect := makeEffect(id, kind, next, event, map[string]any{"id": id, "archived_at": ts, "version": next})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) prepareRhiza(ctx context.Context, principal string, c Command, payload map[string]any) (Actor, []rhiza.SQLStatement, error) {
	zero, one := int64(0), int64(1)
	statements := []rhiza.SQLStatement{{SQL: `SELECT command_id FROM command_ledger WHERE workspace_id=? AND authenticated_principal_id=? AND idempotency_key=?`, Args: []any{c.WorkspaceID, principal, c.IdempotencyKey}, WantRows: true, ExpectedReturnedRows: &zero}}
	var member int
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT 1 FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, c.WorkspaceID, principal).Scan(&member); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Actor{}, nil, reject("forbidden", "workspace membership is required")
		}
		return Actor{}, nil, err
	}
	statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, Args: []any{c.WorkspaceID, principal}, WantRows: true, ExpectedReturnedRows: &one})
	if reactiveRuleLifecycleCommand(c.CommandType) {
		var creator string
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT created_by_principal_id FROM reactive_rules WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, *c.Target.AggregateID).Scan(&creator); err != nil {
			return Actor{}, nil, reject("precondition_failed", "ReactiveRule does not exist")
		}
		if creator != principal {
			return Actor{}, nil, reject("forbidden", "ReactiveRule lifecycle commands require the creator principal")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM reactive_rules WHERE workspace_id=? AND id=? AND archived_at IS NULL AND created_by_principal_id=?`, Args: []any{c.WorkspaceID, *c.Target.AggregateID, principal}, WantRows: true, ExpectedReturnedRows: &one})
	}

	actor := Actor{Type: "user", ID: principal}
	if c.RequestedActor != nil {
		actor = *c.RequestedActor
		if actor.ID == "" {
			return Actor{}, nil, reject("forbidden", "requested actor id is required")
		}
		if actor.Type == "user" {
			if actor.ID != principal {
				return Actor{}, nil, reject("forbidden", "user actor must match the authenticated principal")
			}
		} else {
			if actor.Type != "service" && actor.Type != "automation" {
				return Actor{}, nil, reject("forbidden", "requested actor type is invalid")
			}
			if actor.Type == "automation" && automationActorDenied(c.CommandType) {
				return Actor{}, nil, reject("forbidden", "automation actor may not execute this command")
			}
			relation, _ := payload["relation_definition_id"].(string)
			var capabilityID, capabilityRelation, prefix string
			var max sql.NullInt64
			err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,relation_definition_id,idempotency_key_prefix,max_automation_recursion_depth FROM actor_capabilities WHERE workspace_id=? AND principal_id=? AND actor_type=? AND actor_id=? AND command_type=? AND causation_source=? AND relation_definition_id IN ('',?) ORDER BY relation_definition_id DESC LIMIT 1`, c.WorkspaceID, principal, actor.Type, actor.ID, c.CommandType, c.Causation.Source, relation).Scan(&capabilityID, &capabilityRelation, &prefix, &max)
			if err != nil {
				return Actor{}, nil, reject("forbidden", "requested actor capability is missing")
			}
			if !strings.HasPrefix(c.IdempotencyKey, prefix) {
				return Actor{}, nil, reject("forbidden", "idempotency key is outside the actor capability")
			}
			if actor.Type == "automation" && max.Valid {
				var automation struct {
					RecursionDepth int64 `json:"recursion_depth"`
				}
				if len(c.Causation.Automation) == 0 || json.Unmarshal(c.Causation.Automation, &automation) != nil || automation.RecursionDepth > max.Int64 {
					return Actor{}, nil, reject("automation_recursion_limit", "automation recursion limit exceeded")
				}
			}
			var maxValue any
			if max.Valid {
				maxValue = max.Int64
			}
			statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM actor_capabilities WHERE id=? AND workspace_id=? AND principal_id=? AND actor_type=? AND actor_id=? AND command_type=? AND causation_source=? AND relation_definition_id=? AND idempotency_key_prefix=? AND max_automation_recursion_depth IS ?`, Args: []any{capabilityID, c.WorkspaceID, principal, actor.Type, actor.ID, c.CommandType, c.Causation.Source, capabilityRelation, prefix, maxValue}, WantRows: true, ExpectedReturnedRows: &one})
		}
	}

	d, _ := definition(c.CommandType)
	for _, required := range d.RequiredPreconditions {
		found := false
		for _, precondition := range c.Preconditions {
			found = found || precondition.AggregateType == required
		}
		if !found {
			return Actor{}, nil, reject("precondition_failed", c.CommandType+" requires a "+required+" precondition")
		}
	}
	for _, precondition := range c.Preconditions {
		if precondition.AggregateType == "object_instance" && e.workObjects[precondition.AggregateID] != nil {
			if precondition.ExpectedVersion != 1 {
				return Actor{}, nil, reject("precondition_failed", "new object precondition version must be 1")
			}
			continue
		}
		table := map[string]string{"object_type": "object_types", "object_instance": "object_instances", "object_relation": "object_relations", "relation_definition": "relation_definitions", "reactive_rule": "reactive_rules"}[precondition.AggregateType]
		if table == "" {
			return Actor{}, nil, reject("precondition_failed", "unknown precondition aggregate_type")
		}
		var version int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM `+table+` WHERE workspace_id=? AND id=?`, c.WorkspaceID, precondition.AggregateID).Scan(&version); err != nil {
			return Actor{}, nil, reject("precondition_failed", "precondition aggregate does not exist")
		}
		if version != precondition.ExpectedVersion {
			return Actor{}, nil, reject("precondition_failed", "precondition version does not match")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM ` + table + ` WHERE workspace_id=? AND id=? AND version=?`, Args: []any{c.WorkspaceID, precondition.AggregateID, precondition.ExpectedVersion}, WantRows: true, ExpectedReturnedRows: &one})
	}
	return actor, statements, nil
}

func rhizaAvailabilityError(err error) error {
	if err == nil {
		return nil
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) {
		return err
	}
	if errors.Is(err, rhiza.ErrNotReady) || errors.Is(err, rhiza.ErrQuorumUnavailable) || errors.Is(err, rhiza.ErrDurabilityUnavailable) || errors.Is(err, rhiza.ErrCommitUnknown) {
		return &CommandError{Outcome: "quorum_unavailable", Message: "Rhiza storage is temporarily unavailable; retry", Retryable: true}
	}
	return err
}

func (e *Executor) defineObjectTypeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	fieldRaw, _ := json.Marshal(payload["field_schema"])
	field, err := validateFieldSchema(fieldRaw)
	if err != nil {
		return Receipt{}, err
	}
	id := aggregateID("otp", c.CommandID)
	fieldJSON, relationJSON := canonical(field), `{"links":[]}`
	state, _ := payload["state_machine_id"].(string)
	digest := schemaDigest(1, fieldJSON, relationJSON, state)
	ts := timestamp(e.now().UTC())
	one := int64(1)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `INSERT INTO object_types(id,workspace_id,key,singular,plural,schema_version,field_schema_json,relation_schema_json,state_machine_id,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,1,?,?,NULLIF(?,''),1,?,?,?,?)`, Args: []any{id, c.WorkspaceID, payload["key"], payload["singular"], payload["plural"], fieldJSON, relationJSON, state, ts, ts, principal, principal}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO object_type_schema_revisions(workspace_id,object_type_id,schema_version,field_schema_json,relation_schema_json,state_machine_id,digest,created_at,created_by_principal_id) VALUES(?,?,1,?,?,NULLIF(?,''),?,?,?)`, Args: []any{c.WorkspaceID, id, fieldJSON, relationJSON, state, digest, ts, principal}, ExpectedRowsAffected: &one},
	)
	effect := makeEffect(id, "object_type", 1, "object_type.defined", map[string]any{"id": id, "key": payload["key"], "schema_version": 1, "digest": digest})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) evolveObjectTypeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	id := *c.Target.AggregateID
	fieldRaw, _ := json.Marshal(payload["field_schema"])
	field, err := validateFieldSchema(fieldRaw)
	if err != nil {
		return Receipt{}, err
	}
	var relationJSON, previousFieldJSON string
	var state sql.NullString
	var schemaVersion, version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT relation_schema_json,state_machine_id,schema_version,version,field_schema_json FROM object_types WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&relationJSON, &state, &schemaVersion, &version, &previousFieldJSON); err != nil {
		return Receipt{}, reject("precondition_failed", "object type does not exist")
	}
	var previous map[string]any
	_ = json.Unmarshal([]byte(previousFieldJSON), &previous)
	if requested, exists := field["state_machine"]; exists {
		if !equalJSON(requested, previous["state_machine"]) {
			return Receipt{}, reject("validation_failed", "field_schema.state_machine is inherited and cannot be changed")
		}
	} else if inherited, exists := previous["state_machine"]; exists {
		field["state_machine"] = inherited
	}
	fieldJSON := canonical(field)
	nextSchema, nextVersion := schemaVersion+1, version+1
	digest := schemaDigest(nextSchema, fieldJSON, relationJSON, state.String)
	ts := timestamp(e.now().UTC())
	one := int64(1)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `UPDATE object_types SET field_schema_json=?,schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=? AND schema_version=? AND field_schema_json=? AND relation_schema_json=? AND state_machine_id IS ?`, Args: []any{fieldJSON, nextSchema, nextVersion, ts, principal, c.WorkspaceID, id, version, schemaVersion, previousFieldJSON, relationJSON, nullable(state.String)}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO object_type_schema_revisions VALUES(?,?,?,?,?,?,?,?,?)`, Args: []any{c.WorkspaceID, id, nextSchema, fieldJSON, relationJSON, nullable(state.String), digest, ts, principal}, ExpectedRowsAffected: &one},
	)
	effect := makeEffect(id, "object_type", nextVersion, "object_type.schema_evolved", map[string]any{"id": id, "schema_version": nextSchema, "digest": digest})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

type rhizaRelation struct {
	id, key, from, to, cardinality string
	required                       bool
}

type rhizaObjectType struct {
	id, field, relation string
	state               sql.NullString
	schemaVersion       int64
	version             int64
}

func (e *Executor) defineRelationRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	id := aggregateID("rdf", c.CommandID)
	relation := rhizaRelation{id: id, key: fmt.Sprint(payload["key"]), from: fmt.Sprint(payload["from_object_type_id"]), to: fmt.Sprint(payload["to_object_type_id"]), cardinality: fmt.Sprint(payload["cardinality"]), required: payload["required"] == true}
	types, relations, err := e.readRelationSchemaState(ctx, c.WorkspaceID, relation.from, relation.to)
	if err != nil {
		return Receipt{}, err
	}
	relations = append(relations, relation)
	ts := timestamp(e.now().UTC())
	one, required := int64(1), 0
	if relation.required {
		required = 1
	}
	statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO relation_definitions(id,workspace_id,key,from_object_type_id,to_object_type_id,cardinality,required,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,?,1,?,?,?,?)`, Args: []any{id, c.WorkspaceID, relation.key, relation.from, relation.to, relation.cardinality, required, ts, ts, principal, principal}, ExpectedRowsAffected: &one})
	statements = append(statements, refreshRelationSchemaStatements(c.WorkspaceID, principal, ts, types, relations)...)
	effect := makeEffect(id, "relation_definition", 1, "relation_definition.defined", map[string]any{"id": id, "key": relation.key, "version": 1})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) archiveObjectTypeRhiza(ctx context.Context, principal string, actor Actor, c Command, statements []rhiza.SQLStatement) (Receipt, error) {
	id := *c.Target.AggregateID
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM object_types WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&version); err != nil {
		return Receipt{}, reject("precondition_failed", "object_type does not exist")
	}
	next, ts, one := version+1, timestamp(e.now().UTC()), int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE object_types SET archived_at=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=?`, Args: []any{ts, next, ts, principal, c.WorkspaceID, id, version}, ExpectedRowsAffected: &one})
	effect := makeEffect(id, "object_type", next, "object_type.deleted", map[string]any{"id": id, "archived_at": ts, "version": next})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) archiveRelationRhiza(ctx context.Context, principal string, actor Actor, c Command, statements []rhiza.SQLStatement) (Receipt, error) {
	id := *c.Target.AggregateID
	var relation rhizaRelation
	var required int
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,key,from_object_type_id,to_object_type_id,cardinality,required,version FROM relation_definitions WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&relation.id, &relation.key, &relation.from, &relation.to, &relation.cardinality, &required, &version); err != nil {
		return Receipt{}, reject("precondition_failed", "relation_definition does not exist")
	}
	relation.required = required == 1
	types, relations, err := e.readRelationSchemaState(ctx, c.WorkspaceID, relation.from, relation.to)
	if err != nil {
		return Receipt{}, err
	}
	kept := relations[:0]
	for _, current := range relations {
		if current.id != id {
			kept = append(kept, current)
		}
	}
	next, ts, one := version+1, timestamp(e.now().UTC()), int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE relation_definitions SET archived_at=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=?`, Args: []any{ts, next, ts, principal, c.WorkspaceID, id, version}, ExpectedRowsAffected: &one})
	statements = append(statements, refreshRelationSchemaStatements(c.WorkspaceID, principal, ts, types, kept)...)
	effect := makeEffect(id, "relation_definition", next, "relation_definition.deleted", map[string]any{"id": id, "archived_at": ts, "version": next})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) readRelationSchemaState(ctx context.Context, workspace, from, to string) (map[string]rhizaObjectType, []rhizaRelation, error) {
	types := map[string]rhizaObjectType{}
	relations := []rhizaRelation{}
	_, err := e.store.ReadSnapshot(ctx, store.ReadLinearizable, func(reader store.SnapshotReader) error {
		for _, id := range []string{from, to} {
			if _, exists := types[id]; exists {
				continue
			}
			state := rhizaObjectType{id: id}
			if err := reader.ReadRow(ctx, `SELECT field_schema_json,relation_schema_json,state_machine_id,schema_version,version FROM object_types WHERE workspace_id=? AND id=?`, workspace, id).Scan(&state.field, &state.relation, &state.state, &state.schemaVersion, &state.version); err != nil {
				return reject("validation_failed", "relation endpoint object type does not exist")
			}
			types[id] = state
		}
		rows, err := reader.ReadRows(ctx, `SELECT id,key,from_object_type_id,to_object_type_id,cardinality,required FROM relation_definitions WHERE workspace_id=? AND archived_at IS NULL AND (from_object_type_id IN (?,?) OR to_object_type_id IN (?,?)) ORDER BY key,id`, workspace, from, to, from, to)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var relation rhizaRelation
			var required int
			if err := rows.Scan(&relation.id, &relation.key, &relation.from, &relation.to, &relation.cardinality, &required); err != nil {
				return err
			}
			relation.required = required == 1
			relations = append(relations, relation)
		}
		return rows.Err()
	})
	return types, relations, err
}

func refreshRelationSchemaStatements(workspace, principal, ts string, types map[string]rhizaObjectType, relations []rhizaRelation) []rhiza.SQLStatement {
	ids := make([]string, 0, len(types))
	for id := range types {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	one := int64(1)
	statements := make([]rhiza.SQLStatement, 0, len(ids)*2)
	for _, id := range ids {
		state := types[id]
		links := []map[string]any{}
		for _, relation := range relations {
			if relation.from == id || relation.to == id {
				links = append(links, map[string]any{"id": relation.id, "key": relation.key, "from_object_type_id": relation.from, "to_object_type_id": relation.to, "cardinality": relation.cardinality, "required": relation.required})
			}
		}
		sort.Slice(links, func(i, j int) bool {
			if links[i]["key"] == links[j]["key"] {
				return links[i]["id"].(string) < links[j]["id"].(string)
			}
			return links[i]["key"].(string) < links[j]["key"].(string)
		})
		relationJSON := canonical(map[string]any{"links": links})
		nextSchema, nextVersion := state.schemaVersion+1, state.version+1
		digest := schemaDigest(nextSchema, state.field, relationJSON, state.state.String)
		statements = append(statements,
			rhiza.SQLStatement{SQL: `UPDATE object_types SET relation_schema_json=?,schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND version=? AND schema_version=? AND field_schema_json=? AND relation_schema_json=? AND state_machine_id IS ?`, Args: []any{relationJSON, nextSchema, nextVersion, ts, principal, workspace, id, state.version, state.schemaVersion, state.field, state.relation, nullable(state.state.String)}, ExpectedRowsAffected: &one},
			rhiza.SQLStatement{SQL: `INSERT INTO object_type_schema_revisions VALUES(?,?,?,?,?,?,?,?,?)`, Args: []any{workspace, id, nextSchema, state.field, relationJSON, nullable(state.state.String), digest, ts, principal}, ExpectedRowsAffected: &one},
		)
	}
	return statements
}

func (e *Executor) commitRhiza(ctx context.Context, principal string, actor Actor, c Command, statements []rhiza.SQLStatement, effect effect, ts string) (Receipt, error) {
	if e.rhizaCommit != nil {
		return e.rhizaCommit(ctx, principal, actor, c, statements, effect, ts)
	}
	if len(e.rhizaSecondary) > 0 {
		statements = compactWorkGuards(statements)
	}
	zero, one := int64(0), int64(1)
	journalStatements, err := e.journalStatementsRhiza(ctx, principal, c, effect.journal, ts)
	if err != nil {
		return Receipt{}, err
	}
	version := effect.aggregate.Version
	receipt := Receipt{CommandID: c.CommandID, Outcome: "applied", Aggregate: &effect.aggregate, AggregateVersion: &version, WorkspaceID: &c.WorkspaceID, ProjectionSequence: &zero, ClientRefs: e.workResults}
	effectJSON := canonical(effect.payload)
	outboxEvent, outboxPayload := effect.event, effectJSON
	if effect.outboxEvent != "" {
		outboxEvent, outboxPayload = effect.outboxEvent, canonical(effect.outboxPayload)
	}
	sequenceStatement := len(statements)
	statements = append(statements, rhiza.SQLStatement{SQL: `SELECT COALESCE(MAX(projection_sequence),0)+1 AS projection_sequence FROM projection_events WHERE workspace_id=?`, Args: []any{c.WorkspaceID}, WantRows: true, ExpectedReturnedRows: &one})
	statements = append(statements,
		rhiza.SQLStatement{SQL: `INSERT INTO command_ledger(command_id,workspace_id,authenticated_principal_id,requested_actor_json,effective_actor_json,actor_context_hash,idempotency_key,command_type,target_json,preconditions_json,causation_json,payload_hash,payload_json,outcome,result_json,effective_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'applied',json_set(?, '$.projection_sequence', ? + ` + strconv.Itoa(len(e.rhizaSecondary)) + `),?,?)`, Args: []any{c.CommandID, c.WorkspaceID, principal, canonical(c.RequestedActor), canonical(actor), digestJSON([]byte(canonical(actor))), c.IdempotencyKey, c.CommandType, canonical(c.Target), canonical(c.Preconditions), canonical(c.Causation), digestJSON(c.Payload), string(c.Payload), canonical(receipt), nil, ts, ts}, ExpectedRowsAffected: &one, OutputRefs: []rhiza.SQLStatementOutputRef{{ArgIndex: 14, StatementIndex: sequenceStatement, ColumnName: "projection_sequence"}}},
	)
	statements = append(statements, journalStatements...)
	for index, secondary := range e.rhizaSecondary {
		statements = appendRhizaSecondaryEffect(statements, c, secondary, ts, sequenceStatement, index)
	}
	statements = append(statements,
		rhiza.SQLStatement{SQL: `INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, Args: []any{newID("act"), c.WorkspaceID, c.CommandID, effect.aggregate.Type, effect.aggregate.ID, effect.event, effectJSON, ts}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO outbox_events VALUES(?,?,?,?,?,'pending',?,?)`, Args: []any{newID("evt"), c.WorkspaceID, c.CommandID, outboxEvent, outboxPayload, ts, ts}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,? + ` + strconv.Itoa(len(e.rhizaSecondary)) + `,?,?,?,?,?,?)`, Args: []any{newID("prj"), c.WorkspaceID, c.CommandID, nil, effect.aggregate.Type, effect.aggregate.ID, effect.aggregate.Version, digestJSON([]byte(effectJSON)), effectJSON, ts}, ExpectedRowsAffected: &one, OutputRefs: []rhiza.SQLStatementOutputRef{{ArgIndex: 3, StatementIndex: sequenceStatement, ColumnName: "projection_sequence"}}},
	)
	// The command ledger is the idempotency boundary. A fresh Rhiza request ID
	// lets a retry converge through that guard even though audit timestamps and
	// event IDs are intentionally generated per attempt.
	// ponytail: Rhiza v0.12 caps one transaction at 64 statements; never split an atomic submission.
	if len(statements) > 64 {
		return Receipt{}, reject("validation_failed", "atomic command exceeds Rhiza's 64-statement transaction limit; reduce the number or complexity of changes")
	}
	if err := e.store.ApplyStatements(ctx, newID("rhz"), statements); err != nil {
		if duplicate, found, duplicateErr := duplicateReceiptRhiza(ctx, e.store, principal, c); duplicateErr != nil || found {
			return duplicate, duplicateErr
		}
		if errors.Is(err, store.ErrMutationPreconditionFailed) {
			return Receipt{}, reject("precondition_failed", "command state changed before apply")
		}
		if errors.Is(err, store.ErrMutationExecutionFailed) {
			return Receipt{}, reject("validation_failed", "database constraint rejected the command")
		}
		return Receipt{}, err
	}
	return storedReceiptRhiza(ctx, e.store, c.CommandID, false)
}

func duplicateReceiptRhiza(ctx context.Context, db *store.Store, principal string, c Command) (Receipt, bool, error) {
	var commandID, commandType, storedHash, target, preconditions, causation, requested, result string
	err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT command_id,command_type,payload_hash,target_json,preconditions_json,causation_json,requested_actor_json,result_json FROM command_ledger WHERE workspace_id=? AND authenticated_principal_id=? AND idempotency_key=?`, c.WorkspaceID, principal, c.IdempotencyKey).Scan(&commandID, &commandType, &storedHash, &target, &preconditions, &causation, &requested, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, err
	}
	if commandID != c.CommandID || commandType != c.CommandType || storedHash != digestJSON(c.Payload) || target != canonical(c.Target) || preconditions != canonical(c.Preconditions) || causation != canonical(c.Causation) || requested != canonical(c.RequestedActor) {
		return Receipt{}, true, reject("idempotency_conflict", "idempotency key was already used for a different command")
	}
	var receipt Receipt
	if err := json.Unmarshal([]byte(result), &receipt); err != nil {
		return Receipt{}, true, err
	}
	receipt.Outcome = "duplicate"
	return receipt, true, nil
}

func storedReceiptRhiza(ctx context.Context, db *store.Store, commandID string, duplicate bool) (Receipt, error) {
	var raw string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT result_json FROM command_ledger WHERE command_id=?`, commandID).Scan(&raw); err != nil {
		return Receipt{}, fmt.Errorf("read committed Rhiza receipt: %w", err)
	}
	var receipt Receipt
	if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
		return Receipt{}, err
	}
	if duplicate {
		receipt.Outcome = "duplicate"
	}
	return receipt, nil
}
