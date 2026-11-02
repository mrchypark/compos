package kernel

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/compos/internal/reactiverule"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

type Executor struct {
	AuthorizeCommand CommandAuthorizer
	transact         func(context.Context, string, func(store.Tx) error) error
	store            *store.Store
	now              func() time.Time
	rhizaCommit      func(context.Context, string, Actor, Command, []rhiza.SQLStatement, effect, string) (Receipt, error)
	rhizaSecondary   []effect
	workObjects      map[string]*workPendingObject
	workSequences    *workSequenceReservations
	workResults      map[string]WorkChangeResult
}

func NewExecutor(db *sql.DB) *Executor {
	return &Executor{now: time.Now, transact: func(ctx context.Context, _ string, fn func(store.Tx) error) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	}}
}

func NewStoreExecutor(s *store.Store) *Executor {
	return &Executor{now: time.Now, transact: s.Transact, store: s}
}

type effect struct {
	aggregate     Aggregate
	event         string
	payload       map[string]any
	journal       []journalFact
	outboxEvent   string
	outboxPayload map[string]any
}

func (e *Executor) Execute(ctx context.Context, principal string, c Command) (Receipt, error) {
	if err := ValidateCommand(c); err != nil {
		return Receipt{}, err
	}
	if principal == "" {
		return Receipt{}, reject("forbidden", "authenticated principal is required")
	}
	if (c.CommandType == "function.record_result" || c.CommandType == "agent.record_result") && c.Causation.Source != "adapter_reducer" {
		return Receipt{}, reject("forbidden", "external runtime result commands require adapter_reducer causation")
	}
	ctx = context.WithValue(ctx, commandAuthorizerKey{}, e.AuthorizeCommand)
	if _, err := authorize(ctx, nil, principal, c); err != nil {
		return Receipt{}, err
	}
	if e.store != nil && e.store.Kind() == "rhiza" {
		return e.executeRhiza(ctx, principal, c)
	}
	now := e.now().UTC()
	payloadHash := digestJSON(c.Payload)
	var receipt Receipt
	err := e.transact(ctx, c.CommandID, func(tx store.Tx) error {
		if duplicate, found, err := duplicateReceipt(ctx, tx, principal, c, payloadHash); err != nil || found {
			receipt = duplicate
			return err
		}
		var workspaceExists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM workspaces WHERE id=?`, c.WorkspaceID).Scan(&workspaceExists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return reject("forbidden", "workspace does not exist")
			}
			return err
		}
		effectiveActor, err := authorize(ctx, tx, principal, c)
		if err != nil {
			return err
		}
		if err := checkPreconditions(ctx, tx, c); err != nil {
			return err
		}
		if c.CommandType == "work.submit" {
			var payload map[string]any
			if err := json.Unmarshal(c.Payload, &payload); err != nil {
				return err
			}
			if _, present := payload["changes"]; present {
				receipt, err = e.submitWorkChanges(ctx, tx, principal, effectiveActor, c, payloadHash, payload, now)
				return err
			}
		}
		effect, err := e.apply(ctx, tx, principal, c, now)
		if err != nil {
			return err
		}
		sequence, err := nextProjectionSequence(ctx, tx, c.WorkspaceID)
		if err != nil {
			return err
		}
		version := effect.aggregate.Version
		receipt = Receipt{CommandID: c.CommandID, Outcome: "applied", Aggregate: &effect.aggregate, AggregateVersion: &version, WorkspaceID: &c.WorkspaceID, ProjectionSequence: &sequence}
		return record(ctx, tx, principal, effectiveActor, c, payloadHash, now, sequence, receipt, effect)
	})
	return receipt, err
}

func duplicateReceipt(ctx context.Context, tx store.Tx, principal string, c Command, payloadHash string) (Receipt, bool, error) {
	var commandID, commandType, storedHash, target, preconditions, causation, requested, result string
	err := tx.QueryRowContext(ctx, `SELECT command_id,command_type,payload_hash,target_json,preconditions_json,causation_json,requested_actor_json,result_json FROM command_ledger WHERE workspace_id=? AND authenticated_principal_id=? AND idempotency_key=?`, c.WorkspaceID, principal, c.IdempotencyKey).Scan(&commandID, &commandType, &storedHash, &target, &preconditions, &causation, &requested, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, err
	}
	if commandID != c.CommandID || commandType != c.CommandType || storedHash != payloadHash || target != canonical(c.Target) || preconditions != canonical(c.Preconditions) || causation != canonical(c.Causation) || requested != canonical(c.RequestedActor) {
		return Receipt{}, true, reject("idempotency_conflict", "idempotency key was already used for a different command")
	}
	var receipt Receipt
	if err := json.Unmarshal([]byte(result), &receipt); err != nil {
		return Receipt{}, true, err
	}
	receipt.Outcome = "duplicate"
	return receipt, true, nil
}

func checkPreconditions(ctx context.Context, tx store.Tx, c Command, pending ...map[string]*workPendingObject) error {
	d, _ := definition(c.CommandType)
	for _, required := range d.RequiredPreconditions {
		found := false
		for _, p := range c.Preconditions {
			if p.AggregateType == required {
				found = true
			}
		}
		if !found {
			return reject("precondition_failed", c.CommandType+" requires a "+required+" precondition")
		}
	}
	for _, p := range c.Preconditions {
		if len(pending) > 0 && p.AggregateType == "object_instance" && pending[0][p.AggregateID] != nil {
			if p.ExpectedVersion != 1 {
				return reject("precondition_failed", "new object precondition version must be 1")
			}
			continue
		}
		table := map[string]string{"object_type": "object_types", "object_instance": "object_instances", "object_relation": "object_relations", "relation_definition": "relation_definitions", "reactive_rule": "reactive_rules"}[p.AggregateType]
		if table == "" {
			return reject("precondition_failed", "unknown precondition aggregate_type")
		}
		var version int64
		query := `SELECT version FROM ` + table + ` WHERE workspace_id=? AND id=?`
		if err := tx.QueryRowContext(ctx, query, c.WorkspaceID, p.AggregateID).Scan(&version); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return reject("precondition_failed", "precondition aggregate does not exist")
			}
			return err
		}
		if version != p.ExpectedVersion {
			return reject("precondition_failed", "precondition version does not match")
		}
	}
	return nil
}

func (e *Executor) apply(ctx context.Context, tx store.Tx, principal string, c Command, now time.Time) (effect, error) {
	var p map[string]any
	if err := json.Unmarshal(c.Payload, &p); err != nil {
		return effect{}, err
	}
	switch c.CommandType {
	case "spec.publish":
		return publishSpec(ctx, tx, principal, c, p, now)
	case "work.open":
		return openWork(ctx, tx, principal, c, p, now)
	case "work.route":
		return routeWork(ctx, tx, principal, c, p, now)
	case "work.submit":
		return submitWork(ctx, tx, principal, c, p, now)
	case "work.cancel":
		return cancelWork(ctx, tx, principal, c, p)
	case "function.invoke":
		return invokeFunction(ctx, tx, principal, c, p, now)
	case "function.record_result":
		return recordFunctionResult(ctx, tx, principal, c, p)
	case "agent.record_result":
		return recordAgentResult(ctx, tx, c, p)
	case "workflow.complete":
		return completeWorkflow(ctx, tx, principal, c, p)
	case "work.record_deadline":
		return recordWorkDeadline(ctx, tx, c, p)
	case "event.emit":
		return emitDomainEvent(ctx, tx, principal, c, p)
	case "object_type.define":
		return defineObjectType(ctx, tx, principal, c, p, now)
	case "object_type.schema_evolve":
		return evolveObjectType(ctx, tx, principal, c, p, now)
	case "relation_definition.define":
		return defineRelation(ctx, tx, principal, c, p, now)
	case "object.create":
		return createObject(ctx, tx, principal, c, p, now)
	case "object.update":
		return updateObject(ctx, tx, principal, c, p, now)
	case "object.move":
		return moveObject(ctx, tx, principal, c, p, now)
	case "relation.link":
		return linkRelation(ctx, tx, principal, c, p, now)
	case "relation.unlink":
		return unlinkRelation(ctx, tx, principal, c, p, now)
	case "reactive_rule.define":
		return defineAutomation(ctx, tx, principal, c, p, now)
	case "reactive_rule.publish_revision", "reactive_rule.disable", "reactive_rule.retire":
		return changeAutomation(ctx, tx, principal, c, p, now)
	case "object_type.delete", "object.delete", "relation_definition.delete", "reactive_rule.delete":
		return archiveAggregate(ctx, tx, principal, c, now)
	default:
		return effect{}, reject("validation_failed", "command is not implemented")
	}
}

func validBundleID(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsAny(value, "/\\?#")
}

func defineObjectType(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	fieldRaw, _ := json.Marshal(p["field_schema"])
	field, err := validateFieldSchema(fieldRaw)
	if err != nil {
		return effect{}, err
	}
	if relation, ok := p["relation_schema"].(map[string]any); ok && len(relation) != 0 {
		return effect{}, reject("schema_mismatch", "relation_schema is derived from relation definitions and must be empty")
	}
	id := aggregateID("otp", c.CommandID)
	fieldJSON := canonical(field)
	relationJSON := `{"links":[]}`
	state, _ := p["state_machine_id"].(string)
	digest := schemaDigest(1, fieldJSON, relationJSON, state)
	ts := timestamp(now)
	_, err = tx.ExecContext(ctx, `INSERT INTO object_types(id,workspace_id,key,singular,plural,schema_version,field_schema_json,relation_schema_json,state_machine_id,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,1,?,?,NULLIF(?,''),1,?,?,?,?)`, id, c.WorkspaceID, p["key"], p["singular"], p["plural"], fieldJSON, relationJSON, state, ts, ts, principal, principal)
	if err != nil {
		return effect{}, mapSQLError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO object_type_schema_revisions(workspace_id,object_type_id,schema_version,field_schema_json,relation_schema_json,state_machine_id,digest,created_at,created_by_principal_id) VALUES(?,?,1,?,?,NULLIF(?,''),?,?,?)`, c.WorkspaceID, id, fieldJSON, relationJSON, state, digest, ts, principal)
	if err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_type", 1, "object_type.defined", map[string]any{"id": id, "key": p["key"], "schema_version": 1, "digest": digest}), nil
}

func evolveObjectType(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	fieldRaw, _ := json.Marshal(p["field_schema"])
	field, err := validateFieldSchema(fieldRaw)
	if err != nil {
		return effect{}, err
	}
	fieldJSON := canonical(field)
	var relationJSON, previousFieldJSON string
	var state sql.NullString
	var schemaVersion, version int64
	if err := tx.QueryRowContext(ctx, `SELECT relation_schema_json,state_machine_id,schema_version,version,field_schema_json FROM object_types WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&relationJSON, &state, &schemaVersion, &version, &previousFieldJSON); err != nil {
		return effect{}, reject("precondition_failed", "object type does not exist")
	}
	var previous map[string]any
	_ = json.Unmarshal([]byte(previousFieldJSON), &previous)
	if requested, exists := field["state_machine"]; exists {
		if !equalJSON(requested, previous["state_machine"]) {
			return effect{}, reject("validation_failed", "field_schema.state_machine is inherited and cannot be changed")
		}
	} else if inherited, exists := previous["state_machine"]; exists {
		field["state_machine"] = inherited
	}
	fieldJSON = canonical(field)
	schemaVersion++
	version++
	digest := schemaDigest(schemaVersion, fieldJSON, relationJSON, state.String)
	ts := timestamp(now)
	if _, err := tx.ExecContext(ctx, `UPDATE object_types SET field_schema_json=?,schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, fieldJSON, schemaVersion, version, ts, principal, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_type_schema_revisions VALUES(?,?,?,?,?,?,?,?,?)`, c.WorkspaceID, id, schemaVersion, fieldJSON, relationJSON, state, digest, ts, principal); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_type", version, "object_type.schema_evolved", map[string]any{"id": id, "schema_version": schemaVersion, "digest": digest}), nil
}

func createObject(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	var typeID, schemaJSON string
	var schemaVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT id,schema_version,field_schema_json FROM object_types WHERE workspace_id=? AND key=? AND archived_at IS NULL`, c.WorkspaceID, p["object_type_key"]).Scan(&typeID, &schemaVersion, &schemaJSON); err != nil {
		return effect{}, reject("schema_mismatch", "object_type_key does not exist")
	}
	var schema map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	values, _ := p["values"].(map[string]any)
	if values == nil {
		values = map[string]any{}
	}
	if err := validateValues(values, schema, "payload.values"); err != nil {
		return effect{}, err
	}
	id := aggregateID("obj", c.CommandID)
	ts := timestamp(now)
	relations, _ := p["relations"].(map[string]any)
	for _, g := range generations(schema) {
		if _, exists := values[g.Field]; exists {
			return effect{}, reject("validation_failed", "payload.values."+g.Field+" is generated by the server")
		}
		renderValues := make(map[string]any, len(values))
		for key, value := range values {
			renderValues[key] = value
		}
		for _, match := range relationTokenPattern.FindAllStringSubmatch(g.Template, -1) {
			value, err := resolveRelationPath(ctx, tx, c.WorkspaceID, relations, match[1])
			if err != nil {
				return effect{}, err
			}
			renderValues["relation:"+match[1]] = value
		}
		_, scopeValue, err := renderGenerated(g, now, renderValues, 1)
		if err != nil {
			return effect{}, err
		}
		var counter int64
		if strings.Contains(g.Template, "{{sequence:") {
			_, err = tx.ExecContext(ctx, `INSERT INTO field_sequence_counters(workspace_id,object_type_id,field_name,scope_key,counter) VALUES(?,?,?,?,1) ON CONFLICT(workspace_id,object_type_id,field_name,scope_key) DO UPDATE SET counter=counter+1`, c.WorkspaceID, typeID, g.Field, scopeValue)
			if err != nil {
				return effect{}, err
			}
			if err = tx.QueryRowContext(ctx, `SELECT counter FROM field_sequence_counters WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=?`, c.WorkspaceID, typeID, g.Field, scopeValue).Scan(&counter); err != nil {
				return effect{}, err
			}
		} else {
			counter = 1
		}
		value, _, err := renderGenerated(g, now, renderValues, counter)
		if err != nil {
			return effect{}, err
		}
		values[g.Field] = value
	}
	payloadJSON := canonical(values)
	title, _ := p["title"].(string)
	if strings.TrimSpace(title) == "" {
		return effect{}, reject("validation_failed", "payload.title is required")
	}
	var initial any = nil
	if machine, ok := schema["state_machine"].(map[string]any); ok {
		initial = machine["initial"]
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_instances(id,workspace_id,object_type_id,object_type_schema_version,title,payload_json,state_key,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,?,1,?,?,?,?)`, id, c.WorkspaceID, typeID, schemaVersion, title, payloadJSON, initial, ts, ts, principal, principal); err != nil {
		return effect{}, mapSQLError(err)
	}
	if err := createInlineRelations(ctx, tx, c.WorkspaceID, principal, id, typeID, relations, ts); err != nil {
		return effect{}, err
	}
	for _, g := range generations(schema) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO generated_field_values VALUES(?,?,?,?,?,?)`, c.WorkspaceID, typeID, g.Field, values[g.Field], id, ts); err != nil {
			return effect{}, reject("validation_failed", "generated value is not unique")
		}
	}
	if err := replaceFieldIndex(ctx, tx, c.WorkspaceID, typeID, id, values); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_instance", 1, "object.created", map[string]any{"id": id, "object_type_id": typeID, "object_type_key": p["object_type_key"], "title": title, "values": values, "state_key": initial, "version": 1, "created_at": ts, "updated_at": ts}), nil
}

func updateObject(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	var typeID, typeKey, schemaJSON, oldJSON, title, createdAt string
	var state sql.NullString
	var version, latestSchemaVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT i.object_type_id,t.key,t.field_schema_json,i.payload_json,i.title,i.state_key,i.version,t.schema_version,i.created_at FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.id=? AND i.archived_at IS NULL`, c.WorkspaceID, id).Scan(&typeID, &typeKey, &schemaJSON, &oldJSON, &title, &state, &version, &latestSchemaVersion, &createdAt); err != nil {
		return effect{}, reject("precondition_failed", "object does not exist")
	}
	var schema, old map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	_ = json.Unmarshal([]byte(oldJSON), &old)
	values := old
	valuesReplaced := false
	if incoming, ok := p["values"].(map[string]any); ok {
		for _, g := range generations(schema) {
			if _, exists := incoming[g.Field]; exists {
				return effect{}, reject("validation_failed", "payload.values."+g.Field+" is generated by the server")
			}
			if value, exists := old[g.Field]; exists {
				incoming[g.Field] = value
			}
		}
		if err := validateValues(incoming, schema, "payload.values"); err != nil {
			return effect{}, err
		}
		values = incoming
		valuesReplaced = true
	}
	if next, ok := p["title"].(string); ok {
		if strings.TrimSpace(next) == "" {
			return effect{}, reject("validation_failed", "payload.title is required")
		}
		title = next
	}
	version++
	ts := timestamp(now)
	if valuesReplaced {
		if _, err := tx.ExecContext(ctx, `UPDATE object_instances SET title=?,payload_json=?,object_type_schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, title, canonical(values), latestSchemaVersion, version, ts, principal, c.WorkspaceID, id); err != nil {
			return effect{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE object_instances SET title=?,payload_json=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, title, canonical(values), version, ts, principal, c.WorkspaceID, id); err != nil {
			return effect{}, err
		}
	}
	if err := replaceFieldIndex(ctx, tx, c.WorkspaceID, typeID, id, values); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_instance", version, "object.updated", map[string]any{"id": id, "object_type_id": typeID, "object_type_key": typeKey, "title": title, "values": values, "state_key": nullable(state.String), "version": version, "created_at": createdAt, "updated_at": ts}), nil
}

func moveObject(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	var version int64
	var current sql.NullString
	var schemaJSON string
	if err := tx.QueryRowContext(ctx, `SELECT i.version,i.state_key,t.field_schema_json FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.id=? AND i.archived_at IS NULL`, c.WorkspaceID, id).Scan(&version, &current, &schemaJSON); err != nil {
		return effect{}, reject("precondition_failed", "object does not exist")
	}
	version++
	state, _ := p["to_state"].(string)
	if strings.TrimSpace(state) == "" {
		return effect{}, reject("validation_failed", "to_state is required")
	}
	var schema map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	machine, ok := schema["state_machine"].(map[string]any)
	if !ok {
		return effect{}, reject("validation_failed", "object type has no state machine")
	}
	transitions, _ := machine["transitions"].(map[string]any)
	if _, defined := transitions[state]; !defined {
		return effect{}, reject("validation_failed", "state transition is not allowed")
	}
	force, _ := p["force"].(bool)
	if !force {
		targets, _ := transitions[current.String].([]any)
		allowed := false
		for _, raw := range targets {
			target, _ := raw.(string)
			if target == state || target == "*" {
				allowed = true
			}
		}
		if !allowed {
			return effect{}, reject("validation_failed", "state transition is not allowed")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE object_instances SET state_key=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, state, version, timestamp(now), principal, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_instance", version, "object.moved", map[string]any{"id": id, "state_key": state, "version": version}), nil
}

func defineRelation(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := aggregateID("rdf", c.CommandID)
	ts := timestamp(now)
	required := 0
	if p["required"] == true {
		required = 1
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO relation_definitions(id,workspace_id,key,from_object_type_id,to_object_type_id,cardinality,required,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,?,1,?,?,?,?)`, id, c.WorkspaceID, p["key"], p["from_object_type_id"], p["to_object_type_id"], p["cardinality"], required, ts, ts, principal, principal)
	if err != nil {
		return effect{}, mapSQLError(err)
	}
	if err = refreshRelationSchemas(ctx, tx, c.WorkspaceID, principal, now, fmt.Sprint(p["from_object_type_id"]), fmt.Sprint(p["to_object_type_id"])); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "relation_definition", 1, "relation_definition.defined", map[string]any{"id": id, "key": p["key"], "version": 1}), nil
}

func linkRelation(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	var cardinality string
	var fromType, toType string
	if err := tx.QueryRowContext(ctx, `SELECT cardinality,from_object_type_id,to_object_type_id FROM relation_definitions WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, p["relation_definition_id"]).Scan(&cardinality, &fromType, &toType); err != nil {
		return effect{}, reject("precondition_failed", "relation definition does not exist")
	}
	var actualFrom, actualTo string
	if err := tx.QueryRowContext(ctx, `SELECT object_type_id FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, p["from_object_id"]).Scan(&actualFrom); err != nil {
		return effect{}, reject("precondition_failed", "from object does not exist")
	}
	if err := tx.QueryRowContext(ctx, `SELECT object_type_id FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, p["to_object_id"]).Scan(&actualTo); err != nil {
		return effect{}, reject("precondition_failed", "to object does not exist")
	}
	if actualFrom != fromType || actualTo != toType {
		return effect{}, reject("schema_mismatch", "relation endpoints do not match the relation definition")
	}
	id := aggregateID("rel", c.CommandID)
	ts := timestamp(now)
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_relations VALUES(?,?,?,?,?,1,?,?,?,?)`, id, c.WorkspaceID, p["relation_definition_id"], p["from_object_id"], p["to_object_id"], ts, ts, principal, principal); err != nil {
		return effect{}, mapSQLError(err)
	}
	for _, side := range cardinalitySides(cardinality) {
		objectID := p[side+"_object_id"]
		if _, err := tx.ExecContext(ctx, `INSERT INTO relation_cardinality_slots VALUES(?,?,?,?,?,?,?)`, newID("slot"), c.WorkspaceID, p["relation_definition_id"], id, side, objectID, ts); err != nil {
			return effect{}, reject("precondition_failed", "relation cardinality would be exceeded")
		}
	}
	return makeEffect(id, "object_relation", 1, "relation.linked", map[string]any{"id": id, "relation_definition_id": p["relation_definition_id"], "from_object_id": p["from_object_id"], "to_object_id": p["to_object_id"], "version": 1}), nil
}

func unlinkRelation(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	var version int64
	var definitionID, fromObject string
	if err := tx.QueryRowContext(ctx, `SELECT version,relation_definition_id,from_object_instance_id FROM object_relations WHERE workspace_id=? AND id=?`, c.WorkspaceID, id).Scan(&version, &definitionID, &fromObject); err != nil {
		return effect{}, reject("precondition_failed", "object relation does not exist")
	}
	if p["relation_definition_id"] != definitionID {
		return effect{}, reject("precondition_failed", "relation definition does not match object relation")
	}
	var required int
	var key string
	if err := tx.QueryRowContext(ctx, `SELECT required,key FROM relation_definitions WHERE workspace_id=? AND id=?`, c.WorkspaceID, definitionID).Scan(&required, &key); err != nil {
		return effect{}, err
	}
	if required == 1 {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM object_relations WHERE workspace_id=? AND relation_definition_id=? AND from_object_instance_id=?`, c.WorkspaceID, definitionID, fromObject).Scan(&count); err != nil {
			return effect{}, err
		}
		if count <= 1 {
			return effect{}, reject("validation_failed", "cannot unlink required relation '"+key+"'")
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM relation_cardinality_slots WHERE workspace_id=? AND object_relation_id=?`, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_relations WHERE workspace_id=? AND id=?`, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "object_relation", version+1, "relation.unlinked", map[string]any{"id": id, "deleted": true}), nil
}

func createInlineRelations(ctx context.Context, tx store.Tx, workspace, principal, fromObject, fromType string, relations map[string]any, ts string) error {
	for key, rawTarget := range relations {
		target, ok := rawTarget.(string)
		if !ok || target == "" {
			return reject("validation_failed", "inline relation target must be an object id")
		}
		var definitionID, toType, cardinality string
		if err := tx.QueryRowContext(ctx, `SELECT id,to_object_type_id,cardinality FROM relation_definitions WHERE workspace_id=? AND key=? AND from_object_type_id=? AND archived_at IS NULL`, workspace, key, fromType).Scan(&definitionID, &toType, &cardinality); err != nil {
			return reject("validation_failed", "relation definition '"+key+"' not found for this object type")
		}
		var actualType string
		if err := tx.QueryRowContext(ctx, `SELECT object_type_id FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, target).Scan(&actualType); err != nil {
			return reject("validation_failed", "inline relation target object not found")
		}
		if actualType != toType {
			return reject("schema_mismatch", "inline relation target type does not match")
		}
		relationID := newID("rel")
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_relations VALUES(?,?,?,?,?,1,?,?,?,?)`, relationID, workspace, definitionID, fromObject, target, ts, ts, principal, principal); err != nil {
			return mapSQLError(err)
		}
		for _, side := range cardinalitySides(cardinality) {
			objectID := fromObject
			if side == "to" {
				objectID = target
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO relation_cardinality_slots VALUES(?,?,?,?,?,?,?)`, newID("slot"), workspace, definitionID, relationID, side, objectID, ts); err != nil {
				return reject("precondition_failed", "relation cardinality would be exceeded")
			}
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT key FROM relation_definitions WHERE workspace_id=? AND from_object_type_id=? AND required=1 AND archived_at IS NULL`, workspace, fromType)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		if _, ok := relations[key]; !ok {
			return reject("validation_failed", "required relation '"+key+"' must be provided")
		}
	}
	return rows.Err()
}

func resolveRelationPath(ctx context.Context, tx store.Tx, workspace string, relations map[string]any, path string) (any, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return nil, reject("schema_mismatch", "relation generation path must include a field")
	}
	current, ok := relations[parts[0]].(string)
	if !ok || current == "" {
		return nil, reject("validation_failed", "relation generation requires relation '"+parts[0]+"'")
	}
	for _, relationKey := range parts[1 : len(parts)-1] {
		var next string
		err := tx.QueryRowContext(ctx, `SELECT r.to_object_instance_id FROM object_relations r JOIN relation_definitions d ON d.workspace_id=r.workspace_id AND d.id=r.relation_definition_id WHERE r.workspace_id=? AND r.from_object_instance_id=? AND d.key=? ORDER BY r.id LIMIT 1`, workspace, current, relationKey).Scan(&next)
		if err != nil {
			return nil, reject("validation_failed", "relation generation path is unresolved")
		}
		current = next
	}
	var payload string
	if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, current).Scan(&payload); err != nil {
		return nil, reject("validation_failed", "relation generation target does not exist")
	}
	var values map[string]any
	if json.Unmarshal([]byte(payload), &values) != nil {
		return nil, reject("schema_mismatch", "relation generation target payload is invalid")
	}
	value, exists := values[parts[len(parts)-1]]
	if !exists {
		return nil, reject("validation_failed", "relation generation field is missing")
	}
	switch value.(type) {
	case string, float64, bool:
		return value, nil
	}
	return nil, reject("schema_mismatch", "relation generation field must be scalar")
}

func defineAutomation(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	automationID, _ := p["automation_id"].(string)
	for _, name := range []string{"automation_id", "rule_key", "name"} {
		if value, _ := p[name].(string); strings.TrimSpace(value) == "" {
			return effect{}, reject("schema_mismatch", "ReactiveRule "+name+" must not be empty")
		}
	}
	requested, _ := p["requested_actor"].(map[string]any)
	if err := onlyKeys("ReactiveRule requested_actor", requested, "type", "id"); err != nil {
		return effect{}, err
	}
	if requested["type"] != "automation" || requested["id"] != automationID {
		return effect{}, reject("validation_failed", "ReactiveRule requested_actor must match automation_id")
	}
	action, _ := p["action"].(map[string]any)
	if err := onlyKeys("ReactiveRule action", action, "action_id", "kind", "command_type", "target_template", "payload_template", "preconditions_template"); err != nil {
		return effect{}, err
	}
	actionType, _ := action["command_type"].(string)
	actionDefinition, ok := definition(actionType)
	if !ok || !actionDefinition.AutomationActionAllowed {
		return effect{}, reject("forbidden", "automation action command_type is not allowlisted")
	}
	trigger, _ := p["trigger"].(map[string]any)
	if err := onlyKeys("automation trigger", trigger, "source", "event_type", "event_schema_version", "aggregate_type", "allowed_causation_sources", "excluded_automation_ids"); err != nil {
		return effect{}, err
	}
	if trigger["source"] != "kernel_canonical_event" {
		return effect{}, reject("schema_mismatch", "automation trigger source must be kernel_canonical_event")
	}
	if version, ok := integer(trigger["event_schema_version"]); !ok || version != 1 {
		return effect{}, reject("schema_mismatch", "automation trigger event_schema_version must be 1")
	}
	if eventType, _ := trigger["event_type"].(string); strings.TrimSpace(eventType) == "" {
		return effect{}, reject("schema_mismatch", "automation trigger event_type is required")
	} else if !ReactiveRuleTriggerEventAllowed(eventType) {
		return effect{}, reject("forbidden", "automation trigger event_type is not a public Kernel event")
	}
	if aggregate, exists := trigger["aggregate_type"]; exists {
		if value, ok := aggregate.(string); !ok || strings.TrimSpace(value) == "" {
			return effect{}, reject("schema_mismatch", "automation trigger aggregate_type must be a non-empty string")
		}
	}
	for _, name := range []string{"allowed_causation_sources", "excluded_automation_ids"} {
		if err := validateAutomationStringList(name, trigger[name]); err != nil {
			return effect{}, err
		}
	}
	predicates, _ := p["predicates"].(map[string]any)
	if err := reactiverule.ValidatePredicate(predicates); err != nil {
		return effect{}, reject("schema_mismatch", "automation predicates: "+err.Error())
	}
	if action["kind"] != "emit_command" {
		return effect{}, reject("schema_mismatch", "automation action kind must be emit_command")
	}
	if actionID, _ := action["action_id"].(string); strings.TrimSpace(actionID) == "" {
		return effect{}, reject("schema_mismatch", "automation action_id is required")
	}
	if _, ok := action["target_template"].(map[string]any); !ok {
		return effect{}, reject("schema_mismatch", "automation action target_template must be an object")
	}
	if _, ok := action["payload_template"].(map[string]any); !ok {
		return effect{}, reject("schema_mismatch", "automation action payload_template must be an object")
	}
	if _, ok := action["preconditions_template"].([]any); !ok {
		return effect{}, reject("schema_mismatch", "automation action preconditions_template must be an array")
	}
	for _, name := range []string{"target_template", "payload_template", "preconditions_template"} {
		if err := reactiverule.ValidateTemplate(action[name]); err != nil {
			return effect{}, reject("schema_mismatch", "automation action "+name+": "+err.Error())
		}
	}
	idempotency, _ := p["idempotency"].(map[string]any)
	if len(idempotency) != 0 {
		return effect{}, reject("schema_mismatch", "automation idempotency is system-owned in v1")
	}
	limits, _ := p["limits"].(map[string]any)
	if err := onlyKeys("automation limits", limits, "max_recursion_depth", "max_emits_per_minute", "max_emits_per_hour", "max_pending_dispatches", "failure_threshold"); err != nil {
		return effect{}, err
	}
	for _, name := range []string{"max_recursion_depth", "max_emits_per_minute", "max_emits_per_hour", "max_pending_dispatches", "failure_threshold"} {
		value, ok := integer(limits[name])
		if !ok || value < 0 || name != "max_recursion_depth" && value < 1 {
			return effect{}, reject("schema_mismatch", "automation limits."+name+" is invalid")
		}
	}
	id := aggregateID("arl", c.CommandID)
	revision := aggregateID("arv", c.CommandID)
	ts := timestamp(now)
	definition := canonical(p)
	hash := digestJSON([]byte(definition))
	actor := canonical(p["requested_actor"])
	_, err := tx.ExecContext(ctx, `INSERT INTO reactive_rules(id,workspace_id,automation_id,rule_key,name,status,current_revision_id,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,'draft',NULL,1,?,?,?,?)`, id, c.WorkspaceID, p["automation_id"], p["rule_key"], p["name"], ts, ts, principal, principal)
	if err != nil {
		return effect{}, mapSQLError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO reactive_rule_revisions(id,workspace_id,rule_id,automation_id,definition_schema_version,definition_hash,requested_actor_json,permission_policy,trigger_json,predicates_json,action_json,idempotency_json,limits_json,created_at,created_by_principal_id) VALUES(?,?,?,?,1,?,?,'owner_principal',?,?,?,?,?,?,?)`, revision, c.WorkspaceID, id, p["automation_id"], hash, actor, canonical(p["trigger"]), canonical(p["predicates"]), canonical(p["action"]), canonical(p["idempotency"]), canonical(p["limits"]), ts, principal)
	if err != nil {
		return effect{}, err
	}
	return makeEffect(id, "reactive_rule", 1, "reactive_rule.defined", map[string]any{"id": id, "revision_id": revision, "definition_hash": hash, "version": 1}), nil
}

func validateAutomationStringList(name string, value any) error {
	values, ok := value.([]any)
	if !ok || len(values) > 64 {
		return reject("schema_mismatch", "automation trigger "+name+" must be an array of at most 64 strings")
	}
	for _, raw := range values {
		text, ok := raw.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return reject("schema_mismatch", "automation trigger "+name+" must contain non-empty strings")
		}
		if name == "allowed_causation_sources" && !map[string]bool{"cli": true, "api": true, "automation": true, "adapter_reducer": true}[text] {
			return reject("schema_mismatch", "automation trigger allowed_causation_sources contains an unsupported source")
		}
	}
	return nil
}

func changeAutomation(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT version FROM reactive_rules WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&version); err != nil {
		return effect{}, reject("precondition_failed", "ReactiveRule does not exist")
	}
	version++
	status, event := "active", "reactive_rule.revision_published"
	var revision any = nil
	if c.CommandType == "reactive_rule.publish_revision" {
		revision = p["revision_id"]
		cursor, _ := p["activation_event_cursor"].(string)
		if parsed, err := strconv.ParseInt(cursor, 10, 64); err != nil || parsed < 0 {
			return effect{}, reject("validation_failed", "activation_event_cursor must be a projection sequence")
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM reactive_rule_revisions WHERE workspace_id=? AND rule_id=? AND id=?`, c.WorkspaceID, id, revision).Scan(&exists); err != nil {
			return effect{}, reject("precondition_failed", "ReactiveRule revision does not exist")
		}
	} else if c.CommandType == "reactive_rule.disable" {
		status = "disabled"
		event = "reactive_rule.disabled"
	} else {
		status = "retired"
		event = "reactive_rule.retired"
	}
	ts := timestamp(now)
	if c.CommandType == "reactive_rule.publish_revision" {
		if _, err := tx.ExecContext(ctx, `UPDATE reactive_rule_revisions SET published_at=? WHERE workspace_id=? AND rule_id=? AND id=? AND published_at IS NULL`, ts, c.WorkspaceID, id, revision); err != nil {
			return effect{}, err
		}
	}
	reason, _ := p["reason"].(string)
	circuitBreaker, _ := p["circuit_breaker"].(bool)
	cursor, _ := p["activation_event_cursor"].(string)
	if _, err := tx.ExecContext(ctx, `UPDATE reactive_rules SET status=?,current_revision_id=COALESCE(?,current_revision_id),version=?,updated_at=?,updated_by_principal_id=?,enabled_at=CASE WHEN ?='active' THEN ? ELSE enabled_at END,disabled_at=CASE WHEN ?='disabled' THEN ? ELSE disabled_at END,activation_event_cursor=CASE WHEN ?='active' THEN ? ELSE activation_event_cursor END,last_disabled_reason=CASE WHEN ?='disabled' THEN NULLIF(?,'') ELSE last_disabled_reason END,circuit_breaker_status=CASE WHEN ?='disabled' AND ? THEN 'open' WHEN ?='active' THEN 'closed' ELSE circuit_breaker_status END WHERE workspace_id=? AND id=?`, status, revision, version, ts, principal, status, ts, status, ts, status, cursor, status, reason, status, circuitBreaker, status, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	return makeEffect(id, "reactive_rule", version, event, map[string]any{"id": id, "status": status, "version": version}), nil
}

func archiveAggregate(ctx context.Context, tx store.Tx, principal string, c Command, now time.Time) (effect, error) {
	id := *c.Target.AggregateID
	spec := map[string][3]string{
		"object_type.delete":         {"object_types", "object_type", "object_type.deleted"},
		"object.delete":              {"object_instances", "object_instance", "object.deleted"},
		"relation_definition.delete": {"relation_definitions", "relation_definition", "relation_definition.deleted"},
		"reactive_rule.delete":       {"reactive_rules", "reactive_rule", "reactive_rule.deleted"},
	}[c.CommandType]
	table, kind, event := spec[0], spec[1], spec[2]
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT version FROM `+table+` WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, id).Scan(&version); err != nil {
		return effect{}, reject("precondition_failed", kind+" does not exist")
	}
	version++
	ts := timestamp(now)
	if _, err := tx.ExecContext(ctx, `UPDATE `+table+` SET archived_at=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, ts, version, ts, principal, c.WorkspaceID, id); err != nil {
		return effect{}, err
	}
	if c.CommandType == "relation_definition.delete" {
		var from, to string
		if err := tx.QueryRowContext(ctx, `SELECT from_object_type_id,to_object_type_id FROM relation_definitions WHERE workspace_id=? AND id=?`, c.WorkspaceID, id).Scan(&from, &to); err == nil {
			if err := refreshRelationSchemas(ctx, tx, c.WorkspaceID, principal, now, from, to); err != nil {
				return effect{}, err
			}
		}
	}
	return makeEffect(id, kind, version, event, map[string]any{"id": id, "archived_at": ts, "version": version}), nil
}

func refreshRelationSchemas(ctx context.Context, tx store.Tx, workspace, principal string, now time.Time, typeIDs ...string) error {
	seen := map[string]bool{}
	for _, typeID := range typeIDs {
		if seen[typeID] {
			continue
		}
		seen[typeID] = true
		rows, err := tx.QueryContext(ctx, `SELECT id,key,from_object_type_id,to_object_type_id,cardinality,required FROM relation_definitions WHERE workspace_id=? AND archived_at IS NULL AND (from_object_type_id=? OR to_object_type_id=?) ORDER BY key,id`, workspace, typeID, typeID)
		if err != nil {
			return err
		}
		links := []map[string]any{}
		for rows.Next() {
			var id, key, from, to, cardinality string
			var required int
			if err := rows.Scan(&id, &key, &from, &to, &cardinality, &required); err != nil {
				rows.Close()
				return err
			}
			links = append(links, map[string]any{"id": id, "key": key, "from_object_type_id": from, "to_object_type_id": to, "cardinality": cardinality, "required": required == 1})
		}
		rows.Close()
		relationJSON := canonical(map[string]any{"links": links})
		var fieldJSON string
		var state sql.NullString
		var schemaVersion, version int64
		if err := tx.QueryRowContext(ctx, `SELECT field_schema_json,state_machine_id,schema_version,version FROM object_types WHERE workspace_id=? AND id=?`, workspace, typeID).Scan(&fieldJSON, &state, &schemaVersion, &version); err != nil {
			return err
		}
		schemaVersion++
		version++
		digest := schemaDigest(schemaVersion, fieldJSON, relationJSON, state.String)
		ts := timestamp(now)
		if _, err := tx.ExecContext(ctx, `UPDATE object_types SET relation_schema_json=?,schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=?`, relationJSON, schemaVersion, version, ts, principal, workspace, typeID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_type_schema_revisions VALUES(?,?,?,?,?,?,?,?,?)`, workspace, typeID, schemaVersion, fieldJSON, relationJSON, state, digest, ts, principal); err != nil {
			return err
		}
	}
	return nil
}

func replaceFieldIndex(ctx context.Context, tx store.Tx, workspace, typeID, objectID string, values map[string]any) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM object_field_index WHERE workspace_id=? AND object_instance_id=?`, workspace, objectID); err != nil {
		return err
	}
	for name, value := range values {
		kind := "null"
		var text any
		var number any
		var boolean any
		switch v := value.(type) {
		case string:
			kind = "string"
			text = v
		case float64:
			kind = "number"
			number = v
		case bool:
			kind = "boolean"
			if v {
				boolean = 1
			} else {
				boolean = 0
			}
		case nil:
		default:
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_field_index VALUES(?,?,?,?,?,?,?,?)`, workspace, typeID, objectID, name, kind, text, number, boolean); err != nil {
			return err
		}
	}
	return nil
}

func record(ctx context.Context, tx store.Tx, principal string, effectiveActor Actor, c Command, payloadHash string, now time.Time, sequence int64, receipt Receipt, eff effect) error {
	if err := recordLedger(ctx, tx, principal, effectiveActor, c, payloadHash, now, receipt); err != nil {
		return err
	}
	if err := recordEffect(ctx, tx, c.WorkspaceID, c.CommandID, now, sequence, eff); err != nil {
		return err
	}
	return appendJournal(ctx, tx, principal, c, eff.journal, timestamp(now))
}

func recordLedger(ctx context.Context, tx store.Tx, principal string, effectiveActor Actor, c Command, payloadHash string, now time.Time, receipt Receipt) error {
	requested := canonical(c.RequestedActor)
	effective := canonical(effectiveActor)
	target := canonical(c.Target)
	preconditions := canonical(c.Preconditions)
	causation := canonical(c.Causation)
	result := canonical(receipt)
	ts := timestamp(now)
	actorHash := digestJSON([]byte(effective))
	_, err := tx.ExecContext(ctx, `INSERT INTO command_ledger VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, c.CommandID, c.WorkspaceID, principal, requested, effective, actorHash, c.IdempotencyKey, c.CommandType, target, preconditions, causation, payloadHash, string(c.Payload), "applied", result, ts, ts)
	return err
}

func recordEffect(ctx context.Context, tx store.Tx, workspace, commandID string, now time.Time, sequence int64, eff effect) error {
	payload := canonical(eff.payload)
	hash := digestJSON([]byte(payload))
	ts := timestamp(now)
	if _, err := tx.ExecContext(ctx, `INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, newID("act"), workspace, commandID, eff.aggregate.Type, eff.aggregate.ID, eff.event, payload, ts); err != nil {
		return err
	}
	outboxEvent := eff.event
	outboxPayload := payload
	if eff.outboxEvent != "" {
		outboxEvent = eff.outboxEvent
		outboxPayload = canonical(eff.outboxPayload)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events VALUES(?,?,?,?,?,'pending',?,?)`, newID("evt"), workspace, commandID, outboxEvent, outboxPayload, ts, ts); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, newID("prj"), workspace, commandID, sequence, eff.aggregate.Type, eff.aggregate.ID, eff.aggregate.Version, hash, payload, ts); err != nil {
		return err
	}
	return nil
}

func nextProjectionSequence(ctx context.Context, tx store.Tx, workspace string) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(projection_sequence),0)+1 FROM projection_events WHERE workspace_id=?`, workspace).Scan(&n)
	return n, err
}
func cardinalitySides(c string) []string {
	switch c {
	case "one_to_one":
		return []string{"from", "to"}
	case "one_to_many":
		return []string{"to"}
	case "many_to_one":
		return []string{"from"}
	}
	return nil
}
func makeEffect(id, kind string, version int64, event string, payload map[string]any) effect {
	return effect{aggregate: Aggregate{kind, id, version}, event: event, payload: payload}
}
func timestamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func newID(prefix string) string   { return prefix + "_" + uuid.NewString() }
func aggregateID(prefix, commandID string) string {
	return prefix + "_" + strings.ReplaceAll(commandID, "-", "")
}
func digestJSON(raw []byte) string {
	var v any
	if json.Unmarshal(raw, &v) == nil {
		raw = []byte(canonical(v))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func schemaDigest(version int64, field, relation, state string) string {
	return digestJSON([]byte(canonical(map[string]any{"schema_version": version, "field_schema": jsonValue(field), "relation_schema": jsonValue(relation), "state_machine_id": nullable(state)})))
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func jsonValue(raw string) any { var v any; _ = json.Unmarshal([]byte(raw), &v); return v }
func canonical(v any) string   { b, _ := json.Marshal(v); return string(b) }
func mapSQLError(err error) error {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "unique") || strings.Contains(message, "constraint") {
		return reject("validation_failed", "database constraint rejected the command")
	}
	return err
}
