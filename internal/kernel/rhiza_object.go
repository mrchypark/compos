package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func fieldIndexStatementsRhiza(workspace, typeID, objectID string, values map[string]any) []rhiza.SQLStatement {
	one := int64(1)
	statements := []rhiza.SQLStatement{{SQL: `DELETE FROM object_field_index WHERE workspace_id=? AND object_instance_id=?`, Args: []any{workspace, objectID}}}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value, kind := values[name], "null"
		var text, number, boolean any
		switch v := value.(type) {
		case string:
			kind, text = "string", v
		case float64:
			kind, number = "number", v
		case bool:
			kind, boolean = "boolean", 0
			if v {
				boolean = 1
			}
		case nil:
		default:
			continue
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO object_field_index VALUES(?,?,?,?,?,?,?,?)`, Args: []any{workspace, typeID, objectID, name, kind, text, number, boolean}, ExpectedRowsAffected: &one})
	}
	return statements
}

func (e *Executor) resolveRelationPathRhiza(ctx context.Context, workspace string, relations map[string]any, path string, statements *[]rhiza.SQLStatement) (any, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return nil, reject("schema_mismatch", "relation generation path must include a field")
	}
	current, ok := relations[parts[0]].(string)
	if !ok || current == "" {
		return nil, reject("validation_failed", "relation generation requires relation '"+parts[0]+"'")
	}
	one := int64(1)
	for _, relationKey := range parts[1 : len(parts)-1] {
		if object := e.workObjects[current]; object != nil {
			next, _ := object.Relations[relationKey].(string)
			if next == "" {
				return nil, reject("validation_failed", "pending relation generation path is unresolved")
			}
			current = next
			continue
		}
		var relationID, next string
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT r.id,r.to_object_instance_id FROM object_relations r JOIN relation_definitions d ON d.workspace_id=r.workspace_id AND d.id=r.relation_definition_id WHERE r.workspace_id=? AND r.from_object_instance_id=? AND d.key=? ORDER BY r.id LIMIT 1`, workspace, current, relationKey).Scan(&relationID, &next); err != nil {
			return nil, reject("validation_failed", "relation generation path is unresolved")
		}
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM (SELECT r.id,r.to_object_instance_id FROM object_relations r JOIN relation_definitions d ON d.workspace_id=r.workspace_id AND d.id=r.relation_definition_id WHERE r.workspace_id=? AND r.from_object_instance_id=? AND d.key=? ORDER BY r.id LIMIT 1) selected WHERE id=? AND to_object_instance_id=?`, Args: []any{workspace, current, relationKey, relationID, next}, WantRows: true, ExpectedReturnedRows: &one})
		current = next
	}
	var payload string
	var version int64
	if object := e.workObjects[current]; object != nil {
		payload = canonical(object.Values)
	} else {
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json,version FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, current).Scan(&payload, &version); err != nil {
			return nil, reject("validation_failed", "relation generation target does not exist")
		}
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL AND payload_json=? AND version=?`, Args: []any{workspace, current, payload, version}, WantRows: true, ExpectedReturnedRows: &one})
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

func (e *Executor) workObjectIdentityRhiza(ctx context.Context, workspace, id string, statements *[]rhiza.SQLStatement) (string, int64, error) {
	if object := e.workObjects[id]; object != nil {
		return object.TypeID, 1, nil
	}
	var typ string
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT object_type_id,version FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, id).Scan(&typ, &version); err != nil {
		return "", 0, reject("precondition_failed", "relation endpoint object does not exist")
	}
	one := int64(1)
	*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=? AND version=? AND archived_at IS NULL`, Args: []any{workspace, id, typ, version}, WantRows: true, ExpectedReturnedRows: &one})
	return typ, version, nil
}

func (e *Executor) inlineRelationStatementsRhiza(ctx context.Context, workspace, principal, fromObject, fromType string, relations map[string]any, ts string, statements *[]rhiza.SQLStatement) error {
	one := int64(1)
	keys := make([]string, 0, len(relations))
	for key := range relations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		target, ok := relations[key].(string)
		if !ok || target == "" {
			return reject("validation_failed", "inline relation target must be an object id")
		}
		var definitionID, toType, cardinality string
		var definitionVersion int64
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,to_object_type_id,cardinality,version FROM relation_definitions WHERE workspace_id=? AND key=? AND from_object_type_id=? AND archived_at IS NULL`, workspace, key, fromType).Scan(&definitionID, &toType, &cardinality, &definitionVersion); err != nil {
			return reject("validation_failed", "relation definition '"+key+"' not found for this object type")
		}
		actualType, _, err := e.workObjectIdentityRhiza(ctx, workspace, target, statements)
		if err != nil {
			return err
		}
		if actualType != toType {
			return reject("schema_mismatch", "inline relation target type does not match")
		}
		*statements = append(*statements,
			rhiza.SQLStatement{SQL: `SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND key=? AND from_object_type_id=? AND to_object_type_id=? AND cardinality=? AND version=? AND archived_at IS NULL`, Args: []any{workspace, definitionID, key, fromType, toType, cardinality, definitionVersion}, WantRows: true, ExpectedReturnedRows: &one},
		)
		relationID := newID("rel")
		*statements = append(*statements, rhiza.SQLStatement{SQL: `INSERT INTO object_relations VALUES(?,?,?,?,?,1,?,?,?,?)`, Args: []any{relationID, workspace, definitionID, fromObject, target, ts, ts, principal, principal}, ExpectedRowsAffected: &one})
		for _, side := range cardinalitySides(cardinality) {
			objectID := fromObject
			if side == "to" {
				objectID = target
			}
			*statements = append(*statements, rhiza.SQLStatement{SQL: `INSERT INTO relation_cardinality_slots VALUES(?,?,?,?,?,?,?)`, Args: []any{newID("slot"), workspace, definitionID, relationID, side, objectID, ts}, ExpectedRowsAffected: &one})
		}
	}
	requiredRows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id,key,version FROM relation_definitions WHERE workspace_id=? AND from_object_type_id=? AND required=1 AND archived_at IS NULL ORDER BY id`, workspace, fromType)
	if err != nil {
		return err
	}
	defer requiredRows.Close()
	for requiredRows.Next() {
		var id, key string
		var version int64
		if err := requiredRows.Scan(&id, &key, &version); err != nil {
			return err
		}
		if _, ok := relations[key]; !ok {
			return reject("validation_failed", "required relation '"+key+"' must be provided")
		}
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND key=? AND from_object_type_id=? AND required=1 AND version=? AND archived_at IS NULL`, Args: []any{workspace, id, key, fromType, version}, WantRows: true, ExpectedReturnedRows: &one})
	}
	return requiredRows.Err()
}

func (e *Executor) createObjectRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	var typeID, schemaJSON string
	var schemaVersion, typeVersion int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,schema_version,field_schema_json,version FROM object_types WHERE workspace_id=? AND key=? AND archived_at IS NULL`, c.WorkspaceID, payload["object_type_key"]).Scan(&typeID, &schemaVersion, &schemaJSON, &typeVersion); err != nil {
		return Receipt{}, reject("schema_mismatch", "object_type_key does not exist")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND key=? AND schema_version=? AND field_schema_json=? AND version=? AND archived_at IS NULL`, Args: []any{c.WorkspaceID, typeID, payload["object_type_key"], schemaVersion, schemaJSON, typeVersion}, WantRows: true, ExpectedReturnedRows: &one})
	var schema map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	now := e.now().UTC()
	values, _ := payload["values"].(map[string]any)
	if values == nil {
		values = map[string]any{}
	}
	if err := validateValues(values, schema, "payload.values"); err != nil {
		return Receipt{}, err
	}
	relations, _ := payload["relations"].(map[string]any)
	ts := timestamp(now)
	for _, generation := range generations(schema) {
		if _, exists := values[generation.Field]; exists {
			return Receipt{}, reject("validation_failed", "payload.values."+generation.Field+" is generated by the server")
		}
		renderValues := make(map[string]any, len(values))
		for key, value := range values {
			renderValues[key] = value
		}
		for _, match := range relationTokenPattern.FindAllStringSubmatch(generation.Template, -1) {
			value, err := e.resolveRelationPathRhiza(ctx, c.WorkspaceID, relations, match[1], &statements)
			if err != nil {
				return Receipt{}, err
			}
			renderValues["relation:"+match[1]] = value
		}
		_, scope, err := renderGenerated(generation, now, renderValues, 1)
		if err != nil {
			return Receipt{}, err
		}
		counter := int64(1)
		if strings.Contains(generation.Template, "{{sequence:") {
			if e.workSequences != nil {
				var err error
				counter, err = e.workSequences.reserve(ctx, func(query string, args ...any) store.Row {
					return e.store.ReadRow(ctx, store.ReadLinearizable, query, args...)
				}, c.WorkspaceID, typeID, generation.Field, scope)
				if err != nil {
					return Receipt{}, err
				}
			} else {
				err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT counter FROM field_sequence_counters WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=?`, c.WorkspaceID, typeID, generation.Field, scope).Scan(&counter)
				if errors.Is(err, sql.ErrNoRows) {
					counter = 1
					statements = append(statements,
						rhiza.SQLStatement{SQL: `SELECT 1 FROM field_sequence_counters WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=?`, Args: []any{c.WorkspaceID, typeID, generation.Field, scope}, WantRows: true, ExpectedReturnedRows: &zero},
						rhiza.SQLStatement{SQL: `INSERT INTO field_sequence_counters(workspace_id,object_type_id,field_name,scope_key,counter) VALUES(?,?,?,?,1)`, Args: []any{c.WorkspaceID, typeID, generation.Field, scope}, ExpectedRowsAffected: &one},
					)
				} else if err != nil {
					return Receipt{}, err
				} else {
					previous := counter
					counter++
					statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE field_sequence_counters SET counter=? WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=? AND counter=?`, Args: []any{counter, c.WorkspaceID, typeID, generation.Field, scope, previous}, ExpectedRowsAffected: &one})
				}
			}
		}
		value, _, err := renderGenerated(generation, now, renderValues, counter)
		if err != nil {
			return Receipt{}, err
		}
		values[generation.Field] = value
	}
	title, _ := payload["title"].(string)
	if strings.TrimSpace(title) == "" {
		return Receipt{}, reject("validation_failed", "payload.title is required")
	}
	var initial any
	if machine, ok := schema["state_machine"].(map[string]any); ok {
		initial = machine["initial"]
	}
	id := aggregateID("obj", c.CommandID)
	if object := e.workObjects[id]; object != nil {
		object.Values = values
		if e.workResults == nil {
			e.workResults = map[string]WorkChangeResult{}
		}
		generated := map[string]any{}
		for _, generation := range generations(schema) {
			generated[generation.Field] = values[generation.Field]
		}
		e.workResults[object.ClientRef] = WorkChangeResult{ID: id, Version: 1, GeneratedValues: generated}
	}
	statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO object_instances(id,workspace_id,object_type_id,object_type_schema_version,title,payload_json,state_key,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,?,1,?,?,?,?)`, Args: []any{id, c.WorkspaceID, typeID, schemaVersion, title, canonical(values), initial, ts, ts, principal, principal}, ExpectedRowsAffected: &one})
	if err := e.inlineRelationStatementsRhiza(ctx, c.WorkspaceID, principal, id, typeID, relations, ts, &statements); err != nil {
		return Receipt{}, err
	}
	for _, generation := range generations(schema) {
		statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO generated_field_values VALUES(?,?,?,?,?,?)`, Args: []any{c.WorkspaceID, typeID, generation.Field, values[generation.Field], id, ts}, ExpectedRowsAffected: &one})
	}
	// A new object cannot have stale index rows; unlike update, creation only
	// needs the index inserts. This keeps one Work submission below Rhiza's
	// fixed statement budget without weakening the object INSERT constraint.
	indexes := fieldIndexStatementsRhiza(c.WorkspaceID, typeID, id, values)
	statements = append(statements, indexes[1:]...)
	effect := makeEffect(id, "object_instance", 1, "object.created", map[string]any{"id": id, "object_type_id": typeID, "object_type_key": payload["object_type_key"], "title": title, "values": values, "state_key": initial, "version": 1, "created_at": ts, "updated_at": ts})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) updateObjectRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	now := e.now().UTC()
	effect, statements, err := e.planUpdateObjectRhiza(ctx, principal, c, payload, statements, now)
	if err != nil {
		return Receipt{}, err
	}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(now))
}

func (e *Executor) planUpdateObjectRhiza(ctx context.Context, principal string, c Command, payload map[string]any, statements []rhiza.SQLStatement, now time.Time) (worldEffect effect, planned []rhiza.SQLStatement, err error) {
	id := *c.Target.AggregateID
	var typeID, typeKey, schemaJSON, oldJSON, title, createdAt string
	var state sql.NullString
	var version, latestSchemaVersion, typeVersion int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT i.object_type_id,t.key,t.field_schema_json,i.payload_json,i.title,i.state_key,i.version,t.schema_version,i.created_at,t.version FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.id=? AND i.archived_at IS NULL`, c.WorkspaceID, id).Scan(&typeID, &typeKey, &schemaJSON, &oldJSON, &title, &state, &version, &latestSchemaVersion, &createdAt, &typeVersion); err != nil {
		return effect{}, nil, reject("precondition_failed", "object does not exist")
	}
	oldTitle := title
	var schema, old map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	_ = json.Unmarshal([]byte(oldJSON), &old)
	values, replaced := old, false
	if incoming, ok := payload["values"].(map[string]any); ok {
		for _, generation := range generations(schema) {
			if _, exists := incoming[generation.Field]; exists {
				return effect{}, nil, reject("validation_failed", "payload.values."+generation.Field+" is generated by the server")
			}
			if value, exists := old[generation.Field]; exists {
				incoming[generation.Field] = value
			}
		}
		if err := validateValues(incoming, schema, "payload.values"); err != nil {
			return effect{}, nil, err
		}
		values, replaced = incoming, true
	}
	if next, ok := payload["title"].(string); ok {
		if strings.TrimSpace(next) == "" {
			return effect{}, nil, reject("validation_failed", "payload.title is required")
		}
		title = next
	}
	next, ts, one := version+1, timestamp(now), int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND key=? AND field_schema_json=? AND schema_version=? AND version=?`, Args: []any{c.WorkspaceID, typeID, typeKey, schemaJSON, latestSchemaVersion, typeVersion}, WantRows: true, ExpectedReturnedRows: &one})
	if replaced {
		statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE object_instances SET title=?,payload_json=?,object_type_schema_version=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=? AND payload_json=? AND title=? AND state_key IS ?`, Args: []any{title, canonical(values), latestSchemaVersion, next, ts, principal, c.WorkspaceID, id, version, oldJSON, oldTitle, nullable(state.String)}, ExpectedRowsAffected: &one})
	} else {
		statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE object_instances SET title=?,payload_json=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=? AND payload_json=? AND state_key IS ?`, Args: []any{title, canonical(values), next, ts, principal, c.WorkspaceID, id, version, oldJSON, nullable(state.String)}, ExpectedRowsAffected: &one})
	}
	statements = append(statements, fieldIndexStatementsRhiza(c.WorkspaceID, typeID, id, values)...)
	worldEffect = makeEffect(id, "object_instance", next, "object.updated", map[string]any{"id": id, "object_type_id": typeID, "object_type_key": typeKey, "title": title, "values": values, "state_key": nullable(state.String), "version": next, "created_at": createdAt, "updated_at": ts})
	return worldEffect, statements, nil
}

func (e *Executor) moveObjectRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	now := e.now().UTC()
	effect, statements, err := e.planMoveObjectRhiza(ctx, principal, c, payload, statements, now)
	if err != nil {
		return Receipt{}, err
	}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(now))
}

func (e *Executor) planMoveObjectRhiza(ctx context.Context, principal string, c Command, payload map[string]any, statements []rhiza.SQLStatement, now time.Time) (effect, []rhiza.SQLStatement, error) {
	id := *c.Target.AggregateID
	var typeID, schemaJSON string
	var current sql.NullString
	var version, typeVersion int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT i.object_type_id,i.version,i.state_key,t.field_schema_json,t.version FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.id=? AND i.archived_at IS NULL`, c.WorkspaceID, id).Scan(&typeID, &version, &current, &schemaJSON, &typeVersion); err != nil {
		return effect{}, nil, reject("precondition_failed", "object does not exist")
	}
	state, _ := payload["to_state"].(string)
	if strings.TrimSpace(state) == "" {
		return effect{}, nil, reject("validation_failed", "to_state is required")
	}
	var schema map[string]any
	_ = json.Unmarshal([]byte(schemaJSON), &schema)
	machine, ok := schema["state_machine"].(map[string]any)
	if !ok {
		return effect{}, nil, reject("validation_failed", "object type has no state machine")
	}
	transitions, _ := machine["transitions"].(map[string]any)
	if _, defined := transitions[state]; !defined {
		return effect{}, nil, reject("validation_failed", "state transition is not allowed")
	}
	force, _ := payload["force"].(bool)
	if !force {
		allowed := false
		for _, raw := range anySlice(transitions[current.String]) {
			target, _ := raw.(string)
			allowed = allowed || target == state || target == "*"
		}
		if !allowed {
			return effect{}, nil, reject("validation_failed", "state transition is not allowed")
		}
	}
	next, ts, one := version+1, timestamp(now), int64(1)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND field_schema_json=? AND version=?`, Args: []any{c.WorkspaceID, typeID, schemaJSON, typeVersion}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `UPDATE object_instances SET state_key=?,version=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND id=? AND archived_at IS NULL AND version=? AND state_key IS ?`, Args: []any{state, next, ts, principal, c.WorkspaceID, id, version, nullable(current.String)}, ExpectedRowsAffected: &one},
	)
	eff := makeEffect(id, "object_instance", next, "object.moved", map[string]any{"id": id, "state_key": state, "version": next})
	return eff, statements, nil
}

func (e *Executor) linkObjectRelationRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	definitionID := payload["relation_definition_id"]
	var cardinality, fromType, toType string
	var definitionVersion int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT cardinality,from_object_type_id,to_object_type_id,version FROM relation_definitions WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, definitionID).Scan(&cardinality, &fromType, &toType, &definitionVersion); err != nil {
		return Receipt{}, reject("precondition_failed", "relation definition does not exist")
	}
	fromID, _ := payload["from_object_id"].(string)
	toID, _ := payload["to_object_id"].(string)
	actualFrom, _, err := e.workObjectIdentityRhiza(ctx, c.WorkspaceID, fromID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	actualTo, _, err := e.workObjectIdentityRhiza(ctx, c.WorkspaceID, toID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	if actualFrom != fromType || actualTo != toType {
		return Receipt{}, reject("schema_mismatch", "relation endpoints do not match the relation definition")
	}
	one := int64(1)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND cardinality=? AND from_object_type_id=? AND to_object_type_id=? AND version=? AND archived_at IS NULL`, Args: []any{c.WorkspaceID, definitionID, cardinality, fromType, toType, definitionVersion}, WantRows: true, ExpectedReturnedRows: &one},
	)
	id, ts := aggregateID("rel", c.CommandID), timestamp(e.now().UTC())
	statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO object_relations VALUES(?,?,?,?,?,1,?,?,?,?)`, Args: []any{id, c.WorkspaceID, definitionID, payload["from_object_id"], payload["to_object_id"], ts, ts, principal, principal}, ExpectedRowsAffected: &one})
	for _, side := range cardinalitySides(cardinality) {
		statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO relation_cardinality_slots VALUES(?,?,?,?,?,?,?)`, Args: []any{newID("slot"), c.WorkspaceID, definitionID, id, side, payload[side+"_object_id"], ts}, ExpectedRowsAffected: &one})
	}
	effect := makeEffect(id, "object_relation", 1, "relation.linked", map[string]any{"id": id, "relation_definition_id": definitionID, "from_object_id": payload["from_object_id"], "to_object_id": payload["to_object_id"], "version": 1})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) unlinkObjectRelationRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	id := *c.Target.AggregateID
	var definitionID, fromObject string
	var version int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT version,relation_definition_id,from_object_instance_id FROM object_relations WHERE workspace_id=? AND id=?`, c.WorkspaceID, id).Scan(&version, &definitionID, &fromObject); err != nil {
		return Receipt{}, reject("precondition_failed", "object relation does not exist")
	}
	if payload["relation_definition_id"] != definitionID {
		return Receipt{}, reject("precondition_failed", "relation definition does not match object relation")
	}
	var required int
	var key string
	var definitionVersion int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT required,key,version FROM relation_definitions WHERE workspace_id=? AND id=?`, c.WorkspaceID, definitionID).Scan(&required, &key, &definitionVersion); err != nil {
		return Receipt{}, err
	}
	one := int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND required=? AND key=? AND version=?`, Args: []any{c.WorkspaceID, definitionID, required, key, definitionVersion}, WantRows: true, ExpectedReturnedRows: &one})
	if required == 1 {
		rows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id FROM object_relations WHERE workspace_id=? AND relation_definition_id=? AND from_object_instance_id=?`, c.WorkspaceID, definitionID, fromObject)
		if err != nil {
			return Receipt{}, err
		}
		count := int64(0)
		for rows.Next() {
			count++
		}
		_ = rows.Close()
		if count <= 1 {
			return Receipt{}, reject("validation_failed", "cannot unlink required relation '"+key+"'")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT id FROM object_relations WHERE workspace_id=? AND relation_definition_id=? AND from_object_instance_id=?`, Args: []any{c.WorkspaceID, definitionID, fromObject}, WantRows: true, ExpectedReturnedRows: &count})
	}
	statements = append(statements,
		rhiza.SQLStatement{SQL: `DELETE FROM relation_cardinality_slots WHERE workspace_id=? AND object_relation_id=?`, Args: []any{c.WorkspaceID, id}},
		rhiza.SQLStatement{SQL: `DELETE FROM object_relations WHERE workspace_id=? AND id=? AND version=? AND relation_definition_id=? AND from_object_instance_id=?`, Args: []any{c.WorkspaceID, id, version, definitionID, fromObject}, ExpectedRowsAffected: &one},
	)
	effect := makeEffect(id, "object_relation", version+1, "relation.unlinked", map[string]any{"id": id, "deleted": true})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}
