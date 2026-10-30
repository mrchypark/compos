package kernel

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

// Data bindings describe record scope, not UI layout or a grant of authority.
func validateDataBindings(spec map[string]any) error {
	raw, exists := spec["data_bindings"]
	if !exists {
		return nil
	}
	bindings, ok := raw.([]any)
	if !ok || len(bindings) > 32 {
		return reject("schema_mismatch", "task.data_bindings must contain at most 32 bindings")
	}
	ids := map[string]bool{}
	for _, raw := range bindings {
		b, ok := raw.(map[string]any)
		if !ok || onlyKeys("data binding", b, "id", "object_type_id", "schema_version", "context_key", "relation_path", "writable_fields", "allow_title", "allowed_operations") != nil {
			return reject("schema_mismatch", "invalid data binding")
		}
		id, _ := b["id"].(string)
		typeID, _ := b["object_type_id"].(string)
		key, _ := b["context_key"].(string)
		version, valid := integer(b["schema_version"])
		if !workflowIdentifier(id) || ids[id] || strings.TrimSpace(typeID) == "" || strings.TrimSpace(key) == "" || !valid || version < 1 {
			return reject("schema_mismatch", "binding requires unique id, object_type_id, positive schema_version and context_key")
		}
		ids[id] = true
		input, _ := spec["input_schema"].(map[string]any)
		if err := requireSchemaFields(input, map[string]string{key: "string"}); err != nil {
			return reject("schema_mismatch", "binding context_key must be required as a string in Task input_schema")
		}
		for _, field := range []string{"writable_fields", "allowed_operations"} {
			if err := validateBoundedStringList("binding."+field, b[field], 128); err != nil {
				return err
			}
		}
		for _, raw := range anySlice(b["allowed_operations"]) {
			if !map[string]bool{"object.create": true, "object.update": true, "relation.link": true, "relation.unlink": true}[raw.(string)] {
				return reject("schema_mismatch", "binding operation is not supported")
			}
		}
		if value, exists := b["allow_title"]; exists {
			if _, ok := value.(bool); !ok {
				return reject("schema_mismatch", "binding.allow_title must be boolean")
			}
		}
		if rawPath, exists := b["relation_path"]; exists {
			path, ok := rawPath.([]any)
			if !ok || len(path) > 4 {
				return reject("schema_mismatch", "binding.relation_path supports at most four hops")
			}
			for _, raw := range path {
				hop, ok := raw.(map[string]any)
				if !ok || onlyKeys("binding relation hop", hop, "relation_definition_id", "version", "direction") != nil {
					return reject("schema_mismatch", "invalid binding relation hop")
				}
				id, _ := hop["relation_definition_id"].(string)
				version, ok := integer(hop["version"])
				if id == "" || !ok || version < 1 || (hop["direction"] != "outgoing" && hop["direction"] != "incoming") {
					return reject("schema_mismatch", "binding hop requires relation_definition_id, positive version and direction")
				}
			}
		}
	}
	return nil
}

// Every mutable fact consulted while planning has a matching commit-time guard.
func bindingGuard(read func(string, ...any) store.Row, guards *[]rhiza.SQLStatement, query string, args ...any) error {
	var found int
	if err := read(query, args...).Scan(&found); err != nil {
		return reject("precondition_failed", "data binding scope or schema pin no longer matches")
	}
	one := int64(1)
	*guards = append(*guards, rhiza.SQLStatement{SQL: query, Args: args, WantRows: true, ExpectedReturnedRows: &one})
	return nil
}

func validateDataBindingReferences(read func(string, ...any) store.Row, workspace string, spec map[string]any) ([]rhiza.SQLStatement, error) {
	var guards []rhiza.SQLStatement
	for _, raw := range anySlice(spec["data_bindings"]) {
		b := raw.(map[string]any)
		var schemaJSON string
		if err := read(`SELECT field_schema_json FROM object_type_schema_revisions WHERE workspace_id=? AND object_type_id=? AND schema_version=?`, workspace, b["object_type_id"], b["schema_version"]).Scan(&schemaJSON); err != nil {
			return nil, reject("precondition_failed", "binding schema revision does not exist")
		}
		if err := bindingGuard(read, &guards, `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND schema_version=? AND archived_at IS NULL`, workspace, b["object_type_id"], b["schema_version"]); err != nil {
			return nil, err
		}
		var schema map[string]any
		if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
			return nil, err
		}
		properties, _ := schema["properties"].(map[string]any)
		for _, rawField := range anySlice(b["writable_fields"]) {
			field, ok := properties[rawField.(string)].(map[string]any)
			if !ok || field["readOnly"] == true || field["x-compos-generation"] != nil {
				return nil, reject("schema_mismatch", "binding writable_fields must name editable fields in the pinned schema")
			}
		}
		currentType := b["object_type_id"].(string)
		path := anySlice(b["relation_path"])
		for i := len(path) - 1; i >= 0; i-- {
			hop := path[i].(map[string]any)
			var from, to string
			if err := read(`SELECT from_object_type_id,to_object_type_id FROM relation_definitions WHERE workspace_id=? AND id=? AND version=? AND archived_at IS NULL`, workspace, hop["relation_definition_id"], hop["version"]).Scan(&from, &to); err != nil {
				return nil, reject("precondition_failed", "binding relation definition does not exist at pinned version")
			}
			if err := bindingGuard(read, &guards, `SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND version=? AND from_object_type_id=? AND to_object_type_id=? AND archived_at IS NULL`, workspace, hop["relation_definition_id"], hop["version"], from, to); err != nil {
				return nil, err
			}
			if hop["direction"] == "incoming" {
				from, to = to, from
			}
			if currentType != to {
				return nil, reject("schema_mismatch", "binding relation path has incompatible object types")
			}
			currentType = from
		}
	}
	return guards, nil
}

// A bounded SQL path preserves canonical relation semantics without a graph mirror.
func bindingScopeSQL(workspace, root string, path []any) (string, []any) {
	query := `SELECT id FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`
	args := []any{workspace, root}
	for _, raw := range path {
		hop := raw.(map[string]any)
		from, to := "from_object_instance_id", "to_object_instance_id"
		if hop["direction"] == "incoming" {
			from, to = to, from
		}
		query = `SELECT r.` + to + ` FROM object_relations r JOIN object_instances o ON o.workspace_id=r.workspace_id AND o.id=r.` + to + ` AND o.archived_at IS NULL WHERE r.workspace_id=? AND r.relation_definition_id=? AND r.` + from + ` IN (` + query + `)`
		args = append([]any{workspace, hop["relation_definition_id"]}, args...)
	}
	return query, args
}

func validateWorkChange(_ context.Context, read func(string, ...any) store.Row, workspace, _ string, task, contextValue map[string]any, change workChange, pendingObjects ...map[string]*workPendingObject) ([]rhiza.SQLStatement, error) {
	var pending map[string]*workPendingObject
	if len(pendingObjects) > 0 {
		pending = pendingObjects[0]
	}
	for _, object := range pending {
		if object.TypeID == "" {
			if err := read(`SELECT id FROM object_types WHERE workspace_id=? AND key=? AND archived_at IS NULL`, workspace, object.TypeKey).Scan(&object.TypeID); err != nil {
				return nil, reject("schema_mismatch", "pending object type does not exist")
			}
		}
	}
	var b map[string]any
	for _, raw := range anySlice(task["data_bindings"]) {
		candidate := raw.(map[string]any)
		if candidate["id"] == change.BindingID {
			b = candidate
			break
		}
	}
	if b == nil {
		return nil, reject("forbidden", "change does not name a pinned Task data binding")
	}
	allowed := false
	for _, operation := range anySlice(b["allowed_operations"]) {
		if operation == change.CommandType {
			allowed = true
		}
	}
	if !allowed {
		return nil, reject("forbidden", "operation is not allowed by the Task binding")
	}
	guards, err := validateDataBindingReferences(read, workspace, map[string]any{"data_bindings": []any{b}})
	if err != nil {
		return nil, err
	}
	root, _ := contextValue[b["context_key"].(string)].(string)
	if root == "" {
		return nil, reject("precondition_failed", "Work has no binding context record")
	}
	path := anySlice(b["relation_path"])
	checkScope := func(id string, path []any) error {
		query, args := bindingScopeSQL(workspace, root, path)
		query, args = withPendingBindingScope(query, args, workspace, pending)
		return bindingGuard(read, &guards, `SELECT 1 WHERE ? IN (`+query+`)`, append([]any{id}, args...)...)
	}
	if change.CommandType == "object.update" {
		if change.Target.AggregateID == nil {
			return nil, reject("validation_failed", "update target is required")
		}
		id := *change.Target.AggregateID
		if err := checkScope(id, path); err != nil {
			return nil, err
		}
		if err := bindingGuard(read, &guards, `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=? AND archived_at IS NULL`, workspace, id, b["object_type_id"]); err != nil {
			return nil, err
		}
		var oldJSON string
		if err := read(`SELECT payload_json FROM object_instances WHERE workspace_id=? AND id=?`, workspace, id).Scan(&oldJSON); err != nil {
			return nil, err
		}
		if err := bindingGuard(read, &guards, `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND payload_json=?`, workspace, id, oldJSON); err != nil {
			return nil, err
		}
		var old map[string]any
		if err := json.Unmarshal([]byte(oldJSON), &old); err != nil {
			return nil, err
		}
		// Existing object.update preserves generated fields and forbids supplying them.
		var schemaJSON string
		if err := read(`SELECT field_schema_json FROM object_type_schema_revisions WHERE workspace_id=? AND object_type_id=? AND schema_version=?`, workspace, b["object_type_id"], b["schema_version"]).Scan(&schemaJSON); err != nil {
			return nil, err
		}
		var schema map[string]any
		if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
			return nil, err
		}
		properties, _ := schema["properties"].(map[string]any)
		for key, raw := range properties {
			field, _ := raw.(map[string]any)
			if field["x-compos-generation"] != nil {
				delete(old, key)
			}
		}
		if err := validateBindingFields(b, change.Payload, old); err != nil {
			return nil, err
		}
		return guards, nil
	}
	if len(path) == 0 {
		return nil, reject("forbidden", "creation and relation changes require a relation-scoped binding")
	}
	hop := path[len(path)-1].(map[string]any)
	var relationKey, fromType, toType string
	if err := read(`SELECT key,from_object_type_id,to_object_type_id FROM relation_definitions WHERE workspace_id=? AND id=?`, workspace, hop["relation_definition_id"]).Scan(&relationKey, &fromType, &toType); err != nil {
		return nil, err
	}
	if change.CommandType == "object.create" {
		if hop["direction"] != "incoming" {
			return nil, reject("validation_failed", "create binding must end with an incoming relation to its parent")
		}
		if err := bindingGuard(read, &guards, `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND key=? AND archived_at IS NULL`, workspace, b["object_type_id"], change.Payload["object_type_key"]); err != nil {
			return nil, err
		}
		relations, _ := change.Payload["relations"].(map[string]any)
		parent, _ := relations[relationKey].(string)
		if len(relations) != 1 || parent == "" {
			return nil, reject("forbidden", "create must link only the binding relation to its parent")
		}
		if err := checkScope(parent, path[:len(path)-1]); err != nil {
			return nil, err
		}
		if err := validateBindingFields(b, change.Payload, nil); err != nil {
			return nil, err
		}
		return guards, nil
	}
	if change.Payload["relation_definition_id"] != hop["relation_definition_id"] {
		return nil, reject("forbidden", "relation change is outside the binding path")
	}
	from, _ := change.Payload["from_object_id"].(string)
	to, _ := change.Payload["to_object_id"].(string)
	if change.CommandType == "relation.unlink" {
		if change.Target.AggregateID == nil {
			return nil, reject("validation_failed", "unlink target is required")
		}
		if err := read(`SELECT from_object_instance_id,to_object_instance_id FROM object_relations WHERE workspace_id=? AND id=? AND relation_definition_id=?`, workspace, *change.Target.AggregateID, hop["relation_definition_id"]).Scan(&from, &to); err != nil {
			return nil, reject("precondition_failed", "binding relation edge does not exist")
		}
	}
	for _, endpoint := range []struct{ id, typ string }{{from, fromType}, {to, toType}} {
		if object := pending[endpoint.id]; object != nil {
			if object.TypeID != endpoint.typ {
				return nil, reject("schema_mismatch", "pending relation endpoint type does not match")
			}
			continue
		}
		if err := bindingGuard(read, &guards, `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=? AND archived_at IS NULL`, workspace, endpoint.id, endpoint.typ); err != nil {
			return nil, err
		}
	}
	parent := from
	if hop["direction"] == "incoming" {
		parent = to
	}
	if err := checkScope(parent, path[:len(path)-1]); err != nil {
		return nil, err
	}
	return guards, nil
}

// Pending rows only extend this submission's scope calculation. Each pending
// creation is separately authorized and all rows must commit or none can exist.
func withPendingBindingScope(query string, args []any, workspace string, pending map[string]*workPendingObject) (string, []any) {
	if len(pending) == 0 {
		return query, args
	}
	objects := `SELECT id,workspace_id,archived_at FROM object_instances`
	relations := `SELECT workspace_id,relation_definition_id,from_object_instance_id,to_object_instance_id FROM object_relations`
	var objectArgs, relationArgs []any
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		o := pending[id]
		objects += ` UNION SELECT ?,?,NULL`
		objectArgs = append(objectArgs, id, workspace)
		keys := make([]string, 0, len(o.Relations))
		for key := range o.Relations {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			target, _ := o.Relations[key].(string)
			relations += ` UNION SELECT ?,d.id,?,? FROM relation_definitions d WHERE d.workspace_id=? AND d.key=? AND d.from_object_type_id=? AND d.archived_at IS NULL`
			relationArgs = append(relationArgs, workspace, id, target, workspace, key, o.TypeID)
		}
	}
	query = strings.ReplaceAll(strings.ReplaceAll(query, "object_instances", "binding_objects"), "object_relations", "binding_relations")
	return `WITH binding_objects AS (` + objects + `), binding_relations AS (` + relations + `) ` + query, append(append(objectArgs, relationArgs...), args...)
}

func validateBindingFields(binding, payload, old map[string]any) error {
	if _, exists := payload["title"]; exists && binding["allow_title"] != true {
		return reject("forbidden", "binding does not allow title changes")
	}
	values, exists := payload["values"].(map[string]any)
	if !exists {
		return nil
	}
	writable := map[string]bool{}
	for _, raw := range anySlice(binding["writable_fields"]) {
		writable[raw.(string)] = true
	}
	for key, value := range values {
		if !writable[key] {
			previous, present := old[key]
			if !present || !equalJSON(value, previous) {
				return reject("forbidden", "field is not writable through the Task binding: "+key)
			}
		}
	}
	for key := range old {
		if !writable[key] {
			if _, present := values[key]; !present {
				return reject("forbidden", "replacement values would remove a protected field: "+key)
			}
		}
	}
	return nil
}
