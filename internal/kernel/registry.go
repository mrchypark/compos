package kernel

import (
	"bytes"
	"encoding/json"
	"strings"
)

type Definition struct {
	WireName                    string         `json:"wire_name"`
	Summary                     string         `json:"summary"`
	PayloadSchema               map[string]any `json:"payload_schema"`
	AllowedTargetAggregateTypes []string       `json:"allowed_target_aggregate_types"`
	TargetAggregateID           string         `json:"target_aggregate_id"`
	RequiredPreconditions       []string       `json:"required_preconditions"`
	IdempotencyBehavior         string         `json:"idempotency_behavior"`
	AutomationActionAllowed     bool           `json:"automation_action_allowed"`
	EmittedEvents               []string       `json:"emitted_events"`
}

func schema(required []string, properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "required": required, "properties": properties, "additionalProperties": false}
}

func prop(kind string) map[string]any { return map[string]any{"type": kind} }

var definitions = []Definition{
	def("spec.publish", "Publish one immutable typed specification revision.", "spec_revision", false, nil, []string{"family_id", "revision", "kind", "spec"}, map[string]any{"family_id": prop("string"), "revision": prop("integer"), "kind": map[string]any{"enum": []any{"task", "function", "workflow", "participant", "event", "ontology"}}, "spec": prop("object")}, "spec.published"),
	def("work.open", "Open one work stream pinned to a task specification revision.", "work_stream", false, nil, []string{"task_revision_id", "context"}, map[string]any{"task_revision_id": prop("string"), "context": prop("object"), "deadline_at": prop("string")}, "work.opened"),
	def("work.route", "Route one work stream to a participant without granting authority.", "work_stream", true, nil, []string{"participant"}, map[string]any{"participant": prop("object"), "reason": prop("string"), "workflow_step_id": prop("string"), "workflow_route_id": prop("string")}, "work.routed"),
	def("work.submit", "Append one immutable submission to a work stream.", "work_stream", true, nil, []string{"submission"}, map[string]any{"submission": prop("object"), "changes": prop("array"), "workflow_step_id": prop("string"), "workflow_route_id": prop("string")}, "work.submitted"),
	def("work.cancel", "Cancel one open work stream with an immutable reason.", "work_stream", true, nil, []string{"reason"}, map[string]any{"reason": prop("string")}, "work.cancelled"),
	def("function.invoke", "Invoke one pinned function specification through the Kernel.", "work_stream", true, nil, []string{"function_revision_id", "input"}, map[string]any{"function_revision_id": prop("string"), "input": prop("object"), "workflow_route_id": prop("string")}, "function.invoked"),
	def("function.record_result", "Reduce one authenticated external Function callback.", "work_stream", true, nil, []string{"invocation_id", "callback_id", "runtime_status", "output", "adapter_inbound_event_id", "execution_id", "provider_key", "provider_run_ref", "handler_version", "attempt_count"}, map[string]any{"invocation_id": prop("string"), "callback_id": prop("string"), "runtime_status": map[string]any{"enum": []any{"succeeded", "failed", "cancelled"}}, "output": prop("object"), "adapter_inbound_event_id": prop("string"), "execution_id": prop("string"), "provider_key": prop("string"), "provider_run_ref": prop("string"), "handler_version": prop("string"), "attempt_count": prop("integer"), "error_class": prop("string"), "error_detail": prop("string")}, "function.result_recorded"),
	def("agent.record_result", "Reduce one authenticated external Agent callback and submit its result.", "work_stream", true, nil, []string{"invocation_id", "callback_id", "runtime_status", "output", "adapter_inbound_event_id", "execution_id", "provider_key", "provider_run_ref", "handler_version", "attempt_count"}, map[string]any{"invocation_id": prop("string"), "callback_id": prop("string"), "runtime_status": map[string]any{"enum": []any{"succeeded", "failed", "cancelled"}}, "output": prop("object"), "adapter_inbound_event_id": prop("string"), "execution_id": prop("string"), "provider_key": prop("string"), "provider_run_ref": prop("string"), "handler_version": prop("string"), "attempt_count": prop("integer"), "error_class": prop("string"), "error_detail": prop("string")}, "agent.result_recorded"),
	def("workflow.complete", "Complete one replayed workflow invocation.", "work_stream", true, nil, []string{"invocation_id", "runtime_status", "output"}, map[string]any{"invocation_id": prop("string"), "runtime_status": map[string]any{"enum": []any{"succeeded", "failed"}}, "output": prop("object"), "error_detail": prop("string")}, "workflow.completed"),
	def("work.record_deadline", "Append the scheduler-owned deadline fact for an open Work stream.", "work_stream", true, nil, []string{"deadline_at"}, map[string]any{"deadline_at": prop("string")}, "work.deadline.reached"),
	def("event.emit", "Emit one instance pinned to a Domain Event specification.", "domain_event", false, nil, []string{"event_revision_id", "data"}, map[string]any{"event_revision_id": prop("string"), "data": prop("object"), "subject": prop("object")}, "domain.event.emitted"),
	def("object_type.define", "Define one user-authored business object type.", "object_type", false, nil, []string{"key", "singular", "plural", "field_schema"}, map[string]any{"key": prop("string"), "singular": prop("string"), "plural": prop("string"), "field_schema": prop("object"), "state_machine_id": map[string]any{"type": []any{"string", "null"}}}, "object_type.defined"),
	def("object_type.schema_evolve", "Publish an immutable replacement field schema.", "object_type", true, []string{"object_type"}, []string{"field_schema"}, map[string]any{"field_schema": prop("object")}, "object_type.schema_evolved"),
	def("relation_definition.define", "Define one named relation.", "relation_definition", false, []string{"object_type"}, []string{"key", "from_object_type_id", "to_object_type_id", "cardinality", "required"}, map[string]any{"key": prop("string"), "from_object_type_id": prop("string"), "to_object_type_id": prop("string"), "cardinality": map[string]any{"enum": []any{"one_to_one", "one_to_many", "many_to_one", "many_to_many"}}, "required": prop("boolean")}, "relation_definition.defined"),
	def("object.create", "Create one object instance.", "object_instance", false, nil, []string{"object_type_key", "title"}, map[string]any{"object_type_key": prop("string"), "title": prop("string"), "values": prop("object"), "relations": prop("object")}, "object.created"),
	def("object.update", "Update one object instance.", "object_instance", true, []string{"object_instance"}, nil, map[string]any{"title": prop("string"), "values": prop("object")}, "object.updated"),
	def("object.move", "Move one object through its state machine.", "object_instance", true, []string{"object_instance"}, []string{"to_state"}, map[string]any{"to_state": prop("string"), "reason": prop("string"), "force": prop("boolean")}, "object.moved"),
	def("relation.link", "Create one relation edge.", "object_relation", false, []string{"relation_definition", "object_instance"}, []string{"relation_definition_id", "from_object_id", "to_object_id"}, map[string]any{"relation_definition_id": prop("string"), "from_object_id": prop("string"), "to_object_id": prop("string")}, "relation.linked"),
	def("relation.unlink", "Delete one relation edge.", "object_relation", true, []string{"object_relation"}, []string{"relation_definition_id"}, map[string]any{"relation_definition_id": prop("string")}, "relation.unlinked"),
	def("reactive_rule.define", "Define one ReactiveRule.", "reactive_rule", false, nil, []string{"automation_id", "rule_key", "name", "requested_actor", "trigger", "predicates", "action", "idempotency", "limits"}, automationProperties(), "reactive_rule.defined"),
	def("reactive_rule.publish_revision", "Publish one ReactiveRule revision.", "reactive_rule", true, []string{"reactive_rule"}, []string{"revision_id", "activation_event_cursor"}, map[string]any{"revision_id": prop("string"), "activation_event_cursor": prop("string")}, "reactive_rule.revision_published"),
	def("reactive_rule.disable", "Disable one ReactiveRule.", "reactive_rule", true, []string{"reactive_rule"}, nil, map[string]any{"reason": map[string]any{"type": []any{"string", "null"}}, "circuit_breaker": prop("boolean")}, "reactive_rule.disabled"),
	def("reactive_rule.retire", "Retire one ReactiveRule.", "reactive_rule", true, []string{"reactive_rule"}, nil, map[string]any{"reason": map[string]any{"type": []any{"string", "null"}}}, "reactive_rule.retired"),
	def("object_type.delete", "Archive an object type.", "object_type", true, []string{"object_type"}, nil, deleteProperties(), "object_type.deleted"),
	def("object.delete", "Archive an object instance.", "object_instance", true, []string{"object_instance"}, nil, deleteProperties(), "object.deleted"),
	def("relation_definition.delete", "Archive a relation definition.", "relation_definition", true, []string{"relation_definition"}, nil, deleteProperties(), "relation_definition.deleted"),
	def("reactive_rule.delete", "Archive one ReactiveRule.", "reactive_rule", true, []string{"reactive_rule"}, nil, deleteProperties(), "reactive_rule.deleted"),
}

func automationProperties() map[string]any {
	p := map[string]any{}
	for _, k := range []string{"automation_id", "rule_key", "name"} {
		p[k] = prop("string")
	}
	for _, k := range []string{"requested_actor", "trigger", "predicates", "action", "idempotency", "limits"} {
		p[k] = prop("object")
	}
	return p
}

func deleteProperties() map[string]any {
	return map[string]any{"hard": prop("boolean"), "reason": map[string]any{"type": []any{"string", "null"}}}
}

func def(name, summary, aggregate string, targetID bool, preconditions, required []string, properties map[string]any, event string) Definition {
	target := "must_be_null"
	if targetID {
		target = "required"
	}
	return Definition{WireName: name, Summary: summary, PayloadSchema: schema(required, properties), AllowedTargetAggregateTypes: []string{aggregate}, TargetAggregateID: target, RequiredPreconditions: preconditions, IdempotencyBehavior: "workspace + authenticated principal + idempotency key", AutomationActionAllowed: name == "object.create" || name == "object.move" || name == "relation.link", EmittedEvents: []string{event}}
}

func Registry() []Definition {
	out := make([]Definition, 0, len(definitions)-1)
	for _, definition := range definitions {
		if definition.WireName != "function.record_result" && definition.WireName != "agent.record_result" && definition.WireName != "workflow.complete" && definition.WireName != "work.record_deadline" {
			out = append(out, definition)
		}
	}
	return out
}

func ReactiveRuleTriggerEventAllowed(eventType string) bool {
	if eventType == "work.deadline.reached" {
		return true
	}
	for _, command := range Registry() {
		if strings.HasPrefix(command.WireName, "reactive_rule.") {
			continue
		}
		for _, emitted := range command.EmittedEvents {
			if emitted == eventType {
				return true
			}
		}
	}
	return false
}

func definition(name string) (Definition, bool) {
	for _, d := range definitions {
		if d.WireName == name {
			return d, true
		}
	}
	return Definition{}, false
}

func ValidateCommand(c Command) error {
	d, ok := definition(c.CommandType)
	if !ok {
		return reject("validation_failed", "command_type is not registered")
	}
	if c.CommandID == "" || c.IdempotencyKey == "" || c.WorkspaceID == "" {
		return reject("validation_failed", "command_id, idempotency_key, and workspace_id are required")
	}
	if c.Target.AggregateType != d.AllowedTargetAggregateTypes[0] {
		return reject("validation_failed", "target aggregate_type is not allowed by the command registry")
	}
	if (d.TargetAggregateID == "required") != (c.Target.AggregateID != nil && *c.Target.AggregateID != "") {
		return reject("validation_failed", "target aggregate_id does not match the command registry")
	}
	var payload any
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return reject("validation_failed", "payload must be valid JSON")
	}
	if err := validateSchema("payload", payload, d.PayloadSchema, 0); err != nil {
		return err
	}
	return validateCredentialRevisionLiteral(c)
}

func validateCredentialRevisionLiteral(c Command) error {
	if c.CommandType != "spec.publish" {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(c.Payload))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return reject("validation_failed", "payload must be valid JSON")
	}
	kind, _ := payload["kind"].(string)
	spec, _ := payload["spec"].(map[string]any)
	var pin map[string]any
	var scope string
	switch kind {
	case "function":
		pin, _ = spec["binding"].(map[string]any)
		scope = "function spec.binding"
	case "participant":
		pin, _ = spec["execution_policy"].(map[string]any)
		scope = "agent definition.execution_policy"
	default:
		return nil
	}
	raw, supplied := pin["credential_revision"]
	if !supplied {
		return nil
	}
	number, ok := raw.(json.Number)
	if !ok {
		return reject("schema_mismatch", scope+".credential_revision must be a positive safe integer literal")
	}
	revision, err := number.Int64()
	if err != nil || revision < 1 || revision > maxCredentialRevision {
		return reject("schema_mismatch", scope+".credential_revision must be a positive safe integer literal")
	}
	return nil
}

func validateSchema(path string, value any, s map[string]any, depth int) error {
	if depth > 32 {
		return reject("schema_mismatch", "payload depth limit exceeded at "+path)
	}
	if enums, ok := s["enum"].([]any); ok {
		found := false
		for _, e := range enums {
			if equalJSON(value, e) {
				found = true
				break
			}
		}
		if !found {
			return reject("schema_mismatch", path+" must be one of the declared enum values")
		}
	}
	if t, ok := s["type"]; ok && !matchesType(value, t) {
		return reject("schema_mismatch", path+" does not match declared schema type")
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	props, _ := s["properties"].(map[string]any)
	if req, ok := s["required"].([]string); ok {
		for _, k := range req {
			if _, exists := obj[k]; !exists {
				return reject("schema_mismatch", path+"."+k+" is required")
			}
		}
	}
	if additional, ok := s["additionalProperties"].(bool); ok && !additional {
		for k := range obj {
			if _, exists := props[k]; !exists {
				return reject("schema_mismatch", path+"."+k+" is not declared in the command payload schema")
			}
		}
	}
	for k, v := range obj {
		if raw, exists := props[k]; exists {
			if child, ok := raw.(map[string]any); ok {
				if err := validateSchema(path+"."+k, v, child, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func matchesType(v any, t any) bool {
	if many, ok := t.([]any); ok {
		for _, one := range many {
			if matchesType(v, one) {
				return true
			}
		}
		return false
	}
	kind, _ := t.(string)
	switch kind {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "number":
		_, ok := number(v)
		return ok
	case "integer":
		_, ok := integer(v)
		return ok
	case "null":
		return v == nil
	}
	return false
}
