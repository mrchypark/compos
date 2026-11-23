package kernel

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

type journalFact struct {
	streamID      string
	kind          string
	specRevision  string
	participant   *Actor
	correlationID string
	causationID   string
	payload       map[string]any
}

func publishSpec(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	family, _ := p["family_id"].(string)
	kind, _ := p["kind"].(string)
	revision, ok := integer(p["revision"])
	spec, _ := p["spec"].(map[string]any)
	if strings.TrimSpace(family) == "" || !ok || revision < 1 || spec == nil {
		return effect{}, reject("validation_failed", "family_id, positive revision, kind, and spec are required")
	}
	if err := validateSpec(kind, spec); err != nil {
		return effect{}, err
	}
	if err := validateSpecReferences(ctx, tx, c.WorkspaceID, kind, spec); err != nil {
		return effect{}, err
	}
	if err := validateExecutionGraphAcyclic(ctx, tx, c.WorkspaceID, kind, spec); err != nil {
		return effect{}, err
	}
	id := aggregateID("spc", c.CommandID)
	payload := canonical(spec)
	digest := digestJSON([]byte(canonical(map[string]any{"family_id": family, "revision": revision, "kind": kind, "spec": spec})))
	_, err := tx.ExecContext(ctx, `INSERT INTO spec_revisions(id,workspace_id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id) VALUES(?,?,?,?,?,?,?,?,?)`, id, c.WorkspaceID, family, revision, kind, payload, digest, timestamp(now), principal)
	if err != nil {
		return effect{}, mapSQLError(err)
	}
	return makeEffect(id, "spec_revision", revision, "spec.published", map[string]any{"id": id, "family_id": family, "revision": revision, "kind": kind, "digest": digest}), nil
}

func validateSpec(kind string, spec map[string]any) error {
	switch kind {
	case "task":
		if err := onlyKeys("task spec", spec, "name", "goal", "input_schema", "output_schema", "constraints", "completion", "capability_policy", "ontology_context", "failure_policy", "data_bindings"); err != nil {
			return err
		}
		if err := validateDataBindings(spec); err != nil {
			return err
		}
		name, nameOK := spec["name"].(string)
		if !nameOK || strings.TrimSpace(name) == "" {
			return reject("schema_mismatch", "task spec.name is required")
		}
		goal, goalOK := spec["goal"].(string)
		if !goalOK || strings.TrimSpace(goal) == "" {
			return reject("schema_mismatch", "task spec.goal is required")
		}
		for _, field := range []string{"input_schema", "output_schema"} {
			schema, ok := spec[field].(map[string]any)
			if !ok || schema["type"] != "object" {
				return reject("schema_mismatch", "task spec."+field+" must be an object schema")
			}
			if err := validateRegisteredSchema("task spec."+field, schema, 0); err != nil {
				return err
			}
		}
		if constraints, exists := spec["constraints"]; exists {
			if err := validateBoundedStringList("task spec.constraints", constraints, 64); err != nil {
				return err
			}
		}
		completion, ok := spec["completion"].(map[string]any)
		if !ok {
			return reject("schema_mismatch", "task spec.completion must be an object")
		}
		if err := onlyKeys("task spec.completion", completion, "kind", "state"); err != nil {
			return err
		}
		switch completion["kind"] {
		case "object_state_equals":
			state, stateOK := completion["state"].(string)
			if !stateOK || strings.TrimSpace(state) == "" {
				return reject("schema_mismatch", "object_state_equals completion requires state")
			}
			if err := requireSchemaFields(spec["input_schema"].(map[string]any), map[string]string{"object_id": "string"}); err != nil {
				return reject("schema_mismatch", "object_state_equals task input_schema must require object_id as string")
			}
		case "submission_valid":
			if _, exists := completion["state"]; exists {
				return reject("schema_mismatch", "submission_valid completion does not accept state")
			}
		default:
			return reject("schema_mismatch", "task completion kind must be object_state_equals or submission_valid")
		}
		if policy, exists := spec["capability_policy"]; exists {
			value, ok := policy.(map[string]any)
			if !ok || onlyKeys("task spec.capability_policy", value, "allowed_function_revision_ids") != nil {
				return reject("schema_mismatch", "task spec.capability_policy must contain only allowed_function_revision_ids")
			}
			refs, exists := value["allowed_function_revision_ids"]
			if !exists {
				return reject("schema_mismatch", "task spec.capability_policy.allowed_function_revision_ids is required")
			}
			if err := validateBoundedStringList("task spec.capability_policy.allowed_function_revision_ids", refs, 128); err != nil {
				return err
			}
			if len(refs.([]any)) == 0 {
				return reject("schema_mismatch", "task spec.capability_policy.allowed_function_revision_ids must not be empty")
			}
		}
		if context, exists := spec["ontology_context"]; exists {
			value, ok := context.(map[string]any)
			if !ok || onlyKeys("task spec.ontology_context", value, "ontology_revision_ids", "concept_refs") != nil {
				return reject("schema_mismatch", "task spec.ontology_context supports ontology_revision_ids and concept_refs")
			}
			for _, field := range []string{"ontology_revision_ids", "concept_refs"} {
				if refs, exists := value[field]; exists {
					if err := validateBoundedStringList("task spec.ontology_context."+field, refs, 128); err != nil {
						return err
					}
				}
			}
		}
		if policy, exists := spec["failure_policy"]; exists {
			value, ok := policy.(map[string]any)
			if !ok || onlyKeys("task spec.failure_policy", value, "on_function_failed", "on_agent_failed") != nil {
				return reject("schema_mismatch", "task spec.failure_policy supports on_function_failed and on_agent_failed")
			}
			for _, name := range []string{"on_function_failed", "on_agent_failed"} {
				raw, exists := value[name]
				if !exists {
					continue
				}
				action, ok := raw.(map[string]any)
				if !ok || onlyKeys("task spec.failure_policy."+name, action, "action", "participant", "reason") != nil {
					return reject("schema_mismatch", "task failure policy is invalid")
				}
				switch action["action"] {
				case "route":
					participant, err := participantFrom(action["participant"])
					if err != nil || participant.Type != "human" {
						return reject("schema_mismatch", "route failure policy requires a human participant")
					}
				case "cancel":
					if _, exists := action["participant"]; exists {
						return reject("schema_mismatch", "cancel failure policy does not accept participant")
					}
				default:
					return reject("schema_mismatch", "failure policy action must be route or cancel")
				}
			}
		}
	case "function":
		if err := onlyKeys("function spec", spec, "name", "input_schema", "output_schema", "binding", "emits"); err != nil {
			return err
		}
		name, nameOK := spec["name"].(string)
		if !nameOK || strings.TrimSpace(name) == "" {
			return reject("schema_mismatch", "function spec.name is required")
		}
		inputSchema, ok := spec["input_schema"].(map[string]any)
		if !ok || inputSchema["type"] != "object" {
			return reject("schema_mismatch", "function spec.input_schema must be an object schema")
		}
		if err := validateRegisteredSchema("function spec.input_schema", inputSchema, 0); err != nil {
			return err
		}
		if output, exists := spec["output_schema"]; exists {
			outputSchema, ok := output.(map[string]any)
			if !ok || outputSchema["type"] != "object" {
				return reject("schema_mismatch", "function spec.output_schema must be an object schema")
			}
			if err := validateRegisteredSchema("function spec.output_schema", outputSchema, 0); err != nil {
				return err
			}
		}
		binding, ok := spec["binding"].(map[string]any)
		if !ok {
			return reject("schema_mismatch", "function spec.binding must be an object")
		}
		if err := validateFunctionBinding(inputSchema, binding); err != nil {
			return err
		}
		if emits, exists := spec["emits"]; exists {
			items, ok := emits.([]any)
			if !ok || len(items) > 64 {
				return reject("schema_mismatch", "function spec.emits must be an array of at most 64 Event revision IDs")
			}
			for _, item := range items {
				if value, ok := item.(string); !ok || strings.TrimSpace(value) == "" {
					return reject("schema_mismatch", "function spec.emits must contain non-empty Event revision IDs")
				}
			}
		}
	case "workflow":
		return validateWorkflowSpec(spec)
	case "participant":
		return validateAgentDefinition(spec)
	case "event":
		if err := onlyKeys("event spec", spec, "name", "payload_schema"); err != nil {
			return err
		}
		if name, ok := spec["name"].(string); !ok || strings.TrimSpace(name) == "" {
			return reject("schema_mismatch", "event spec.name is required")
		}
		payloadSchema, ok := spec["payload_schema"].(map[string]any)
		if !ok || payloadSchema["type"] != "object" {
			return reject("schema_mismatch", "event spec.payload_schema must be an object schema")
		}
		return validateRegisteredSchema("event spec.payload_schema", payloadSchema, 0)
	case "ontology":
		return validateOntologySpec(spec)
	default:
		return reject("schema_mismatch", "unsupported spec kind")
	}
	return nil
}

func validateAgentDefinition(spec map[string]any) error {
	if err := onlyKeys("agent definition", spec, "name", "instructions", "model_policy", "context_policy", "tools", "permissions", "memory_policy", "execution_policy"); err != nil {
		return err
	}
	if err := requireNonEmptyStrings(spec, "name", "instructions"); err != nil {
		return err
	}
	for _, name := range []string{"model_policy", "context_policy", "permissions", "memory_policy"} {
		if value, exists := spec[name]; exists {
			if object, ok := value.(map[string]any); !ok || len(object) > 64 {
				return reject("schema_mismatch", "agent definition."+name+" must be an object with at most 64 entries")
			}
		}
	}
	if tools, exists := spec["tools"]; exists {
		if err := validateBoundedStringList("agent definition.tools", tools, 128); err != nil {
			return err
		}
	}
	execution, ok := spec["execution_policy"].(map[string]any)
	if !ok || onlyKeys("agent definition.execution_policy", execution, "handler_key", "handler_version", "connection_id", "credential_revision", "timeout_seconds", "retry_policy") != nil {
		return reject("schema_mismatch", "agent definition.execution_policy is required")
	}
	if err := requireNonEmptyStrings(execution, "handler_key", "handler_version"); err != nil {
		return err
	}
	if err := validateCredentialPin("agent definition.execution_policy", execution); err != nil {
		return err
	}
	return validateExecutionPolicy(execution)
}

func validateFunctionBinding(inputSchema, binding map[string]any) error {
	kind, _ := binding["kind"].(string)
	switch kind {
	case "kernel_command":
		if err := onlyKeys("function spec.binding", binding, "kind", "command_type", "to_state", "force"); err != nil {
			return err
		}
		if err := requireSchemaFields(inputSchema, map[string]string{"object_id": "string", "expected_version": "integer"}); err != nil {
			return err
		}
		switch binding["command_type"] {
		case "object.move":
			toState, stateOK := binding["to_state"].(string)
			if !stateOK || strings.TrimSpace(toState) == "" {
				return reject("schema_mismatch", "object.move binding requires to_state")
			}
			if force, exists := binding["force"]; exists {
				if _, ok := force.(bool); !ok {
					return reject("schema_mismatch", "function spec.binding.force must be boolean")
				}
			}
		case "object.update":
			if _, exists := binding["to_state"]; exists {
				return reject("schema_mismatch", "object.update binding does not accept to_state")
			}
			if _, exists := binding["force"]; exists {
				return reject("schema_mismatch", "object.update binding does not accept force")
			}
			properties, _ := inputSchema["properties"].(map[string]any)
			title, hasTitle := properties["title"].(map[string]any)
			values, hasValues := properties["values"].(map[string]any)
			if !hasTitle && !hasValues {
				return reject("schema_mismatch", "object.update input requires title or values")
			}
			if hasTitle && title["type"] != "string" {
				return reject("schema_mismatch", "object.update input.title must be a string")
			}
			if hasValues && values["type"] != "object" {
				return reject("schema_mismatch", "object.update input.values must be an object")
			}
		default:
			return reject("schema_mismatch", "kernel command binding must target allowlisted object.move or object.update")
		}
	case "connector":
		if err := onlyKeys("function spec.binding", binding, "kind", "handler_version", "provider", "operation", "connection_id", "credential_revision", "timeout_seconds", "retry_policy"); err != nil {
			return err
		}
		if err := requireNonEmptyStrings(binding, "handler_version", "provider", "operation"); err != nil {
			return err
		}
		if err := validateCredentialPin("function spec.binding", binding); err != nil {
			return err
		}
		if err := validateExecutionPolicy(binding); err != nil {
			return err
		}
	case "native":
		if err := onlyKeys("function spec.binding", binding, "kind", "handler_key", "handler_version", "operation", "timeout_seconds", "retry_policy"); err != nil {
			return err
		}
		if err := requireNonEmptyStrings(binding, "handler_key", "handler_version"); err != nil {
			return err
		}
		if operation, exists := binding["operation"]; exists {
			if value, ok := operation.(string); !ok || strings.TrimSpace(value) == "" {
				return reject("schema_mismatch", "function spec.binding.operation must be a non-empty string")
			}
		}
		if err := validateExecutionPolicy(binding); err != nil {
			return err
		}
	case "composite":
		if err := onlyKeys("function spec.binding", binding, "kind", "workflow_revision_id"); err != nil {
			return err
		}
		return requireNonEmptyStrings(binding, "workflow_revision_id")
	default:
		return reject("schema_mismatch", "function spec.binding.kind must be kernel_command, connector, native, or composite")
	}
	return nil
}

func validateCredentialPin(scope string, value map[string]any) error {
	connection, hasConnection := value["connection_id"]
	if hasConnection {
		if text, ok := connection.(string); !ok || strings.TrimSpace(text) == "" {
			return reject("schema_mismatch", scope+".connection_id must be a non-empty string")
		}
	}
	if revision, supplied := value["credential_revision"]; supplied {
		if number, ok := integer(revision); !ok || number < 1 || number > maxCredentialRevision {
			return reject("schema_mismatch", scope+".credential_revision must be a positive safe integer")
		}
		if !hasConnection {
			return reject("schema_mismatch", scope+".credential_revision requires connection_id")
		}
	}
	return nil
}

const maxCredentialRevision int64 = 9007199254740991

func validateWorkflowSpec(spec map[string]any) error {
	if err := onlyKeys("workflow spec", spec, "name", "steps"); err != nil {
		return err
	}
	if name, ok := spec["name"].(string); !ok || strings.TrimSpace(name) == "" {
		return reject("schema_mismatch", "workflow spec.name is required")
	}
	steps, ok := spec["steps"].([]any)
	if !ok || len(steps) == 0 {
		return reject("schema_mismatch", "workflow spec.steps must not be empty")
	}
	count := 0
	return validateWorkflowSteps(steps, map[string]bool{}, &count, 0)
}

func validateWorkflowSteps(steps []any, ids map[string]bool, count *int, depth int) error {
	if depth > 8 {
		return reject("schema_mismatch", "workflow nesting exceeds 8 levels")
	}
	for _, raw := range steps {
		*count++
		if *count > 128 {
			return reject("schema_mismatch", "workflow may contain at most 128 steps")
		}
		step, ok := raw.(map[string]any)
		if !ok {
			return reject("schema_mismatch", "workflow step must be an object")
		}
		id, _ := step["id"].(string)
		kind, _ := step["kind"].(string)
		if !workflowIdentifier(id) || ids[id] {
			return reject("schema_mismatch", "workflow step IDs must be unique ASCII identifiers")
		}
		ids[id] = true
		switch kind {
		case "route":
			if err := onlyKeys("workflow route step", step, "id", "kind", "participant", "reason", "output_schema", "output_mapping", "data_binding_ids"); err != nil {
				return err
			}
			if _, err := participantFrom(step["participant"]); err != nil {
				return err
			}
			if output, exists := step["output_schema"]; exists {
				schema, ok := output.(map[string]any)
				if !ok || schema["type"] != "object" {
					return reject("schema_mismatch", "workflow route step.output_schema must be an object schema")
				}
				if err := validateRegisteredSchema("workflow route step.output_schema", schema, 0); err != nil {
					return err
				}
			}
			if err := validateWorkflowMapping(step, "output_mapping"); err != nil {
				return err
			}
			if raw, exists := step["data_binding_ids"]; exists {
				ids, ok := raw.([]any)
				if !ok || len(ids) == 0 || len(ids) > 32 {
					return reject("schema_mismatch", "workflow route step.data_binding_ids must contain 1 to 32 IDs")
				}
				seen := map[string]bool{}
				for _, rawID := range ids {
					bindingID, ok := rawID.(string)
					if !ok || !workflowIdentifier(bindingID) || seen[bindingID] {
						return reject("schema_mismatch", "workflow route step.data_binding_ids must be unique identifiers")
					}
					seen[bindingID] = true
				}
			}
		case "invoke":
			if err := onlyKeys("workflow invoke step", step, "id", "kind", "function_revision_id", "input_mapping", "output_mapping"); err != nil {
				return err
			}
			if value, ok := step["function_revision_id"].(string); !ok || strings.TrimSpace(value) == "" {
				return reject("schema_mismatch", "workflow invoke step.function_revision_id is required")
			}
			if err := validateWorkflowMapping(step, "input_mapping"); err != nil {
				return err
			}
			if err := validateWorkflowMapping(step, "output_mapping"); err != nil {
				return err
			}
		case "branch":
			if err := onlyKeys("workflow branch step", step, "id", "kind", "condition", "then", "else", "output_mapping"); err != nil {
				return err
			}
			if err := validateWorkflowCondition(step["condition"]); err != nil {
				return err
			}
			thenSteps, ok := step["then"].([]any)
			if !ok || len(thenSteps) == 0 {
				return reject("schema_mismatch", "workflow branch step.then must not be empty")
			}
			elseSteps, ok := step["else"].([]any)
			if !ok {
				return reject("schema_mismatch", "workflow branch step.else must be an array")
			}
			if err := validateWorkflowSteps(thenSteps, ids, count, depth+1); err != nil {
				return err
			}
			if err := validateWorkflowSteps(elseSteps, ids, count, depth+1); err != nil {
				return err
			}
			if err := validateWorkflowMapping(step, "output_mapping"); err != nil {
				return err
			}
		case "parallel":
			if err := onlyKeys("workflow parallel step", step, "id", "kind", "branches", "output_mapping"); err != nil {
				return err
			}
			branches, ok := step["branches"].([]any)
			if !ok || len(branches) < 2 || len(branches) > 16 {
				return reject("schema_mismatch", "workflow parallel step.branches must contain 2 to 16 branches")
			}
			branchIDs := map[string]bool{}
			for _, rawBranch := range branches {
				branch, ok := rawBranch.(map[string]any)
				if !ok || onlyKeys("workflow parallel branch", branch, "id", "steps") != nil {
					return reject("schema_mismatch", "workflow parallel branches require id and steps")
				}
				branchID, _ := branch["id"].(string)
				branchSteps, ok := branch["steps"].([]any)
				if !workflowIdentifier(branchID) || branchIDs[branchID] || !ok || len(branchSteps) == 0 {
					return reject("schema_mismatch", "workflow parallel branch IDs must be unique and steps must not be empty")
				}
				branchIDs[branchID] = true
				if err := validateWorkflowSteps(branchSteps, ids, count, depth+1); err != nil {
					return err
				}
			}
			if err := validateWorkflowMapping(step, "output_mapping"); err != nil {
				return err
			}
		default:
			return reject("schema_mismatch", "workflow step.kind must be route, invoke, branch, or parallel")
		}
	}
	return nil
}

func validateWorkflowMapping(step map[string]any, name string) error {
	raw, exists := step[name]
	if !exists {
		return nil
	}
	mapping, ok := raw.(map[string]any)
	if !ok || len(mapping) == 0 || len(mapping) > 128 {
		return reject("schema_mismatch", "workflow "+name+" must be a non-empty object with at most 128 entries")
	}
	for key, value := range mapping {
		if strings.TrimSpace(key) == "" {
			return reject("schema_mismatch", "workflow mapping keys must not be empty")
		}
		if selector, ok := value.(string); ok && strings.HasPrefix(selector, "$") && !strings.HasPrefix(selector, "$.") {
			return reject("schema_mismatch", "workflow selectors must start with $.")
		}
	}
	return nil
}

// WorkflowRouteContract is the pinned submission contract of one Workflow route.
// It is derived from the immutable Workflow spec, never from a work.route payload.
type WorkflowRouteContract struct {
	OutputSchema   map[string]any
	DataBindingIDs []string
}

// WorkflowRouteContractForSpec finds a route by its deterministic command identity.
// The bool is false when this Workflow invocation did not create that route.
func WorkflowRouteContractForSpec(spec map[string]any, invocationID, stepID, routeID string) (WorkflowRouteContract, bool) {
	if workflowRouteCommandID(invocationID, stepID) != routeID {
		return WorkflowRouteContract{}, false
	}
	return workflowRouteContractByStep(spec["steps"], stepID)
}

func workflowRouteContractByStep(steps any, stepID string) (WorkflowRouteContract, bool) {
	for _, raw := range anySlice(steps) {
		step, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if step["kind"] == "route" && step["id"] == stepID {
			contract := WorkflowRouteContract{}
			contract.OutputSchema, _ = step["output_schema"].(map[string]any)
			for _, rawID := range anySlice(step["data_binding_ids"]) {
				if bindingID, ok := rawID.(string); ok {
					contract.DataBindingIDs = append(contract.DataBindingIDs, bindingID)
				}
			}
			return contract, true
		}
		if step["kind"] == "branch" {
			if contract, found := workflowRouteContractByStep(step["then"], stepID); found {
				return contract, true
			}
			if contract, found := workflowRouteContractByStep(step["else"], stepID); found {
				return contract, true
			}
		}
		if step["kind"] == "parallel" {
			for _, rawBranch := range anySlice(step["branches"]) {
				branch, _ := rawBranch.(map[string]any)
				if contract, found := workflowRouteContractByStep(branch["steps"], stepID); found {
					return contract, true
				}
			}
		}
	}
	return WorkflowRouteContract{}, false
}

func workflowRouteCommandID(invocation, step string) string {
	sum := sha256.Sum256([]byte(invocation + "\x00" + step + "\x00route"))
	return "cmd_wf_" + hex.EncodeToString(sum[:16])
}

func validateWorkflowCondition(raw any) error {
	condition, ok := raw.(map[string]any)
	if !ok || onlyKeys("workflow condition", condition, "left", "operator", "right") != nil {
		return reject("schema_mismatch", "workflow condition must contain left, operator, and optional right")
	}
	left, _ := condition["left"].(string)
	operator, _ := condition["operator"].(string)
	if !strings.HasPrefix(left, "$.") || !map[string]bool{"eq": true, "not_eq": true, "exists": true, "not_exists": true}[operator] {
		return reject("schema_mismatch", "workflow condition uses an invalid selector or operator")
	}
	_, hasRight := condition["right"]
	if (operator == "eq" || operator == "not_eq") != hasRight {
		return reject("schema_mismatch", "workflow eq/not_eq conditions require right and exists conditions forbid it")
	}
	return nil
}

func workflowIdentifier(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func validateOntologySpec(spec map[string]any) error {
	if err := onlyKeys("ontology spec", spec, "name", "concepts", "relations"); err != nil {
		return err
	}
	if name, ok := spec["name"].(string); !ok || strings.TrimSpace(name) == "" {
		return reject("schema_mismatch", "ontology spec.name is required")
	}
	concepts, ok := spec["concepts"].([]any)
	if !ok || len(concepts) == 0 || len(concepts) > 1000 {
		return reject("schema_mismatch", "ontology spec.concepts must contain 1 to 1000 concepts")
	}
	ids := map[string]bool{}
	for _, raw := range concepts {
		concept, ok := raw.(map[string]any)
		if !ok || onlyKeys("ontology concept", concept, "id", "description") != nil {
			return reject("schema_mismatch", "ontology concepts must contain only id and description")
		}
		id, _ := concept["id"].(string)
		if strings.TrimSpace(id) == "" || ids[id] {
			return reject("schema_mismatch", "ontology concept IDs must be non-empty and unique")
		}
		ids[id] = true
		if description, exists := concept["description"]; exists {
			if value, ok := description.(string); !ok || strings.TrimSpace(value) == "" {
				return reject("schema_mismatch", "ontology concept descriptions must be non-empty strings")
			}
		}
	}
	relations, ok := spec["relations"].([]any)
	if !ok || len(relations) > 5000 {
		return reject("schema_mismatch", "ontology spec.relations must be an array of at most 5000 relations")
	}
	for _, raw := range relations {
		relation, ok := raw.(map[string]any)
		if !ok || onlyKeys("ontology relation", relation, "source", "predicate", "target", "cardinality") != nil {
			return reject("schema_mismatch", "ontology relations have an invalid shape")
		}
		source, _ := relation["source"].(string)
		target, _ := relation["target"].(string)
		predicate, _ := relation["predicate"].(string)
		if !ids[source] || !ids[target] || strings.TrimSpace(predicate) == "" {
			return reject("schema_mismatch", "ontology relations must reference declared concepts and a predicate")
		}
		if cardinality, exists := relation["cardinality"]; exists && !map[string]bool{"one_to_one": true, "one_to_many": true, "many_to_one": true, "many_to_many": true}[fmt.Sprint(cardinality)] {
			return reject("schema_mismatch", "ontology relation cardinality is invalid")
		}
	}
	return nil
}

func validateSpecReferences(ctx context.Context, tx store.Tx, workspace, kind string, spec map[string]any) error {
	if kind == "task" {
		if _, err := validateDataBindingReferences(func(query string, args ...any) store.Row {
			return sqlWorkChangeRow{row: tx.QueryRowContext(ctx, query, args...)}
		}, workspace, spec); err != nil {
			return err
		}
	}
	refs := map[string]string{}
	switch kind {
	case "task":
		if policy, ok := spec["capability_policy"].(map[string]any); ok {
			for _, raw := range anySlice(policy["allowed_function_revision_ids"]) {
				refs[raw.(string)] = "function"
			}
		}
		if ontology, ok := spec["ontology_context"].(map[string]any); ok {
			for _, raw := range anySlice(ontology["ontology_revision_ids"]) {
				refs[raw.(string)] = "ontology"
			}
		}
	case "workflow":
		collectWorkflowReferences(spec["steps"].([]any), refs)
	case "function":
		binding := spec["binding"].(map[string]any)
		if binding["kind"] == "composite" {
			refs[binding["workflow_revision_id"].(string)] = "workflow"
		}
		if emits, ok := spec["emits"].([]any); ok {
			for _, raw := range emits {
				refs[raw.(string)] = "event"
			}
		}
	case "participant":
		for _, raw := range anySlice(spec["tools"]) {
			refs[raw.(string)] = "function"
		}
	}
	for id, expectedKind := range refs {
		var actualKind string
		if err := tx.QueryRowContext(ctx, `SELECT kind FROM spec_revisions WHERE workspace_id=? AND id=?`, workspace, id).Scan(&actualKind); err != nil || actualKind != expectedKind {
			return reject("precondition_failed", expectedKind+" spec revision "+id+" does not exist")
		}
	}
	return nil
}

func validateExecutionGraphAcyclic(ctx context.Context, tx store.Tx, workspace, kind string, spec map[string]any) error {
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string, string, map[string]any) error
	visit = func(id, nodeKind string, node map[string]any) error {
		if visiting[id] {
			return reject("validation_failed", "workflow/function execution graph contains a cycle")
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for ref, refKind := range executionGraphReferences(nodeKind, node) {
			var raw string
			if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM spec_revisions WHERE workspace_id=? AND id=? AND kind=?`, workspace, ref, refKind).Scan(&raw); err != nil {
				return reject("precondition_failed", refKind+" spec revision "+ref+" does not exist")
			}
			var child map[string]any
			if err := json.Unmarshal([]byte(raw), &child); err != nil {
				return err
			}
			if err := visit(ref, refKind, child); err != nil {
				return err
			}
		}
		delete(visiting, id)
		visited[id] = true
		return nil
	}
	return visit("$candidate", kind, spec)
}

func executionGraphReferences(kind string, spec map[string]any) map[string]string {
	refs := map[string]string{}
	if kind == "workflow" {
		collectWorkflowReferences(spec["steps"].([]any), refs)
		for id, refKind := range refs {
			if refKind != "function" {
				delete(refs, id)
			}
		}
	} else if kind == "function" {
		binding := spec["binding"].(map[string]any)
		if binding["kind"] == "composite" {
			refs[binding["workflow_revision_id"].(string)] = "workflow"
		}
	}
	return refs
}

func validateBoundedStringList(path string, raw any, limit int) error {
	values, ok := raw.([]any)
	if !ok || len(values) > limit {
		return reject("schema_mismatch", path+" must be an array of at most "+fmt.Sprint(limit)+" strings")
	}
	for _, value := range values {
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return reject("schema_mismatch", path+" must contain non-empty strings")
		}
	}
	return nil
}

func anySlice(raw any) []any {
	values, _ := raw.([]any)
	return values
}

func collectWorkflowReferences(steps []any, refs map[string]string) {
	for _, raw := range steps {
		step := raw.(map[string]any)
		switch step["kind"] {
		case "invoke":
			refs[step["function_revision_id"].(string)] = "function"
		case "route":
			participant := step["participant"].(map[string]any)
			if participant["kind"] == "agent" {
				refs[participant["id"].(string)] = "participant"
			}
		case "branch":
			collectWorkflowReferences(step["then"].([]any), refs)
			collectWorkflowReferences(step["else"].([]any), refs)
		case "parallel":
			for _, rawBranch := range step["branches"].([]any) {
				collectWorkflowReferences(rawBranch.(map[string]any)["steps"].([]any), refs)
			}
		}
	}
}

func requireNonEmptyStrings(value map[string]any, names ...string) error {
	for _, name := range names {
		text, ok := value[name].(string)
		if !ok || strings.TrimSpace(text) == "" {
			return reject("schema_mismatch", "function spec.binding."+name+" is required")
		}
	}
	return nil
}

func validateExecutionPolicy(binding map[string]any) error {
	if timeout, exists := binding["timeout_seconds"]; exists {
		seconds, ok := integer(timeout)
		if !ok || seconds < 1 || seconds > 86400 {
			return reject("schema_mismatch", "function spec.binding.timeout_seconds must be an integer from 1 to 86400")
		}
	}
	if raw, exists := binding["retry_policy"]; exists {
		policy, ok := raw.(map[string]any)
		if !ok {
			return reject("schema_mismatch", "function spec.binding.retry_policy must be an object")
		}
		if err := onlyKeys("function spec.binding.retry_policy", policy, "max_attempts", "backoff"); err != nil {
			return err
		}
		attempts, ok := integer(policy["max_attempts"])
		if !ok || attempts < 1 {
			return reject("schema_mismatch", "function spec.binding.retry_policy.max_attempts must be a positive integer")
		}
		if backoff, exists := policy["backoff"]; exists {
			switch value := backoff.(type) {
			case string:
				if strings.TrimSpace(value) == "" {
					return reject("schema_mismatch", "function spec.binding.retry_policy.backoff must not be empty")
				}
			case float64:
				if value < 0 {
					return reject("schema_mismatch", "function spec.binding.retry_policy.backoff must not be negative")
				}
			default:
				return reject("schema_mismatch", "function spec.binding.retry_policy.backoff must be a string or number")
			}
		}
	}
	return nil
}

func onlyKeys(path string, value map[string]any, allowed ...string) error {
	set := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		set[key] = true
	}
	for key := range value {
		if !set[key] {
			return reject("schema_mismatch", path+" uses unsupported property "+key)
		}
	}
	return nil
}

func requireSchemaFields(schema map[string]any, required map[string]string) error {
	props, _ := schema["properties"].(map[string]any)
	names, _ := schema["required"].([]any)
	for name, kind := range required {
		field, ok := props[name].(map[string]any)
		if !ok || field["type"] != kind {
			return reject("schema_mismatch", "function input_schema must declare "+name+" as "+kind)
		}
		found := false
		for _, raw := range names {
			found = found || raw == name
		}
		if !found {
			return reject("schema_mismatch", "function input_schema must require "+name)
		}
	}
	return nil
}

// Work binding contexts are record identities, not arbitrary strings. Checking
// them when the Work opens prevents an immutable, but unusable, Work context.
type workContextRow interface{ Scan(...any) error }

func validateWorkBindingContexts(read func(string, ...any) workContextRow, guard func(string, ...any), workspace string, task, contextValue map[string]any) error {
	for _, raw := range anySlice(task["data_bindings"]) {
		binding := raw.(map[string]any)
		key := binding["context_key"].(string)
		rootID, _ := contextValue[key].(string)
		if rootID == "" {
			return reject("precondition_failed", "Work has no binding context record")
		}
		rootType := binding["object_type_id"].(string)
		for i := len(anySlice(binding["relation_path"])) - 1; i >= 0; i-- {
			hop := anySlice(binding["relation_path"])[i].(map[string]any)
			var fromType, toType string
			if err := read(`SELECT from_object_type_id,to_object_type_id FROM relation_definitions WHERE workspace_id=? AND id=? AND version=? AND archived_at IS NULL`, workspace, hop["relation_definition_id"], hop["version"]).Scan(&fromType, &toType); err != nil {
				return reject("precondition_failed", "binding relation definition does not exist at pinned version")
			}
			if guard != nil {
				guard(`SELECT 1 FROM relation_definitions WHERE workspace_id=? AND id=? AND version=? AND from_object_type_id=? AND to_object_type_id=? AND archived_at IS NULL`, workspace, hop["relation_definition_id"], hop["version"], fromType, toType)
			}
			if hop["direction"] == "incoming" {
				fromType, toType = toType, fromType
			}
			if rootType != toType {
				return reject("precondition_failed", "binding relation path is incompatible")
			}
			rootType = fromType
		}
		var version int64
		if err := read(`SELECT version FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=? AND archived_at IS NULL`, workspace, rootID, rootType).Scan(&version); err != nil {
			return reject("precondition_failed", "Work binding context record does not exist or is incompatible")
		}
		if guard != nil {
			guard(`SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=? AND archived_at IS NULL AND version=?`, workspace, rootID, rootType, version)
		}
	}
	return nil
}

func openWork(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	taskID, _ := p["task_revision_id"].(string)
	contextValue, _ := p["context"].(map[string]any)
	taskSpec, err := loadSpec(ctx, tx, c.WorkspaceID, taskID, "task")
	if err != nil {
		return effect{}, err
	}
	if err := validateValues(contextValue, taskSpec["input_schema"].(map[string]any), "payload.context"); err != nil {
		return effect{}, err
	}
	if err := validateWorkBindingContexts(func(query string, args ...any) workContextRow { return tx.QueryRowContext(ctx, query, args...) }, nil, c.WorkspaceID, taskSpec, contextValue); err != nil {
		return effect{}, err
	}
	if objectID, ok := contextValue["object_id"].(string); ok {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, c.WorkspaceID, objectID).Scan(&exists); err != nil {
			return effect{}, reject("precondition_failed", "work context object does not exist")
		}
	}
	streamID := aggregateID("wrk", c.CommandID)
	payload := map[string]any{"task_revision_id": taskID, "context": contextValue, "owner_principal_id": principal}
	if deadline, _ := p["deadline_at"].(string); strings.TrimSpace(deadline) != "" {
		parsed, err := time.Parse(time.RFC3339, deadline)
		if err != nil {
			return effect{}, reject("schema_mismatch", "payload.deadline_at must be RFC3339")
		}
		payload["deadline_at"] = parsed.UTC().Format(time.RFC3339)
	}
	fact := journalFact{streamID: streamID, kind: "work.opened", specRevision: taskID, payload: payload}
	effectPayload := map[string]any{"id": streamID, "task_revision_id": taskID, "context": contextValue, "owner_principal_id": principal}
	if deadline, exists := payload["deadline_at"]; exists {
		effectPayload["deadline_at"] = deadline
	}
	eff := makeEffect(streamID, "work_stream", 1, "work.opened", effectPayload)
	eff.journal = []journalFact{fact}
	return eff, nil
}

func recordWorkDeadline(ctx context.Context, tx store.Tx, c Command, p map[string]any) (effect, error) {
	if c.Causation.Source != "scheduler" {
		return effect{}, reject("forbidden", "Work deadline facts require scheduler causation")
	}
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	deadline, _ := p["deadline_at"].(string)
	parsed, err := time.Parse(time.RFC3339, deadline)
	if err != nil {
		return effect{}, reject("schema_mismatch", "payload.deadline_at must be RFC3339")
	}
	deadline = parsed.UTC().Format(time.RFC3339)
	var openedDeadline string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(json_extract(payload_json,'$.deadline_at'),'') FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.opened'`, c.WorkspaceID, streamID).Scan(&openedDeadline); err != nil || openedDeadline != deadline {
		return effect{}, reject("precondition_failed", "Work deadline does not match the opened Work")
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.deadline.reached'`, c.WorkspaceID, streamID).Scan(&existing); err != nil {
		return effect{}, err
	}
	if existing != 0 {
		return effect{}, reject("precondition_failed", "Work deadline was already recorded")
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	payload := map[string]any{"deadline_at": deadline}
	eff := makeEffect(streamID, "work_stream", version, "work.deadline.reached", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "work.deadline.reached", payload: payload}}
	return eff, nil
}

func routeWork(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	participant, err := participantFrom(p["participant"])
	if err != nil {
		return effect{}, err
	}
	payload := map[string]any{"participant": map[string]any{"kind": participant.Type, "id": participant.ID}}
	workflowStepID, _ := p["workflow_step_id"].(string)
	workflowRouteID, _ := p["workflow_route_id"].(string)
	if workflowStepID != "" {
		if c.Causation.Source != "workflow" || !workflowIdentifier(workflowStepID) || workflowRouteID != c.CommandID {
			return effect{}, reject("forbidden", "Workflow route identity must match its command")
		}
		payload["workflow_step_id"], payload["workflow_route_id"] = workflowStepID, workflowRouteID
	} else if workflowRouteID != "" {
		return effect{}, reject("schema_mismatch", "workflow_route_id requires workflow_step_id")
	}
	if reason, _ := p["reason"].(string); strings.TrimSpace(reason) != "" {
		payload["reason"] = reason
	}
	eff := makeEffect(streamID, "work_stream", version, "work.routed", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "work.routed", participant: participant, payload: payload}}
	if participant.Type == "agent" {
		agent, err := loadSpec(ctx, tx, c.WorkspaceID, participant.ID, "participant")
		if err != nil {
			return effect{}, err
		}
		taskID, contextValue, err := workTask(ctx, tx, c.WorkspaceID, streamID)
		if err != nil {
			return effect{}, err
		}
		task, err := loadSpec(ctx, tx, c.WorkspaceID, taskID, "task")
		if err != nil {
			return effect{}, err
		}
		invocationID := aggregateID("agn", c.CommandID)
		execution := agent["execution_policy"].(map[string]any)
		handlerKey := execution["handler_key"].(string)
		compos := map[string]any{
			"workspace_id": c.WorkspaceID, "execution_request_id": invocationID,
			"handler_key": handlerKey, "handler_version": execution["handler_version"],
			"workflow_instance_id": streamID, "task_id": streamID,
			"actor": map[string]any{"type": "agent", "id": participant.ID},
		}
		for _, name := range []string{"connection_id", "credential_revision", "timeout_seconds", "retry_policy"} {
			if value, exists := execution[name]; exists {
				compos[name] = value
			}
		}
		eff.aggregate.Version++
		requestPayload := map[string]any{"agent_revision_id": participant.ID, "task_revision_id": taskID, "input": contextValue, "handler_key": handlerKey, "handler_version": execution["handler_version"]}
		for _, name := range []string{"connection_id", "credential_revision"} {
			if value, exists := execution[name]; exists {
				requestPayload[name] = value
			}
		}
		if workflowStepID != "" {
			requestPayload["workflow_step_id"] = workflowStepID
			requestPayload["workflow_route_id"] = workflowRouteID
			compos["workflow_step_id"] = workflowStepID
			compos["workflow_route_id"] = workflowRouteID
		}
		eff.journal = append(eff.journal, journalFact{streamID: streamID, kind: "agent.requested", specRevision: participant.ID, participant: participant, correlationID: invocationID, payload: requestPayload})
		eff.outboxEvent = handlerKey
		eff.outboxPayload = map[string]any{"agent_definition": agent, "task_definition": task, "input": contextValue, "subject_ref": contextValue, "_compos": compos}
	}
	return eff, nil
}

func submitWork(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	actor := commandParticipant(c, principal)
	requestedStepID, _ := p["workflow_step_id"].(string)
	requestedRouteID, _ := p["workflow_route_id"].(string)
	if requestedStepID != "" && !workflowIdentifier(requestedStepID) {
		return effect{}, reject("schema_mismatch", "workflow_step_id must be a valid Workflow step ID")
	}
	if (requestedStepID == "") != (requestedRouteID == "") {
		return effect{}, reject("schema_mismatch", "workflow_step_id and workflow_route_id must be provided together")
	}
	workflowStepID, workflowRouteID, err := currentWorkRoute(ctx, tx, c.WorkspaceID, streamID, actor, requestedStepID, requestedRouteID, true)
	if err != nil {
		return effect{}, err
	}
	if requestedRouteID == "" && workflowRouteID != "" {
		return effect{}, reject("schema_mismatch", "Workflow submissions require workflow_step_id and workflow_route_id")
	}
	submission, _ := p["submission"].(map[string]any)
	taskID, contextValue, err := workTask(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	taskSpec, err := loadSpec(ctx, tx, c.WorkspaceID, taskID, "task")
	if err != nil {
		return effect{}, err
	}
	submissionSchema, err := workflowSubmissionSchema(ctx, tx, c.WorkspaceID, streamID, taskSpec, workflowStepID, workflowRouteID)
	if err != nil {
		return effect{}, err
	}
	if err := validateValues(submission, submissionSchema, "payload.submission"); err != nil {
		return effect{}, err
	}
	payload := map[string]any{"submission": submission}
	if workflowStepID != "" {
		payload["workflow_step_id"] = workflowStepID
		payload["workflow_route_id"] = workflowRouteID
	}
	eff := makeEffect(streamID, "work_stream", version, "work.submitted", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "work.submitted", participant: actor, payload: payload}}
	completion := taskSpec["completion"].(map[string]any)
	if completion["kind"] == "submission_valid" && workflowStepID == "" {
		objectID, _ := contextValue["object_id"].(string)
		eff.aggregate.Version++
		eff.journal = append(eff.journal, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, participant: actor, payload: map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "object_id": objectID, "submission_sequence": version}})
	}
	return eff, nil
}

func cancelWork(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	reason, _ := p["reason"].(string)
	if strings.TrimSpace(reason) == "" {
		return effect{}, reject("schema_mismatch", "work cancellation requires reason")
	}
	actor := commandParticipant(c, principal)
	payload := map[string]any{"reason": reason}
	eff := makeEffect(streamID, "work_stream", version, "work.cancelled", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "work.cancelled", participant: actor, payload: payload}}
	return eff, nil
}

func invokeFunction(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	workVersion, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	actor := commandParticipant(c, principal)
	workflowRouteID, _ := p["workflow_route_id"].(string)
	if workflowRouteID != "" && c.Causation.Source != "workflow" {
		return effect{}, reject("forbidden", "workflow_route_id is reserved for Workflow invocations")
	}
	if _, _, err := currentWorkRoute(ctx, tx, c.WorkspaceID, streamID, actor, "", workflowRouteID, false); err != nil {
		return effect{}, err
	}
	functionID, _ := p["function_revision_id"].(string)
	functionSpec, err := loadSpec(ctx, tx, c.WorkspaceID, functionID, "function")
	if err != nil {
		return effect{}, err
	}
	input, _ := p["input"].(map[string]any)
	inputSchema, _ := functionSpec["input_schema"].(map[string]any)
	if err := validateValues(input, inputSchema, "payload.input"); err != nil {
		return effect{}, err
	}
	taskID, contextValue, err := workTask(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	taskSpec, err := loadSpec(ctx, tx, c.WorkspaceID, taskID, "task")
	if err != nil {
		return effect{}, err
	}
	if policy, ok := taskSpec["capability_policy"].(map[string]any); ok {
		allowed := anySlice(policy["allowed_function_revision_ids"])
		permitted := false
		for _, raw := range allowed {
			permitted = permitted || raw == functionID
		}
		if !permitted {
			return effect{}, reject("forbidden", "function is not allowed by the pinned task capability policy")
		}
	}
	objectID, hasObjectID := input["object_id"].(string)
	if hasObjectID && contextValue["object_id"] != objectID {
		return effect{}, reject("precondition_failed", "function input object does not match work context")
	}
	binding, _ := functionSpec["binding"].(map[string]any)
	kind, _ := binding["kind"].(string)
	if kind == "composite" {
		workflowID, _ := binding["workflow_revision_id"].(string)
		if _, err := loadSpec(ctx, tx, c.WorkspaceID, workflowID, "workflow"); err != nil {
			return effect{}, err
		}
		invocationID := aggregateID("inv", c.CommandID)
		payload := map[string]any{"function_revision_id": functionID, "workflow_revision_id": workflowID, "input": input, "status": "started"}
		eff := makeEffect(streamID, "work_stream", workVersion+1, "workflow.started", payload)
		eff.journal = []journalFact{
			{streamID: streamID, kind: "function.requested", specRevision: functionID, participant: actor, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "input": input, "binding_kind": kind}},
			{streamID: streamID, kind: "workflow.started", specRevision: workflowID, participant: actor, correlationID: invocationID, payload: payload},
		}
		return eff, nil
	}
	if kind == "connector" || kind == "native" {
		return requestExternalFunction(streamID, workVersion, taskID, functionID, kind, binding, input, contextValue, actor, c)
	}
	expectedVersion, ok := integer(input["expected_version"])
	if strings.TrimSpace(objectID) == "" || !ok || expectedVersion < 1 {
		return effect{}, reject("schema_mismatch", "kernel function input requires object_id and positive expected_version")
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
				return effect{}, reject("schema_mismatch", "kernel object.update input.title must be a non-empty string")
			}
			innerPayload["title"] = value
		}
		if values, exists := input["values"]; exists {
			value, ok := values.(map[string]any)
			if !ok {
				return effect{}, reject("schema_mismatch", "kernel object.update input.values must be an object")
			}
			innerPayload["values"] = value
		}
		if len(innerPayload) == 0 {
			return effect{}, reject("schema_mismatch", "kernel object.update input requires title or values")
		}
	default:
		return effect{}, reject("schema_mismatch", "unsupported kernel function binding")
	}
	innerRaw, _ := json.Marshal(innerPayload)
	inner := Command{WorkspaceID: c.WorkspaceID, CommandType: commandType, Target: Target{AggregateType: "object_instance", AggregateID: &objectID}, Preconditions: []Precondition{{AggregateType: "object_instance", AggregateID: objectID, ExpectedVersion: expectedVersion}}, Payload: innerRaw}
	if err := checkPreconditions(ctx, tx, inner); err != nil {
		return effect{}, err
	}
	var worldEffect effect
	if commandType == "object.move" {
		worldEffect, err = moveObject(ctx, tx, principal, inner, innerPayload, now)
	} else {
		worldEffect, err = updateObject(ctx, tx, principal, inner, innerPayload, now)
	}
	if err != nil {
		return effect{}, err
	}
	invocationID := aggregateID("inv", c.CommandID)
	functionOutput := make(map[string]any, len(worldEffect.payload))
	for key, value := range worldEffect.payload {
		functionOutput[key] = value
	}
	if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
		if err := validateValues(functionOutput, outputSchema, "function output"); err != nil {
			return effect{}, err
		}
	}
	facts := []journalFact{
		{streamID: streamID, kind: "function.requested", specRevision: functionID, participant: actor, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "input": input}},
		{streamID: streamID, kind: "function.succeeded", specRevision: functionID, participant: actor, correlationID: invocationID, payload: map[string]any{"function_revision_id": functionID, "output": functionOutput}},
	}
	eventFacts, err := functionEventFacts(ctx, tx, c.WorkspaceID, streamID, invocationID, objectID, actor, functionSpec, functionOutput)
	if err != nil {
		return effect{}, err
	}
	facts = append(facts, eventFacts...)
	if c.Causation.Source != "workflow" {
		satisfied, err := taskSatisfied(ctx, tx, c.WorkspaceID, streamID, taskID, objectID)
		if err != nil {
			return effect{}, err
		}
		if satisfied {
			facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: invocationID, payload: map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "object_id": objectID, "object_version": worldEffect.aggregate.Version}})
		}
	}
	worldEffect.payload["work_stream_id"] = streamID
	worldEffect.payload["function_revision_id"] = functionID
	worldEffect.payload["invocation_id"] = invocationID
	worldEffect.journal = facts
	return worldEffect, nil
}

func completeWorkflow(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any) (effect, error) {
	if c.Causation.Source != "workflow" {
		return effect{}, reject("forbidden", "workflow completion requires workflow causation")
	}
	streamID := *c.Target.AggregateID
	invocationID, _ := p["invocation_id"].(string)
	status, _ := p["runtime_status"].(string)
	output, _ := p["output"].(map[string]any)
	if strings.TrimSpace(invocationID) == "" || output == nil {
		return effect{}, reject("schema_mismatch", "workflow completion requires invocation_id and output")
	}
	var functionID, workflowID, participantKind, participantID string
	err := tx.QueryRowContext(ctx, `SELECT f.spec_revision_id,w.spec_revision_id,COALESCE(f.participant_kind,''),COALESCE(f.participant_id,'') FROM journal_records f JOIN journal_records w ON w.workspace_id=f.workspace_id AND w.stream_id=f.stream_id AND w.correlation_id=f.correlation_id AND w.kind='workflow.started' WHERE f.workspace_id=? AND f.stream_id=? AND f.correlation_id=? AND f.kind='function.requested'`, c.WorkspaceID, streamID, invocationID).Scan(&functionID, &workflowID, &participantKind, &participantID)
	if err != nil {
		return effect{}, reject("precondition_failed", "workflow invocation does not exist")
	}
	var terminal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('workflow.completed','workflow.failed')`, c.WorkspaceID, streamID, invocationID).Scan(&terminal); err != nil {
		return effect{}, err
	}
	if terminal != 0 {
		return effect{}, reject("precondition_failed", "workflow invocation is already terminal")
	}
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	actor := &Actor{Type: participantKind, ID: participantID}
	taskID, contextValue, err := workTask(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	objectID, _ := contextValue["object_id"].(string)
	var eventFacts []journalFact
	if status == "succeeded" {
		functionSpec, err := loadSpec(ctx, tx, c.WorkspaceID, functionID, "function")
		if err != nil {
			return effect{}, err
		}
		if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
			if err := validateValues(output, outputSchema, "function output"); err != nil {
				status = "failed"
				p["error_detail"] = err.Error()
			}
		}
		if status == "succeeded" {
			eventFacts, err = functionEventFacts(ctx, tx, c.WorkspaceID, streamID, invocationID, objectID, actor, functionSpec, output)
			if err != nil {
				status = "failed"
				p["error_detail"] = err.Error()
				eventFacts = nil
			}
		}
	}
	workflowKind, functionKind := "workflow.completed", "function.succeeded"
	if status != "succeeded" {
		workflowKind, functionKind = "workflow.failed", "function.failed"
	}
	payload := map[string]any{"workflow_revision_id": workflowID, "function_revision_id": functionID, "invocation_id": invocationID, "runtime_status": status, "output": output}
	if detail, _ := p["error_detail"].(string); detail != "" {
		payload["error_detail"] = detail
	}
	facts := []journalFact{
		{streamID: streamID, kind: workflowKind, specRevision: workflowID, participant: actor, correlationID: invocationID, payload: payload},
		{streamID: streamID, kind: functionKind, specRevision: functionID, participant: actor, correlationID: invocationID, payload: payload},
	}
	facts = append(facts, eventFacts...)
	if status != "succeeded" {
		policyFacts, err := functionFailurePolicyFacts(ctx, tx, c.WorkspaceID, streamID, taskID, invocationID, c.CommandID)
		if err != nil {
			return effect{}, err
		}
		facts = append(facts, policyFacts...)
	}
	if status == "succeeded" {
		var resolved int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, c.WorkspaceID, streamID).Scan(&resolved); err != nil {
			return effect{}, err
		}
		if resolved == 0 {
			satisfied, err := taskSatisfied(ctx, tx, c.WorkspaceID, streamID, taskID, objectID)
			if err != nil {
				return effect{}, err
			}
			if satisfied {
				proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID}
				if objectID != "" {
					var objectVersion int64
					if err := tx.QueryRowContext(ctx, `SELECT version FROM object_instances WHERE workspace_id=? AND id=?`, c.WorkspaceID, objectID).Scan(&objectVersion); err != nil {
						return effect{}, err
					}
					proof["object_id"], proof["object_version"] = objectID, objectVersion
				}
				facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: invocationID, payload: proof})
			}
		}
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	eff := makeEffect(streamID, "work_stream", version+int64(len(facts))-1, workflowKind, payload)
	eff.journal = facts
	return eff, nil
}

func requestExternalFunction(streamID string, workVersion int64, taskID, functionID, kind string, binding, input, contextValue map[string]any, actor *Actor, c Command) (effect, error) {
	invocationID := aggregateID("inv", c.CommandID)
	handlerKey := "connector.invoke"
	if kind == "native" {
		handlerKey, _ = binding["handler_key"].(string)
	}
	handlerInput := make(map[string]any, len(input)+2)
	for key, value := range input {
		handlerInput[key] = value
	}
	if kind == "connector" {
		handlerInput["provider"] = binding["provider"]
		handlerInput["operation"] = binding["operation"]
	}
	operation, _ := binding["operation"].(string)
	compos := map[string]any{
		"workspace_id": c.WorkspaceID, "execution_request_id": invocationID,
		"handler_key": handlerKey, "handler_version": binding["handler_version"],
		"operation": operation, "workflow_instance_id": streamID, "task_id": streamID,
		"actor": map[string]any{"type": conductorActorType(actor.Type), "id": actor.ID},
	}
	for _, name := range []string{"connection_id", "credential_revision", "timeout_seconds", "retry_policy"} {
		if value, exists := binding[name]; exists {
			compos[name] = value
		}
	}
	data := make(map[string]any, len(handlerInput)+2)
	for key, value := range handlerInput {
		data[key] = value
	}
	data["_compos"] = compos
	data["subject_ref"] = contextValue
	payload := map[string]any{
		"work_stream_id": streamID, "task_revision_id": taskID,
		"function_revision_id": functionID, "invocation_id": invocationID,
		"binding_kind": kind, "status": "requested",
	}
	eff := makeEffect(streamID, "work_stream", workVersion, "function.requested", payload)
	journalPayload := map[string]any{"function_revision_id": functionID, "input": input, "binding_kind": kind, "handler_key": handlerKey, "handler_version": binding["handler_version"]}
	for _, name := range []string{"connection_id", "credential_revision"} {
		if value, exists := binding[name]; exists {
			journalPayload[name] = value
		}
	}
	eff.journal = []journalFact{{streamID: streamID, kind: "function.requested", specRevision: functionID, participant: actor, correlationID: invocationID, payload: journalPayload}}
	eff.outboxEvent = handlerKey
	eff.outboxPayload = data
	return eff, nil
}

func conductorActorType(kind string) string {
	if kind == "human" {
		return "user"
	}
	return kind
}

func externalResultDigest(runtimeStatus string, output map[string]any, errorClass, errorDetail string) string {
	return digestJSON([]byte(canonical(map[string]any{"runtime_status": runtimeStatus, "output": output, "error_class": errorClass, "error_detail": errorDetail})))
}

func recordFunctionResult(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	workVersion, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	invocationID, _ := p["invocation_id"].(string)
	callbackID, _ := p["callback_id"].(string)
	adapterEventID, _ := p["adapter_inbound_event_id"].(string)
	executionID, _ := p["execution_id"].(string)
	providerKey, _ := p["provider_key"].(string)
	providerRunRef, _ := p["provider_run_ref"].(string)
	handlerVersion, _ := p["handler_version"].(string)
	attemptCount, attemptOK := integer(p["attempt_count"])
	status, _ := p["runtime_status"].(string)
	if strings.TrimSpace(invocationID) == "" || strings.TrimSpace(callbackID) == "" || adapterEventID != callbackID || strings.TrimSpace(executionID) == "" || strings.TrimSpace(providerKey) == "" || strings.TrimSpace(providerRunRef) == "" || strings.TrimSpace(handlerVersion) == "" || !attemptOK || attemptCount < 1 {
		return effect{}, reject("schema_mismatch", "external result requires bound callback, execution, provider, and positive attempt")
	}
	var functionID, participantKind, participantID, requestCausation, handlerKey, pinnedHandlerVersion string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(j.spec_revision_id,''),COALESCE(j.participant_kind,''),COALESCE(j.participant_id,''),COALESCE(json_extract(c.causation_json,'$.source'),''),COALESCE(json_extract(j.payload_json,'$.handler_key'),''),COALESCE(json_extract(j.payload_json,'$.handler_version'),'') FROM journal_records j JOIN command_ledger c ON c.workspace_id=j.workspace_id AND c.command_id=j.command_id WHERE j.workspace_id=? AND j.stream_id=? AND j.correlation_id=? AND j.kind='function.requested' ORDER BY j.stream_sequence LIMIT 1`, c.WorkspaceID, streamID, invocationID).Scan(&functionID, &participantKind, &participantID, &requestCausation, &handlerKey, &pinnedHandlerVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return effect{}, reject("precondition_failed", "function invocation does not exist")
	}
	if err != nil {
		return effect{}, err
	}
	if providerKey != handlerKey || handlerVersion != pinnedHandlerVersion {
		return effect{}, reject("precondition_failed", "external result does not match the pinned handler identity")
	}
	var terminal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('function.succeeded','function.failed')`, c.WorkspaceID, streamID, invocationID).Scan(&terminal); err != nil {
		return effect{}, err
	}
	if terminal != 0 {
		return effect{}, reject("precondition_failed", "function invocation is already terminal")
	}
	output, _ := p["output"].(map[string]any)
	if output == nil {
		output = map[string]any{}
	}
	errorClass, _ := p["error_class"].(string)
	errorDetail, _ := p["error_detail"].(string)
	resultDigest := externalResultDigest(status, output, errorClass, errorDetail)
	var taskID, objectID string
	var functionSpec map[string]any
	if status == "succeeded" {
		var contextValue map[string]any
		taskID, contextValue, err = workTask(ctx, tx, c.WorkspaceID, streamID)
		if err != nil {
			return effect{}, err
		}
		objectID, _ = contextValue["object_id"].(string)
		functionSpec, err = loadSpec(ctx, tx, c.WorkspaceID, functionID, "function")
		if err != nil {
			return effect{}, err
		}
		if outputSchema, ok := functionSpec["output_schema"].(map[string]any); ok {
			if err := validateValues(output, outputSchema, "payload.output"); err != nil {
				status, errorClass, errorDetail = "failed", "output_schema_mismatch", err.Error()
			}
		}
	}
	participant := &Actor{Type: participantKind, ID: participantID}
	var eventFacts []journalFact
	if status == "succeeded" {
		eventFacts, err = functionEventFacts(ctx, tx, c.WorkspaceID, streamID, invocationID, objectID, participant, functionSpec, output)
		if err != nil {
			status, errorClass, errorDetail = "failed", "event_schema_mismatch", err.Error()
			eventFacts = nil
		}
	}
	kind := "function.succeeded"
	payload := map[string]any{"function_revision_id": functionID, "invocation_id": invocationID, "callback_id": callbackID, "runtime_status": status, "output": output, "execution_id": executionID, "provider_key": providerKey, "provider_run_ref": providerRunRef, "handler_version": handlerVersion, "attempt_count": attemptCount, "result_digest": resultDigest}
	if status != "succeeded" {
		kind = "function.failed"
		payload["error_class"], payload["error_detail"] = errorClass, errorDetail
	}
	facts := []journalFact{{streamID: streamID, kind: kind, specRevision: functionID, participant: participant, correlationID: invocationID, causationID: callbackID, payload: payload}}
	facts = append(facts, eventFacts...)
	if status != "succeeded" {
		if failedTaskID, _, taskErr := workTask(ctx, tx, c.WorkspaceID, streamID); taskErr == nil {
			policyFacts, err := functionFailurePolicyFacts(ctx, tx, c.WorkspaceID, streamID, failedTaskID, invocationID, callbackID)
			if err != nil {
				return effect{}, err
			}
			facts = append(facts, policyFacts...)
		}
	}
	if status == "succeeded" && requestCausation != "workflow" {
		var resolved int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, c.WorkspaceID, streamID).Scan(&resolved); err != nil {
			return effect{}, err
		}
		if resolved == 0 {
			satisfied, err := taskSatisfied(ctx, tx, c.WorkspaceID, streamID, taskID, objectID)
			if err != nil {
				return effect{}, err
			}
			if satisfied {
				proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID}
				if objectID != "" {
					var objectVersion int64
					if err := tx.QueryRowContext(ctx, `SELECT version FROM object_instances WHERE workspace_id=? AND id=?`, c.WorkspaceID, objectID).Scan(&objectVersion); err != nil {
						return effect{}, err
					}
					proof["object_id"], proof["object_version"] = objectID, objectVersion
				}
				facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, causationID: callbackID, payload: proof})
			}
		}
	}
	eff := makeEffect(streamID, "work_stream", workVersion+int64(len(facts))-1, kind, payload)
	eff.journal = facts
	updated, err := tx.ExecContext(ctx, `UPDATE adapter_inbound_events SET status='reduced' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, c.WorkspaceID, adapterEventID)
	if err != nil {
		return effect{}, err
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		return effect{}, reject("precondition_failed", "external result requires appended Conductor callback evidence")
	}
	return eff, nil
}

func recordAgentResult(ctx context.Context, tx store.Tx, c Command, p map[string]any) (effect, error) {
	streamID := *c.Target.AggregateID
	if err := ensureWorkOpen(ctx, tx, c.WorkspaceID, streamID); err != nil {
		return effect{}, err
	}
	version, err := nextWorkVersion(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	invocationID, _ := p["invocation_id"].(string)
	callbackID, _ := p["callback_id"].(string)
	adapterEventID, _ := p["adapter_inbound_event_id"].(string)
	executionID, _ := p["execution_id"].(string)
	providerKey, _ := p["provider_key"].(string)
	providerRunRef, _ := p["provider_run_ref"].(string)
	handlerVersion, _ := p["handler_version"].(string)
	attemptCount, attemptOK := integer(p["attempt_count"])
	status, _ := p["runtime_status"].(string)
	if strings.TrimSpace(invocationID) == "" || strings.TrimSpace(callbackID) == "" || adapterEventID != callbackID || strings.TrimSpace(executionID) == "" || strings.TrimSpace(providerKey) == "" || strings.TrimSpace(providerRunRef) == "" || strings.TrimSpace(handlerVersion) == "" || !attemptOK || attemptCount < 1 {
		return effect{}, reject("schema_mismatch", "external result requires bound callback, execution, provider, and positive attempt")
	}
	var agentID, participantKind, participantID, handlerKey, pinnedHandlerVersion, workflowStepID, workflowRouteID string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(spec_revision_id,''),COALESCE(participant_kind,''),COALESCE(participant_id,''),COALESCE(json_extract(payload_json,'$.handler_key'),''),COALESCE(json_extract(payload_json,'$.handler_version'),''),COALESCE(json_extract(payload_json,'$.workflow_step_id'),''),COALESCE(json_extract(payload_json,'$.workflow_route_id'),'') FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind='agent.requested' ORDER BY stream_sequence LIMIT 1`, c.WorkspaceID, streamID, invocationID).Scan(&agentID, &participantKind, &participantID, &handlerKey, &pinnedHandlerVersion, &workflowStepID, &workflowRouteID)
	if errors.Is(err, sql.ErrNoRows) {
		return effect{}, reject("precondition_failed", "agent invocation does not exist")
	}
	if err != nil {
		return effect{}, err
	}
	if providerKey != handlerKey || handlerVersion != pinnedHandlerVersion {
		return effect{}, reject("precondition_failed", "external result does not match the pinned handler identity")
	}
	var terminal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND correlation_id=? AND kind IN ('agent.succeeded','agent.failed')`, c.WorkspaceID, streamID, invocationID).Scan(&terminal); err != nil {
		return effect{}, err
	}
	if terminal != 0 {
		return effect{}, reject("precondition_failed", "agent invocation is already terminal")
	}
	output, _ := p["output"].(map[string]any)
	if output == nil {
		output = map[string]any{}
	}
	errorClass, _ := p["error_class"].(string)
	errorDetail, _ := p["error_detail"].(string)
	resultDigest := externalResultDigest(status, output, errorClass, errorDetail)
	taskID, contextValue, err := workTask(ctx, tx, c.WorkspaceID, streamID)
	if err != nil {
		return effect{}, err
	}
	task, err := loadSpec(ctx, tx, c.WorkspaceID, taskID, "task")
	if err != nil {
		return effect{}, err
	}
	if status == "succeeded" {
		outputSchema, err := workflowSubmissionSchema(ctx, tx, c.WorkspaceID, streamID, task, workflowStepID, workflowRouteID)
		if err != nil {
			return effect{}, err
		}
		if err := validateValues(output, outputSchema, "payload.output"); err != nil {
			status, errorClass, errorDetail = "failed", "output_schema_mismatch", err.Error()
		}
	}
	participant := &Actor{Type: participantKind, ID: participantID}
	kind := "agent.succeeded"
	payload := map[string]any{"agent_revision_id": agentID, "invocation_id": invocationID, "callback_id": callbackID, "runtime_status": status, "output": output, "execution_id": executionID, "provider_key": providerKey, "provider_run_ref": providerRunRef, "handler_version": handlerVersion, "attempt_count": attemptCount, "result_digest": resultDigest}
	facts := []journalFact{}
	if status == "succeeded" {
		submissionPayload := map[string]any{"submission": output}
		if workflowStepID != "" {
			submissionPayload["workflow_step_id"] = workflowStepID
			submissionPayload["workflow_route_id"] = workflowRouteID
		}
		facts = append(facts,
			journalFact{streamID: streamID, kind: kind, specRevision: agentID, participant: participant, correlationID: invocationID, causationID: callbackID, payload: payload},
			journalFact{streamID: streamID, kind: "work.submitted", specRevision: taskID, participant: participant, correlationID: invocationID, causationID: callbackID, payload: submissionPayload},
		)
		if task["completion"].(map[string]any)["kind"] == "submission_valid" && workflowStepID == "" {
			proof := map[string]any{"task_revision_id": taskID, "proof_command_id": c.CommandID, "submission_sequence": version + 1}
			if objectID, _ := contextValue["object_id"].(string); objectID != "" {
				proof["object_id"] = objectID
			}
			facts = append(facts, journalFact{streamID: streamID, kind: "work.satisfied", specRevision: taskID, participant: participant, correlationID: invocationID, causationID: callbackID, payload: proof})
		}
	} else {
		kind = "agent.failed"
		payload["error_class"], payload["error_detail"] = errorClass, errorDetail
		facts = append(facts, journalFact{streamID: streamID, kind: kind, specRevision: agentID, participant: participant, correlationID: invocationID, causationID: callbackID, payload: payload})
		policyFacts, err := failurePolicyFacts(ctx, tx, c.WorkspaceID, streamID, taskID, "on_agent_failed", invocationID, callbackID)
		if err != nil {
			return effect{}, err
		}
		facts = append(facts, policyFacts...)
	}
	eff := makeEffect(streamID, "work_stream", version+int64(len(facts))-1, kind, payload)
	eff.journal = facts
	updated, err := tx.ExecContext(ctx, `UPDATE adapter_inbound_events SET status='reduced' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, c.WorkspaceID, adapterEventID)
	if err != nil {
		return effect{}, err
	}
	if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
		return effect{}, reject("precondition_failed", "external result requires appended Conductor callback evidence")
	}
	return eff, nil
}

func functionFailurePolicyFacts(ctx context.Context, tx store.Tx, workspace, streamID, taskID, invocationID, causationID string) ([]journalFact, error) {
	return failurePolicyFacts(ctx, tx, workspace, streamID, taskID, "on_function_failed", invocationID, causationID)
}

func failurePolicyFacts(ctx context.Context, tx store.Tx, workspace, streamID, taskID, policyName, invocationID, causationID string) ([]journalFact, error) {
	var resolved int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, workspace, streamID).Scan(&resolved); err != nil || resolved != 0 {
		return nil, err
	}
	taskSpec, err := loadSpec(ctx, tx, workspace, taskID, "task")
	if err != nil {
		return nil, err
	}
	policy, _ := taskSpec["failure_policy"].(map[string]any)
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

func loadSpec(ctx context.Context, tx store.Tx, workspace, id, kind string) (map[string]any, error) {
	var payload string
	if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM spec_revisions WHERE workspace_id=? AND id=? AND kind=?`, workspace, id, kind).Scan(&payload); err != nil {
		return nil, reject("precondition_failed", kind+" spec revision does not exist")
	}
	var spec map[string]any
	if err := json.Unmarshal([]byte(payload), &spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func workflowSubmissionSchema(ctx context.Context, tx store.Tx, workspace, stream string, task map[string]any, stepID, routeID string) (map[string]any, error) {
	fallback, _ := task["output_schema"].(map[string]any)
	if routeID == "" {
		return fallback, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT correlation_id,spec_revision_id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='workflow.started' ORDER BY stream_sequence`, workspace, stream)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var invocationID, workflowID string
		if err := rows.Scan(&invocationID, &workflowID); err != nil {
			return nil, err
		}
		workflow, err := loadSpec(ctx, tx, workspace, workflowID, "workflow")
		if err != nil {
			return nil, err
		}
		contract, found := WorkflowRouteContractForSpec(workflow, invocationID, stepID, routeID)
		if !found {
			continue
		}
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

func workTask(ctx context.Context, tx store.Tx, workspace, stream string) (string, map[string]any, error) {
	var taskID, payload string
	err := tx.QueryRowContext(ctx, `SELECT spec_revision_id,payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.opened' ORDER BY stream_sequence LIMIT 1`, workspace, stream).Scan(&taskID, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, reject("precondition_failed", "work stream does not exist")
	}
	if err != nil {
		return "", nil, err
	}
	var opened map[string]any
	if err := json.Unmarshal([]byte(payload), &opened); err != nil {
		return "", nil, err
	}
	contextValue, _ := opened["context"].(map[string]any)
	return taskID, contextValue, nil
}

func nextWorkVersion(ctx context.Context, tx store.Tx, workspace, stream string) (int64, error) {
	var opened, sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(stream_sequence),0) FROM journal_records WHERE workspace_id=? AND stream_id=?`, workspace, stream).Scan(&opened, &sequence); err != nil {
		return 0, err
	}
	if opened == 0 {
		return 0, reject("precondition_failed", "work stream does not exist")
	}
	return sequence + 1, nil
}

func participantFrom(raw any) (*Actor, error) {
	value, ok := raw.(map[string]any)
	if !ok {
		return nil, reject("schema_mismatch", "participant must be an object")
	}
	if err := onlyKeys("participant", value, "kind", "id"); err != nil {
		return nil, err
	}
	kind, _ := value["kind"].(string)
	id, _ := value["id"].(string)
	if !map[string]bool{"human": true, "agent": true, "automation": true}[kind] || strings.TrimSpace(id) == "" {
		return nil, reject("schema_mismatch", "participant requires kind human, agent, or automation and a non-empty id")
	}
	return &Actor{Type: kind, ID: id}, nil
}

func commandParticipant(c Command, principal string) *Actor {
	if c.RequestedActor == nil {
		return &Actor{Type: "human", ID: principal}
	}
	switch c.RequestedActor.Type {
	case "user", "human":
		return &Actor{Type: "human", ID: c.RequestedActor.ID}
	case "automation":
		return &Actor{Type: "automation", ID: c.RequestedActor.ID}
	default:
		return &Actor{Type: "agent", ID: c.RequestedActor.ID}
	}
}

func currentWorkRoute(ctx context.Context, tx store.Tx, workspace, stream string, actor *Actor, requestedStepID, requestedRouteID string, unresolved bool) (string, string, error) {
	var kind, id, stepID, routeID string
	err := tx.QueryRowContext(ctx, `
		SELECT participant_kind,participant_id,COALESCE(json_extract(payload_json,'$.workflow_step_id'),''),COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')
		FROM journal_records route
		WHERE workspace_id=? AND stream_id=? AND kind='work.routed'
		  AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_step_id'),'')=?)
		  AND (?='' OR COALESCE(json_extract(payload_json,'$.workflow_route_id'),'')=?)
		  AND (?=0 OR NOT EXISTS (
		    SELECT 1 FROM journal_records submitted
		    WHERE submitted.workspace_id=route.workspace_id AND submitted.stream_id=route.stream_id
		      AND submitted.kind='work.submitted' AND submitted.stream_sequence>route.stream_sequence
		      AND COALESCE(json_extract(submitted.payload_json,'$.workflow_route_id'),'')=COALESCE(json_extract(route.payload_json,'$.workflow_route_id'),'')))
		ORDER BY stream_sequence DESC LIMIT 1`, workspace, stream, requestedStepID, requestedStepID, requestedRouteID, requestedRouteID, unresolved).Scan(&kind, &id, &stepID, &routeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", reject("precondition_failed", "work stream has no current participant")
	}
	if err != nil {
		return "", "", err
	}
	if actor.Type != kind || actor.ID != id {
		return "", "", reject("forbidden", "requested actor is not the current work participant")
	}
	return stepID, routeID, nil
}

func ensureWorkOpen(ctx context.Context, tx store.Tx, workspace, stream string) error {
	var opened, terminal int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE kind IN ('work.satisfied','work.cancelled')) FROM journal_records WHERE workspace_id=? AND stream_id=?`, workspace, stream).Scan(&opened, &terminal); err != nil {
		return err
	}
	if opened == 0 {
		return reject("precondition_failed", "work stream does not exist")
	}
	if terminal != 0 {
		return reject("precondition_failed", "work stream is already terminal")
	}
	return nil
}

func taskSatisfied(ctx context.Context, tx store.Tx, workspace, streamID, taskID, objectID string) (bool, error) {
	taskSpec, err := loadSpec(ctx, tx, workspace, taskID, "task")
	if err != nil {
		return false, err
	}
	completion, _ := taskSpec["completion"].(map[string]any)
	switch completion["kind"] {
	case "object_state_equals":
		var state sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT state_key FROM object_instances WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, objectID).Scan(&state); err != nil {
			return false, err
		}
		return state.String == completion["state"], nil
	case "submission_valid":
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='work.submitted'`, workspace, streamID).Scan(&count); err != nil {
			return false, err
		}
		return count != 0, nil
	default:
		return false, reject("schema_mismatch", "unsupported task completion policy")
	}
}

func appendJournal(ctx context.Context, tx store.Tx, principal string, c Command, facts []journalFact, ts string) error {
	sequences := map[string]int64{}
	for _, fact := range facts {
		sequence, ok := sequences[fact.streamID]
		if !ok {
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(stream_sequence),0) FROM journal_records WHERE workspace_id=? AND stream_id=?`, c.WorkspaceID, fact.streamID).Scan(&sequence); err != nil {
				return err
			}
		}
		sequence++
		sequences[fact.streamID] = sequence
		var participantKind, participantID any
		if fact.participant != nil {
			participantKind, participantID = fact.participant.Type, fact.participant.ID
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO journal_records(id,workspace_id,stream_id,stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,causation_id,command_id,payload_json,created_at) VALUES(?,?,?,?,?,NULLIF(?,''),?,?,?,?,NULLIF(?,''),?,?,?)`, newID("jnl"), c.WorkspaceID, fact.streamID, sequence, fact.kind, fact.specRevision, participantKind, participantID, principal, nullableDB(fact.correlationID), fact.causationID, c.CommandID, canonical(fact.payload), ts)
		if err != nil {
			return err
		}
	}
	return nil
}

func nullableDB(value string) any {
	if value == "" {
		return nil
	}
	return value
}
