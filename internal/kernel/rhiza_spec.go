package kernel

import (
	"context"
	"strings"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (e *Executor) validateSpecReferencesRhiza(ctx context.Context, workspace, kind string, spec map[string]any, statements *[]rhiza.SQLStatement) error {
	if kind == "task" {
		guards, err := validateDataBindingReferences(func(query string, args ...any) store.Row {
			return e.store.ReadRow(ctx, store.ReadLinearizable, query, args...)
		}, workspace, spec)
		if err != nil {
			return err
		}
		*statements = append(*statements, guards...)
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
		for _, raw := range anySlice(spec["emits"]) {
			refs[raw.(string)] = "event"
		}
	case "participant":
		for _, raw := range anySlice(spec["tools"]) {
			refs[raw.(string)] = "function"
		}
	}
	one := int64(1)
	for id, expected := range refs {
		var actual, payload string
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT kind,payload_json FROM spec_revisions WHERE workspace_id=? AND id=?`, workspace, id).Scan(&actual, &payload); err != nil || actual != expected {
			return reject("precondition_failed", expected+" spec revision "+id+" does not exist")
		}
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM spec_revisions WHERE workspace_id=? AND id=? AND kind=? AND payload_json=?`, Args: []any{workspace, id, actual, payload}, WantRows: true, ExpectedReturnedRows: &one})
	}
	return nil
}

func (e *Executor) validateExecutionGraphAcyclicRhiza(ctx context.Context, workspace, kind string, spec map[string]any, statements *[]rhiza.SQLStatement) error {
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
			child, err := e.loadSpecRhiza(ctx, workspace, ref, refKind, statements)
			if err != nil {
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

func (e *Executor) publishSpecRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	family, _ := payload["family_id"].(string)
	kind, _ := payload["kind"].(string)
	revision, ok := integer(payload["revision"])
	spec, _ := payload["spec"].(map[string]any)
	if strings.TrimSpace(family) == "" || !ok || revision < 1 || spec == nil {
		return Receipt{}, reject("validation_failed", "family_id, positive revision, kind, and spec are required")
	}
	if err := validateSpec(kind, spec); err != nil {
		return Receipt{}, err
	}
	if err := e.validateSpecReferencesRhiza(ctx, c.WorkspaceID, kind, spec, &statements); err != nil {
		return Receipt{}, err
	}
	if err := e.validateExecutionGraphAcyclicRhiza(ctx, c.WorkspaceID, kind, spec, &statements); err != nil {
		return Receipt{}, err
	}
	id, ts, one := aggregateID("spc", c.CommandID), timestamp(e.now().UTC()), int64(1)
	specJSON := canonical(spec)
	digest := digestJSON([]byte(canonical(map[string]any{"family_id": family, "revision": revision, "kind": kind, "spec": spec})))
	statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO spec_revisions(id,workspace_id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id) VALUES(?,?,?,?,?,?,?,?,?)`, Args: []any{id, c.WorkspaceID, family, revision, kind, specJSON, digest, ts, principal}, ExpectedRowsAffected: &one})
	effect := makeEffect(id, "spec_revision", revision, "spec.published", map[string]any{"id": id, "family_id": family, "revision": revision, "kind": kind, "digest": digest})
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}
