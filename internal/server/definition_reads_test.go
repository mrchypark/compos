package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
)

func TestDefinitionReadsListFilterAndImmutableDetail(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	token := definitionReadAuthToken(t, db, handler)
	task := execute("cmd_definition_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "quote.review", "revision": 1, "kind": "task", "spec": workflowTaskSpec("Review quote", "Review", "done")})
	execute("cmd_definition_task_v2", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "quote.review", "revision": 2, "kind": "task", "spec": workflowTaskSpec("Review quote v2", "Review", "done")})
	execute("cmd_definition_function", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "quote.calculate", "revision": 1, "kind": "function", "spec": map[string]any{"name": "Calculate", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "binding": map[string]any{"kind": "native", "handler_key": "quote.calculate", "handler_version": "1"}}})
	execute("cmd_definition_workflow", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "quote.flow", "revision": 1, "kind": "workflow", "spec": map[string]any{"name": "Quote flow", "steps": []any{map[string]any{"id": "review", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}}})

	list := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/definitions?kind=task&family_id=quote.review&limit=1", token, nil)
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"revision":2`)) || !bytes.Contains(list.Body.Bytes(), []byte(`"next_cursor":1`)) {
		t.Fatalf("definition list status=%d body=%s", list.Code, list.Body.String())
	}
	if bytes.Contains(list.Body.Bytes(), []byte(`quote.calculate`)) {
		t.Fatalf("kind filter leaked other definitions: %s", list.Body.String())
	}
	detail := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/definitions/"+task.Aggregate.ID, token, nil)
	if detail.Code != http.StatusOK || !bytes.Contains(detail.Body.Bytes(), []byte(`"id":"`+task.Aggregate.ID+`"`)) || !bytes.Contains(detail.Body.Bytes(), []byte(`"spec"`)) {
		t.Fatalf("definition detail status=%d body=%s", detail.Code, detail.Body.String())
	}
	invalid := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/definitions?kind=ontology", token, nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid kind status=%d body=%s", invalid.Code, invalid.Body.String())
	}
}

func TestObjectRelationReadsHonorDirectionDefinitionAndWorkspace(t *testing.T) {
	db, handler, execute := newWorkflowTestRuntime(t)
	token := definitionReadAuthToken(t, db, handler)
	fromType := execute("cmd_relation_from_type", "object_type.define", "object_type", nil, map[string]any{"key": "quotes", "singular": "Quote", "plural": "Quotes", "field_schema": map[string]any{"type": "object"}})
	toType := execute("cmd_relation_to_type", "object_type.define", "object_type", nil, map[string]any{"key": "customers", "singular": "Customer", "plural": "Customers", "field_schema": map[string]any{"type": "object"}})
	relationCommand := kernel.Command{CommandID: "cmd_relation_definition", IdempotencyKey: "idem_cmd_relation_definition", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: "relation_definition.define", Target: kernel.Target{AggregateType: "relation_definition"}, Preconditions: []kernel.Precondition{{AggregateType: "object_type", AggregateID: fromType.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_type", AggregateID: toType.Aggregate.ID, ExpectedVersion: 1}}, Payload: json.RawMessage(canonical(map[string]any{"key": "quote_customer", "from_object_type_id": fromType.Aggregate.ID, "to_object_type_id": toType.Aggregate.ID, "cardinality": "many_to_one", "required": false})), Causation: kernel.Causation{Source: "api"}}
	relation, err := handler.kernel.Execute(context.Background(), "dev", relationCommand)
	if err != nil {
		t.Fatal(err)
	}
	quote := execute("cmd_relation_quote", "object.create", "object_instance", nil, map[string]any{"object_type_key": "quotes", "title": "Q-1", "values": map[string]any{}})
	customer := execute("cmd_relation_customer", "object.create", "object_instance", nil, map[string]any{"object_type_key": "customers", "title": "Customer A", "values": map[string]any{}})
	linkCommand := kernel.Command{CommandID: "cmd_relation_link", IdempotencyKey: "idem_cmd_relation_link", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: "relation.link", Target: kernel.Target{AggregateType: "object_relation"}, Preconditions: []kernel.Precondition{{AggregateType: "relation_definition", AggregateID: relation.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_instance", AggregateID: quote.Aggregate.ID, ExpectedVersion: 1}, {AggregateType: "object_instance", AggregateID: customer.Aggregate.ID, ExpectedVersion: 1}}, Payload: json.RawMessage(canonical(map[string]any{"relation_definition_id": relation.Aggregate.ID, "from_object_id": quote.Aggregate.ID, "to_object_id": customer.Aggregate.ID})), Causation: kernel.Causation{Source: "api"}}
	if _, err := handler.kernel.Execute(context.Background(), "dev", linkCommand); err != nil {
		t.Fatal(err)
	}

	outgoing := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/objects/"+quote.Aggregate.ID+"/relations?direction=outgoing&relation_definition_id="+relation.Aggregate.ID, token, nil)
	if outgoing.Code != http.StatusOK || !bytes.Contains(outgoing.Body.Bytes(), []byte(`"direction":"outgoing"`)) || !bytes.Contains(outgoing.Body.Bytes(), []byte(`"id":"`+customer.Aggregate.ID+`"`)) {
		t.Fatalf("outgoing relation status=%d body=%s", outgoing.Code, outgoing.Body.String())
	}
	incoming := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/objects/"+customer.Aggregate.ID+"/relations?direction=incoming", token, nil)
	if incoming.Code != http.StatusOK || !bytes.Contains(incoming.Body.Bytes(), []byte(`"direction":"incoming"`)) || !bytes.Contains(incoming.Body.Bytes(), []byte(`"id":"`+quote.Aggregate.ID+`"`)) {
		t.Fatalf("incoming relation status=%d body=%s", incoming.Code, incoming.Body.String())
	}
	missing := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/objects/missing/relations", token, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing object status=%d body=%s", missing.Code, missing.Body.String())
	}
	if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_other','other','Other',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	crossWorkspace := authRequest(t, handler, http.MethodGet, "/api/v1/workspaces/wks_other/objects/"+quote.Aggregate.ID+"/relations", token, nil)
	if crossWorkspace.Code != http.StatusNotFound {
		t.Fatalf("cross-workspace relation read status=%d body=%s", crossWorkspace.Code, crossWorkspace.Body.String())
	}
}

func definitionReadAuthToken(t *testing.T, db *store.Store, handler *Server) string {
	t.Helper()
	return ""
}
