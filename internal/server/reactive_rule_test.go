package server

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestReactiveRuleEmitsOneKernelCommandAndRecoversByEvaluation(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	execute("cmd_automation_type", "object_type.define", "object_type", nil, map[string]any{"key": "notes", "singular": "Note", "plural": "Notes", "field_schema": map[string]any{"type": "object", "additionalProperties": false}})
	if _, err := s.DB().Exec(`INSERT INTO actor_capabilities(id,workspace_id,principal_id,actor_type,actor_id,command_type,causation_source,relation_definition_id,idempotency_key_prefix,max_automation_recursion_depth,created_at,updated_at) VALUES('cap_auto','wks_1','dev','automation','auto-followup','object.create','automation','','automation:auto-followup:',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	rule := execute("cmd_reactive_rule", "reactive_rule.define", "reactive_rule", nil, map[string]any{
		"automation_id": "auto-followup", "rule_key": "note-followup", "name": "Create one follow-up", "requested_actor": map[string]any{"type": "automation", "id": "auto-followup"},
		"trigger":     map[string]any{"source": "kernel_canonical_event", "event_type": "object.created", "event_schema_version": 1, "aggregate_type": "object_instance", "allowed_causation_sources": []any{"api", "automation"}, "excluded_automation_ids": []any{"self"}},
		"predicates":  map[string]any{"===": []any{map[string]any{"var": "payload.title"}, "Seed"}},
		"action":      map[string]any{"action_id": "create-followup", "kind": "emit_command", "command_type": "object.create", "target_template": map[string]any{"aggregate_type": "object_instance"}, "payload_template": map[string]any{"object_type_key": "notes", "title": "Follow-up", "values": map[string]any{}}, "preconditions_template": []any{}},
		"idempotency": map[string]any{}, "limits": map[string]any{"max_recursion_depth": 1, "max_emits_per_minute": 10, "max_emits_per_hour": 100, "max_pending_dispatches": 10, "failure_threshold": 3},
	})
	var revisionID string
	if err := s.DB().QueryRow(`SELECT id FROM reactive_rule_revisions WHERE rule_id=?`, rule.Aggregate.ID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	var cursor int64
	if err := s.DB().QueryRow(`SELECT MAX(projection_sequence) FROM projection_events WHERE workspace_id='wks_1'`).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	ruleID := rule.Aggregate.ID
	publishPayload, err := json.Marshal(map[string]any{"revision_id": revisionID, "activation_event_cursor": strconv.FormatInt(cursor, 10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.kernel.Execute(ctx, "dev", kernel.Command{
		CommandID: "cmd_automation_publish", IdempotencyKey: "idem_cmd_automation_publish", WorkspaceID: "wks_1",
		RequestedActor: &kernel.Actor{Type: "user", ID: "dev"}, CommandType: "reactive_rule.publish_revision",
		Target:        kernel.Target{AggregateType: "reactive_rule", AggregateID: &ruleID},
		Preconditions: []kernel.Precondition{{AggregateType: "reactive_rule", AggregateID: ruleID, ExpectedVersion: 1}},
		Payload:       publishPayload, Causation: kernel.Causation{Source: "api"},
	}); err != nil {
		t.Fatal(err)
	}
	execute("cmd_automation_seed", "object.create", "object_instance", nil, map[string]any{"object_type_key": "notes", "title": "Seed", "values": map[string]any{}})
	handler.wakeWorkflow()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var objects, applied int
		_ = s.DB().QueryRow(`SELECT COUNT(*) FROM object_instances i JOIN object_types t ON t.id=i.object_type_id WHERE i.workspace_id='wks_1' AND t.key='notes'`).Scan(&objects)
		_ = s.DB().QueryRow(`SELECT COUNT(*) FROM reactive_rule_evaluations WHERE rule_revision_id=? AND predicate_result='matched' AND dispatch_status='applied'`, revisionID).Scan(&applied)
		if objects == 2 && applied == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("objects=%d applied=%d", objects, applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := handler.advanceReactiveRules(ctx); err != nil {
		t.Fatal(err)
	}
	var followups int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM object_instances WHERE workspace_id='wks_1' AND title='Follow-up'`).Scan(&followups); err != nil || followups != 1 {
		t.Fatalf("followups=%d err=%v", followups, err)
	}
	if _, err := s.DB().Exec(`UPDATE reactive_rule_evaluations SET dispatch_status='rejected',dispatch_attempt_count=3 WHERE rule_revision_id=?`, revisionID); err != nil {
		t.Fatal(err)
	}
	if err := handler.openAutomationCircuitIfThreshold(ctx, "wks_1", ruleID, revisionID, 3); err != nil {
		t.Fatal(err)
	}
	var status, circuit string
	if err := s.DB().QueryRow(`SELECT status,circuit_breaker_status FROM reactive_rules WHERE id=?`, ruleID).Scan(&status, &circuit); err != nil || status != "disabled" || circuit != "open" {
		t.Fatalf("status=%s circuit=%s err=%v", status, circuit, err)
	}
	var disabledEvents int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM activities WHERE workspace_id='wks_1' AND event_type='reactive_rule.disabled'`).Scan(&disabledEvents); err != nil || disabledEvents != 1 {
		t.Fatalf("disabled events=%d err=%v", disabledEvents, err)
	}
}

func TestProjectionEventsAndReactiveRulesKeepMultiEffectParentEventsPaired(t *testing.T) {
	ctx := context.Background()
	s, handler, execute := newWorkflowTestRuntime(t)
	execute("cmd_multi_type", "object_type.define", "object_type", nil, map[string]any{"key": "notes", "singular": "Note", "plural": "Notes", "field_schema": map[string]any{"type": "object", "additionalProperties": false}})
	seed := execute("cmd_multi_parent", "object.create", "object_instance", nil, map[string]any{"object_type_key": "notes", "title": "Seed", "values": map[string]any{}})
	if seed.Aggregate == nil || seed.ProjectionSequence == nil {
		t.Fatal("seed receipt is incomplete")
	}
	sequence := *seed.ProjectionSequence
	now := "2026-01-01T00:00:00Z"
	if _, err := s.DB().Exec(`INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, "act_multi_relation", "wks_1", "cmd_multi_parent", "object_relation", "rel_multi", "relation.linked", `{ "id":"rel_multi" }`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, "prj_multi_relation", "wks_1", "cmd_multi_parent", sequence+1, "object_relation", "rel_multi", 1, "sha256:multi", `{ "id":"rel_multi" }`, now); err != nil {
		t.Fatal(err)
	}

	page := func(after int64) []struct {
		ID        string `json:"id"`
		EventType string `json:"event_type"`
	} {
		response := request(t, handler, "GET", "/api/v1/workspaces/wks_1/projection/events?after="+strconv.FormatInt(after, 10)+"&limit=1", nil)
		if response.Code != 200 {
			t.Fatalf("projection events status=%d body=%s", response.Code, response.Body.String())
		}
		var body struct {
			Events []struct {
				ID        string `json:"id"`
				EventType string `json:"event_type"`
			} `json:"events"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Events
	}
	first, second := page(sequence-1), page(sequence)
	if len(first) != 1 || first[0].ID == "prj_multi_relation" || first[0].EventType != "object.created" || len(second) != 1 || second[0].ID != "prj_multi_relation" || second[0].EventType != "relation.linked" {
		t.Fatalf("projection pages were not uniquely paired: first=%#v second=%#v", first, second)
	}

	if _, err := s.DB().Exec(`INSERT INTO reactive_rules(id,workspace_id,automation_id,rule_key,name,status,current_revision_id,version,activation_event_cursor,circuit_breaker_status,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,'active',?,1,?,'closed',?,?,?,?)`, "rule_multi", "wks_1", "auto_multi", "multi", "multi", "revision_multi", strconv.FormatInt(sequence-1, 10), now, now, "dev", "dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO reactive_rule_revisions(id,workspace_id,rule_id,automation_id,definition_schema_version,definition_hash,requested_actor_json,permission_policy,trigger_json,predicates_json,action_json,idempotency_json,limits_json,created_at,created_by_principal_id) VALUES(?,?,?,?,1,?,'{}','owner_principal',?,'{}','{}','{}',?,?,'dev')`, "revision_multi", "wks_1", "rule_multi", "auto_multi", "sha256:multi", `{"source":"kernel_canonical_event","event_type":"object.created"}`, `{"max_recursion_depth":1}`, now); err != nil {
		t.Fatal(err)
	}
	candidates, err := handler.reactiveRuleCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("multi-effect parent must yield one candidate per projection event, got=%d", len(candidates))
	}
	seen := map[string]string{}
	for _, candidate := range candidates {
		seen[candidate.aggregateID] = candidate.eventType
	}
	if seen[seed.Aggregate.ID] != "object.created" || seen["rel_multi"] != "relation.linked" || len(seen) != 2 {
		t.Fatalf("ReactiveRule saw mismatched parent events: %#v", seen)
	}
}

func TestRhizaProjectionEventsAndReactiveRulesKeepMultiEffectParentEventsPaired(t *testing.T) {
	ctx := context.Background()
	s, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "projection-pairing", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	defer handler.Close()
	created := request(t, handler, "POST", "/api/v1/workspaces", map[string]any{"name": "Pairing", "key": "pairing", "idempotency_key": "pairing-workspace"})
	if created.Code != 201 {
		t.Fatalf("workspace status=%d body=%s", created.Code, created.Body.String())
	}
	var workspace struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &workspace); err != nil {
		t.Fatal(err)
	}
	sequence, now, one := int64(1), "2026-01-01T00:00:00Z", int64(1)
	if err := s.ApplyStatements(ctx, "rhiza-multi-pairing", []rhiza.SQLStatement{
		{SQL: `INSERT INTO command_ledger(command_id,workspace_id,authenticated_principal_id,requested_actor_json,effective_actor_json,actor_context_hash,idempotency_key,command_type,target_json,preconditions_json,causation_json,payload_hash,payload_json,outcome,result_json,effective_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'applied',?,?,?)`, Args: []any{"cmd_rhiza_multi_parent", workspace.Data.ID, "dev", `{"type":"user","id":"dev"}`, `{"type":"user","id":"dev"}`, "sha256:multi", "idem_rhiza_multi_parent", "work.submit", `{"aggregate_type":"work_stream","aggregate_id":"wrk_multi"}`, `[]`, `{"source":"api"}`, "sha256:multi", `{}`, `{"command_id":"cmd_rhiza_multi_parent"}`, now, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, Args: []any{"act_rhiza_multi_object", workspace.Data.ID, "cmd_rhiza_multi_parent", "object_instance", "obj_rhiza_multi", "object.created", `{"id":"obj_rhiza_multi","title":"Seed"}`, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, Args: []any{"prj_rhiza_multi_object", workspace.Data.ID, "cmd_rhiza_multi_parent", sequence, "object_instance", "obj_rhiza_multi", 1, "sha256:multi", `{"id":"obj_rhiza_multi","title":"Seed"}`, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, Args: []any{"act_rhiza_multi_relation", workspace.Data.ID, "cmd_rhiza_multi_parent", "object_relation", "rel_rhiza_multi", "relation.linked", `{"id":"rel_rhiza_multi"}`, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, Args: []any{"prj_rhiza_multi_relation", workspace.Data.ID, "cmd_rhiza_multi_parent", sequence + 1, "object_relation", "rel_rhiza_multi", 1, "sha256:multi", `{"id":"rel_rhiza_multi"}`, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO reactive_rules(id,workspace_id,automation_id,rule_key,name,status,current_revision_id,version,activation_event_cursor,circuit_breaker_status,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,'active',?,1,?,'closed',?,?,?,?)`, Args: []any{"rule_rhiza_multi", workspace.Data.ID, "auto_rhiza_multi", "multi", "multi", "revision_rhiza_multi", strconv.FormatInt(sequence-1, 10), now, now, "dev", "dev"}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO reactive_rule_revisions(id,workspace_id,rule_id,automation_id,definition_schema_version,definition_hash,requested_actor_json,permission_policy,trigger_json,predicates_json,action_json,idempotency_json,limits_json,created_at,created_by_principal_id) VALUES(?,?,?,?,1,?,'{}','owner_principal',?,'{}','{}','{}',?,?,'dev')`, Args: []any{"revision_rhiza_multi", workspace.Data.ID, "rule_rhiza_multi", "auto_rhiza_multi", "sha256:multi", `{"source":"kernel_canonical_event","event_type":"object.created"}`, `{"max_recursion_depth":1}`, now}, ExpectedRowsAffected: &one},
	}); err != nil {
		t.Fatal(err)
	}
	page := func(after int64) []struct {
		ID        string `json:"id"`
		EventType string `json:"event_type"`
	} {
		response := request(t, handler, "GET", "/api/v1/workspaces/"+workspace.Data.ID+"/projection/events?after="+strconv.FormatInt(after, 10)+"&limit=1", nil)
		if response.Code != 200 {
			t.Fatalf("projection events status=%d body=%s", response.Code, response.Body.String())
		}
		var body struct {
			Events []struct {
				ID        string `json:"id"`
				EventType string `json:"event_type"`
			} `json:"events"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Events
	}
	first, second := page(sequence-1), page(sequence)
	if len(first) != 1 || first[0].ID == "prj_rhiza_multi_relation" || first[0].EventType != "object.created" || len(second) != 1 || second[0].ID != "prj_rhiza_multi_relation" || second[0].EventType != "relation.linked" {
		t.Fatalf("Rhiza projection pages were not uniquely paired: first=%#v second=%#v", first, second)
	}
	candidates, err := handler.reactiveRuleCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("Rhiza multi-effect parent must yield one candidate per projection event, got=%d", len(candidates))
	}
	seen := map[string]string{}
	for _, candidate := range candidates {
		seen[candidate.aggregateID] = candidate.eventType
	}
	if seen["obj_rhiza_multi"] != "object.created" || seen["rel_rhiza_multi"] != "relation.linked" || len(seen) != 2 {
		t.Fatalf("Rhiza ReactiveRule saw mismatched parent events: %#v", seen)
	}
}
