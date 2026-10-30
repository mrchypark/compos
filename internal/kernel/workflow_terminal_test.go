package kernel

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestWorkflowCompletionCannotCrossTerminalWork(t *testing.T) {
	for _, rhizaStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "rhiza"}[rhizaStore], func(t *testing.T) {
			ctx := context.Background()
			var s *store.Store
			var err error
			if rhizaStore {
				s, err = store.OpenRhiza(ctx, rhiza.Config{ClusterID: "workflow-terminal", NodeID: "n1", DataDir: t.TempDir()})
			} else {
				s, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			now := "2026-01-01T00:00:00Z"
			if rhizaStore {
				one := int64(1)
				err = s.ApplyStatements(ctx, "fixture", []rhiza.SQLStatement{{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, Args: []any{now, now}, ExpectedRowsAffected: &one}, {SQL: `INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, Args: []any{now}, ExpectedRowsAffected: &one}})
			} else {
				_, err = s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now)
				if err == nil {
					_, err = s.DB().Exec(`INSERT INTO workspace_memberships VALUES('wks_1','dev','owner',?)`, now)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var e *Executor
			if rhizaStore {
				e = NewStoreExecutor(s)
			} else {
				e = NewExecutor(s.DB())
			}
			e.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
			exec := func(id, typ, aggregate string, target *string, payload any, source string) Receipt {
				raw, _ := json.Marshal(payload)
				r, err := e.Execute(ctx, "dev", Command{CommandID: id, IdempotencyKey: "i_" + id, WorkspaceID: "wks_1", RequestedActor: &Actor{Type: "user", ID: "dev"}, CommandType: typ, Target: Target{AggregateType: aggregate, AggregateID: target}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: source}})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			task := exec("task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "task", "revision": 1, "kind": "task", "spec": map[string]any{"name": "Task", "goal": "Goal", "input_schema": map[string]any{"type": "object", "additionalProperties": false}, "output_schema": map[string]any{"type": "object", "additionalProperties": false}, "completion": map[string]any{"kind": "submission_valid"}, "failure_policy": map[string]any{"on_function_failed": map[string]any{"action": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}}}, "api")
			workflow := exec("flow", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "flow", "revision": 1, "kind": "workflow", "spec": map[string]any{"name": "Flow", "steps": []any{map[string]any{"id": "route", "kind": "route", "participant": map[string]any{"kind": "human", "id": "dev"}}}}}, "api")
			composite := exec("composite", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "composite", "revision": 1, "kind": "function", "spec": map[string]any{"name": "Composite", "input_schema": map[string]any{"type": "object", "additionalProperties": false}, "output_schema": map[string]any{"type": "object", "additionalProperties": false}, "binding": map[string]any{"kind": "composite", "workflow_revision_id": workflow.Aggregate.ID}}}, "api")
			work := exec("open", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}}, "api")
			wid := work.Aggregate.ID
			exec("route", "work.route", "work_stream", &wid, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}, "api")
			exec("invoke", "function.invoke", "work_stream", &wid, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}}, "api")
			exec("cancel", "work.cancel", "work_stream", &wid, map[string]any{"reason": "stop"}, "api")
			invocation := aggregateID("inv", "invoke")
			raw, _ := json.Marshal(map[string]any{"invocation_id": invocation, "runtime_status": "failed", "output": map[string]any{}})
			_, err = e.Execute(ctx, "dev", Command{CommandID: "late", IdempotencyKey: "i_late", WorkspaceID: "wks_1", CommandType: "workflow.complete", Target: Target{AggregateType: "work_stream", AggregateID: &wid}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: "workflow"}})
			if err == nil {
				t.Fatal("late workflow completion accepted")
			}
			var terminals int
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? AND kind IN ('workflow.completed','workflow.failed')`, wid).Scan(&terminals); err != nil || terminals != 0 {
				t.Fatalf("workflow terminals=%d err=%v", terminals, err)
			}
			second := exec("open2", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}}, "api")
			wid2 := second.Aggregate.ID
			exec("route2", "work.route", "work_stream", &wid2, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}, "api")
			exec("invoke2", "function.invoke", "work_stream", &wid2, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}}, "api")
			raw, _ = json.Marshal(map[string]any{"invocation_id": aggregateID("inv", "invoke2"), "runtime_status": "failed", "output": map[string]any{}})
			if _, err = e.Execute(ctx, "dev", Command{CommandID: "complete2", IdempotencyKey: "i_complete2", WorkspaceID: "wks_1", CommandType: "workflow.complete", Target: Target{AggregateType: "work_stream", AggregateID: &wid2}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: "workflow"}}); err != nil {
				t.Fatal(err)
			}
			var routed int
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? AND kind='work.routed'`, wid2).Scan(&routed); err != nil || routed != 2 {
				t.Fatalf("failure policy routes=%d err=%v", routed, err)
			}
			cancelTask := exec("cancel-task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "cancel-task", "revision": 1, "kind": "task", "spec": map[string]any{"name": "Cancel task", "goal": "Goal", "input_schema": map[string]any{"type": "object", "additionalProperties": false}, "output_schema": map[string]any{"type": "object", "additionalProperties": false}, "completion": map[string]any{"kind": "submission_valid"}, "failure_policy": map[string]any{"on_function_failed": map[string]any{"action": "cancel"}}}}, "api")
			third := exec("open3", "work.open", "work_stream", nil, map[string]any{"task_revision_id": cancelTask.Aggregate.ID, "context": map[string]any{}}, "api")
			wid3 := third.Aggregate.ID
			exec("route3", "work.route", "work_stream", &wid3, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}, "api")
			exec("invoke3", "function.invoke", "work_stream", &wid3, map[string]any{"function_revision_id": composite.Aggregate.ID, "input": map[string]any{}}, "api")
			raw, _ = json.Marshal(map[string]any{"invocation_id": aggregateID("inv", "invoke3"), "runtime_status": "succeeded", "output": map[string]any{"extra": true}})
			complete := Command{CommandID: "complete3", IdempotencyKey: "i_complete3", WorkspaceID: "wks_1", CommandType: "workflow.complete", Target: Target{AggregateType: "work_stream", AggregateID: &wid3}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: "workflow"}}
			if _, err = e.Execute(ctx, "dev", complete); err != nil {
				t.Fatal(err)
			}
			if duplicate, err := e.Execute(ctx, "dev", complete); err != nil || duplicate.Outcome != "duplicate" {
				t.Fatalf("duplicate=%#v err=%v", duplicate, err)
			}
			var failedWorkflow, failedFunction, cancelled int
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? AND kind='workflow.failed'`, wid3).Scan(&failedWorkflow); err != nil {
				t.Fatal(err)
			}
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? AND kind='function.failed'`, wid3).Scan(&failedFunction); err != nil {
				t.Fatal(err)
			}
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id='wks_1' AND stream_id=? AND kind='work.cancelled'`, wid3).Scan(&cancelled); err != nil || failedWorkflow != 1 || failedFunction != 1 || cancelled != 1 {
				t.Fatalf("workflow=%d function=%d cancelled=%d err=%v", failedWorkflow, failedFunction, cancelled, err)
			}
		})
	}
}
