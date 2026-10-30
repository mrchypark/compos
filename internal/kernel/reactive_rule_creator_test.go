package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestReactiveRuleLifecycleRequiresCreator(t *testing.T) {
	for _, mode := range []string{"sqlite", "rhiza"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var s *store.Store
			var err error
			if mode == "rhiza" {
				s, err = store.OpenRhiza(ctx, rhiza.Config{ClusterID: "rule-creator-" + mode, NodeID: "n1", DataDir: t.TempDir()})
			} else {
				s, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			one := int64(1)
			if mode == "rhiza" {
				err = s.ApplyStatements(ctx, "rule-creator-fixture", []rhiza.SQLStatement{
					{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
					{SQL: `INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
					{SQL: `INSERT INTO workspace_memberships VALUES('wks_1','other','member','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one},
				})
			} else {
				_, err = s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'); INSERT INTO workspace_memberships VALUES('wks_1','dev','owner','2026-01-01T00:00:00Z'); INSERT INTO workspace_memberships VALUES('wks_1','other','member','2026-01-01T00:00:00Z')`)
			}
			if err != nil {
				t.Fatal(err)
			}
			e := NewStoreExecutor(s)
			for _, commandType := range []string{"reactive_rule.publish_revision", "reactive_rule.disable", "reactive_rule.retire", "reactive_rule.delete"} {
				rule := defineReactiveRuleForCreatorTest(t, ctx, e, commandType)
				payload := map[string]any{"reason": "not mine"}
				if commandType == "reactive_rule.publish_revision" {
					var revision string
					if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT id FROM reactive_rule_revisions WHERE workspace_id='wks_1' AND rule_id=?`, rule).Scan(&revision); err != nil {
						t.Fatal(err)
					}
					payload = map[string]any{"revision_id": revision, "activation_event_cursor": "0"}
				}
				raw, _ := json.Marshal(payload)
				_, err := e.Execute(ctx, "other", Command{CommandID: "cmd_other_" + commandType, IdempotencyKey: "idem_other_" + commandType, WorkspaceID: "wks_1", RequestedActor: &Actor{Type: "user", ID: "other"}, CommandType: commandType, Target: Target{AggregateType: "reactive_rule", AggregateID: &rule}, Preconditions: []Precondition{{AggregateType: "reactive_rule", AggregateID: rule, ExpectedVersion: 1}}, Payload: raw, Causation: Causation{Source: "api"}})
				var rejected *CommandError
				if !errors.As(err, &rejected) || rejected.Outcome != "forbidden" {
					t.Fatalf("%s error=%v", commandType, err)
				}
				var version int64
				if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT version FROM reactive_rules WHERE workspace_id='wks_1' AND id=? AND archived_at IS NULL`, rule).Scan(&version); err != nil || version != 1 {
					t.Fatalf("%s version=%d err=%v", commandType, version, err)
				}
			}

			rule := defineReactiveRuleForCreatorTest(t, ctx, e, "creator-circuit")
			raw, _ := json.Marshal(map[string]any{"reason": "threshold", "circuit_breaker": true})
			if _, err := e.Execute(ctx, "dev", Command{CommandID: "cmd_creator_circuit", IdempotencyKey: "idem_creator_circuit", WorkspaceID: "wks_1", RequestedActor: &Actor{Type: "user", ID: "dev"}, CommandType: "reactive_rule.disable", Target: Target{AggregateType: "reactive_rule", AggregateID: &rule}, Preconditions: []Precondition{{AggregateType: "reactive_rule", AggregateID: rule, ExpectedVersion: 1}}, Payload: raw, Causation: Causation{Source: "automation"}}); err != nil {
				t.Fatal(err)
			}
			var status, circuit string
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT status,circuit_breaker_status FROM reactive_rules WHERE workspace_id='wks_1' AND id=?`, rule).Scan(&status, &circuit); err != nil || status != "disabled" || circuit != "open" {
				t.Fatalf("status=%s circuit=%s err=%v", status, circuit, err)
			}
		})
	}
}

func TestRhizaAvailabilityErrorsAreRetryable(t *testing.T) {
	for _, err := range []error{rhiza.ErrNotReady, rhiza.ErrQuorumUnavailable, rhiza.ErrDurabilityUnavailable, rhiza.ErrCommitUnknown} {
		var rejected *CommandError
		if !errors.As(rhizaAvailabilityError(err), &rejected) || rejected.Outcome != "quorum_unavailable" || !rejected.Retryable {
			t.Fatalf("error=%v classified=%v", err, rhizaAvailabilityError(err))
		}
	}
	ordinary := errors.New("ordinary failure")
	if got := rhizaAvailabilityError(ordinary); got != ordinary {
		t.Fatalf("ordinary error was classified: %v", got)
	}
}

func defineReactiveRuleForCreatorTest(t *testing.T, ctx context.Context, e *Executor, suffix string) string {
	t.Helper()
	payload := map[string]any{
		"automation_id": "auto-" + suffix, "rule_key": "rule-" + suffix, "name": "Rule " + suffix,
		"requested_actor": map[string]any{"type": "automation", "id": "auto-" + suffix},
		"trigger":         map[string]any{"source": "kernel_canonical_event", "event_type": "object.created", "event_schema_version": 1, "allowed_causation_sources": []any{}, "excluded_automation_ids": []any{}},
		"predicates":      map[string]any{"==": []any{true, true}},
		"action":          map[string]any{"action_id": "create", "kind": "emit_command", "command_type": "object.create", "target_template": map[string]any{"aggregate_type": "object_instance"}, "payload_template": map[string]any{"object_type_key": "notes", "title": "Note"}, "preconditions_template": []any{}},
		"idempotency":     map[string]any{}, "limits": map[string]any{"max_recursion_depth": 1, "max_emits_per_minute": 1, "max_emits_per_hour": 1, "max_pending_dispatches": 1, "failure_threshold": 1},
	}
	raw, _ := json.Marshal(payload)
	receipt, err := e.Execute(ctx, "dev", Command{CommandID: "cmd_rule_" + suffix, IdempotencyKey: "idem_rule_" + suffix, WorkspaceID: "wks_1", RequestedActor: &Actor{Type: "user", ID: "dev"}, CommandType: "reactive_rule.define", Target: Target{AggregateType: "reactive_rule"}, Preconditions: []Precondition{}, Payload: raw, Causation: Causation{Source: "api"}})
	if err != nil {
		t.Fatal(err)
	}
	return receipt.Aggregate.ID
}
