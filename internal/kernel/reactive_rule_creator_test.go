package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza"
)

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
