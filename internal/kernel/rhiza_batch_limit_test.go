package kernel

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestCommitRhizaRejectsOversizePlanWithoutSecondaryEffects(t *testing.T) {
	ctx := context.Background()
	db := openRhizaKernelStore(t, ctx, "rhiza-batch-limit")
	defer db.Close()
	executor := NewStoreExecutor(db)

	statements := make([]rhiza.SQLStatement, 60)
	for i := range statements {
		statements[i] = rhiza.SQLStatement{SQL: `UPDATE workspaces SET name=name WHERE id='wks_1'`}
	}
	command := command("cmd_oversize", "idem_oversize", "object.create", "object_instance", nil, map[string]any{})
	_, err := executor.commitRhiza(ctx, "dev", Actor{Type: "user", ID: "dev"}, command, statements, effect{
		aggregate: Aggregate{ID: "obj_rhiza_oversize", Type: "object_instance", Version: 1},
		event:     "object.created",
		payload:   map[string]any{"id": "obj_rhiza_oversize"},
		journal:   []journalFact{{streamID: "obj_rhiza_oversize", kind: "object.created", payload: map[string]any{}}},
	}, "2026-01-01T00:00:00Z")
	var rejected *CommandError
	if !errors.As(err, &rejected) || rejected.Outcome != "validation_failed" || !strings.Contains(rejected.Message, "64-statement transaction limit") {
		t.Fatalf("error=%v", err)
	}

	for query, count := range map[string]int{
		`SELECT COUNT(*) FROM command_ledger WHERE command_id='cmd_oversize'`:    0,
		`SELECT COUNT(*) FROM journal_records WHERE command_id='cmd_oversize'`:   0,
		`SELECT COUNT(*) FROM activities WHERE command_id='cmd_oversize'`:        0,
		`SELECT COUNT(*) FROM outbox_events WHERE command_id='cmd_oversize'`:     0,
		`SELECT COUNT(*) FROM projection_events WHERE command_id='cmd_oversize'`: 0,
	} {
		var got int
		if err := db.ReadRow(ctx, store.ReadLinearizable, query).Scan(&got); err != nil || got != count {
			t.Fatalf("query=%q count=%d err=%v", query, got, err)
		}
	}
}
