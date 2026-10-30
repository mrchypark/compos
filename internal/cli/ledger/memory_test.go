package ledger

import (
	"context"
	"testing"
	"time"
)

func TestMemoryLedgerDerivesCommittedStatusFromAppendOnlyEvents(t *testing.T) {
	journal := NewMemoryJournal()
	attemptID, err := journal.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendSubmitted(context.Background(), AttemptSubmitted{AttemptID: attemptID, CommandID: "cmd_1", SubmittedAt: time.Unix(11, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendCommitted(context.Background(), AttemptCommitted{AttemptID: attemptID, CommandID: "cmd_1", CommittedAt: time.Unix(20, 0)}); err != nil {
		t.Fatal(err)
	}
	if got := len(journal.Events()); got != 3 {
		t.Fatalf("expected 3 immutable events, got %d", got)
	}
	status, ok := journal.Status(attemptID)
	if !ok || status != AttemptStatusCommitted {
		t.Fatalf("expected committed projection, got %q %v", status, ok)
	}
}
