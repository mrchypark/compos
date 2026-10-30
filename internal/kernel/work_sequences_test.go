package kernel

import (
	"context"
	"database/sql"
	"math"
	"testing"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

type sequenceTestRow struct {
	counter int64
	err     error
}

func (r sequenceTestRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*int64)) = r.counter
	return nil
}
func (sequenceTestRow) Version() store.ReadVersion { return store.ReadVersion{} }

func sequenceReader(values map[string]int64) func(string, ...any) store.Row {
	return func(_ string, args ...any) store.Row {
		key := args[0].(string) + "/" + args[1].(string) + "/" + args[2].(string) + "/" + args[3].(string)
		counter, ok := values[key]
		if !ok {
			return sequenceTestRow{err: sql.ErrNoRows}
		}
		return sequenceTestRow{counter: counter}
	}
}

func TestWorkSequenceReservationsReserveExistingAndNewScopes(t *testing.T) {
	reservations := newWorkSequenceReservations()
	calls := 0
	baseRead := sequenceReader(map[string]int64{"w/t/number/2026-09": 7})
	read := func(query string, args ...any) store.Row {
		calls++
		return baseRead(query, args...)
	}
	first, err := reservations.reserve(context.Background(), read, "w", "t", "number", "2026-09")
	if err != nil || first != 8 {
		t.Fatalf("first reserve = %d, %v", first, err)
	}
	second, err := reservations.reserve(context.Background(), read, "w", "t", "number", "2026-09")
	if err != nil || second != 9 {
		t.Fatalf("second reserve = %d, %v", second, err)
	}
	if calls != 1 {
		t.Fatalf("same scope reads = %d, want 1", calls)
	}
	other, err := reservations.reserve(context.Background(), read, "w", "t", "number", "2026-10")
	if err != nil || other != 1 {
		t.Fatalf("new scope reserve = %d, %v", other, err)
	}
	statements := []rhiza.SQLStatement{}
	reservations.appendStatements(&statements)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2", len(statements))
	}
	if got := statements[0].Args[len(statements[0].Args)-1]; got != int64(7) {
		t.Fatalf("existing counter guard = %#v, want 7", got)
	}
	if got := statements[0].Args[0]; got != int64(9) {
		t.Fatalf("existing final counter = %#v, want 9", got)
	}
	if got := statements[1].Args[len(statements[1].Args)-1]; got != int64(1) {
		t.Fatalf("new final counter = %#v, want 1", got)
	}
}

func TestWorkSequenceReservationsRejectOverflow(t *testing.T) {
	reservations := newWorkSequenceReservations()
	_, err := reservations.reserve(context.Background(), sequenceReader(map[string]int64{"w/t/number/never": math.MaxInt64}), "w", "t", "number", "never")
	commandErr, ok := err.(*CommandError)
	if !ok || commandErr.Outcome != "validation_failed" {
		t.Fatalf("reserve error = %#v, want validation_failed", err)
	}
}

func TestWorkSequenceReservationsFinalMutationIsGuarded(t *testing.T) {
	reservations := newWorkSequenceReservations()
	_, _ = reservations.reserve(context.Background(), sequenceReader(map[string]int64{"w/t/number/never": 3}), "w", "t", "number", "never")
	statements := []rhiza.SQLStatement{}
	reservations.appendStatements(&statements)
	if len(statements) != 1 || statements[0].ExpectedRowsAffected == nil || *statements[0].ExpectedRowsAffected != 1 {
		t.Fatalf("final reservation mutation must be a guarded update: %#v", statements)
	}
	if got := statements[0].Args[len(statements[0].Args)-1]; got != int64(3) {
		t.Fatalf("guarded old counter = %#v, want 3", got)
	}
}
