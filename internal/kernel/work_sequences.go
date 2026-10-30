package kernel

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sort"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

// workSequenceReservations keeps generated counters local to one atomic Work
// submission. It is deliberately not shared by Executor instances.
type workSequenceReservations struct {
	entries map[workSequenceKey]*workSequenceReservation
}

type workSequenceKey struct{ workspace, objectType, field, scope string }

type workSequenceReservation struct {
	key     workSequenceKey
	initial int64
	next    int64
	exists  bool
}

func newWorkSequenceReservations() *workSequenceReservations {
	return &workSequenceReservations{entries: map[workSequenceKey]*workSequenceReservation{}}
}

func (r *workSequenceReservations) reserve(ctx context.Context, read func(string, ...any) store.Row, workspace, objectType, field, scope string) (int64, error) {
	key := workSequenceKey{workspace, objectType, field, scope}
	entry := r.entries[key]
	if entry == nil {
		counter := int64(0)
		err := read(`SELECT counter FROM field_sequence_counters WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=?`, workspace, objectType, field, scope).Scan(&counter)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		entry = &workSequenceReservation{key: key, initial: counter, next: counter, exists: err == nil}
		r.entries[key] = entry
	}
	if entry.next == math.MaxInt64 {
		return 0, reject("validation_failed", "generated sequence counter is exhausted")
	}
	entry.next++
	return entry.next, nil
}

// appendStatements emits exactly one final guarded mutation per counter scope.
// It must run before the collected object DML so a failed submission rolls all
// counter reservations back with its objects, relations, and Work submission.
func (r *workSequenceReservations) appendStatements(statements *[]rhiza.SQLStatement) {
	if r == nil || len(r.entries) == 0 {
		return
	}
	entries := make([]*workSequenceReservation, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].key, entries[j].key
		return a.workspace+"\x00"+a.objectType+"\x00"+a.field+"\x00"+a.scope < b.workspace+"\x00"+b.objectType+"\x00"+b.field+"\x00"+b.scope
	})
	one := int64(1)
	for _, entry := range entries {
		key := entry.key
		if entry.exists {
			*statements = append(*statements, rhiza.SQLStatement{SQL: `UPDATE field_sequence_counters SET counter=? WHERE workspace_id=? AND object_type_id=? AND field_name=? AND scope_key=? AND counter=?`, Args: []any{entry.next, key.workspace, key.objectType, key.field, key.scope, entry.initial}, ExpectedRowsAffected: &one})
			continue
		}
		*statements = append(*statements, rhiza.SQLStatement{SQL: `INSERT INTO field_sequence_counters(workspace_id,object_type_id,field_name,scope_key,counter) VALUES(?,?,?,?,?) ON CONFLICT(workspace_id,object_type_id,field_name,scope_key) DO NOTHING`, Args: []any{key.workspace, key.objectType, key.field, key.scope, entry.next}, ExpectedRowsAffected: &one})
	}
}
