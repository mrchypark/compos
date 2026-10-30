package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

type fakeBboltStore struct {
	keys    [][]byte
	rows    map[string]map[string][]byte
	cellErr error
	fyncErr error
}

func (s *fakeBboltStore) AppendEvent(eventKey, indexRow []byte, indexColumn string, value []byte) error {
	if s.cellErr != nil {
		return s.cellErr
	}
	s.keys = append(s.keys, append([]byte(nil), eventKey...))
	return s.BatchSetCells([][]byte{indexRow}, []map[string][]byte{{indexColumn: value}})
}
func (s *fakeBboltStore) BatchSetCells(rowKeys [][]byte, entries []map[string][]byte) error {
	if s.cellErr != nil {
		return s.cellErr
	}
	if s.rows == nil {
		s.rows = map[string]map[string][]byte{}
	}
	for i, rowKey := range rowKeys {
		if s.rows[string(rowKey)] == nil {
			s.rows[string(rowKey)] = map[string][]byte{}
		}
		for column, value := range entries[i] {
			s.rows[string(rowKey)][column] = append([]byte(nil), value...)
		}
	}
	return nil
}
func (s *fakeBboltStore) ScanRowCells(rowKey []byte, filter func([]byte) bool) (map[string][]byte, error) {
	row := s.rows[string(rowKey)]
	if len(row) == 0 {
		return nil, errors.New("key not found")
	}
	result := map[string][]byte{}
	for column, value := range row {
		if filter == nil || filter([]byte(column)) {
			result[column] = append([]byte(nil), value...)
		}
	}
	return result, nil
}
func (s *fakeBboltStore) FSync() error { return s.fyncErr }
func (s *fakeBboltStore) Close() error { return nil }

func TestBboltLedgerAppendsAttemptAsImmutableEvent(t *testing.T) {
	store := &fakeBboltStore{}
	journal := NewBboltJournalWithStore(store)
	attemptID, err := journal.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if attemptID != "attempt_1" || len(store.keys) != 1 || string(store.keys[0]) != "events/attempt_1/00000000010000000000/attempt_appended" || len(store.rows[ledgerEventsRow]) != 1 {
		t.Fatalf("unexpected immutable append: id=%q keys=%q rows=%#v", attemptID, store.keys, store.rows)
	}
}

func TestBboltLedgerDoesNotLeaveRawEventWhenAppendFails(t *testing.T) {
	store := &fakeBboltStore{cellErr: errors.New("index write failed")}
	_, err := NewBboltJournalWithStore(store).AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)})
	if err == nil || !strings.Contains(err.Error(), "index write failed") || len(store.keys) != 0 || len(store.rows[ledgerEventsRow]) != 0 {
		t.Fatalf("expected atomic append failure, err=%v keys=%q rows=%#v", err, store.keys, store.rows)
	}
}

func TestBboltLedgerReturnsFSyncFailure(t *testing.T) {
	_, err := NewBboltJournalWithStore(&fakeBboltStore{fyncErr: errors.New("disk full")}).AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)})
	if err == nil {
		t.Fatal("expected fsync error")
	}
}

func TestBboltLedgerRejectsSameAttemptTimestampAndKindWithoutOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	journal, err := NewBboltJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	at := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	if err := journal.AppendRejected(context.Background(), AttemptRejected{AttemptID: "attempt_1", Code: "first", RejectedAt: at}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	err = journal.AppendRejected(context.Background(), AttemptRejected{AttemptID: "attempt_1", Code: "second", RejectedAt: at})
	if !errors.Is(err, ErrDuplicateLedgerEvent) {
		t.Fatalf("expected duplicate event rejection, got %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("duplicate event changed ledger bytes")
	}
	records, err := journal.EventRecords(context.Background(), EventFilter{})
	if err != nil || len(records) != 1 || records[0].Event.Rejected == nil || records[0].Event.Rejected.Code != "first" {
		t.Fatalf("expected only first rejection, records=%#v err=%v", records, err)
	}
}

func TestBboltLedgerReadsIndexedEventsInTimeOrderAndByAttempt(t *testing.T) {
	journal := NewBboltJournalWithStore(&fakeBboltStore{})
	for _, event := range []AttemptAppended{{AttemptID: "attempt_2", CreatedAt: time.Unix(20, 0)}, {AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)}} {
		if _, err := journal.AppendAttempt(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	records, err := journal.EventRecords(context.Background(), EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	got := []AttemptID{records[0].Event.AttemptID, records[1].Event.AttemptID}
	if want := []AttemptID{"attempt_1", "attempt_2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected order got %v want %v", got, want)
	}
	records, err = journal.EventRecords(context.Background(), EventFilter{AttemptID: "attempt_2"})
	if err != nil || len(records) != 1 || records[0].Event.AttemptID != "attempt_2" {
		t.Fatalf("unexpected filtered records %#v, err=%v", records, err)
	}
}

func TestBboltLedgerQuarantinesCorruptEventAndBlocksAppends(t *testing.T) {
	store := &fakeBboltStore{rows: map[string]map[string][]byte{ledgerEventsRow: {"00000000010000000000/attempt_1/attempt_appended": []byte(`{not-json`)}}}
	journal := NewBboltJournalWithStore(store)
	_, err := journal.EventRecords(context.Background(), EventFilter{})
	if err == nil || !strings.Contains(err.Error(), "decode ledger event") {
		t.Fatalf("expected decode error, got %v", err)
	}
	quarantined, err := journal.IsQuarantined(context.Background())
	if err != nil || !quarantined {
		t.Fatalf("expected quarantine, got %v %v", quarantined, err)
	}
	_, err = journal.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_2", CreatedAt: time.Unix(20, 0)})
	if err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("expected quarantined append, got %v", err)
	}
}

func TestBboltLedgerPersistsAttemptForRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	at := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	journal, err := NewBboltJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CommandType: "object.create", IdempotencyKey: "key_1", CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendSubmitted(context.Background(), AttemptSubmitted{AttemptID: "attempt_1", CommandID: "cmd_1", SubmittedAt: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	journal, err = NewBboltJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	records, err := journal.EventRecords(context.Background(), EventFilter{AttemptID: "attempt_1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Event.Appended == nil || records[0].Event.Appended.IdempotencyKey != "key_1" || records[1].Event.Submitted == nil {
		t.Fatalf("unexpected persisted attempt records: %#v", records)
	}
}

func TestBboltLedgerRejectsLegacyAuditJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucket([]byte(legacyAuditBboltBucket)); return err }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBboltReader(path); !errors.Is(err, ErrLegacyAuditJournal) {
		t.Fatalf("expected legacy audit rejection, got %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("legacy journal was modified")
	}
}

func TestOpenBboltReaderReadsWithoutChangingLedgerOrAllowingWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	writer, err := NewBboltJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := OpenBboltReader(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := reader.EventRecords(context.Background(), EventFilter{})
	if err != nil || len(records) != 1 || records[0].Event.AttemptID != "attempt_1" {
		t.Fatalf("unexpected reader records %#v, err=%v", records, err)
	}
	if _, err := reader.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_2"}); !errors.Is(err, ErrReadOnlyLedger) {
		t.Fatalf("expected read-only append rejection, got %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only ledger was modified")
	}
}

func TestOpenBboltReaderDoesNotCreateMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-ledger.db")
	if _, err := OpenBboltReader(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing ledger error, got %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reader created a missing ledger: %v", err)
	}
}

func TestOpenBboltReaderDoesNotQuarantineCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	writer, err := NewBboltJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.AppendAttempt(context.Background(), AttemptAppended{AttemptID: "attempt_1", CreatedAt: time.Unix(10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(ledgerBboltBucket)).Put(ledgerKeyColumn([]byte(ledgerEventsRow), []byte("00000000010000000000/attempt_1/attempt_appended")), []byte(`{not-json`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := OpenBboltReader(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.EventRecords(context.Background(), EventFilter{})
	if err == nil || !strings.Contains(err.Error(), "decode ledger event") {
		t.Fatalf("expected corruption error, got %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only corruption check modified ledger")
	}
}

func TestAttemptCommittedUsesAttemptIDJSON(t *testing.T) {
	body, err := json.Marshal(AttemptCommitted{AttemptID: "attempt_1"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"attempt_id":"attempt_1"`)) || bytes.Contains(body, []byte("audit_id")) {
		t.Fatalf("unexpected committed JSON: %s", body)
	}
}
