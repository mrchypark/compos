package ledger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

const ledgerEventsRow = "ledger_events"
const ledgerQuarantineRow = "ledger_quarantine"
const ledgerBboltBucket = "compos_cli_ledger"
const legacyAuditBboltBucket = "compos_cli_audit"

var errLedgerKeyNotFound = errors.New("key not found")
var ErrLegacyAuditJournal = errors.New("legacy audit journal is unsupported; start with a new ledger")
var ErrUnsupportedLedgerJournal = errors.New("ledger journal format is unsupported")
var ErrReadOnlyLedger = errors.New("ledger is read-only")
var ErrDuplicateLedgerEvent = errors.New("duplicate ledger event")

type journalStore interface {
	AppendEvent(eventKey []byte, indexRow []byte, indexColumn string, value []byte) error
	BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error
	ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error)
	FSync() error
	Close() error
}

type BboltJournal struct {
	store    journalStore
	readOnly bool
}

func NewBboltJournal(path string) (*BboltJournal, error) {
	if path == "" {
		return nil, fmt.Errorf("ledger path is required")
	}
	store, err := openBboltJournalStore(filepath.Clean(path), false)
	if err != nil {
		return nil, err
	}
	return &BboltJournal{store: store}, nil
}

func OpenBboltReader(path string) (*BboltJournal, error) {
	if path == "" {
		return nil, fmt.Errorf("ledger path is required")
	}
	store, err := openBboltJournalStore(filepath.Clean(path), true)
	if err != nil {
		return nil, err
	}
	return &BboltJournal{store: store, readOnly: true}, nil
}

func NewBboltJournalWithStore(store journalStore) *BboltJournal { return &BboltJournal{store: store} }
func (j *BboltJournal) Close() error                            { return j.store.Close() }

func (j *BboltJournal) AppendAttempt(ctx context.Context, event AttemptAppended) (AttemptID, error) {
	if event.AttemptID == "" {
		return "", fmt.Errorf("attempt id is required")
	}
	return event.AttemptID, j.append(ctx, Event{Kind: EventAttemptAppended, AttemptID: event.AttemptID, Appended: &event}, event.CreatedAt)
}

func (j *BboltJournal) AppendSubmitted(ctx context.Context, event AttemptSubmitted) error {
	return j.append(ctx, Event{Kind: EventAttemptSubmitted, AttemptID: event.AttemptID, Submitted: &event}, event.SubmittedAt)
}
func (j *BboltJournal) AppendRejected(ctx context.Context, event AttemptRejected) error {
	return j.append(ctx, Event{Kind: EventAttemptRejected, AttemptID: event.AttemptID, Rejected: &event}, event.RejectedAt)
}
func (j *BboltJournal) AppendCommitted(ctx context.Context, event AttemptCommitted) error {
	return j.append(ctx, Event{Kind: EventAttemptCommitted, AttemptID: event.AttemptID, Committed: &event}, event.CommittedAt)
}

func (j *BboltJournal) append(ctx context.Context, event Event, at time.Time) error {
	if j.readOnly {
		return ErrReadOnlyLedger
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	quarantined, err := j.IsQuarantined(ctx)
	if err != nil {
		return err
	}
	if quarantined {
		return fmt.Errorf("request ledger is quarantined")
	}
	if event.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	key := fmt.Sprintf("events/%s/%020d/%s", event.AttemptID, at.UnixNano(), event.Kind)
	if err := j.store.AppendEvent([]byte(key), []byte(ledgerEventsRow), ledgerEventColumn(event, at), body); err != nil {
		return err
	}
	return j.store.FSync()
}

func (j *BboltJournal) IsQuarantined(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	cells, err := j.store.ScanRowCells([]byte(ledgerQuarantineRow), nil)
	if err != nil {
		if isKeyNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return len(cells) > 0, nil
}

func (j *BboltJournal) EventRecords(ctx context.Context, filter EventFilter) ([]EventRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cells, err := j.store.ScanRowCells([]byte(ledgerEventsRow), func(columnKey []byte) bool {
		return filter.AttemptID == "" || strings.Contains(string(columnKey), "/"+string(filter.AttemptID)+"/")
	})
	if err != nil {
		if isKeyNotFound(err) {
			return []EventRecord{}, nil
		}
		return nil, err
	}
	keys := make([]string, 0, len(cells))
	for key := range cells {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if filter.Limit > 0 && len(keys) > filter.Limit {
		keys = keys[:filter.Limit]
	}
	records := make([]EventRecord, 0, len(keys))
	for _, key := range keys {
		var event Event
		if err := json.Unmarshal(cells[key], &event); err != nil {
			if j.readOnly {
				return nil, fmt.Errorf("decode ledger event %q: %w", key, err)
			}
			if quarantineErr := j.quarantine(ctx, key, err); quarantineErr != nil {
				return nil, quarantineErr
			}
			return nil, fmt.Errorf("decode ledger event %q: %w", key, err)
		}
		records = append(records, EventRecord{Key: key, Event: event})
	}
	return records, nil
}

func (j *BboltJournal) quarantine(ctx context.Context, corruptedKey string, cause error) error {
	if j.readOnly {
		return ErrReadOnlyLedger
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		CorruptedKey  string `json:"corrupted_key"`
		Reason        string `json:"reason"`
		QuarantinedAt string `json:"quarantined_at"`
	}{corruptedKey, cause.Error(), time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	column := fmt.Sprintf("%020d/%s", time.Now().UTC().UnixNano(), corruptedKey)
	if err := j.store.BatchSetCells([][]byte{[]byte(ledgerQuarantineRow)}, []map[string][]byte{{column: body}}); err != nil {
		return err
	}
	return j.store.FSync()
}

func isKeyNotFound(err error) bool {
	return errors.Is(err, errLedgerKeyNotFound) || strings.Contains(err.Error(), "key not found")
}
func ledgerEventColumn(event Event, at time.Time) string {
	return fmt.Sprintf("%020d/%s/%s", at.UnixNano(), event.AttemptID, event.Kind)
}

type bboltJournalStore struct{ db *bbolt.DB }

func openBboltJournalStore(path string, readOnly bool) (*bboltJournalStore, error) {
	if readOnly {
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
	}
	options := &bbolt.Options{ReadOnly: readOnly, Timeout: time.Second}
	db, err := bbolt.Open(path, 0o600, options)
	if err != nil {
		return nil, err
	}
	legacy, ledger := false, false
	if err := db.View(func(tx *bbolt.Tx) error {
		legacy = tx.Bucket([]byte(legacyAuditBboltBucket)) != nil
		ledger = tx.Bucket([]byte(ledgerBboltBucket)) != nil
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	if legacy {
		_ = db.Close()
		return nil, ErrLegacyAuditJournal
	}
	if readOnly && !ledger {
		_ = db.Close()
		return nil, ErrUnsupportedLedgerJournal
	}
	if readOnly {
		return &bboltJournalStore{db: db}, nil
	}
	if err := db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte(ledgerBboltBucket)); return err }); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &bboltJournalStore{db: db}, nil
}

func (s *bboltJournalStore) AppendEvent(eventKey []byte, indexRow []byte, indexColumn string, value []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ledgerBboltBucket))
		rawKey := ledgerKeyKV(eventKey)
		indexKey := ledgerKeyColumn(indexRow, []byte(indexColumn))
		if bucket.Get(rawKey) != nil || bucket.Get(indexKey) != nil {
			return fmt.Errorf("%w: %s", ErrDuplicateLedgerEvent, eventKey)
		}
		if err := bucket.Put(rawKey, value); err != nil {
			return err
		}
		return bucket.Put(indexKey, value)
	})
}
func (s *bboltJournalStore) BatchSetCells(rowKeys [][]byte, columnEntriesPerRow []map[string][]byte) error {
	if len(rowKeys) != len(columnEntriesPerRow) {
		return fmt.Errorf("invalid row cell batch")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(ledgerBboltBucket))
		for i, rowKey := range rowKeys {
			for column, value := range columnEntriesPerRow[i] {
				if err := bucket.Put(ledgerKeyColumn(rowKey, []byte(column)), value); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
func (s *bboltJournalStore) ScanRowCells(rowKey []byte, filter func(columnKey []byte) bool) (map[string][]byte, error) {
	prefix := ledgerRowKey(rowKey)
	entries := map[string][]byte{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket([]byte(ledgerBboltBucket)).Cursor()
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			if len(k) == len(prefix) {
				continue
			}
			column := k[len(prefix):]
			if filter != nil && !filter(column) {
				continue
			}
			entries[string(column)] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errLedgerKeyNotFound
	}
	return entries, nil
}
func (s *bboltJournalStore) FSync() error { return s.db.Sync() }
func (s *bboltJournalStore) Close() error { return s.db.Close() }
func ledgerKeyKV(key []byte) []byte {
	const keyTypeKV = byte(254)
	if len(key) > 0 && key[0] == keyTypeKV {
		return key
	}
	return append([]byte{keyTypeKV}, key...)
}
func ledgerRowKey(row []byte) []byte {
	const keyTypeWideColumn = byte(253)
	if len(row) > 0 && row[0] == keyTypeWideColumn {
		return row
	}
	key := make([]byte, 1+4+len(row))
	key[0] = keyTypeWideColumn
	binary.BigEndian.PutUint32(key[1:], uint32(len(row)))
	copy(key[5:], row)
	return key
}
func ledgerKeyColumn(row, column []byte) []byte {
	key := ledgerRowKey(row)
	if len(column) == 0 {
		return key
	}
	out := make([]byte, len(key)+len(column))
	copy(out, key)
	copy(out[len(key):], column)
	return out
}
