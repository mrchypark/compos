package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mrchypark/rhiza"
)

type ReadConsistency string

const (
	ReadLocal        ReadConsistency = rhiza.ConsistencyLocal
	ReadLinearizable ReadConsistency = rhiza.ConsistencyLinearizable
)

type ReadVersion struct {
	AppliedSlot  uint64
	ConsensusTip uint64
}

type Row interface {
	Scan(...any) error
	Version() ReadVersion
}

type Rows interface {
	Next() bool
	Scan(...any) error
	Close() error
	Err() error
	Version() ReadVersion
}

type SnapshotReader interface {
	ReadRow(context.Context, string, ...any) Row
	ReadRows(context.Context, string, ...any) (Rows, error)
}

const maxReadSnapshotAttempts = 8

var errReadSnapshotChanged = errors.New("read snapshot changed")

// ReadSnapshot runs bounded reads against one materialized state. SQLite uses
// one read transaction; Rhiza retries when individual Query responses report
// different applied slots.
func (s *Store) ReadSnapshot(ctx context.Context, consistency ReadConsistency, fn func(SnapshotReader) error) (ReadVersion, error) {
	if err := validateReadConsistency(consistency); err != nil {
		return ReadVersion{}, err
	}
	if s.rhiza == nil {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return ReadVersion{}, err
		}
		reader := sqlSnapshotReader{tx: tx}
		if err := fn(reader); err != nil {
			_ = tx.Rollback()
			return ReadVersion{}, err
		}
		return ReadVersion{}, tx.Commit()
	}
	for range maxReadSnapshotAttempts {
		reader := &rhizaSnapshotReader{store: s, consistency: consistency}
		if err := fn(reader); err != nil {
			if errors.Is(err, errReadSnapshotChanged) {
				continue
			}
			return reader.version, err
		}
		return reader.version, nil
	}
	return ReadVersion{}, fmt.Errorf("read snapshot changed during %d attempts", maxReadSnapshotAttempts)
}

func (s *Store) ReadRow(ctx context.Context, consistency ReadConsistency, query string, args ...any) Row {
	if err := validateReadConsistency(consistency); err != nil {
		return memoryRow{err: err}
	}
	if s.rhiza == nil {
		return sqlRow{Row: s.db.QueryRowContext(ctx, query, args...)}
	}
	response, err := s.rhiza.Query(ctx, rhiza.QueryRequest{SQL: query, Args: args, Consistency: string(consistency)})
	version := ReadVersion{AppliedSlot: response.AppliedSlot, ConsensusTip: response.ConsensusTip}
	if err != nil {
		return memoryRow{err: err, version: version}
	}
	if len(response.Rows) == 0 {
		return memoryRow{err: sql.ErrNoRows, version: version}
	}
	return memoryRow{values: response.Rows[0], version: version}
}

func (s *Store) ReadRows(ctx context.Context, consistency ReadConsistency, query string, args ...any) (Rows, error) {
	if err := validateReadConsistency(consistency); err != nil {
		return nil, err
	}
	if s.rhiza == nil {
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return sqlRows{Rows: rows}, nil
	}
	response, err := s.rhiza.Query(ctx, rhiza.QueryRequest{SQL: query, Args: args, Consistency: string(consistency)})
	if err != nil {
		return nil, err
	}
	return &memoryRows{
		rows: response.Rows, index: -1,
		version: ReadVersion{AppliedSlot: response.AppliedSlot, ConsensusTip: response.ConsensusTip},
	}, nil
}

func validateReadConsistency(consistency ReadConsistency) error {
	if consistency != ReadLocal && consistency != ReadLinearizable {
		return fmt.Errorf("unsupported read consistency %q", consistency)
	}
	return nil
}

type sqlRow struct{ *sql.Row }

func (sqlRow) Version() ReadVersion { return ReadVersion{} }

type sqlRows struct{ *sql.Rows }

func (sqlRows) Version() ReadVersion { return ReadVersion{} }

type sqlSnapshotReader struct{ tx *sql.Tx }

func (r sqlSnapshotReader) ReadRow(ctx context.Context, query string, args ...any) Row {
	return sqlRow{Row: r.tx.QueryRowContext(ctx, query, args...)}
}

func (r sqlSnapshotReader) ReadRows(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := r.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return sqlRows{Rows: rows}, nil
}

type rhizaSnapshotReader struct {
	store       *Store
	consistency ReadConsistency
	version     ReadVersion
	set         bool
}

func (r *rhizaSnapshotReader) ReadRow(ctx context.Context, query string, args ...any) Row {
	row := r.store.ReadRow(ctx, r.consistency, query, args...)
	if err := r.accept(row.Version()); err != nil {
		return memoryRow{err: err, version: row.Version()}
	}
	return row
}

func (r *rhizaSnapshotReader) ReadRows(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := r.store.ReadRows(ctx, r.consistency, query, args...)
	if err != nil {
		return nil, err
	}
	if err := r.accept(rows.Version()); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return rows, nil
}

func (r *rhizaSnapshotReader) accept(version ReadVersion) error {
	if !r.set {
		r.version, r.set = version, true
		return nil
	}
	if version.AppliedSlot != r.version.AppliedSlot {
		return errReadSnapshotChanged
	}
	if version.ConsensusTip > r.version.ConsensusTip {
		r.version.ConsensusTip = version.ConsensusTip
	}
	return nil
}

type memoryRow struct {
	values  []any
	err     error
	version ReadVersion
}

func (r memoryRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanValues(r.values, dest)
}

func (r memoryRow) Version() ReadVersion { return r.version }

type memoryRows struct {
	rows    [][]any
	index   int
	err     error
	closed  bool
	version ReadVersion
}

func (r *memoryRows) Next() bool {
	if r.closed || r.err != nil || r.index+1 >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

func (r *memoryRows) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.rows) {
		return errors.New("Scan called without a current row")
	}
	return scanValues(r.rows[r.index], dest)
}

func (r *memoryRows) Close() error {
	r.closed = true
	return nil
}

func (r *memoryRows) Err() error           { return r.err }
func (r *memoryRows) Version() ReadVersion { return r.version }

func scanValues(values []any, dest []any) error {
	if len(values) != len(dest) {
		return fmt.Errorf("sql: expected %d destination arguments in Scan, not %d", len(values), len(dest))
	}
	for i := range values {
		if scanner, ok := dest[i].(sql.Scanner); ok {
			if err := scanner.Scan(values[i]); err != nil {
				return fmt.Errorf("column %d: %w", i, err)
			}
			continue
		}
		if err := assignValue(dest[i], values[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

func assignValue(dest, source any) error {
	switch target := dest.(type) {
	case *any:
		*target = cloneReadValue(source)
		return nil
	case *string:
		if source == nil {
			return fmt.Errorf("converting NULL to string is unsupported")
		}
		switch value := source.(type) {
		case string:
			*target = value
		case []byte:
			*target = string(value)
		case time.Time:
			*target = value.UTC().Format(time.RFC3339Nano)
		default:
			*target = fmt.Sprint(value)
		}
		return nil
	case *[]byte:
		switch value := source.(type) {
		case nil:
			*target = nil
		case []byte:
			*target = append((*target)[:0], value...)
		case string:
			*target = append((*target)[:0], value...)
		case time.Time:
			*target = append((*target)[:0], value.UTC().Format(time.RFC3339Nano)...)
		default:
			*target = append((*target)[:0], fmt.Sprint(value)...)
		}
		return nil
	case *int:
		value, err := int64Value(source)
		*target = int(value)
		return err
	case *int64:
		value, err := int64Value(source)
		*target = value
		return err
	case *float64:
		value, err := float64Value(source)
		*target = value
		return err
	case *bool:
		value, err := boolValue(source)
		*target = value
		return err
	default:
		return fmt.Errorf("unsupported Scan destination %T", dest)
	}
}

func int64Value(value any) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case float64:
		return int64(value), nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func float64Value(value any) (float64, error) {
	switch value := value.(type) {
	case float64:
		return value, nil
	case int64:
		return float64(value), nil
	case string:
		return strconv.ParseFloat(value, 64)
	case []byte:
		return strconv.ParseFloat(string(value), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func boolValue(value any) (bool, error) {
	switch value := value.(type) {
	case bool:
		return value, nil
	case int64:
		return value != 0, nil
	case string:
		return strconv.ParseBool(value)
	case []byte:
		return strconv.ParseBool(string(value))
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func cloneReadValue(value any) any {
	if bytes, ok := value.([]byte); ok {
		return append([]byte(nil), bytes...)
	}
	return value
}
