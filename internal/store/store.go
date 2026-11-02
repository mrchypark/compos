package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
)

const (
	SchemaIdentity = "compos-go-sqlite-external-authority-v5"
)

//go:embed schema.sql
var schemaSQL string

type Options struct {
	Path            string
	MaxOpenConns    int
	BusyTimeout     time.Duration
	SynchronousMode string
}

type Store struct {
	db      *sql.DB
	dsn     string
	path    string
	close   func() error
	kind    string
	cluster string
	rhiza   *rhiza.DB
}

func Open(ctx context.Context, options Options) (*Store, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, errors.New("sqlite path is required")
	}
	if options.MaxOpenConns <= 0 {
		options.MaxOpenConns = 8
	}
	if options.BusyTimeout <= 0 {
		options.BusyTimeout = 10 * time.Second
	}
	if options.SynchronousMode == "" {
		options.SynchronousMode = "NORMAL"
	}
	if options.SynchronousMode != "NORMAL" && options.SynchronousMode != "FULL" {
		return nil, fmt.Errorf("sqlite synchronous mode must be NORMAL or FULL")
	}

	dsn, path, err := dataSourceName(options)
	if err != nil {
		return nil, err
	}
	db, err := sqlite3driver.Open(dsn, fts5.Register)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(options.MaxOpenConns)
	db.SetMaxIdleConns(options.MaxOpenConns)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	store := &Store{db: db, dsn: dsn, path: path, kind: "sqlite"}
	if err := store.initialize(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA optimize=0x10002`); err != nil {
		db.Close()
		return nil, fmt.Errorf("optimize sqlite planner statistics: %w", err)
	}
	return store, nil
}

func dataSourceName(options Options) (string, string, error) {
	var uri *url.URL
	if strings.HasPrefix(options.Path, "file:") {
		parsed, err := url.Parse(options.Path)
		if err != nil {
			return "", "", fmt.Errorf("parse sqlite URI: %w", err)
		}
		uri = parsed
	} else {
		path, err := filepath.Abs(options.Path)
		if err != nil {
			return "", "", fmt.Errorf("resolve sqlite path: %w", err)
		}
		uri = &url.URL{Scheme: "file", Path: path}
	}
	query := uri.Query()
	query.Set("_txlock", "immediate")
	query.Set("_timefmt", "rfc3339")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", options.BusyTimeout.Milliseconds()))
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous("+options.SynchronousMode+")")
	uri.RawQuery = query.Encode()
	return uri.String(), uri.Path, nil
}

func (s *Store) initialize(ctx context.Context) error {
	var userTables int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		  AND name NOT LIKE '\_rhiza\_%' ESCAPE '\'`).Scan(&userTables); err != nil {
		return fmt.Errorf("inspect sqlite schema: %w", err)
	}
	if userTables == 0 {
		statements, err := splitSchema()
		if err != nil {
			return fmt.Errorf("split sqlite schema: %w", err)
		}
		return s.Transact(ctx, "compos-schema-v1", func(tx Tx) error {
			var initialized int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_metadata'`).Scan(&initialized); err != nil {
				return err
			}
			if initialized != 0 {
				return nil
			}
			for _, statement := range statements {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("apply sqlite schema: %w", err)
				}
			}
			_, err := tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO schema_metadata(id, identity, applied_at) VALUES (1, ?, ?)`,
				SchemaIdentity, "1970-01-01T00:00:00Z")
			return err
		})
	}

	var identity string
	err := s.db.QueryRowContext(ctx,
		`SELECT identity FROM schema_metadata WHERE id = 1`).Scan(&identity)
	if errors.Is(err, sql.ErrNoRows) || strings.Contains(fmt.Sprint(err), "no such table") {
		return errors.New("refusing legacy or partially initialized SQLite database without schema identity")
	}
	if err != nil {
		return fmt.Errorf("read sqlite schema identity: %w", err)
	}
	if identity != SchemaIdentity {
		return fmt.Errorf("unsupported sqlite schema identity %q", identity)
	}
	return nil
}

func splitSchema() ([]string, error) {
	db, err := sqlite3driver.Open(":memory:", fts5.Register)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var statements []string
	err = conn.Raw(func(raw any) error {
		driverConn, ok := raw.(sqlite3driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected sqlite driver connection %T", raw)
		}
		rest := schemaSQL
		for strings.TrimSpace(rest) != "" {
			stmt, tail, err := driverConn.Raw().Prepare(rest)
			if err != nil {
				return err
			}
			consumed := strings.TrimSpace(rest[:len(rest)-len(tail)])
			rest = tail
			if stmt == nil {
				continue
			}
			if err := stmt.Exec(); err != nil {
				stmt.Close()
				return err
			}
			if err := stmt.Close(); err != nil {
				return err
			}
			if !strings.HasPrefix(strings.ToUpper(consumed), "PRAGMA ") {
				statements = append(statements, consumed)
			}
		}
		return nil
	})
	return statements, err
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error {
	if s.close != nil {
		return s.close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	optimizeErr := s.Optimize(ctx)
	return errors.Join(optimizeErr, s.db.Close())
}

func (s *Store) Kind() string { return s.kind }
func (s *Store) ClusterID() string {
	if s.cluster != "" {
		return s.cluster
	}
	return "sqlite-local"
}

func (s *Store) Ready() bool { return s.rhiza == nil || s.rhiza.Ready() }

func (s *Store) Optimize(ctx context.Context) error {
	if s.kind == "rhiza" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `PRAGMA optimize`)
	return err
}

func (s *Store) Path() string { return s.path }

func (s *Store) Health(ctx context.Context) error {
	if !s.Ready() {
		return errors.New("embedded Rhiza is not ready")
	}
	var foreignKeys int
	if err := s.ReadRow(ctx, ReadLocal, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		return err
	}
	if foreignKeys != 1 {
		return errors.New("sqlite foreign_keys is disabled")
	}
	if s.rhiza != nil {
		return nil
	}
	return s.db.PingContext(ctx)
}

func (s *Store) Backup(ctx context.Context, destination string) error {
	if s.kind == "rhiza" {
		return errors.New("embedded Rhiza owns HA checkpoints; direct SQLite backup is disabled")
	}
	if strings.TrimSpace(destination) == "" {
		return errors.New("backup destination is required")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(raw any) error {
		driverConn, ok := raw.(sqlite3driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected sqlite driver connection %T", raw)
		}
		return driverConn.Raw().Backup("main", destination)
	})
}

func FormatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
