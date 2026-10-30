package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestReadBoundaryOnSQLite(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: t.TempDir() + "/compos.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Exec(ctx, "read-fixture", `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES('w2','two','Two',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),('w1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}

	var name string
	if err := s.ReadRow(ctx, ReadLinearizable, `SELECT name FROM workspaces WHERE id='w1'`).Scan(&name); err != nil || name != "One" {
		t.Fatalf("name=%q error=%v", name, err)
	}
	if err := s.ReadRow(ctx, ReadLocal, `SELECT name FROM workspaces WHERE id='missing'`).Scan(&name); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing row error=%v", err)
	}

	rows, err := s.ReadRows(ctx, ReadLocal, `SELECT id FROM workspaces ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "w1" || ids[1] != "w2" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestMemoryRowScan(t *testing.T) {
	row := memoryRow{values: []any{"value", int64(7), nil}, version: ReadVersion{AppliedSlot: 9, ConsensusTip: 10}}
	var text string
	var number int
	var nullable sql.NullString
	if err := row.Scan(&text, &number, &nullable); err != nil {
		t.Fatal(err)
	}
	if text != "value" || number != 7 || nullable.Valid || row.Version().AppliedSlot != 9 {
		t.Fatalf("text=%q number=%d nullable=%#v version=%#v", text, number, nullable, row.Version())
	}
}

func TestMemoryRowScanNormalizesTimeAsRFC3339(t *testing.T) {
	row := memoryRow{values: []any{time.Date(2026, 1, 2, 3, 4, 5, 600, time.FixedZone("offset", 9*60*60))}}
	var value string
	if err := row.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "2026-01-01T18:04:05.0000006Z" {
		t.Fatalf("value=%q", value)
	}
}

func TestReadSnapshotUsesOneSQLiteTransaction(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: t.TempDir() + "/compos.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Exec(ctx, "snapshot-fixture", `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES('w1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}

	_, err = s.ReadSnapshot(ctx, ReadLocal, func(reader SnapshotReader) error {
		var count int
		if err := reader.ReadRow(ctx, `SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("count=%d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRhizaSnapshotReaderRejectsChangedAppliedSlot(t *testing.T) {
	reader := rhizaSnapshotReader{}
	if err := reader.accept(ReadVersion{AppliedSlot: 7, ConsensusTip: 8}); err != nil {
		t.Fatal(err)
	}
	if err := reader.accept(ReadVersion{AppliedSlot: 7, ConsensusTip: 9}); err != nil {
		t.Fatal(err)
	}
	if reader.version.ConsensusTip != 9 {
		t.Fatalf("version=%#v", reader.version)
	}
	if err := reader.accept(ReadVersion{AppliedSlot: 8, ConsensusTip: 9}); !errors.Is(err, errReadSnapshotChanged) {
		t.Fatalf("error=%v", err)
	}
}
