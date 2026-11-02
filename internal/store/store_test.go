package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenInitializesStrictSchemaAndEveryConnectionUsesForeignKeys(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "compos.db")
	store, err := Open(ctx, Options{Path: path, MaxOpenConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var identity string
	if err := store.DB().QueryRowContext(ctx, `SELECT identity FROM schema_metadata WHERE id=1`).Scan(&identity); err != nil {
		t.Fatal(err)
	}
	if identity != SchemaIdentity {
		t.Fatalf("identity = %q", identity)
	}
	for _, table := range []string{"workspace_memberships", "auth_provider_identities", "auth_sessions", "actor_capabilities", "principal_person_bindings", "person_binding_claims", "knowledge_documents", "knowledge_relations", "ontology_groundings"} {
		var count int
		if err := store.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("retired table %s count=%d err=%v", table, count, err)
		}
	}
	var templates int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name='object_type_templates'`).Scan(&templates); err != nil || templates != 0 {
		t.Fatalf("retired template table count=%d err=%v", templates, err)
	}
	for range 4 {
		conn, err := store.DB().Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var enabled int
		if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&enabled); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		conn.Close()
		if enabled != 1 {
			t.Fatalf("foreign_keys = %d", enabled)
		}
	}
}

func TestOpenRejectsLegacyDatabaseWithoutIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE legacy(id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := Open(ctx, Options{Path: path}); err == nil {
		t.Fatal("expected legacy database rejection")
	}
}

func TestOpenRejectsPreviousIdentityWithoutMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "previous.db")
	db, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_metadata(id INTEGER PRIMARY KEY, identity TEXT NOT NULL, applied_at TEXT NOT NULL); INSERT INTO schema_metadata VALUES(1,'compos-go-sqlite-world-spec-journal-reactive-agent-v2','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := Open(ctx, Options{Path: path}); err == nil {
		t.Fatal("expected previous schema identity rejection")
	}
}

func TestJSONChecksRejectInvalidPayload(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, Options{Path: filepath.Join(t.TempDir(), "compos.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO workspaces(id,key,name,created_at,updated_at) VALUES('w','w','W','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO object_types(
		id,workspace_id,key,singular,plural,field_schema_json,relation_schema_json,
		created_at,updated_at,created_by_principal_id,updated_by_principal_id
	) VALUES('t','w','things','thing','things','{broken','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','p','p')`); err == nil {
		t.Fatal("expected invalid JSON rejection")
	}
}

func TestTimestampChecksRejectUnixSeconds(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: filepath.Join(t.TempDir(), "time.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`INSERT INTO workspaces(id,key,name,created_at,updated_at) VALUES('w','w','W','1710000000','1710000000')`); err == nil {
		t.Fatal("expected Unix timestamp rejection")
	}
}

func TestOnlineBackupCanBeOpened(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: filepath.Join(t.TempDir(), "source.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := FormatTime(time.Now())
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_backup','backup','Backup',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup.sqlite")
	if err := s.Backup(ctx, destination); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(ctx, Options{Path: destination})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var count int
	if err := backup.DB().QueryRow(`SELECT COUNT(*) FROM workspaces WHERE id='wks_backup'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("backup workspace count=%d", count)
	}
}
