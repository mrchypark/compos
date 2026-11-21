package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func TestArchiveRecoveryRetainsRequestReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config := rhiza.Config{
		ClusterID: "release-replay", NodeID: "node-1", DataDir: t.TempDir(),
		PeerAddr: "127.0.0.1:0", ObjStoreProvider: rhiza.ObjectStoreProviderFilesystem,
		ObjStoreDir: t.TempDir(), ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	}
	original, err := OpenRhiza(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	const query = `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES(?,?,?,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`
	if err := original.Exec(ctx, "replay-write", query, "replay", "replay", "Original"); err != nil {
		_ = original.Close()
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	config.DataDir = t.TempDir()
	recovered, err := OpenRhiza(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	var id, key, name string
	if err := recovered.ReadRow(ctx, ReadLinearizable, `SELECT id, key, name FROM workspaces WHERE id = ?`, "replay").Scan(&id, &key, &name); err != nil || id != "replay" || key != "replay" || name != "Original" {
		t.Fatalf("recovered workspace: id=%q key=%q name=%q err=%v", id, key, name, err)
	}
	if err := recovered.Exec(ctx, "replay-write", query, "different", "different", "Different"); !errors.Is(err, rhiza.ErrRequestConflict) {
		t.Fatalf("changed request must conflict, got %v", err)
	}
	if err := recovered.Exec(ctx, "replay-write", query, "replay", "replay", "Original"); err != nil {
		t.Fatalf("replay after recovery: %v", err)
	}
	var count int
	if err := recovered.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("workspace count=%d err=%v", count, err)
	}
}
