package store

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

// Exercise recovery after graceful close in the non-local single-node mode used by cmd/server.
func TestReleaseSingleNodeArchiveRecovery(t *testing.T) {
	for _, provider := range releaseProviders() {
		t.Run(provider, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			config := rhiza.Config{
				ClusterID: "release-recovery", NodeID: "node-1", DataDir: t.TempDir(),
				PeerAddr: "127.0.0.1:0", ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
			}
			configureReleaseProvider(t, ctx, provider, &config)
			original, err := OpenRhiza(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := original.Exec(ctx, "release-recovery-write", `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES('recovered','recovered','Recovered',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
				_ = original.Close()
				t.Fatal(err)
			}
			if err := original.Close(); err != nil {
				t.Fatal(err)
			}
			config.DataDir = t.TempDir()
			recovered, err := OpenRhiza(ctx, config)
			if err != nil {
				t.Fatalf("open from archive with empty data directory: %v", err)
			}
			defer recovered.Close()
			var name string
			if err := recovered.ReadRow(ctx, ReadLinearizable, `SELECT name FROM workspaces WHERE id='recovered'`).Scan(&name); err != nil || name != "Recovered" {
				t.Fatalf("recovered name=%q err=%v", name, err)
			}
		})
	}
}
