package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestRhizaAdapterInboundEventConcurrentIdempotency(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "adapter-inbound-race", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	one := int64(1)
	if err := db.ApplyStatements(ctx, "adapter-inbound-workspace", []rhiza.SQLStatement{{SQL: `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, ExpectedRowsAffected: &one}}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: db}
	payload := `{"callback_id":"same"}`
	payloadHash := sha([]byte(payload))
	type result struct {
		id, receivedAt string
		created        bool
		err            error
	}
	results := make([]result, 8)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := range results {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			results[i].id, results[i].receivedAt, results[i].created, results[i].err = server.appendAdapterEventRhiza(ctx, fmt.Sprintf("ain_%d", i), "wks_1", "conductor", "cb_same", payloadHash, payload, fmt.Sprintf("2026-01-01T00:00:%02dZ", i))
		}(i)
	}
	close(start)
	wait.Wait()

	created := 0
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.id != results[0].id || result.receivedAt != results[0].receivedAt {
			t.Fatalf("results do not converge: %#v", results)
		}
		if result.created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created=%d", created)
	}
	if _, _, _, err := server.appendAdapterEventRhiza(ctx, "ain_changed", "wks_1", "conductor", "cb_same", sha([]byte(`{"callback_id":"changed"}`)), `{"callback_id":"changed"}`, "2026-01-02T00:00:00Z"); err == nil || err.Error() != "adapter event idempotency conflict" {
		t.Fatalf("changed payload error=%v", err)
	}
	var rows int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM adapter_inbound_events WHERE workspace_id='wks_1' AND adapter_id='conductor' AND inbound_event_id='cb_same'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}
