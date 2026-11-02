package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
	"github.com/mrchypark/compos/internal/kernel"
	composserver "github.com/mrchypark/compos/internal/server"
	"github.com/mrchypark/compos/internal/store"
)

func TestDynamicObjectTypeCreateUsesLiveCommandSurface(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := "2026-01-01T00:00:00Z"
	if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"key": "orders", "singular": "order", "plural": "orders", "field_schema": map[string]any{"type": "object"}})
	executor := kernel.NewExecutor(db.DB())
	executor.AuthorizeCommand = integrationAuthority{}.AuthorizeCommand
	if _, err := executor.Execute(ctx, "dev", kernel.Command{
		CommandID: "cmd_type", IdempotencyKey: "idem_type", WorkspaceID: "wks_1", RequestedActor: &kernel.Actor{Type: "user", ID: "dev"},
		CommandType: "object_type.define", Target: kernel.Target{AggregateType: "object_type"}, Preconditions: []kernel.Precondition{}, Payload: payload, Causation: kernel.Causation{Source: "api"},
	}); err != nil {
		t.Fatal(err)
	}
	handler := composserver.NewWithAuthority(db, integrationAuthority{})
	defer handler.Close()
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()

	err = runWithTestDeps([]string{"orders", "create", "--server", httpServer.URL, "--workspace", "wks_1", "--title", "First order"}, ledger.NewMemoryJournal(), runtimeDeps{
		stdout: io.Discard, stderr: io.Discard, client: http.DefaultClient,
		now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }, newID: fixedIDGenerator(), identityProvider: func() (cliSession, error) {
			return cliSession{Server: httpServer.URL, WorkspaceID: "wks_1", PrincipalID: "dev", Credential: "test-external-token"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id='wks_1' AND t.key='orders' AND i.title='First order'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("created objects=%d err=%v", count, err)
	}
}

type integrationAuthority struct{}

func (integrationAuthority) AuthorizeRequest(*http.Request) (string, error) { return "dev", nil }
func (integrationAuthority) AuthorizeCommand(_ context.Context, principal string, c kernel.Command) (kernel.Actor, error) {
	if c.RequestedActor != nil {
		return *c.RequestedActor, nil
	}
	return kernel.Actor{Type: "user", ID: principal}, nil
}
