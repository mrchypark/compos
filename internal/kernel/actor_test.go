package kernel

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/compos/internal/store"
)

func testExecutor(db *sql.DB) *Executor {
	return NewExecutor(db)
}

func testStoreExecutor(s *store.Store) *Executor {
	return NewStoreExecutor(s)
}

func TestResolveActorAttributesRequestedActor(t *testing.T) {
	for _, test := range []struct {
		name    string
		command Command
		want    Actor
		invalid bool
	}{
		{name: "default user", command: Command{}, want: Actor{Type: "user", ID: "dev"}},
		{name: "requested user", command: Command{RequestedActor: &Actor{Type: "user", ID: "dev"}}, want: Actor{Type: "user", ID: "dev"}},
		{name: "service", command: Command{RequestedActor: &Actor{Type: "service", ID: "svc"}}, want: Actor{Type: "service", ID: "svc"}},
		{name: "automation", command: Command{RequestedActor: &Actor{Type: "automation", ID: "auto"}}, want: Actor{Type: "automation", ID: "auto"}},
		{name: "different user", command: Command{RequestedActor: &Actor{Type: "user", ID: "other"}}, want: Actor{Type: "user", ID: "other"}},
		{name: "empty type", command: Command{RequestedActor: &Actor{ID: "dev"}}, invalid: true},
		{name: "empty id", command: Command{RequestedActor: &Actor{Type: "user"}}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor, err := resolveActor("dev", test.command)
			if test.invalid {
				var rejected *CommandError
				if !errors.As(err, &rejected) || rejected.Outcome != "validation_failed" {
					t.Fatalf("actor=%+v err=%v", actor, err)
				}
				return
			}
			if err != nil || actor != test.want {
				t.Fatalf("actor=%+v want=%+v err=%v", actor, test.want, err)
			}
		})
	}
}

func TestExecuteRecordsRequestedActorWithoutPrincipalMatching(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "actor.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	executor := testStoreExecutor(s)
	c := command("internal-actor", "internal-actor", "object_type.define", "object_type", nil, map[string]any{"key": "items", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}})
	c.RequestedActor, c.Causation.Source = &Actor{Type: "automation", ID: "auto"}, "workflow"
	if _, err := executor.Execute(ctx, "dev", c); err != nil {
		t.Fatal(err)
	}
	var actorJSON string
	if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT effective_actor_json FROM command_ledger WHERE command_id='internal-actor'`).Scan(&actorJSON); err != nil || actorJSON != `{"type":"automation","id":"auto"}` {
		t.Fatalf("effective actor=%s err=%v", actorJSON, err)
	}
	duplicate, err := executor.Execute(ctx, "dev", c)
	if err != nil || duplicate.Outcome != "duplicate" {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
}
