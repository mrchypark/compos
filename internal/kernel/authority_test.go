package kernel

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/compos/internal/store"
)

// Domain tests inject an external authority; production constructors deny by default.
func testCommandAuthorizer(_ context.Context, principal string, c Command) (Actor, error) {
	if c.RequestedActor != nil {
		return *c.RequestedActor, nil
	}
	return Actor{Type: "user", ID: principal}, nil
}

func testExecutor(db *sql.DB) *Executor {
	e := NewExecutor(db)
	e.AuthorizeCommand = testCommandAuthorizer
	return e
}

func testStoreExecutor(s *store.Store) *Executor {
	e := NewStoreExecutor(s)
	e.AuthorizeCommand = testCommandAuthorizer
	return e
}

func TestExternalAuthorityRequiredBeforeMutationAndReplay(t *testing.T) {
	for _, backend := range []string{"sqlite", "rhiza"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var s *store.Store
			if backend == "rhiza" {
				s = openRhizaKernelStore(t, ctx, "authority-denial")
			} else {
				var err error
				s, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "authority.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Exec(ctx, "fixture", `INSERT INTO workspaces VALUES('wks_1','one','One',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
					t.Fatal(err)
				}
			}
			defer s.Close()
			c := command("authority-command", "authority-key", "object_type.define", "object_type", nil, map[string]any{"key": "items", "singular": "Item", "plural": "Items", "field_schema": map[string]any{"type": "object"}})
			e := NewStoreExecutor(s)
			assertCommandOutcome(t, e, c, "external_authority_unavailable")
			e.AuthorizeCommand = func(context.Context, string, Command) (Actor, error) { return Actor{}, ErrAuthorizationDenied }
			assertCommandOutcome(t, e, c, "forbidden")
			var count int
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("denial ledger=%d err=%v", count, err)
			}
			e.AuthorizeCommand = testCommandAuthorizer
			if _, err := e.Execute(ctx, "dev", c); err != nil {
				t.Fatal(err)
			}
			e.AuthorizeCommand = nil
			assertCommandOutcome(t, e, c, "external_authority_unavailable")
			if err := s.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM command_ledger`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("replay ledger=%d err=%v", count, err)
			}
		})
	}
}

func TestExternalAuthorityDistinguishesDenialFromOutage(t *testing.T) {
	for _, failure := range []error{nil, context.Canceled, context.DeadlineExceeded, errors.New("private transport detail"), ErrAuthorizationDenied} {
		var check CommandAuthorizer
		if failure != nil {
			check = func(context.Context, string, Command) (Actor, error) { return Actor{}, failure }
		}
		ctx := context.WithValue(context.Background(), commandAuthorizerKey{}, check)
		_, err := authorize(ctx, nil, "dev", Command{})
		var rejected *CommandError
		if !errors.As(err, &rejected) {
			t.Fatalf("failure=%v err=%v", failure, err)
		}
		denied := errors.Is(failure, ErrAuthorizationDenied)
		if rejected.Retryable == denied || (denied && rejected.Outcome != "forbidden") || (!denied && rejected.Outcome != "external_authority_unavailable") {
			t.Fatalf("failure=%v result=%+v", failure, rejected)
		}
	}
}

func TestExternalAuthorityRejectsInvalidOrChangedActor(t *testing.T) {
	c := command("actor-command", "actor-key", "object_type.define", "object_type", nil, map[string]any{})
	for _, actor := range []Actor{{}, {Type: "user", ID: "other"}, {Type: "root", ID: "dev"}, {Type: "service", ID: "unrequested"}} {
		ctx := context.WithValue(context.Background(), commandAuthorizerKey{}, CommandAuthorizer(func(context.Context, string, Command) (Actor, error) { return actor, nil }))
		if _, err := authorize(ctx, nil, "dev", c); err == nil {
			t.Fatalf("accepted actor=%+v", actor)
		}
	}
}
