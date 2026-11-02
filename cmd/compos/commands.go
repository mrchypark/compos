package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

var version = "0.1.0"

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type runtimeDeps struct {
	stdout           io.Writer
	stderr           io.Writer
	client           httpDoer
	now              func() time.Time
	newID            func(prefix string) string
	sleep            func(context.Context, time.Duration) error
	identityProvider func() (cliSession, error)
}

type closeableCommandJournal interface {
	ledger.CommandAttemptJournal
	Close() error
}

func run(args []string) error {
	deps := runtimeDeps{
		stdout: os.Stdout,
		stderr: os.Stderr,
		client: &http.Client{Timeout: 30 * time.Second},
		now:    func() time.Time { return time.Now().UTC() },
		newID:  newID,
	}
	if commandNeedsLedger(args) {
		journal, err := defaultJournal()
		if err != nil {
			return err
		}
		defer journal.Close()
		return runWithDeps(args, journal, deps)
	}
	if commandNeedsLedgerReader(args) {
		journal, err := ledger.OpenBboltReader(defaultLedgerPath())
		if err != nil {
			if os.IsNotExist(err) {
				return runWithDeps(args, ledger.NewMemoryJournal(), deps)
			}
			return err
		}
		defer journal.Close()
		return runWithDeps(args, journal, deps)
	}
	return runWithDeps(args, ledger.NewMemoryJournal(), deps)
}

type commandRoute struct {
	needsLedger bool
	run         func(context.Context, ledger.CommandAttemptJournal, runtimeDeps) error
}

func commandRouteFor(args []string) commandRoute {
	tail := args[1:]
	switch args[0] {
	case "help":
		return commandRoute{
			run: func(ctx context.Context, _ ledger.CommandAttemptJournal, deps runtimeDeps) error {
				if len(tail) == 0 {
					_, err := fmt.Fprintln(deps.stdout, usage())
					return err
				}
				if tail[0] == "workspace" {
					_, err := fmt.Fprintln(deps.stdout, workspaceUsage())
					return err
				}
				return runObjectTypeWordHelp(ctx, tail[0], tail[1:], deps)
			},
		}
	case "version":
		return commandRoute{
			run: func(_ context.Context, _ ledger.CommandAttemptJournal, deps runtimeDeps) error {
				_, err := fmt.Fprintln(deps.stdout, version)
				return err
			},
		}
	case "setup", "login", "logout", "whoami":
		return commandRoute{run: func(_ context.Context, _ ledger.CommandAttemptJournal, _ runtimeDeps) error {
			return fmt.Errorf("local account commands have been removed; use the external identity integration")
		}}
	case "workspace":
		return commandRoute{run: func(ctx context.Context, _ ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runWorkspace(ctx, tail, deps)
		}}
	case "web":
		return commandRoute{needsLedger: hasFlag(tail, "--enable-writes"), run: func(_ context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runWeb(tail, journal, deps)
		}}
	case "define":
		return commandRoute{needsLedger: true, run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runObjectTypeWordDefine(ctx, tail, journal, deps)
		}}
	case "object":
		return commandRoute{needsLedger: commandSubcommandIs(tail, "create", "update", "move"), run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runObject(ctx, tail, journal, deps)
		}}
	case "object-type":
		return commandRoute{needsLedger: commandSubcommandIs(tail, "define", "schema-evolve"), run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runObjectType(ctx, tail, journal, deps)
		}}
	case "relation":
		return commandRoute{needsLedger: commandSubcommandIs(tail, "define", "link", "unlink"), run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runRelation(ctx, tail, journal, deps)
		}}
	case "objects":
		return commandRoute{run: func(ctx context.Context, _ ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runObjectsRead(ctx, tail, deps)
		}}
	case "forwarder", "task", "submission":
		return commandRoute{run: func(_ context.Context, _ ledger.CommandAttemptJournal, _ runtimeDeps) error {
			return fmt.Errorf("unknown command %q", args[0])
		}}
	case "ledger":
		return commandRoute{needsLedger: ledgerCommandNeedsDefaultJournal(tail), run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runLedger(ctx, tail, journal, deps)
		}}
	case "audit":
		return commandRoute{run: func(_ context.Context, _ ledger.CommandAttemptJournal, _ runtimeDeps) error {
			return fmt.Errorf("unknown command %q", args[0])
		}}
	case "search":
		return commandRoute{run: func(ctx context.Context, _ ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runSearch(ctx, tail, deps)
		}}
	default:
		objectTypeKey := args[0]
		return commandRoute{needsLedger: commandSubcommandIs(tail, "create", "move", "link"), run: func(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
			return runObjectTypeWord(ctx, objectTypeKey, tail, journal, deps)
		}}
	}
}

func commandNeedsLedger(args []string) bool {
	if len(args) == 0 || hasRemovedTokenFlag(args) {
		return false
	}
	return commandRouteFor(args).needsLedger
}

func commandNeedsLedgerReader(args []string) bool {
	return len(args) > 1 && args[0] == "ledger" && args[1] == "list" && !hasFlag(args[2:], "--journal")
}

func commandSubcommandIs(args []string, names ...string) bool {
	if len(args) == 0 {
		return false
	}
	for _, name := range names {
		if args[0] == name {
			return true
		}
	}
	return false
}

func ledgerCommandNeedsDefaultJournal(args []string) bool {
	if !commandSubcommandIs(args, "retry") {
		return false
	}
	return !hasFlag(args[1:], "--journal")
}

func runWithDeps(args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if deps.stdout == nil {
		deps.stdout = io.Discard
	}
	if deps.stderr == nil {
		deps.stderr = io.Discard
	}
	if deps.client == nil {
		deps.client = &http.Client{Timeout: 30 * time.Second}
	}
	if deps.now == nil {
		deps.now = func() time.Time { return time.Now().UTC() }
	}
	if deps.newID == nil {
		deps.newID = newID
	}
	if deps.sleep == nil {
		deps.sleep = sleepContext
	}
	if len(args) == 0 {
		_, err := fmt.Fprintln(deps.stdout, usage())
		return err
	}
	if hasRemovedTokenFlag(args) {
		return removedTokenFlagError()
	}
	return commandRouteFor(args).run(context.Background(), journal, deps)
}

func runObject(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: compos object <create|update|move|get>")
	}
	switch args[0] {
	case "create":
		return runObjectCreate(ctx, args[1:], journal, deps, "")
	case "update":
		return runObjectUpdate(ctx, args[1:], journal, deps)
	case "move":
		return runObjectMove(ctx, args[1:], journal, deps)
	case "get":
		return runObjectGet(ctx, args[1:], deps)
	default:
		return fmt.Errorf("unknown object command %q", args[0])
	}
}

func runObjectType(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: compos object-type <define|schema-evolve>")
	}
	switch args[0] {
	case "define":
		return runObjectTypeDefine(ctx, args[1:], journal, deps)
	case "schema-evolve":
		return runObjectTypeSchemaEvolve(ctx, args[1:], journal, deps)
	default:
		return fmt.Errorf("unknown object-type command %q", args[0])
	}
}
func usage() string {
	return strings.TrimSpace(`
usage: compos <command>

commands:
  version
  workspace create [--server URL] --name NAME [--key KEY] [--idempotency-key KEY]
  web [--listen 127.0.0.1:8787] [--no-open] [--server URL] [--workspace W]
	      --enable-writes records browser form POST attempts locally (default is read-only)
  define [--workspace W] --key K --singular S --plural P [--field-schema JSON] [--state-machine-id ID]
  help PLURAL_KEY [--workspace W]
  PLURAL_KEY help [--workspace W]
  PLURAL_KEY create [--workspace W] --title TITLE [--values JSON]
  PLURAL_KEY move [--workspace W] --id OBJ --version N --to-state STATE [--force]
  PLURAL_KEY get [--workspace W] --id OBJ
  PLURAL_KEY link [--workspace W] --definition RDF --definition-version N --from OBJ --from-version N --to OBJ --to-version N
  object-type define [--workspace W] --key K --singular S --plural P [--field-schema JSON] [--state-machine-id ID]
  object-type schema-evolve [--workspace W] --id ID --version N --field-schema JSON [--idempotency-key KEY]
  object create [--workspace W] --type K --title TITLE [--values JSON]
  object update [--workspace W] --id OBJ --version N [--title TITLE] [--values JSON]
  object move [--workspace W] --id OBJ --version N --to-state STATE [--force]
  object get [--workspace W] --id OBJ
  objects list [--workspace W] [--type K]
  relation define [--workspace W] --key K --from-type OT --from-type-version N --to-type OT --to-type-version N --cardinality C
  relation unlink [--workspace W] --id REL --version N --definition RDF
  relation link [--workspace W] --definition RDF --definition-version N --from OBJ --from-version N --to OBJ --to-version N
  search --server URL --q QUERY [--index INDEX] [--limit N] [--filter EXPR] [--sort EXPR] [--offset N]
  ledger list [--journal PATH] [--status unsent|unknown|rejected|committed] [--workspace W] [--limit N]
  ledger retry [--journal PATH] [--attempt ID] [--limit N] [--server URL]
`)
}
