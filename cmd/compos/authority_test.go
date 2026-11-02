package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func writeTestIdentity(path string, session cliSession) error {
	body, err := json.Marshal(session)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o600)
}

func testIdentityProvider(path string) func() (cliSession, error) {
	return func() (cliSession, error) {
		var identity cliSession
		body, err := os.ReadFile(path)
		if err != nil {
			return identity, err
		}
		err = json.Unmarshal(body, &identity)
		return identity, err
	}
}

func runWithTestDeps(args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if deps.identityProvider == nil {
		deps.identityProvider = testIdentityProvider(os.Getenv("COMPOS_SESSION_PATH"))
	}
	return runWithDeps(args, journal, deps)
}

func TestStandaloneCLIDoesNotLoadLocalAccountOrTrustExplicitServer(t *testing.T) {
	client := &fakeHTTPClient{status: 200, body: `{}`}
	for _, args := range [][]string{{"workspace", "create", "--server", "https://example.test", "--name", "Denied"}, {"objects", "list", "--server", "https://example.test", "--workspace", "wks_1"}} {
		// TestMain leaves an old session fixture on disk. The production entry
		// must ignore that file rather than treating it as an external identity.
		err := runWithDeps(args, ledger.NewMemoryJournal(), runtimeDeps{client: client})
		if err == nil || !strings.Contains(err.Error(), "external identity integration is not configured") {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
	if client.calls != 0 {
		t.Fatalf("unconfigured HTTP calls=%d", client.calls)
	}
}
