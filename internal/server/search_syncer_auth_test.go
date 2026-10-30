package server

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestSearchSyncerTokenIsBoundToOneWorkspaceAndProjectionReads(t *testing.T) {
	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "syncer.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := New(db)
	defer handler.Close()

	if err := handler.ConfigureSearchSyncer("", ""); err != nil {
		t.Fatal(err)
	}
	if err := handler.ConfigureSearchSyncer("short", "workspace"); err == nil {
		t.Fatal("short token accepted")
	}
	if err := handler.ConfigureSearchSyncer("0123456789abcdef0123456789abcdef", "missing"); err == nil {
		t.Fatal("missing workspace accepted")
	}
	created := request(t, handler, http.MethodPost, "/api/v1/workspaces", map[string]any{"name": "Search", "key": "search", "idempotency_key": "syncer"})
	if created.Code != http.StatusCreated {
		t.Fatalf("workspace=%d %s", created.Code, created.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	workspaceID := body["data"].(map[string]any)["id"].(string)
	first := "0123456789abcdef0123456789abcdef"
	if err := handler.ConfigureSearchSyncer(first, workspaceID); err != nil {
		t.Fatal(err)
	}
	if err := handler.ConfigureAuth(AuthConfig{Mode: AuthModePocketBase, SessionTTL: time.Hour, AllowedEmailDomains: []string{"example.com"}, PocketBaseURL: "http://127.0.0.1", PocketBaseCollection: "users", PocketBaseAutoWorkspaceID: workspaceID}); err != nil {
		t.Fatal(err)
	}
	session, failure := handler.issueSession(context.Background(), verifiedIdentity{principalID: "ppr_member", providerKey: "pocketbase:member", provider: "pocketbase", email: "member@example.com", expiresAt: time.Now().Add(time.Hour).Unix()}, "", workspaceID)
	if failure != nil {
		t.Fatal(failure)
	}
	sessionToken := session["session"].(map[string]any)["token"].(string)

	call := func(method, path, token string) int { return authRequest(t, handler, method, path, token, nil).Code }
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", ""); got != http.StatusUnauthorized {
		t.Fatalf("missing discovery token=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", "bad-token-that-is-long-enough-to-be-plausible"); got != http.StatusUnauthorized {
		t.Fatalf("invalid discovery token=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", first); got != http.StatusOK {
		t.Fatalf("discovery=%d", got)
	}
	if got := call(http.MethodHead, "/api/v1/search-syncer/workspace", first); got != http.StatusForbidden {
		t.Fatalf("HEAD discovery=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", sessionToken); got != http.StatusUnauthorized {
		t.Fatalf("session discovery=%d", got)
	}
	for _, path := range []string{
		"/api/v1/workspaces/" + workspaceID + "/projection/snapshot",
		"/api/v1/workspaces/" + workspaceID + "/projection/events",
	} {
		if got := call(http.MethodGet, path, first); got != http.StatusOK {
			t.Fatalf("allowed %s=%d", path, got)
		}
	}
	if got := call(http.MethodGet, "/api/v1/workspaces/"+workspaceID+"/projection/events", ""); got != http.StatusUnauthorized {
		t.Fatalf("missing projection token=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/workspaces/"+workspaceID+"/projection/events", "invalid-token-that-is-long-enough-to-not-be-a-configuration-error"); got != http.StatusUnauthorized {
		t.Fatalf("invalid projection token=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/workspaces/"+workspaceID+"/projection/events", sessionToken); got != http.StatusOK {
		t.Fatalf("member projection read=%d", got)
	}
	for _, request := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/workspaces/" + workspaceID + "/projection/snapshot"},
		{http.MethodGet, "/api/v1/workspaces/other/projection/snapshot"},
		{http.MethodGet, "/api/v1/workspaces/other/projection/events"},
		{http.MethodGet, "/api/v1/workspaces/" + workspaceID + "/objects"},
		{http.MethodPost, "/api/v1/workspaces/" + workspaceID + "/search"},
		{http.MethodPost, "/api/v1/kernel/commands"},
		{http.MethodGet, "/api/v1/auth/session"},
		{http.MethodPost, "/api/v1/auth/logout"},
		{http.MethodGet, "/api/v1/workspaces/" + workspaceID + "/actor-capabilities"},
		{http.MethodPost, "/internal/v1/conductor/callbacks"},
	} {
		if got := call(request.method, request.path, first); got != http.StatusForbidden {
			t.Fatalf("out of scope %s %s=%d", request.method, request.path, got)
		}
	}
	second := "abcdefghijklmnopqrstuvwxyz0123456789abcdef"
	if err := handler.ConfigureSearchSyncer(second, workspaceID); err != nil {
		t.Fatal(err)
	}
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", first); got != http.StatusUnauthorized {
		t.Fatalf("old token after rotation=%d", got)
	}
	if got := call(http.MethodGet, "/api/v1/search-syncer/workspace", second); got != http.StatusOK {
		t.Fatalf("rotated token=%d", got)
	}
	var memberships, sessions int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM workspace_memberships WHERE principal_id=?`, searchSyncerPrincipal).Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM auth_sessions WHERE token_hash=?`, sessionTokenHash(second)).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if memberships != 0 || sessions != 0 {
		t.Fatalf("service token persisted memberships=%d sessions=%d", memberships, sessions)
	}
}
