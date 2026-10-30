package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

// Optional actual IdP integration; the issuer and credential files are caller-owned.
func TestOAuth2LiveE2E(t *testing.T) {
	if os.Getenv("COMPOS_E2E_OAUTH2") != "1" {
		t.Skip("opt-in actual OIDC resource-server test; NOT VERIFIED")
	}
	cfg, err := loadGoAuthyPilot(func(name string) string {
		return os.Getenv(strings.Replace(name, "COMPOS_E2E_GOAUTHY_", "COMPOS_E2E_OAUTH2_", 1))
	})
	if err != nil {
		t.Fatal("invalid OIDC pilot configuration or fixture file (details redacted)")
	}
	runOAuth2LivePilot(t, cfg)
}

func TestOAuth2PilotLoaderPreservesTrailingIssuerSlash(t *testing.T) {
	var issuer string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		base := strings.TrimRight(issuer, "/")
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "introspection_endpoint": base + "/inspect", "userinfo_endpoint": base + "/userinfo"})
	}))
	defer upstream.Close()
	issuer = upstream.URL + "/tenant/"
	values := map[string]string{
		"COMPOS_E2E_OAUTH2_ISSUER":       issuer,
		"COMPOS_E2E_OAUTH2_RESOURCE":     "https://compos.local.test",
		"COMPOS_E2E_OAUTH2_CLIENT_ID":    "pilot",
		"COMPOS_E2E_OAUTH2_EMAIL_DOMAIN": "example.test",
	}
	for _, name := range []string{"CLIENT_SECRET", "TOKEN"} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		values["COMPOS_E2E_OAUTH2_"+name+"_FILE"] = path
	}
	cfg, err := loadGoAuthyPilot(func(name string) string {
		return values[strings.Replace(name, "COMPOS_E2E_GOAUTHY_", "COMPOS_E2E_OAUTH2_", 1)]
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.issuer != values["COMPOS_E2E_OAUTH2_ISSUER"] {
		t.Fatalf("issuer changed: got %q", cfg.issuer)
	}
	s := &Server{authClient: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if err := s.ConfigureAuth(AuthConfig{Mode: AuthModeOAuth2, OAuth2Issuer: cfg.issuer, OAuth2Resource: cfg.resource, OAuth2RequiredScope: "compos.api", OAuth2ClientID: cfg.clientID, OAuth2ClientSecret: cfg.secret}); err != nil {
		t.Fatalf("trailing-slash issuer discovery failed: %v", err)
	}
}

func runOAuth2LivePilot(t *testing.T, cfg goauthyPilot) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "oauth2-live-pilot", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal("pilot storage startup failed")
	}
	defer db.Close()
	s := New(db)
	defer s.Close()
	workspace, err := s.BootstrapLoginWorkspace(ctx, "Explicit local OAuth membership fixture")
	if err != nil {
		t.Fatal("pilot workspace fixture failed")
	}
	if err := s.ConfigureAuth(AuthConfig{Mode: AuthModeOAuth2, OAuth2Issuer: cfg.issuer, OAuth2Resource: cfg.resource, OAuth2RequiredScope: "compos.api", OAuth2ClientID: cfg.clientID, OAuth2ClientSecret: cfg.secret}); err != nil {
		t.Fatal("pilot OAuth discovery/configuration failed (details redacted)")
	}
	server := httptest.NewServer(s)
	defer server.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	call := func(method, path, token string, want int, target any) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, nil)
		if err != nil {
			t.Fatal("pilot request failed")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("pilot HTTP failed (details redacted)")
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("pilot HTTP status=%d want=%d (body redacted)", resp.StatusCode, want)
		}
		if target != nil && json.NewDecoder(resp.Body).Decode(target) != nil {
			t.Fatal("pilot response invalid (body redacted)")
		}
	}
	var identity struct {
		Data struct {
			Principal   string   `json:"principal_id"`
			Memberships []string `json:"workspace_memberships"`
		} `json:"data"`
	}
	call("POST", "/api/v1/auth/identity", cfg.token, 200, &identity)
	if identity.Data.Principal == "" || len(identity.Data.Memberships) != 0 {
		t.Fatal("identity registration unexpectedly enrolled membership")
	}
	call("POST", "/api/v1/auth/identity", cfg.token, 200, nil)
	path := "/api/v1/workspaces/" + workspace + "/projection/snapshot"
	call("GET", path, cfg.token, 403, nil)
	// Explicit test fixture grant, not an authentication-side enrollment policy.
	if db.Exec(ctx, "explicit-local-membership", `INSERT INTO workspace_memberships VALUES(?,?,'member',?)`, workspace, identity.Data.Principal, time.Now().UTC().Format(time.RFC3339Nano)) != nil {
		t.Fatal("explicit membership fixture failed")
	}
	call("GET", path, cfg.token, 200, nil)
	call("GET", "/api/v1/auth/session", cfg.token, 200, &identity)
	if !contains(identity.Data.Memberships, workspace) {
		t.Fatal("explicit membership missing")
	}
	call("GET", "/api/v1/workspaces/wks_unrelated/projection/snapshot", cfg.token, 403, nil)
	for _, name := range []string{"WRONG_AUD", "WRONG_SCOPE", "EXPIRED", "REVOKED"} {
		token, ok := cfg.negative[name]
		if !ok {
			t.Run(name, func(t *testing.T) { t.Skip("fixture missing; NOT VERIFIED") })
			continue
		}
		t.Log("negative fixture:", name)
		call("GET", "/api/v1/auth/session", token, 401, nil)
	}
	if db.Exec(ctx, "remove-local-membership", `DELETE FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, workspace, identity.Data.Principal) != nil {
		t.Fatal("membership removal fixture failed")
	}
	call("GET", path, cfg.token, 403, nil)
	call("POST", "/api/v1/auth/logout", cfg.token, 405, nil)
	var count int
	if db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM auth_sessions WHERE principal_id=?`, identity.Data.Principal).Scan(&count) != nil || count != 0 {
		t.Fatal("direct bearer mode issued a local session")
	}
}
