package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

type goauthyPilot struct {
	issuer, resource, clientID, secret, domain, token string
	negative                                          map[string]string
}

// Only fixture FILE paths, never credentials, are supplied through the environment.
func TestGoAuthyLiveE2E(t *testing.T) {
	if os.Getenv("COMPOS_E2E_GOAUTHY") != "1" {
		t.Skip("opt-in GoAuthy live pilot; not live verification")
	}
	cfg, err := loadGoAuthyPilot(os.Getenv)
	if err != nil {
		t.Fatal(err) // Loader errors contain labels only, never input or file contents.
	}
	runGoAuthyPilot(t, cfg)
}

func loadGoAuthyPilot(getenv func(string) string) (goauthyPilot, error) {
	cfg := goauthyPilot{
		issuer:   getenv("COMPOS_E2E_GOAUTHY_ISSUER"),
		resource: getenv("COMPOS_E2E_GOAUTHY_RESOURCE"),
		clientID: getenv("COMPOS_E2E_GOAUTHY_CLIENT_ID"),
		domain:   getenv("COMPOS_E2E_GOAUTHY_EMAIL_DOMAIN"),
		negative: map[string]string{},
	}
	u, err := url.Parse(cfg.issuer)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		return cfg, errors.New("GoAuthy pilot requires a host-local issuer without credentials, query or fragment")
	}
	u, err = url.Parse(cfg.resource)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || cfg.clientID == "" || cfg.domain == "" {
		return cfg, errors.New("GoAuthy pilot requires HTTPS resource, client ID and email domain")
	}
	for _, item := range []struct {
		name   string
		target *string
	}{
		{"CLIENT_SECRET", &cfg.secret}, {"TOKEN", &cfg.token},
	} {
		value, err := readGoAuthyFixture(getenv("COMPOS_E2E_GOAUTHY_" + item.name + "_FILE"))
		if err != nil {
			return cfg, errors.New("invalid GoAuthy " + item.name + " fixture file")
		}
		*item.target = value
	}
	for _, name := range []string{"WRONG_AUD", "WRONG_SCOPE", "EXPIRED", "REVOKED"} {
		path := getenv("COMPOS_E2E_GOAUTHY_" + name + "_TOKEN_FILE")
		if path == "" {
			continue
		}
		value, err := readGoAuthyFixture(path)
		if err != nil {
			return cfg, errors.New("invalid GoAuthy " + name + " fixture file")
		}
		if value == cfg.token {
			return cfg, errors.New("negative GoAuthy fixture must differ from positive fixture")
		}
		cfg.negative[name] = value
	}
	return cfg, nil
}

func readGoAuthyFixture(path string) (string, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 {
		return "", errors.New("fixture must be a regular non-symlink 0600 file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("fixture unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !os.SameFile(before, info) {
		return "", errors.New("fixture changed during open")
	}
	after, err := os.Lstat(path)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(after, info) {
		return "", errors.New("fixture changed during open")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 64<<10+1))
	value := strings.TrimSpace(string(raw))
	if err != nil || len(raw) > 64<<10 || value == "" || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("invalid fixture")
	}
	return value, nil
}

func runGoAuthyPilot(t *testing.T, cfg goauthyPilot) {
	t.Helper()
	legacyIssuer := strings.TrimRight(cfg.issuer, "/")
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "compos-goauthy-pilot", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal("pilot Rhiza startup failed")
	}
	defer db.Close()
	s := New(db)
	defer s.Close()
	workspace, err := s.BootstrapLoginWorkspace(ctx, "GoAuthy local pilot")
	if err != nil {
		t.Fatal("pilot workspace bootstrap failed")
	}
	if err := s.ConfigureAuth(AuthConfig{Mode: AuthModeRauthy, SessionTTL: 5 * time.Minute,
		AllowedEmailDomains: []string{cfg.domain}, RauthyIssuer: legacyIssuer,
		RauthyClientID: cfg.clientID, RauthyClientSecret: cfg.secret,
		RauthyResource: cfg.resource, RauthyRequiredScope: "openid", RauthyAutoWorkspaceID: workspace,
	}); err != nil {
		t.Fatal("pilot authentication configuration failed")
	}
	// Do not forward fixture credentials to redirects, including a different local service.
	s.authClient = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	compos := httptest.NewServer(s)
	defer compos.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	request := func(method, path, credential string, payload any, expected int, result any) {
		t.Helper()
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal("pilot request encoding failed")
		}
		req, err := http.NewRequest(method, compos.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal("pilot request construction failed")
		}
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal("pilot HTTP transport failed (details redacted)")
		}
		defer response.Body.Close()
		if response.StatusCode != expected {
			t.Fatalf("pilot HTTP status: got %d, want %d (body redacted)", response.StatusCode, expected)
		}
		if result != nil && json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(result) != nil {
			t.Fatal("pilot response decoding failed (body redacted)")
		}
	}
	type loginResponse struct {
		Data struct {
			PrincipalID string `json:"principal_id"`
			PersonID    string `json:"person_id"`
			WorkspaceID string `json:"workspace_id"`
			Session     struct {
				Token string `json:"token"`
			} `json:"session"`
		} `json:"data"`
	}
	var first loginResponse
	request("POST", "/api/v1/auth/rauthy", "", map[string]string{"access_token": cfg.token}, 200, &first)
	if first.Data.PrincipalID == "" || first.Data.PersonID == "" || first.Data.WorkspaceID != workspace || first.Data.Session.Token == "" {
		t.Fatal("pilot login contract incomplete (response redacted)")
	}
	var second loginResponse
	request("POST", "/api/v1/auth/rauthy", "", map[string]string{"access_token": cfg.token}, 200, &second)
	if second.Data.PrincipalID != first.Data.PrincipalID || second.Data.PersonID != first.Data.PersonID {
		t.Fatal("repeat login changed identity or person")
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM workspace_memberships WHERE workspace_id=? AND principal_id=? AND role='member'`,
		`SELECT COUNT(*) FROM principal_person_bindings WHERE workspace_id=? AND principal_id=?`,
	} {
		var count int
		if db.ReadRow(ctx, store.ReadLinearizable, query, workspace, first.Data.PrincipalID).Scan(&count) != nil || count != 1 {
			t.Fatal("pilot membership/person binding is not unique")
		}
	}
	var session struct {
		Data struct {
			PrincipalID string   `json:"principal_id"`
			Memberships []string `json:"workspace_memberships"`
		} `json:"data"`
	}
	request("GET", "/api/v1/auth/session", first.Data.Session.Token, nil, 200, &session)
	if session.Data.PrincipalID != first.Data.PrincipalID || !contains(session.Data.Memberships, workspace) {
		t.Fatal("pilot session identity/membership mismatch")
	}
	request("POST", "/api/v1/auth/rauthy", "", map[string]string{"access_token": cfg.token, "workspace_id": "wks_not_a_member"}, 403, nil)
	for _, name := range []string{"WRONG_AUD", "WRONG_SCOPE", "EXPIRED", "REVOKED"} {
		token, supplied := cfg.negative[name]
		if !supplied {
			t.Run(name, func(t *testing.T) { t.Skip("negative fixture not supplied; NOT VERIFIED") })
			continue
		}
		t.Log("negative fixture:", name)
		request("POST", "/api/v1/auth/rauthy", "", map[string]string{"access_token": token}, 401, nil)
	}
	request("POST", "/api/v1/auth/logout", first.Data.Session.Token, nil, 200, nil)
	request("GET", "/api/v1/auth/session", first.Data.Session.Token, nil, 401, nil)
	request("GET", "/api/v1/auth/session", second.Data.Session.Token, nil, 200, nil)
	request("POST", "/api/v1/auth/logout", second.Data.Session.Token, nil, 200, nil)
	request("GET", "/api/v1/auth/session", second.Data.Session.Token, nil, 401, nil)
}

func TestGoAuthyPilotHarnessContract(t *testing.T) {
	// This exercises the harness, not a live GoAuthy issuer.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oidc/introspect":
			id, secret, ok := r.BasicAuth()
			if !ok || id != "pilot" || secret != "fixture-secret" {
				w.WriteHeader(401)
				return
			}
			claims := map[string]any{"active": true, "sub": "pilot-user", "aud": []string{"https://compos.local.test"}, "scope": "openid email profile", "exp": time.Now().Add(time.Hour).Unix()}
			switch r.FormValue("token") {
			case "positive":
			case "WRONG_AUD":
				claims["aud"] = []string{"https://other.local.test"}
			case "WRONG_SCOPE":
				claims["scope"] = "email profile"
			case "EXPIRED":
				claims["exp"] = time.Now().Add(-time.Hour).Unix()
			default:
				claims["active"] = false
			}
			_ = json.NewEncoder(w).Encode(claims)
		case "/oidc/userinfo":
			if r.Header.Get("Authorization") != "Bearer positive" {
				w.WriteHeader(401)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "pilot-user", "email": "pilot@example.test", "email_verified": true, "name": "Pilot"})
		default:
			w.WriteHeader(404)
		}
	}))
	defer upstream.Close()
	runGoAuthyPilot(t, goauthyPilot{issuer: upstream.URL, resource: "https://compos.local.test", clientID: "pilot", secret: "fixture-secret", domain: "example.test", token: "positive", negative: map[string]string{"WRONG_AUD": "WRONG_AUD", "WRONG_SCOPE": "WRONG_SCOPE", "EXPIRED": "EXPIRED", "REVOKED": "REVOKED"}})
}

func TestGoAuthyPilotFixtureValidation(t *testing.T) {
	values := map[string]string{"COMPOS_E2E_GOAUTHY_ISSUER": "http://localhost:17889", "COMPOS_E2E_GOAUTHY_RESOURCE": "https://compos.local.test", "COMPOS_E2E_GOAUTHY_CLIENT_ID": "pilot", "COMPOS_E2E_GOAUTHY_EMAIL_DOMAIN": "example.test"}
	for _, name := range []string{"CLIENT_SECRET", "TOKEN", "WRONG_AUD_TOKEN", "WRONG_SCOPE_TOKEN", "EXPIRED_TOKEN", "REVOKED_TOKEN"} {
		path := filepath.Join(t.TempDir(), "fixture")
		if err := os.WriteFile(path, []byte(name+"\n"), 0600); err != nil {
			t.Fatal("fixture setup failed")
		}
		values["COMPOS_E2E_GOAUTHY_"+name+"_FILE"] = path
	}
	get := func(key string) string { return values[key] }
	for _, issuer := range []string{"http://localhost:17889/tenant", "http://localhost:17889/tenant/"} {
		values["COMPOS_E2E_GOAUTHY_ISSUER"] = issuer
		cfg, err := loadGoAuthyPilot(get)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.issuer != issuer {
			t.Fatalf("issuer changed: got %q, want %q", cfg.issuer, issuer)
		}
	}
	values["COMPOS_E2E_GOAUTHY_ISSUER"] = "https://external.example.test"
	if _, err := loadGoAuthyPilot(get); err == nil {
		t.Fatal("non-local issuer accepted")
	}
	values["COMPOS_E2E_GOAUTHY_ISSUER"] = "http://localhost:17889"
	path := values["COMPOS_E2E_GOAUTHY_TOKEN_FILE"]
	const sensitive = "never-print-this-fixture"
	if os.WriteFile(path, []byte(sensitive+"\nsecond-line"), 0600) != nil {
		t.Fatal("fixture setup failed")
	}
	if _, err := loadGoAuthyPilot(get); err == nil || strings.Contains(err.Error(), sensitive) || strings.Contains(err.Error(), path) {
		t.Fatal("invalid fixture must fail without exposing contents or path")
	}
	if os.WriteFile(path, bytes.Repeat([]byte("x"), (64<<10)+1), 0600) != nil {
		t.Fatal("fixture setup failed")
	}
	if _, err := readGoAuthyFixture(path); err == nil {
		t.Fatal("oversized fixture accepted")
	}
	if os.Chmod(path, 0644) != nil {
		t.Fatal("fixture setup failed")
	}
	if _, err := readGoAuthyFixture(path); err == nil {
		t.Fatal("insecure fixture permissions accepted")
	}
	if os.Chmod(path, 0600) != nil {
		t.Fatal("fixture setup failed")
	}
	link := filepath.Join(t.TempDir(), "symlink")
	if os.Symlink(path, link) != nil {
		t.Fatal("fixture setup failed")
	}
	if _, err := readGoAuthyFixture(link); err == nil {
		t.Fatal("symlink fixture accepted")
	}
}
