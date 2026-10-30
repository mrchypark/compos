package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func TestOAuth2DirectBearerIdentityAndAuthorization(t *testing.T) {
	var revoked atomic.Bool
	var inspections atomic.Int64
	var issuer string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "introspection_endpoint": issuer + "/inspect", "userinfo_endpoint": issuer + "/user"})
		case "/inspect":
			inspections.Add(1)
			client, secret, ok := r.BasicAuth()
			if !ok || client != "resource%2Bclient%3Awith+space" || secret != "fixture%26secret%3Dword" {
				w.WriteHeader(401)
				return
			}
			token := r.FormValue("token")
			if token == "outage" {
				w.WriteHeader(503)
				return
			}
			if token == "malformed" {
				_, _ = w.Write([]byte(`{"active":`))
				return
			}
			claims := map[string]any{"active": true, "sub": "user-one", "aud": []string{"https://compos.local.test"}, "scope": "openid compos.api", "exp": time.Now().Add(time.Hour).Unix(), "iss": issuer, "token_type": "Bearer", "email": "shared@example.test"}
			switch token {
			case "good":
				claims["active"] = !revoked.Load()
			case "other":
				claims["sub"] = "user-two"
			case "wrong-aud":
				claims["aud"] = "https://other.local.test"
			case "wrong-scope":
				claims["scope"] = "openid compos.api.extra"
			case "no-openid":
				claims["scope"] = "compos.api"
			case "expired":
				claims["exp"] = time.Now().Add(-time.Second).Unix()
			case "no-exp":
				delete(claims, "exp")
			case "fractional-exp":
				claims["exp"] = 123.5
			case "no-sub":
				delete(claims, "sub")
			case "future":
				claims["nbf"] = time.Now().Add(time.Hour).Unix()
			case "bad-nbf":
				claims["nbf"] = "tomorrow"
			case "wrong-issuer":
				claims["iss"] = "https://other.local.test"
			case "bound":
				claims["cnf"] = map[string]any{"jkt": "key"}
			case "refresh", "mismatch":
			default:
				claims["active"] = false
			}
			_ = json.NewEncoder(w).Encode(claims)
		case "/user":
			token := bearerToken(r)
			if token == "refresh" {
				w.WriteHeader(401)
				return
			}
			sub := "user-one"
			if token == "other" || token == "mismatch" {
				sub = "user-two"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": sub})
		default:
			w.WriteHeader(404)
		}
	}))
	defer upstream.Close()
	issuer = upstream.URL
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "oauth2-auth-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(db)
	defer s.Close()
	workspace, err := s.BootstrapLoginWorkspace(ctx, "OAuth resource test")
	if err != nil {
		t.Fatal(err)
	}
	cfg := AuthConfig{Mode: AuthModeOAuth2, OAuth2Issuer: issuer, OAuth2Resource: "https://compos.local.test", OAuth2RequiredScope: "compos.api", OAuth2ClientID: "resource+client:with space", OAuth2ClientSecret: "fixture&secret=word"}
	if err := s.ConfigureAuth(cfg); err != nil {
		t.Fatal(err)
	}
	principalID := providerID("rpr_", "compos:rauthy-principal:v1", issuer, "user-one")
	workspaceRequest := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader(`{"name":"OAuth owner workspace","key":"oauth-owner","idempotency_key":"oauth-owner-workspace"}`))
	workspaceRequest.Header.Set("Content-Type", "application/json")
	workspaceRequest.Header.Set("Authorization", "Bearer good")
	workspaceResponse := httptest.NewRecorder()
	s.ServeHTTP(workspaceResponse, workspaceRequest)
	if workspaceResponse.Code != http.StatusCreated {
		t.Fatalf("direct bearer workspace status=%d", workspaceResponse.Code)
	}
	var createdWorkspace struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(workspaceResponse.Body.Bytes(), &createdWorkspace); err != nil || createdWorkspace.Data.ID == "" {
		t.Fatal("direct bearer workspace response was invalid")
	}
	var role string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT role FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, createdWorkspace.Data.ID, principalID).Scan(&role); err != nil || role != "owner" {
		t.Fatalf("direct bearer workspace owner membership=%q err=%v", role, err)
	}
	var identityCount int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM auth_provider_identities WHERE principal_id=?`, principalID).Scan(&identityCount); err != nil || identityCount != 0 {
		t.Fatalf("workspace creation implicitly registered identity count=%d err=%v", identityCount, err)
	}
	var bindingsBeforeIdentity int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM principal_person_bindings WHERE principal_id=?`, principalID).Scan(&bindingsBeforeIdentity); err != nil || bindingsBeforeIdentity != 1 {
		t.Fatalf("workspace creation person binding count=%d err=%v", bindingsBeforeIdentity, err)
	}
	call := func(method, path, token string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s status=%d want=%d (body redacted)", method, path, w.Code, want)
		}
		return w
	}
	identityPath := "/api/v1/auth/identity"
	first := call("POST", identityPath, "good", 200)
	call("POST", identityPath, "good", 200)
	call("POST", identityPath, "other", 200)
	if strings.Contains(first.Body.String(), "session") || strings.Contains(first.Body.String(), "token") {
		t.Fatal("identity registration issued a session")
	}
	var identityResponse struct {
		Data struct {
			Memberships []string `json:"workspace_memberships"`
		} `json:"data"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &identityResponse); err != nil || !contains(identityResponse.Data.Memberships, createdWorkspace.Data.ID) {
		t.Fatal("identity registration did not return explicit owner membership")
	}
	var key string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT provider_identity_key FROM auth_provider_identities WHERE principal_id=?`, principalID).Scan(&key); err != nil || key != "rauthy:"+issuer+":user-one" {
		t.Fatal("identity namespace changed")
	}
	var sessions, bindingsAfterIdentity int
	if db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM auth_sessions WHERE principal_id=?`, principalID).Scan(&sessions) != nil || sessions != 0 {
		t.Fatal("identity registration created a session")
	}
	if db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM principal_person_bindings WHERE principal_id=?`, principalID).Scan(&bindingsAfterIdentity) != nil || bindingsAfterIdentity != bindingsBeforeIdentity {
		t.Fatal("identity registration changed person bindings")
	}
	var count int
	if db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM workspace_memberships WHERE principal_id=?`, principalID).Scan(&count) != nil || count != 1 {
		t.Fatal("identity registration changed explicit workspace membership")
	}
	if db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM auth_provider_identities`).Scan(&count) != nil || count != 2 {
		t.Fatal("distinct subjects with same email were merged")
	}
	path := "/api/v1/workspaces/" + workspace + "/projection/snapshot"
	call("GET", path, "good", 403)
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	if err := db.Exec(ctx, "test-explicit-membership", `INSERT INTO workspace_memberships VALUES(?,?,'member',?)`, workspace, principalID, stamp); err != nil {
		t.Fatal(err)
	}
	before := inspections.Load()
	call("GET", path, "good", 200)
	call("GET", "/api/v1/auth/session", "good", 200)
	if inspections.Load() != before+2 {
		t.Fatal("direct requests reused cached authentication")
	}
	call("GET", path, "other", 403)
	call("GET", "/api/v1/workspaces/wks_other/projection/snapshot", "good", 403)
	for _, token := range []string{"", "wrong-aud", "wrong-scope", "no-openid", "expired", "no-exp", "fractional-exp", "no-sub", "future", "bad-nbf", "wrong-issuer", "bound", "refresh", "mismatch", "csn_local-session"} {
		call("GET", "/api/v1/auth/session", token, 401)
	}
	call("GET", "/api/v1/auth/session", "outage", 503)
	call("GET", "/api/v1/auth/session", "malformed", 502)
	cfg.OAuth2ClientSecret = "wrong-client-secret"
	if err := s.ConfigureAuth(cfg); err != nil {
		t.Fatal(err)
	}
	call("GET", path, "good", 401)
	cfg.OAuth2ClientSecret = "fixture&secret=word"
	if err := s.ConfigureAuth(cfg); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	call("GET", path, "good", 401)
	revoked.Store(false)
	if err := db.Exec(ctx, "test-remove-membership", `DELETE FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, workspace, principalID); err != nil {
		t.Fatal(err)
	}
	call("GET", path, "good", 403)
	call("POST", "/api/v1/auth/logout", "good", 405)
	if err := db.Exec(ctx, "test-corrupt-identity", `UPDATE auth_provider_identities SET provider_identity_key='unrelated' WHERE principal_id=?`, principalID); err != nil {
		t.Fatal(err)
	}
	call("POST", identityPath, "good", 409)
	t.Run("standalone-live-harness-contract", func(t *testing.T) {
		runOAuth2LivePilot(t, goauthyPilot{issuer: issuer, resource: cfg.OAuth2Resource, clientID: cfg.OAuth2ClientID, secret: cfg.OAuth2ClientSecret, token: "good", negative: map[string]string{"WRONG_AUD": "wrong-aud", "WRONG_SCOPE": "wrong-scope", "EXPIRED": "expired", "REVOKED": "inactive"}})
	})
}

func TestOAuth2IdentityReadFailureIsRetryable(t *testing.T) {
	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "identity.sqlite"), MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Server{store: db, auth: AuthConfig{Mode: AuthModeOAuth2}}
	conn, err := db.DB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var deny atomic.Bool
	deny.Store(true)
	err = conn.Raw(func(raw any) error {
		return raw.(sqlite3driver.Conn).Raw().SetAuthorizer(func(action sqlite3.AuthorizerActionCode, table, column, _, _ string) sqlite3.AuthorizerReturnCode {
			if deny.Load() && action == sqlite3.AUTH_READ && table == "auth_provider_identities" && column == "provider_identity_key" {
				return sqlite3.AUTH_DENY
			}
			return sqlite3.AUTH_OK
		})
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	identity := verifiedIdentity{principalID: "rpr_read_failure", providerKey: "oauth2:fixture:subject"}
	ctx := context.WithValue(context.Background(), principalContextKey{}, identity.principalID)
	ctx = context.WithValue(ctx, verifiedIdentityContextKey{}, identity)
	call := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.authIdentity(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/identity", nil).WithContext(ctx))
		return w
	}
	w := call()
	if w.Code != 500 || !strings.Contains(w.Body.String(), `"retryable":true`) || strings.Contains(w.Body.String(), "identity_conflict") {
		t.Fatalf("read failure status=%d body=%s", w.Code, w.Body)
	}
	deny.Store(false)
	var key string
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT provider_identity_key FROM auth_provider_identities WHERE principal_id=?`, identity.principalID).Scan(&key); err != nil || key != identity.providerKey {
		t.Fatalf("insert must commit before failed read: key=%q err=%v", key, err)
	}
	if w := call(); w.Code != 200 {
		t.Fatalf("retry status=%d body=%s", w.Code, w.Body)
	}
	// A unique provider key owned by another principal is still a real conflict.
	if err := db.Exec(ctx, "move-test-identity", `UPDATE auth_provider_identities SET principal_id='rpr_other' WHERE principal_id=?`, identity.principalID); err != nil {
		t.Fatal(err)
	}
	if w := call(); w.Code != 409 || !strings.Contains(w.Body.String(), "identity_conflict") {
		t.Fatalf("missing principal conflict status=%d body=%s", w.Code, w.Body)
	}
}

func TestOAuth2DiscoveryUsesIssuerPath(t *testing.T) {
	var origin string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		issuer := origin + "/tenant"
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "introspection_endpoint": issuer + "/inspect", "userinfo_endpoint": issuer + "/user"})
	}))
	defer upstream.Close()
	origin = upstream.URL
	s := &Server{authClient: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	if err := s.ConfigureAuth(AuthConfig{Mode: AuthModeOAuth2, OAuth2Issuer: origin + "/tenant", OAuth2Resource: "https://compos.local.test", OAuth2RequiredScope: "compos.api", OAuth2ClientID: "client", OAuth2ClientSecret: "secret"}); err != nil {
		t.Fatalf("path issuer discovery failed: %v", err)
	}
}

func TestOAuth2DiscoveryFailsClosed(t *testing.T) {
	for _, behavior := range []string{"issuer-mismatch", "redirect", "missing-userinfo", "http-remote", "query-endpoint"} {
		t.Run(behavior, func(t *testing.T) {
			var issuer string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if behavior == "redirect" {
					http.Redirect(w, r, "/elsewhere", 302)
					return
				}
				metadata := map[string]any{"issuer": issuer, "introspection_endpoint": issuer + "/inspect", "userinfo_endpoint": issuer + "/user"}
				switch behavior {
				case "issuer-mismatch":
					metadata["issuer"] = issuer + "/different"
				case "missing-userinfo":
					delete(metadata, "userinfo_endpoint")
				case "http-remote":
					metadata["introspection_endpoint"] = "http://example.test/inspect"
				case "query-endpoint":
					metadata["introspection_endpoint"] = issuer + "/inspect?key=value"
				}
				_ = json.NewEncoder(w).Encode(metadata)
			}))
			defer upstream.Close()
			issuer = upstream.URL
			s := &Server{authClient: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
			if s.ConfigureAuth(AuthConfig{Mode: AuthModeOAuth2, OAuth2Issuer: issuer, OAuth2Resource: "https://compos.local.test", OAuth2RequiredScope: "compos.api", OAuth2ClientID: "client", OAuth2ClientSecret: "secret"}) == nil {
				t.Fatal("invalid discovery accepted")
			}
		})
	}
}
