package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
)

func TestRauthyLoginCreatesPersonSessionAndLogout(t *testing.T) {
	expires := time.Now().Add(time.Hour).Unix()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oidc/introspect":
			user, password, ok := r.BasicAuth()
			if !ok || user != "client" || password != "secret" || r.FormValue("token") != "access" {
				t.Fatalf("invalid introspection request")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "sub": "user-1", "aud": []string{"compos"}, "scope": "openid compos", "nbf": time.Now().Add(-time.Minute).Unix(), "exp": expires})
		case "/oidc/userinfo":
			if r.Header.Get("Authorization") != "Bearer access" {
				t.Fatalf("invalid userinfo authorization")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": "user-1", "email": "User@Example.com", "email_verified": true, "name": "Example User"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	db, handler, workspace := authTestServer(t)
	defer db.Close()
	defer handler.Close()
	if err := handler.ConfigureAuth(AuthConfig{
		Mode: AuthModeRauthy, SessionTTL: 30 * time.Minute, AllowedEmailDomains: []string{"example.com"},
		RauthyIssuer: upstream.URL, RauthyClientID: "client", RauthyClientSecret: "secret", RauthyResource: "compos", RauthyRequiredScope: "compos", RauthyAutoWorkspaceID: workspace,
	}); err != nil {
		t.Fatal(err)
	}
	login := authRequest(t, handler, http.MethodPost, "/api/v1/auth/rauthy", "", map[string]any{"access_token": "access"})
	if login.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	var body map[string]any
	_ = json.Unmarshal(login.Body.Bytes(), &body)
	data := body["data"].(map[string]any)
	token := data["session"].(map[string]any)["token"].(string)
	if data["workspace_id"] != workspace || data["person_id"] == nil || data["email"] != "user@example.com" || !strings.HasPrefix(token, "csn_") {
		t.Fatalf("unexpected login response: %#v", data)
	}
	var bindings, people int
	_ = db.DB().QueryRow(`SELECT COUNT(*) FROM principal_person_bindings WHERE workspace_id=? AND principal_id=?`, workspace, data["principal_id"]).Scan(&bindings)
	_ = db.DB().QueryRow(`SELECT COUNT(*) FROM object_instances WHERE workspace_id=? AND object_type_id='otp_builtin_person_'||?`, workspace, workspace).Scan(&people)
	if bindings != 1 || people != 2 {
		t.Fatalf("bindings=%d people=%d", bindings, people)
	}
	session := authRequest(t, handler, http.MethodGet, "/api/v1/auth/session", token, nil)
	if session.Code != http.StatusOK || !strings.Contains(session.Body.String(), workspace) {
		t.Fatalf("session status=%d body=%s", session.Code, session.Body.String())
	}
	capabilityPath := "/api/v1/workspaces/" + workspace + "/actor-capabilities"
	if denied := authRequest(t, handler, http.MethodGet, capabilityPath, token, nil); denied.Code != http.StatusForbidden {
		t.Fatalf("non-admin capability read status=%d body=%s", denied.Code, denied.Body.String())
	}
	handler.auth.AdminPrincipals = []string{data["principal_id"].(string)}
	if allowed := authRequest(t, handler, http.MethodGet, capabilityPath, token, nil); allowed.Code != http.StatusOK {
		t.Fatalf("admin capability read status=%d body=%s", allowed.Code, allowed.Body.String())
	}
	handler.auth.AdminBearerToken = "operator-secret"
	if allowed := authRequest(t, handler, http.MethodGet, capabilityPath, "operator-secret", nil); allowed.Code != http.StatusOK {
		t.Fatalf("operator capability read status=%d body=%s", allowed.Code, allowed.Body.String())
	}
	logout := authRequest(t, handler, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if logout.Code != http.StatusOK {
		t.Fatalf("logout status=%d body=%s", logout.Code, logout.Body.String())
	}
	if after := authRequest(t, handler, http.MethodGet, "/api/v1/auth/session", token, nil); after.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session status=%d body=%s", after.Code, after.Body.String())
	}
}

func TestRauthyAcceptsIntegralDecimalExpiryAndRejectsFutureNotBefore(t *testing.T) {
	claims := map[string]string{
		"exp-decimal":   `{"active":true,"sub":"decimal-user","aud":["compos"],"scope":"openid compos","nbf":0,"exp":4102444800.0}`,
		"exp-exponent":  `{"active":true,"sub":"exponent-user","aud":["compos"],"scope":"openid compos","nbf":0,"exp":4.1024448e9}`,
		"nbf-decimal":   `{"active":true,"sub":"future-decimal","aud":["compos"],"scope":"openid compos","nbf":4102444800.0,"exp":4102444800}`,
		"nbf-exponent":  `{"active":true,"sub":"future-exponent","aud":["compos"],"scope":"openid compos","nbf":4.1024448e9,"exp":4102444800}`,
		"nbf-integer":   `{"active":true,"sub":"future-integer","aud":["compos"],"scope":"openid compos","nbf":4102444800,"exp":4102444800}`,
		"nbf-malformed": `{"active":true,"sub":"malformed-nbf","aud":["compos"],"scope":"openid compos","nbf":"tomorrow","exp":4102444800}`,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oidc/introspect":
			if raw, ok := claims[r.FormValue("token")]; ok {
				_, _ = w.Write([]byte(raw))
				return
			}
		case "/oidc/userinfo":
			subject := map[string]string{"exp-decimal": "decimal-user", "exp-exponent": "exponent-user", "nbf-decimal": "future-decimal", "nbf-exponent": "future-exponent", "nbf-malformed": "malformed-nbf"}[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": subject, "email": "user@example.com", "email_verified": true})
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()
	db, handler, workspace := authTestServer(t)
	defer db.Close()
	defer handler.Close()
	if err := handler.ConfigureAuth(AuthConfig{Mode: AuthModeRauthy, AllowedEmailDomains: []string{"example.com"}, RauthyIssuer: upstream.URL, RauthyClientID: "client", RauthyClientSecret: "secret", RauthyResource: "compos", RauthyRequiredScope: "compos", RauthyAutoWorkspaceID: workspace}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		token string
		want  int
	}{{"exp-decimal", http.StatusOK}, {"exp-exponent", http.StatusOK}, {"nbf-decimal", http.StatusUnauthorized}, {"nbf-exponent", http.StatusUnauthorized}, {"nbf-integer", http.StatusUnauthorized}, {"nbf-malformed", http.StatusUnauthorized}} {
		if response := authRequest(t, handler, http.MethodPost, "/api/v1/auth/rauthy", "", map[string]any{"access_token": test.token}); response.Code != test.want {
			t.Fatalf("%s status=%d want=%d", test.token, response.Code, test.want)
		}
	}
}

func TestPocketBaseLoginUsesRefreshedTokenExpiry(t *testing.T) {
	expires := time.Now().Add(time.Hour).Unix()
	payload, _ := json.Marshal(map[string]any{"exp": expires})
	jwt := "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/collections/users/auth-refresh" || r.Header.Get("Authorization") != "opaque" {
			t.Fatalf("request path=%q authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"token": jwt, "record": map[string]any{"id": "record-1", "email": "person@example.com", "verified": true, "name": "Person"}})
	}))
	defer upstream.Close()

	db, handler, workspace := authTestServer(t)
	defer db.Close()
	defer handler.Close()
	if err := handler.ConfigureAuth(AuthConfig{Mode: AuthModePocketBase, SessionTTL: 30 * time.Minute, AllowedEmailDomains: []string{"example.com"}, PocketBaseURL: upstream.URL, PocketBaseCollection: "users", PocketBaseAutoWorkspaceID: workspace}); err != nil {
		t.Fatal(err)
	}
	login := authRequest(t, handler, http.MethodPost, "/api/v1/auth/pocketbase", "", map[string]any{"token": "opaque"})
	if login.Code != http.StatusOK || !strings.Contains(login.Body.String(), `"person_id":"obj_`) {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	var storedExpiry int64
	if err := db.DB().QueryRow(`SELECT expires_at FROM auth_sessions`).Scan(&storedExpiry); err != nil || storedExpiry > expires || storedExpiry <= time.Now().Unix() {
		t.Fatalf("stored expiry=%d provider expiry=%d err=%v", storedExpiry, expires, err)
	}
}

func TestNoneModeHasNoAuthRoutes(t *testing.T) {
	db, handler, _ := authTestServer(t)
	defer db.Close()
	defer handler.Close()
	response := authRequest(t, handler, http.MethodGet, "/api/v1/auth/session", "", nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func authTestServer(t *testing.T) (*store.Store, *Server, string) {
	t.Helper()
	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "auth.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	handler := New(db)
	workspaceResponse := request(t, handler, http.MethodPost, "/api/v1/workspaces", map[string]any{"name": "Auth", "key": "auth", "idempotency_key": strconv.FormatInt(time.Now().UnixNano(), 10)})
	var body map[string]any
	_ = json.Unmarshal(workspaceResponse.Body.Bytes(), &body)
	return db, handler, body["data"].(map[string]any)["id"].(string)
}

func authRequest(t *testing.T, handler http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw strings.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		raw = *strings.NewReader(string(encoded))
	}
	request := httptest.NewRequest(method, path, &raw)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
