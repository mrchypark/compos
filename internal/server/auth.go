package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

type AuthMode string

const (
	AuthModeNone       AuthMode = "none"
	AuthModeRauthy     AuthMode = "rauthy"
	AuthModePocketBase AuthMode = "pocketbase"
	AuthModeOAuth2     AuthMode = "oauth2"
)

type AuthConfig struct {
	Mode                      AuthMode
	AllowedEmailDomains       []string
	SessionTTL                time.Duration
	RauthyIssuer              string
	RauthyClientID            string
	RauthyClientSecret        string
	RauthyResource            string
	RauthyRequiredScope       string
	RauthyAutoWorkspaceID     string
	PocketBaseURL             string
	PocketBaseCollection      string
	PocketBaseAutoWorkspaceID string
	AdminPrincipals           []string
	AdminBearerToken          string
	OAuth2Issuer              string
	OAuth2Resource            string
	OAuth2RequiredScope       string
	OAuth2ClientID            string
	OAuth2ClientSecret        string
}

type principalContextKey struct{}
type verifiedIdentityContextKey struct{}

const clusterOperatorPrincipal = "cluster_operator"

type verifiedIdentity struct {
	providerKey, principalID, email, name, provider string
	expiresAt                                       int64
}

type authFailure struct {
	status    int
	code      string
	category  string
	retryable bool
	message   string
}

func (e *authFailure) Error() string { return e.message }

// BootstrapLoginWorkspace provisions the configured Rhiza deployment's login home.
func (s *Server) BootstrapLoginWorkspace(ctx context.Context, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || s.store.Kind() != "rhiza" {
		return "", errors.New("login workspace bootstrap requires a name and Rhiza storage")
	}
	payload := canonical(map[string]any{"name": name, "key": "compos", "idempotency_key": "bootstrap-login-workspace"})
	response, _, err := s.createWorkspaceRhiza(ctx, "system:bootstrap", name, "compos", "bootstrap-login-workspace", payload, sha([]byte(payload)))
	if err != nil {
		return "", err
	}
	return response.(map[string]any)["data"].(map[string]any)["workspace_id"].(string), nil
}

func AuthConfigFromEnv() (AuthConfig, error) {
	mode := AuthMode(strings.TrimSpace(os.Getenv("COMPOS_AUTH_MODE")))
	if mode == "" {
		mode = AuthModeRauthy
	}
	config := AuthConfig{
		Mode: mode, SessionTTL: 12 * time.Hour,
		AllowedEmailDomains: splitCSV(os.Getenv("COMPOS_AUTH_ALLOWED_EMAIL_DOMAINS")),
		AdminPrincipals:     splitCSV(os.Getenv("COMPOS_ADMIN_PRINCIPALS")), AdminBearerToken: strings.TrimSpace(os.Getenv("COMPOS_ADMIN_BEARER_TOKEN")),
	}
	if raw := strings.TrimSpace(os.Getenv("COMPOS_AUTH_SESSION_TTL_SECONDS")); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 60 {
			return AuthConfig{}, errors.New("COMPOS_AUTH_SESSION_TTL_SECONDS must be at least 60")
		}
		config.SessionTTL = time.Duration(seconds) * time.Second
	}
	switch mode {
	case AuthModeNone:
		return config, nil
	case AuthModeOAuth2:
		config.OAuth2Issuer = strings.TrimSpace(os.Getenv("COMPOS_OAUTH2_ISSUER"))
		config.OAuth2Resource = strings.TrimSpace(os.Getenv("COMPOS_OAUTH2_RESOURCE"))
		config.OAuth2RequiredScope = strings.TrimSpace(os.Getenv("COMPOS_OAUTH2_REQUIRED_SCOPE"))
		config.OAuth2ClientID = strings.TrimSpace(os.Getenv("COMPOS_OAUTH2_INTROSPECTION_CLIENT_ID"))
		config.OAuth2ClientSecret = os.Getenv("COMPOS_OAUTH2_INTROSPECTION_CLIENT_SECRET")
		return config, nil
	case AuthModeRauthy:
		config.RauthyIssuer = strings.TrimRight(strings.TrimSpace(os.Getenv("COMPOS_RAUTHY_ISSUER")), "/")
		config.RauthyClientID = strings.TrimSpace(os.Getenv("COMPOS_RAUTHY_INTROSPECTION_CLIENT_ID"))
		config.RauthyClientSecret = os.Getenv("COMPOS_RAUTHY_INTROSPECTION_CLIENT_SECRET")
		config.RauthyResource = strings.TrimSpace(os.Getenv("COMPOS_RAUTHY_RESOURCE"))
		config.RauthyRequiredScope = strings.TrimSpace(os.Getenv("COMPOS_RAUTHY_REQUIRED_SCOPE"))
		config.RauthyAutoWorkspaceID = strings.TrimSpace(os.Getenv("COMPOS_RAUTHY_AUTO_WORKSPACE_ID"))
	case AuthModePocketBase:
		config.PocketBaseURL = strings.TrimRight(strings.TrimSpace(os.Getenv("COMPOS_POCKETBASE_URL")), "/")
		config.PocketBaseCollection = strings.TrimSpace(os.Getenv("COMPOS_POCKETBASE_AUTH_COLLECTION"))
		config.PocketBaseAutoWorkspaceID = strings.TrimSpace(os.Getenv("COMPOS_POCKETBASE_AUTO_WORKSPACE_ID"))
	default:
		return AuthConfig{}, errors.New("COMPOS_AUTH_MODE must be none, rauthy, pocketbase, or oauth2")
	}
	if len(config.AllowedEmailDomains) == 0 {
		return AuthConfig{}, errors.New("COMPOS_AUTH_ALLOWED_EMAIL_DOMAINS is required")
	}
	return config, nil
}

func (s *Server) ConfigureAuth(config AuthConfig) error {
	if config.SessionTTL <= 0 {
		config.SessionTTL = 12 * time.Hour
	}
	for i, domain := range config.AllowedEmailDomains {
		domain = strings.ToLower(strings.TrimSpace(domain))
		if domain == "" || strings.Contains(domain, "@") || !ascii(domain) {
			return errors.New("allowed email domains must be non-empty ASCII domains")
		}
		config.AllowedEmailDomains[i] = domain
	}
	switch config.Mode {
	case AuthModeNone:
	case AuthModeOAuth2:
		endpoint, userinfo, err := s.discoverOAuth2(config)
		if err != nil {
			return err
		}
		s.oauth2IntrospectionEndpoint = endpoint
		s.oauth2UserInfoEndpoint = userinfo
	case AuthModeRauthy:
		issuer, err := url.Parse(config.RauthyIssuer)
		if err != nil || issuer.Scheme == "" || issuer.Host == "" || config.RauthyClientID == "" || config.RauthyClientSecret == "" || config.RauthyResource == "" || config.RauthyRequiredScope == "" || len(config.AllowedEmailDomains) == 0 {
			return errors.New("Rauthy authentication configuration is incomplete")
		}
		if issuer.Scheme != "https" && issuer.Hostname() != "127.0.0.1" && issuer.Hostname() != "localhost" {
			return errors.New("Rauthy issuer must use https")
		}
	case AuthModePocketBase:
		base, err := url.Parse(config.PocketBaseURL)
		if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || config.PocketBaseCollection == "" || len(config.AllowedEmailDomains) == 0 {
			return errors.New("PocketBase authentication configuration is incomplete")
		}
		if strings.ContainsAny(config.PocketBaseCollection, "/?#") {
			return errors.New("PocketBase auth collection is invalid")
		}
	default:
		return errors.New("auth mode must be none, rauthy, pocketbase, or oauth2")
	}
	s.auth = config
	return nil
}

// ConfigureSearchSyncer grants one deployment token read-only projection access
// to one existing workspace. It must be called during startup before serving.
// Empty values disable the integration.
func (s *Server) ConfigureSearchSyncer(token, workspaceID string) error {
	token, workspaceID = strings.TrimSpace(token), strings.TrimSpace(workspaceID)
	if token == "" && workspaceID == "" {
		s.searchSyncerToken, s.searchSyncerWorkspaceID = "", ""
		return nil
	}
	if token == "" || workspaceID == "" {
		return errors.New("search syncer token and workspace ID must both be set")
	}
	if len([]byte(token)) < 32 || !ascii(token) {
		return errors.New("search syncer token must be at least 32 ASCII bytes")
	}
	for _, char := range workspaceID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return errors.New("search syncer workspace ID is invalid")
		}
	}
	var exists int
	if err := s.store.ReadRow(context.Background(), store.ReadLinearizable, `SELECT 1 FROM workspaces WHERE id=?`, workspaceID).Scan(&exists); err != nil {
		return errors.New("search syncer workspace does not exist")
	}
	s.searchSyncerToken, s.searchSyncerWorkspaceID = token, workspaceID
	return nil
}

func (s *Server) authenticateRequest(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	path, token := r.URL.Path, bearerToken(r)
	if path == "/api/v1/search-syncer/workspace" {
		if !s.searchSyncerTokenMatches(token) {
			writeAuthFailure(w, unauthenticated("search syncer token is required"))
			return r, false
		}
		if r.Method != http.MethodGet {
			writeAuthFailure(w, forbidden("search syncer token is not authorized for this endpoint"))
			return r, false
		}
		return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, searchSyncerPrincipal)), true
	}
	if s.searchSyncerTokenMatches(token) {
		if r.Method == http.MethodGet && (path == "/api/v1/workspaces/"+s.searchSyncerWorkspaceID+"/projection/snapshot" || path == "/api/v1/workspaces/"+s.searchSyncerWorkspaceID+"/projection/events") {
			return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, searchSyncerPrincipal)), true
		}
		writeAuthFailure(w, forbidden("search syncer token is not authorized for this endpoint"))
		return r, false
	}
	if s.auth.Mode == AuthModeNone {
		return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, "dev")), true
	}
	if path == "/api/v1/health" || path == "/api/v1/auth/rauthy" || path == "/api/v1/auth/pocketbase" || path == "/api/v1/auth/session" || path == "/api/v1/auth/logout" || strings.HasPrefix(path, "/internal/") || strings.HasPrefix(path, "/v1/connector/") {
		return r, true
	}
	if adminControlPath(path) && s.auth.AdminBearerToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.auth.AdminBearerToken)) == 1 {
		return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, clusterOperatorPrincipal)), true
	}
	var principal string
	var failure *authFailure
	if s.auth.Mode == AuthModeOAuth2 {
		identity, authFailure := s.verifyOAuth2(r.Context(), token)
		principal, failure = identity.principalID, authFailure
		r = r.WithContext(context.WithValue(r.Context(), verifiedIdentityContextKey{}, identity))
	} else {
		principal, failure = s.sessionPrincipal(r.Context(), token)
	}
	if failure != nil {
		writeAuthFailure(w, failure)
		return r, false
	}
	if workspace := workspaceFromPath(path); workspace != "" && !s.isWorkspaceMember(r.Context(), workspace, principal) {
		problem(w, 403, "forbidden", "authz", false, "workspace membership is required")
		return r, false
	}
	return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)), true
}

const searchSyncerPrincipal = "search_syncer"

func (s *Server) searchSyncerTokenMatches(token string) bool {
	return s.searchSyncerToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.searchSyncerToken)) == 1
}

func (s *Server) searchSyncerWorkspace(w http.ResponseWriter, r *http.Request) {
	if principal(r) != searchSyncerPrincipal {
		writeAuthFailure(w, unauthenticated("search syncer token is required"))
		return
	}
	write(w, http.StatusOK, map[string]any{"workspace_id": s.searchSyncerWorkspaceID})
}

func adminControlPath(path string) bool {
	return strings.HasPrefix(path, "/api/v1/workspaces/") && strings.HasSuffix(path, "/actor-capabilities")
}

func (s *Server) authorizeAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.auth.Mode == AuthModeNone || principal(r) == clusterOperatorPrincipal {
		return true
	}
	for _, id := range s.auth.AdminPrincipals {
		if principal(r) == id {
			return true
		}
	}
	problem(w, 403, "forbidden", "authz", false, "administrative principal is required")
	return false
}

func workspaceFromPath(path string) string {
	const prefix = "/api/v1/workspaces/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	value := strings.TrimPrefix(path, prefix)
	if end := strings.IndexByte(value, '/'); end >= 0 {
		value = value[:end]
	}
	return value
}

func (s *Server) authRauthy(w http.ResponseWriter, r *http.Request) {
	if s.auth.Mode != AuthModeRauthy {
		http.NotFound(w, r)
		return
	}
	var request struct {
		AccessToken string `json:"access_token"`
		WorkspaceID string `json:"workspace_id"`
	}
	if !decode(w, r, &request) {
		return
	}
	identity, failure := s.verifyRauthy(r.Context(), strings.TrimSpace(request.AccessToken))
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	s.finishLogin(w, r, identity, strings.TrimSpace(request.WorkspaceID), s.auth.RauthyAutoWorkspaceID)
}

func (s *Server) authPocketBase(w http.ResponseWriter, r *http.Request) {
	if s.auth.Mode != AuthModePocketBase {
		http.NotFound(w, r)
		return
	}
	var request struct {
		Token       string `json:"token"`
		WorkspaceID string `json:"workspace_id"`
	}
	if !decode(w, r, &request) {
		return
	}
	identity, failure := s.verifyPocketBase(r.Context(), strings.TrimSpace(request.Token))
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	s.finishLogin(w, r, identity, strings.TrimSpace(request.WorkspaceID), s.auth.PocketBaseAutoWorkspaceID)
}

func (s *Server) authSession(w http.ResponseWriter, r *http.Request) {
	if s.auth.Mode == AuthModeNone {
		http.NotFound(w, r)
		return
	}
	principal, failure := s.requestPrincipal(r.Context(), bearerToken(r))
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	memberships, err := s.workspaceMemberships(r.Context(), principal)
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": map[string]any{"principal_id": principal, "workspace_memberships": memberships}})
}

func (s *Server) authLogout(w http.ResponseWriter, r *http.Request) {
	if s.auth.Mode == AuthModeOAuth2 {
		problem(w, http.StatusMethodNotAllowed, "unsupported_operation", "request", false, "direct OAuth bearer mode has no Compos session; the client owns token revocation and logout")
		return
	}
	if s.auth.Mode == AuthModeNone {
		http.NotFound(w, r)
		return
	}
	token := bearerToken(r)
	if _, failure := s.sessionPrincipal(r.Context(), token); failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := s.store.Exec(r.Context(), "auth-logout:"+uuid.NewString(), `UPDATE auth_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at IS NULL`, now, sessionTokenHash(token)); err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": map[string]any{"status": "logged_out"}})
}

func (s *Server) finishLogin(w http.ResponseWriter, r *http.Request, identity verifiedIdentity, requestedWorkspace, autoWorkspace string) {
	if !s.emailAllowed(identity.email) {
		writeAuthFailure(w, unauthenticated("verified email domain is not allowed"))
		return
	}
	result, failure := s.issueSession(r.Context(), identity, requestedWorkspace, autoWorkspace)
	if failure != nil {
		writeAuthFailure(w, failure)
		return
	}
	write(w, 200, map[string]any{"data": result})
}

func (s *Server) issueSession(ctx context.Context, identity verifiedIdentity, requestedWorkspace, autoWorkspace string) (map[string]any, *authFailure) {
	memberships, err := s.workspaceMemberships(ctx, identity.principalID)
	if err != nil {
		return nil, serverAuthFailure("workspace membership query failed")
	}
	target := requestedWorkspace
	if target == "" {
		if autoWorkspace != "" {
			target = autoWorkspace
		} else if len(memberships) == 1 {
			target = memberships[0]
		} else {
			return nil, forbidden("a workspace_id is required")
		}
	}
	if !contains(memberships, target) {
		if target != autoWorkspace || autoWorkspace == "" {
			return nil, forbidden("authenticated principal is not a member of the requested workspace")
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		rowsAffected, err := s.store.ExecAffected(ctx, "auth-enroll:"+uuid.NewString(), `INSERT OR IGNORE INTO workspace_memberships(workspace_id,principal_id,role,created_at) SELECT id,?,'member',? FROM workspaces WHERE id=?`, identity.principalID, now, target)
		if err != nil {
			return nil, serverAuthFailure("workspace enrollment failed")
		}
		if rowsAffected == 0 && !s.isWorkspaceMember(ctx, target, identity.principalID) {
			return nil, forbidden("automatic enrollment workspace does not exist")
		}
		memberships = append(memberships, target)
	}
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339Nano)
	if err := s.store.Exec(ctx, "auth-identity:"+uuid.NewString(), `INSERT INTO auth_provider_identities(principal_id,provider_identity_key,email,email_verified,name,picture,created_at,updated_at,last_login_at) VALUES(?,?,?,?,NULLIF(?,''),NULL,?,?,?) ON CONFLICT(principal_id) DO UPDATE SET email=excluded.email,email_verified=excluded.email_verified,name=excluded.name,updated_at=excluded.updated_at,last_login_at=excluded.last_login_at`, identity.principalID, identity.providerKey, identity.email, 1, identity.name, stamp, stamp, stamp); err != nil {
		return nil, serverAuthFailure("authentication identity persistence failed")
	}
	personID, failure := s.ensureLoginPerson(ctx, identity, target, stamp)
	if failure != nil {
		return nil, failure
	}
	expires := identity.expiresAt
	if ttl := now.Add(s.auth.SessionTTL).Unix(); ttl < expires {
		expires = ttl
	}
	token, err := randomSessionToken()
	if err != nil {
		return nil, serverAuthFailure("session token generation failed")
	}
	sessionID := "ses_" + uuid.NewString()
	if err := s.store.Exec(ctx, "auth-session:"+sessionID, `INSERT INTO auth_sessions(id,principal_id,token_hash,expires_at,created_at,last_used_at) VALUES(?,?,?,?,?,?)`, sessionID, identity.principalID, sessionTokenHash(token), expires, stamp, stamp); err != nil {
		return nil, serverAuthFailure("authentication session persistence failed")
	}
	memberships, _ = s.workspaceMemberships(ctx, identity.principalID)
	return map[string]any{
		"principal_id": identity.principalID, "person_id": personID, "email": identity.email, "name": nullableString(identity.name),
		"workspace_id": target, "workspace_memberships": memberships, "session": map[string]any{"token": token, "expires_at": strconv.FormatInt(expires, 10)},
	}, nil
}

func (s *Server) ensureLoginPerson(ctx context.Context, identity verifiedIdentity, workspace, stamp string) (string, *authFailure) {
	var personID string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT person_object_instance_id FROM principal_person_bindings WHERE workspace_id=? AND principal_id=?`, workspace, identity.principalID).Scan(&personID)
	if err == nil {
		return personID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", serverAuthFailure("person binding lookup failed")
	}
	digest := sha256.Sum256([]byte(workspace + "\x00" + identity.principalID))
	suffix := hex.EncodeToString(digest[:16])
	payload, _ := json.Marshal(map[string]any{
		"object_type_key": "people", "title": firstNonEmpty(identity.name, identity.email),
		"values": map[string]any{"display_name": firstNonEmpty(identity.name, identity.email), "email": identity.email},
	})
	receipt, err := s.kernel.Execute(ctx, identity.principalID, kernel.Command{
		CommandID: "cmd_auth_person_" + suffix, IdempotencyKey: "auth-person:" + suffix, WorkspaceID: workspace,
		CommandType: "object.create", Target: kernel.Target{AggregateType: "object_instance"}, Preconditions: []kernel.Precondition{}, Payload: payload, Causation: kernel.Causation{Source: "api"},
	})
	if err != nil || receipt.Aggregate == nil {
		return "", serverAuthFailure("login person creation failed")
	}
	personID = receipt.Aggregate.ID
	if s.store.Kind() == "rhiza" {
		err = s.store.ApplyStatements(ctx, "auth-binding:"+suffix, []rhiza.SQLStatement{
			{SQL: `INSERT OR IGNORE INTO person_binding_claims(workspace_id,person_object_instance_id,claim_type,normalized_claim,created_at,created_by_principal_id) VALUES(?,?,'email',?,?,?)`, Args: []any{workspace, personID, identity.email, stamp, identity.principalID}},
			{SQL: `INSERT OR IGNORE INTO principal_person_bindings(workspace_id,principal_id,person_object_instance_id,binding_method,verified_claim,created_at) VALUES(?,?,?,?,?,?)`, Args: []any{workspace, identity.principalID, personID, identity.provider, identity.email, stamp}},
		})
	} else {
		err = s.store.Transact(ctx, "auth-binding:"+suffix, func(tx store.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO person_binding_claims(workspace_id,person_object_instance_id,claim_type,normalized_claim,created_at,created_by_principal_id) VALUES(?,?,'email',?,?,?)`, workspace, personID, identity.email, stamp, identity.principalID); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO principal_person_bindings(workspace_id,principal_id,person_object_instance_id,binding_method,verified_claim,created_at) VALUES(?,?,?,?,?,?)`, workspace, identity.principalID, personID, identity.provider, identity.email, stamp)
			return err
		})
	}
	if err != nil {
		return "", serverAuthFailure("person binding persistence failed")
	}
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT person_object_instance_id FROM principal_person_bindings WHERE workspace_id=? AND principal_id=?`, workspace, identity.principalID).Scan(&personID); err != nil {
		return "", serverAuthFailure("person binding persistence failed")
	}
	return personID, nil
}

func (s *Server) verifyRauthy(ctx context.Context, token string) (verifiedIdentity, *authFailure) {
	if token == "" {
		return verifiedIdentity{}, unauthenticated("access_token is required")
	}
	form := url.Values{"token": []string{token}}
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.auth.RauthyIssuer+"/oidc/introspect", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth(s.auth.RauthyClientID, s.auth.RauthyClientSecret)
	var claims map[string]any
	if failure := s.authUpstreamJSON(request, &claims); failure != nil {
		return verifiedIdentity{}, failure
	}
	now := time.Now().Unix()
	subject, _ := claims["sub"].(string)
	expires, expOK := numberInt64(claims["exp"])
	notBefore, nbfOK := numberInt64(claims["nbf"])
	_, hasNBF := claims["nbf"]
	active, _ := claims["active"].(bool)
	if !active || strings.TrimSpace(subject) == "" || !audienceContains(claims["aud"], s.auth.RauthyResource) || !scopeContains(claims["scope"], s.auth.RauthyRequiredScope) || (hasNBF && (!nbfOK || notBefore > now)) || !expOK || expires <= now {
		return verifiedIdentity{}, unauthenticated("access token was rejected")
	}
	request, _ = http.NewRequestWithContext(ctx, http.MethodGet, s.auth.RauthyIssuer+"/oidc/userinfo", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	var user map[string]any
	if failure := s.authUpstreamJSON(request, &user); failure != nil {
		return verifiedIdentity{}, failure
	}
	if user["sub"] != subject || user["email_verified"] != true {
		return verifiedIdentity{}, unauthenticated("verified email is required")
	}
	email, ok := normalizeEmail(user["email"])
	if !ok {
		return verifiedIdentity{}, unauthenticated("verified email is required")
	}
	name, _ := user["name"].(string)
	return verifiedIdentity{providerKey: "rauthy:" + s.auth.RauthyIssuer + ":" + subject, principalID: providerID("rpr_", "compos:rauthy-principal:v1", s.auth.RauthyIssuer, subject), email: email, name: strings.TrimSpace(name), expiresAt: expires, provider: "rauthy"}, nil
}

func (s *Server) verifyPocketBase(ctx context.Context, token string) (verifiedIdentity, *authFailure) {
	if token == "" {
		return verifiedIdentity{}, unauthenticated("token is required")
	}
	endpoint := s.auth.PocketBaseURL + "/api/collections/" + url.PathEscape(s.auth.PocketBaseCollection) + "/auth-refresh"
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	request.Header.Set("Authorization", token)
	var payload struct {
		Token  string `json:"token"`
		Record struct {
			ID       string `json:"id"`
			Email    string `json:"email"`
			Name     string `json:"name"`
			Verified bool   `json:"verified"`
		} `json:"record"`
	}
	if failure := s.authUpstreamJSON(request, &payload); failure != nil {
		return verifiedIdentity{}, failure
	}
	email, ok := normalizeEmail(payload.Record.Email)
	expires, expOK := jwtExpiry(payload.Token)
	if strings.TrimSpace(payload.Record.ID) == "" || !payload.Record.Verified || !ok || !expOK || expires <= time.Now().Unix() {
		return verifiedIdentity{}, &authFailure{status: 502, code: "upstream_error", category: "auth", retryable: false, message: "PocketBase authentication response was invalid"}
	}
	base := strings.TrimRight(s.auth.PocketBaseURL, "/")
	principal := providerID("ppr_", "compos:pocketbase-principal:v1", base, s.auth.PocketBaseCollection, payload.Record.ID)
	providerKey := providerID("pocketbase:v1:", "compos:pocketbase-identity:v1", base, s.auth.PocketBaseCollection, payload.Record.ID)
	return verifiedIdentity{providerKey: providerKey, principalID: principal, email: email, name: strings.TrimSpace(payload.Record.Name), expiresAt: expires, provider: "pocketbase"}, nil
}

func (s *Server) authUpstreamJSON(request *http.Request, target any) *authFailure {
	response, err := s.authClient.Do(request)
	if err != nil {
		return &authFailure{status: 503, code: "upstream_unavailable", category: "auth", retryable: true, message: "authentication provider is unavailable"}
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		return &authFailure{status: 503, code: "upstream_unavailable", category: "auth", retryable: true, message: "authentication provider is unavailable"}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return unauthenticated("token was rejected")
	}
	limited := io.LimitReader(response.Body, 64<<10+1)
	raw, err := io.ReadAll(limited)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if s.auth.Mode == AuthModeOAuth2 {
		decoder.UseNumber()
	}
	var trailing any
	if err != nil || len(raw) > 64<<10 || decoder.Decode(target) != nil || decoder.Decode(&trailing) != io.EOF {
		return &authFailure{status: 502, code: "upstream_error", category: "auth", retryable: false, message: "authentication provider returned invalid JSON"}
	}
	return nil
}

func (s *Server) sessionPrincipal(ctx context.Context, token string) (string, *authFailure) {
	if token == "" {
		return "", unauthenticated("missing bearer token")
	}
	var principal string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT principal_id FROM auth_sessions WHERE token_hash=? AND revoked_at IS NULL AND expires_at>?`, sessionTokenHash(token), time.Now().Unix()).Scan(&principal)
	if errors.Is(err, sql.ErrNoRows) {
		return "", unauthenticated("invalid or expired session")
	}
	if err != nil {
		return "", serverAuthFailure("session lookup failed")
	}
	_ = s.store.Exec(ctx, "auth-last-used:"+uuid.NewString(), `UPDATE auth_sessions SET last_used_at=? WHERE token_hash=?`, time.Now().UTC().Format(time.RFC3339Nano), sessionTokenHash(token))
	return principal, nil
}

func (s *Server) workspaceMemberships(ctx context.Context, principal string) ([]string, error) {
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `SELECT workspace_id FROM workspace_memberships WHERE principal_id=? ORDER BY workspace_id`, principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Server) isWorkspaceMember(ctx context.Context, workspace, principal string) bool {
	var exists int
	return s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT 1 FROM workspace_memberships WHERE workspace_id=? AND principal_id=?`, workspace, principal).Scan(&exists) == nil
}

func (s *Server) emailAllowed(email string) bool {
	_, domain, ok := strings.Cut(email, "@")
	if !ok {
		return false
	}
	for _, allowed := range s.auth.AllowedEmailDomains {
		if subtle.ConstantTimeCompare([]byte(domain), []byte(allowed)) == 1 {
			return true
		}
	}
	return false
}

func bearerToken(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
}

func randomSessionToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "csn_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func sessionTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func providerID(prefix, domain string, values ...string) string {
	buffer := bytes.NewBufferString(domain)
	for _, value := range values {
		_ = json.NewEncoder(buffer).Encode(value)
	}
	digest := sha256.Sum256(buffer.Bytes())
	return prefix + base64.RawURLEncoding.EncodeToString(digest[:])
}

func normalizeEmail(value any) (string, bool) {
	email, ok := value.(string)
	email = strings.ToLower(strings.TrimSpace(email))
	if !ok || !ascii(email) || strings.Count(email, "@") != 1 || strings.HasPrefix(email, "@") || strings.HasSuffix(email, "@") {
		return "", false
	}
	return email, true
}

func ascii(value string) bool {
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func jwtExpiry(token string) (int64, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, false
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return 0, false
	}
	return numberInt64(payload["exp"])
}

func numberInt64(value any) (int64, bool) {
	switch value := value.(type) {
	case float64:
		return int64(value), value == float64(int64(value))
	case json.Number:
		result, err := value.Int64()
		return result, err == nil
	default:
		return 0, false
	}
}

func audienceContains(value any, expected string) bool {
	if value == expected {
		return true
	}
	values, _ := value.([]any)
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func scopeContains(value any, expected string) bool {
	scopes, _ := value.(string)
	for _, scope := range strings.Fields(scopes) {
		if scope == expected {
			return true
		}
	}
	return false
}

func splitCSV(value string) []string {
	values := []string{}
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func unauthenticated(message string) *authFailure {
	return &authFailure{status: 401, code: "unauthenticated", category: "auth", message: message}
}

func forbidden(message string) *authFailure {
	return &authFailure{status: 403, code: "forbidden", category: "authz", message: message}
}

func serverAuthFailure(message string) *authFailure {
	return &authFailure{status: 500, code: "internal_error", category: "server", retryable: true, message: message}
}

func writeAuthFailure(w http.ResponseWriter, failure *authFailure) {
	problem(w, failure.status, failure.code, failure.category, failure.retryable, failure.message)
}
