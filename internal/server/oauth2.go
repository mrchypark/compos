package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/compos/internal/store"
)

// This is RFC 7662 resource-server authentication, not a token exchange grant.
func (s *Server) discoverOAuth2(config AuthConfig) (string, string, error) {
	issuer, err := oauth2URL(config.OAuth2Issuer)
	if err != nil || config.OAuth2Resource == "" || config.OAuth2ClientID == "" || config.OAuth2ClientSecret == "" || len(strings.Fields(config.OAuth2RequiredScope)) != 1 || config.OAuth2RequiredScope == "openid" {
		return "", "", errors.New("OAuth2 requires a valid issuer, resource, confidential introspection client and one application scope distinct from openid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(config.OAuth2Issuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return "", "", errors.New("invalid OAuth2 discovery URL")
	}
	var metadata struct {
		Issuer        string `json:"issuer"`
		Introspection string `json:"introspection_endpoint"`
		UserInfo      string `json:"userinfo_endpoint"`
	}
	if failure := s.authUpstreamJSON(req, &metadata); failure != nil {
		return "", "", errors.New("OAuth2 discovery unavailable or invalid")
	}
	endpoint, err := oauth2URL(metadata.Introspection)
	if metadata.Issuer != config.OAuth2Issuer || err != nil || (endpoint.Scheme == "http" && (issuer.Scheme != "http" || endpoint.Host != issuer.Host)) {
		return "", "", errors.New("OAuth2 discovery issuer or introspection endpoint rejected")
	}
	userinfo, err := oauth2URL(metadata.UserInfo)
	if err != nil || (userinfo.Scheme == "http" && (issuer.Scheme != "http" || userinfo.Host != issuer.Host)) {
		return "", "", errors.New("OAuth2 discovery UserInfo endpoint rejected")
	}
	return metadata.Introspection, metadata.UserInfo, nil
}

func oauth2URL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
		return nil, errors.New("OAuth2 URL requires HTTPS (HTTP only on loopback), no credentials, query or fragment")
	}
	return u, nil
}

func (s *Server) requestPrincipal(ctx context.Context, token string) (string, *authFailure) {
	if s.auth.Mode != AuthModeOAuth2 {
		return s.sessionPrincipal(ctx, token)
	}
	identity, failure := s.verifyOAuth2(ctx, token)
	return identity.principalID, failure
}

func (s *Server) verifyOAuth2(ctx context.Context, token string) (verifiedIdentity, *authFailure) {
	if token == "" || len(token) > 64<<10 || strings.ContainsAny(token, "\r\n\t ") {
		return verifiedIdentity{}, unauthenticated("invalid bearer token")
	}
	form := url.Values{"token": {token}, "token_type_hint": {"access_token"}}
	req, err := http.NewRequestWithContext(ctx, "POST", s.oauth2IntrospectionEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return verifiedIdentity{}, serverAuthFailure("OAuth2 introspection is not configured")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(s.auth.OAuth2ClientID), url.QueryEscape(s.auth.OAuth2ClientSecret))
	var claims map[string]any
	if failure := s.authUpstreamJSON(req, &claims); failure != nil {
		return verifiedIdentity{}, failure
	}
	sub, _ := claims["sub"].(string)
	exp, validExp := numberInt64(claims["exp"])
	now := time.Now().Unix()
	if claims["active"] != true || sub == "" || strings.TrimSpace(sub) != sub || !validExp || exp <= now || !audienceContains(claims["aud"], s.auth.OAuth2Resource) || !scopeContains(claims["scope"], s.auth.OAuth2RequiredScope) || !scopeContains(claims["scope"], "openid") {
		return verifiedIdentity{}, unauthenticated("access token was rejected")
	}
	if iss, exists := claims["iss"]; exists && iss != s.auth.OAuth2Issuer {
		return verifiedIdentity{}, unauthenticated("access token issuer was rejected")
	}
	if value, exists := claims["nbf"]; exists {
		nbf, valid := numberInt64(value)
		if !valid || nbf > now {
			return verifiedIdentity{}, unauthenticated("access token validity was rejected")
		}
	}
	if kind, exists := claims["token_type"]; exists && kind != "Bearer" && kind != "bearer" {
		return verifiedIdentity{}, unauthenticated("unsupported token type")
	}
	if _, bound := claims["cnf"]; bound {
		return verifiedIdentity{}, unauthenticated("sender-constrained tokens are not supported")
	}
	// token_type_hint is advisory in RFC 7662 and may also resolve refresh tokens.
	// A matching OIDC UserInfo response establishes a usable end-user access token.
	req, err = http.NewRequestWithContext(ctx, "GET", s.oauth2UserInfoEndpoint, nil)
	if err != nil {
		return verifiedIdentity{}, serverAuthFailure("OAuth2 UserInfo is not configured")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	var user struct {
		Subject string `json:"sub"`
	}
	if failure := s.authUpstreamJSON(req, &user); failure != nil {
		return verifiedIdentity{}, failure
	}
	if user.Subject != sub || exp <= time.Now().Unix() {
		return verifiedIdentity{}, unauthenticated("access token subject or validity was rejected")
	}
	// Preserve identities from the existing Rauthy-compatible issuer/sub adapter.
	// The namespace is a storage compatibility detail, not an IdP restriction.
	return verifiedIdentity{principalID: providerID("rpr_", "compos:rauthy-principal:v1", s.auth.OAuth2Issuer, sub), providerKey: "rauthy:" + s.auth.OAuth2Issuer + ":" + sub, provider: "oauth2", expiresAt: exp}, nil
}

// Identity registration never enrolls a workspace, creates a Person, or issues a session.
func (s *Server) authIdentity(w http.ResponseWriter, r *http.Request) {
	if s.auth.Mode != AuthModeOAuth2 {
		http.NotFound(w, r)
		return
	}
	id := principal(r) // Already verified by the authentication middleware.
	identity, verified := r.Context().Value(verifiedIdentityContextKey{}).(verifiedIdentity)
	if !verified || identity.principalID != id || identity.providerKey == "" {
		writeAuthFailure(w, unauthenticated("verified identity is required"))
		return
	}
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	// The principal is a domain-separated hash of the configured issuer and verified sub.
	// Email is deliberately absent: it is not an account-linking key.
	if err := s.store.Exec(r.Context(), "oauth2-identity:"+uuid.NewString(), `INSERT OR IGNORE INTO auth_provider_identities(principal_id,provider_identity_key,email,email_verified,created_at,updated_at,last_login_at) VALUES(?,?,'',0,?,?,?)`, id, identity.providerKey, stamp, stamp, stamp); err != nil {
		dbError(w, err)
		return
	}
	var storedKey string
	err := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT provider_identity_key FROM auth_provider_identities WHERE principal_id=?`, id).Scan(&storedKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		dbError(w, err)
		return
	}
	if errors.Is(err, sql.ErrNoRows) || storedKey != identity.providerKey {
		problem(w, 409, "identity_conflict", "conflict", false, "identity mapping requires administrative resolution")
		return
	}
	memberships, err := s.workspaceMemberships(r.Context(), id)
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": map[string]any{"principal_id": id, "workspace_memberships": memberships}})
}
