package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/mrchypark/compos/internal/kernel"
)

// Authority is implemented by a trusted Goauthy/Grant adapter. No provider protocol
// or identity headers are interpreted by Compos. A nil Authority denies requests.
type Authority interface {
	// Approve the complete HTTP operation, including resource scope and credentials,
	// before returning a verified issuer/subject identity. Authentication alone is insufficient.
	AuthorizeRequest(*http.Request) (string, error)
	// Background/service calls need their own explicit external contract. This seam
	// does not turn a user Grant check or a stored principal ID into delegation.
	AuthorizeCommand(context.Context, string, kernel.Command) (kernel.Actor, error)
}

type principalContextKey struct{}

func (s *Server) authenticateRequest(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if r.Method == http.MethodGet && r.URL.Path == "/api/v1/health" {
		return r, true
	}
	if s.authority == nil {
		problem(w, 503, "external_authority_unavailable", "dependency", true, "external identity and authorization integration is not configured")
		return r, false
	}
	principal, err := s.authority.AuthorizeRequest(r)
	if err != nil && !errors.Is(err, kernel.ErrAuthorizationDenied) {
		problem(w, 503, "external_authority_unavailable", "dependency", true, "external identity and authorization integration is unavailable")
		return r, false
	}
	if err != nil || strings.TrimSpace(principal) == "" {
		problem(w, 403, "forbidden", "authz", false, "external request authorization denied")
		return r, false
	}
	return r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)), true
}

// ConfigureSearchSyncer selects a data namespace; its credential is validated by Authority.
func (s *Server) ConfigureSearchSyncer(workspace string) error {
	s.searchSyncerWorkspaceID = strings.TrimSpace(workspace)
	return nil
}
func (s *Server) searchSyncerWorkspace(w http.ResponseWriter, r *http.Request) {
	if s.searchSyncerWorkspaceID == "" {
		problem(w, 503, "workspace_unavailable", "configuration", false, "search workspace is not configured")
		return
	}
	write(w, 200, map[string]any{"workspace_id": s.searchSyncerWorkspaceID})
}
