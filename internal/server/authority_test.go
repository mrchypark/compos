package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
)

type testAuthority struct {
	request func(*http.Request) (string, error)
	command kernel.CommandAuthorizer
}

func (a testAuthority) AuthorizeRequest(r *http.Request) (string, error) {
	if a.request != nil {
		return a.request(r)
	}
	return "dev", nil
}

func (a testAuthority) AuthorizeCommand(ctx context.Context, principal string, c kernel.Command) (kernel.Actor, error) {
	if a.command != nil {
		return a.command(ctx, principal, c)
	}
	if c.RequestedActor != nil {
		return *c.RequestedActor, nil
	}
	return kernel.Actor{Type: "user", ID: principal}, nil
}

func newTestServer(s *store.Store) *Server {
	return NewWithAuthority(s, testAuthority{})
}

func authRequest(t *testing.T, handler http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

var errTestAuthorityDenied = kernel.ErrAuthorizationDenied

func bearerToken(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}

func typedObjectAuthorityFixture(t *testing.T, handler *Server) string {
	t.Helper()
	const token = "typed-object-external-token"
	authority := testAuthority{request: func(r *http.Request) (string, error) {
		if bearerToken(r) != token {
			return "", errTestAuthorityDenied
		}
		return "dev", nil
	}, command: func(_ context.Context, principal string, c kernel.Command) (kernel.Actor, error) {
		if c.IdempotencyKey == "typed_archive_denied" {
			return kernel.Actor{}, errTestAuthorityDenied
		}
		return kernel.Actor{Type: "user", ID: principal}, nil
	}}
	handler.authority = authority
	handler.kernel.AuthorizeCommand = authority.AuthorizeCommand
	return token
}

func TestExternalAuthorityFailsClosedWithoutTrustingHeaders(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "authority.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := New(s)
	defer handler.Close()
	for _, path := range []string{"/api/v1/workspaces", "/api/v1/auth/session", "/internal/v1/conductor/callbacks"} {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"name":"Spoofed","idempotency_key":"spoofed"}`))
		for key, value := range map[string]string{"Authorization": "Bearer forged", "X-Principal-ID": "admin", "X-User-ID": "admin", "X-Organization-ID": "org", "X-Grant-App-Auth-Key": "forged"} {
			r.Header.Set(key, value)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("unconfigured %s: status=%d body=%s", path, w.Code, w.Body.String())
		}
	}
	if w := request(t, handler, http.MethodGet, "/api/v1/health", nil); w.Code != http.StatusOK {
		t.Fatalf("health=%d", w.Code)
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("spoofed workspaces=%d err=%v", count, err)
	}
}

func TestExternalAuthorityControlsReadsAndCommands(t *testing.T) {
	s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "authority.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deny := testAuthority{request: func(*http.Request) (string, error) { return "", errTestAuthorityDenied }}
	handler := NewWithAuthority(s, deny)
	defer handler.Close()
	if w := request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/objects", nil); w.Code != http.StatusForbidden {
		t.Fatalf("denied read=%d", w.Code)
	}
	handler.authority = testAuthority{request: func(*http.Request) (string, error) { return "", nil }}
	if w := request(t, handler, http.MethodPost, "/api/v1/workspaces", nil); w.Code != http.StatusForbidden {
		t.Fatalf("empty identity=%d", w.Code)
	}
	for _, path := range []string{"/api/v1/auth/session", "/api/v1/auth/logout", "/api/v1/workspaces/wks_1/capabilities"} {
		handler.authority = testAuthority{}
		if w := request(t, handler, http.MethodPost, path, nil); w.Code != http.StatusNotFound {
			t.Fatalf("retired route %s=%d", path, w.Code)
		}
	}
}

func TestExternalAuthorityHTTPDistinguishesDenialFromOutage(t *testing.T) {
	for _, failure := range []error{kernel.ErrAuthorizationDenied, context.Canceled, context.DeadlineExceeded, errors.New("private transport detail")} {
		t.Run(failure.Error(), func(t *testing.T) {
			s, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "authority.sqlite")})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			handler := NewWithAuthority(s, testAuthority{request: func(*http.Request) (string, error) { return "", failure }})
			defer handler.Close()
			w := request(t, handler, http.MethodGet, "/api/v1/workspaces/wks_1/objects", nil)
			want := http.StatusServiceUnavailable
			if errors.Is(failure, kernel.ErrAuthorizationDenied) {
				want = http.StatusForbidden
			}
			if w.Code != want || strings.Contains(w.Body.String(), "private transport detail") || (want == 503 && !strings.Contains(w.Body.String(), `"retryable":true`)) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
