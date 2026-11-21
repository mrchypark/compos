package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/compos/internal/store"
)

func newTestServer(s *store.Store) *Server { return New(s) }

func authRequest(t *testing.T, handler http.Handler, method, path, _ string, body any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, r)
	return recorder
}

func typedObjectAuthFixture(*testing.T, *Server) string { return "" }
