package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/compos/internal/store"
)

func TestSearchProxiesMeilisearchWithWorkspaceFilter(t *testing.T) {
	var received map[string]any
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/indexes/compos/search" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("request path=%q authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": []any{map[string]any{"id": "core:customer", "type": "other"}}, "estimatedTotalHits": 1})
	}))
	defer search.Close()

	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "search.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := newTestServer(db)
	defer handler.Close()
	if err := handler.EnableSearch(search.URL, "compos", "secret"); err != nil {
		t.Fatal(err)
	}
	userFilter := `type = "other") OR workspace_id = "wks_other" OR (type = "other"`
	response := request(t, handler, "POST", "/api/v1/workspaces/wks_1/search", map[string]any{"q": "customer", "filter": userFilter, "limit": 10})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"returnedHits":0`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	filter, _ := received["filter"].([]any)
	if len(filter) != 2 || filter[0] != `workspace_id = "wks_1"` || filter[1] != userFilter {
		t.Fatalf("workspace filter not independently enforced: %#v", filter)
	}
}

func TestSearchPreservesCanonicalObjectIDAndRequestedAttributes(t *testing.T) {
	const canonicalID = "obj_example/type/record/v1"
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Attributes []string `json:"attributesToRetrieve"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		all := len(body.Attributes) == 0 || slices.Contains(body.Attributes, "*")
		object, other := map[string]any{}, map[string]any{}
		for key, value := range map[string]any{"id": "obj_engine_hash", "object_id": canonicalID, "title": "Customer"} {
			if all || slices.Contains(body.Attributes, key) {
				object[key] = value
			}
		}
		if all || slices.Contains(body.Attributes, "id") {
			other["id"] = "external_engine_id"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": []any{object, other}})
	}))
	defer search.Close()
	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "search.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := newTestServer(db)
	defer handler.Close()
	if err := handler.EnableSearch(search.URL, "compos", "secret"); err != nil {
		t.Fatal(err)
	}
	for _, attributes := range [][]string{nil, {"id"}, {"id", "object_id"}, {"title"}, {"object_id"}, {"*"}} {
		t.Run(strings.Join(attributes, ","), func(t *testing.T) {
			response := request(t, handler, "POST", "/api/v1/workspaces/wks_1/search", map[string]any{"q": "", "attributesToRetrieve": attributes})
			if response.Code != 200 {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var result struct {
				Data struct {
					Hits []map[string]any `json:"hits"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			all := len(attributes) == 0 || slices.Contains(attributes, "*")
			if len(result.Data.Hits) != 1 {
				t.Fatalf("non-object hit returned: %#v", result.Data.Hits)
			}
			object := result.Data.Hits[0]
			if all || slices.Contains(attributes, "id") {
				if object["id"] != canonicalID {
					t.Fatalf("canonical object ID changed: %#v", result.Data.Hits)
				}
			} else if _, ok := object["id"]; ok {
				t.Fatal("unrequested id returned")
			}
			_, hasObjectID := object["object_id"]
			if hasObjectID != (all || slices.Contains(attributes, "object_id")) {
				t.Fatalf("object_id attribute selection not preserved: %#v", object)
			}
		})
	}
}

func TestSearchExcludesLegacyKnowledgeAfterServerOnlyUpgrade(t *testing.T) {
	search := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Filter []string `json:"filter"`
			Offset int      `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		hits := []any{map[string]any{"id": "obj_hash", "object_id": "obj_knowledge", "type": "knowledge"}}
		if slices.Contains(body.Filter, "object_id EXISTS") {
			http.Error(w, "object_id is not filterable on this existing index", http.StatusBadRequest)
			return
		}
		hits = append(hits, map[string]any{"id": "knw_hash", "knowledge_id": "core:legacy", "type": "knowledge"}, map[string]any{"object_id": ""}, map[string]any{"object_id": " "}, map[string]any{"object_id": 42}, nil)
		if body.Offset == 1 {
			hits = hits[1:]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": hits, "estimatedTotalHits": 99, "totalHits": 99, "totalPages": 5, "facetDistribution": map[string]any{"type": map[string]any{"knowledge": 98}}, "facetStats": map[string]any{}, "semanticHitCount": 99})
	}))
	defer search.Close()
	db, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "search.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := newTestServer(db)
	defer handler.Close()
	if err := handler.EnableSearch(search.URL, "compos", ""); err != nil {
		t.Fatal(err)
	}
	response := request(t, handler, "POST", "/api/v1/workspaces/wks_1/search", map[string]any{"filter": `type = "knowledge"`})
	if response.Code != 200 || strings.Contains(response.Body.String(), "knw_hash") || !strings.Contains(response.Body.String(), `"id":"obj_knowledge"`) {
		t.Fatalf("search: status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Data["returnedHits"] != float64(1) {
		t.Fatalf("incorrect page count: %s", response.Body.String())
	}
	for _, key := range []string{"estimatedTotalHits", "totalHits", "totalPages", "facetDistribution", "facetStats", "semanticHitCount"} {
		if _, exists := result.Data[key]; exists {
			t.Fatalf("legacy metadata leaked: %s", key)
		}
	}
	response = request(t, handler, "POST", "/api/v1/workspaces/wks_1/search", map[string]any{"offset": 1})
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"hits":[]`) || !strings.Contains(response.Body.String(), `"returnedHits":0`) {
		t.Fatalf("legacy-only page: status=%d body=%s", response.Code, response.Body.String())
	}
}
