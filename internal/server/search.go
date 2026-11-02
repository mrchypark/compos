package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

func (s *Server) EnableSearch(endpoint, index, key string) error {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("Meilisearch URL must be an absolute http(s) URL")
	}
	if index = strings.TrimSpace(index); index == "" {
		index = "compos-objects"
	}
	s.searchURL, s.searchIndex, s.searchKey = endpoint, index, key
	return nil
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	if s.searchURL == "" {
		problem(w, http.StatusServiceUnavailable, "search_unavailable", "dependency", true, "Meilisearch is not configured")
		return
	}
	var body struct {
		Q          string   `json:"q"`
		Filter     string   `json:"filter"`
		Sort       []string `json:"sort"`
		Limit      int      `json:"limit"`
		Offset     int      `json:"offset"`
		Attributes []string `json:"attributesToRetrieve"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Limit == 0 {
		body.Limit = 20
	}
	if body.Limit < 1 || body.Limit > 100 || body.Offset < 0 {
		problem(w, 400, "invalid_request", "validation", false, "limit must be 1..100 and offset must be non-negative")
		return
	}
	filter := []any{`workspace_id = ` + strconvQuote(r.PathValue("workspace_id"))}
	if strings.TrimSpace(body.Filter) != "" {
		filter = append(filter, body.Filter)
	}
	attributes := body.Attributes
	includeObjectID := len(attributes) == 0 || slices.Contains(attributes, "*") || slices.Contains(attributes, "object_id")
	if !includeObjectID {
		attributes = append(slices.Clone(attributes), "object_id")
	}
	payload, _ := json.Marshal(map[string]any{
		"q": body.Q, "filter": filter, "sort": body.Sort, "limit": body.Limit,
		"offset": body.Offset, "attributesToRetrieve": attributes,
	})
	request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.searchURL+"/indexes/"+url.PathEscape(s.searchIndex)+"/search", bytes.NewReader(payload))
	if err != nil {
		problem(w, 503, "search_unavailable", "dependency", true, err.Error())
		return
	}
	request.Header.Set("Content-Type", "application/json")
	if s.searchKey != "" {
		request.Header.Set("Authorization", "Bearer "+s.searchKey)
	}
	response, err := s.searchClient.Do(request)
	if err != nil {
		problem(w, 503, "search_unavailable", "dependency", true, err.Error())
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		problem(w, 502, "search_unavailable", "dependency", true, "Meilisearch rejected the request")
		return
	}
	var result map[string]any
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		problem(w, 502, "search_unavailable", "dependency", true, "Meilisearch returned invalid JSON")
		return
	}
	// Projection IDs are engine-safe; the API keeps canonical object IDs.
	hits, _ := result["hits"].([]any)
	objects := make([]any, 0, len(hits))
	for _, value := range hits {
		hit, _ := value.(map[string]any)
		objectID, ok := hit["object_id"].(string)
		if !ok || strings.TrimSpace(objectID) == "" {
			continue
		}
		if _, hasID := hit["id"]; hasID {
			hit["id"] = objectID
		}
		if !includeObjectID {
			delete(hit, "object_id")
		}
		objects = append(objects, hit)
	}
	result["hits"] = objects
	// Engine totals and facets can include legacy documents outside this page.
	for _, key := range []string{"estimatedTotalHits", "totalHits", "totalPages", "facetDistribution", "facetStats", "semanticHitCount"} {
		delete(result, key)
	}
	result["returnedHits"] = len(objects)
	write(w, 200, map[string]any{"data": result})
}

func strconvQuote(value string) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
