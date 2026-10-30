package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func runSearch(ctx context.Context, args []string, deps runtimeDeps) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	server := fs.String("server", "", "Compos server URL")
	workspace := fs.String("workspace", "", "workspace id")
	query := fs.String("q", "", "Search query")
	limit := fs.Int("limit", 20, "Maximum number of results")
	filter := fs.String("filter", "", "Filter expression (e.g., type=contract)")
	sort := fs.String("sort", "", "Sort expression (e.g., updated_at:desc)")
	offset := fs.Int("offset", 0, "Offset for pagination")
	attributes := fs.String("attributes", "", "Comma-separated list of attributes to retrieve")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *query == "" && *filter == "" {
		return fmt.Errorf("either --q or --filter is required")
	}

	session, err := sessionForCommand(deps, *server, *workspace)
	if err != nil {
		return err
	}

	body := map[string]interface{}{
		"limit":  *limit,
		"offset": *offset,
	}
	if *query != "" {
		body["q"] = *query
	}
	if *filter != "" {
		body["filter"] = *filter
	}
	if *sort != "" {
		body["sort"] = strings.Split(*sort, ",")
	}
	if *attributes != "" {
		body["attributesToRetrieve"] = strings.Split(*attributes, ",")
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal search request: %w", err)
	}

	searchURL := fmt.Sprintf("%s/api/v1/workspaces/%s/search",
		trimSlash(session.Server), url.PathEscape(session.WorkspaceID))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, searchURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setBearerCredential(req, session.Credential)

	resp, err := deps.client.Do(req)
	if err != nil {
		return fmt.Errorf("search request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := readHTTPBody(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("search failed with status %d: %s", resp.StatusCode, truncateBodyForMessage(respBody, maxHTTPErrorMessageBytes))
	}

	_, err = fmt.Fprintln(deps.stdout, string(respBody))
	return err
}

func searchUsage() string {
	return strings.TrimSpace(`
usage: compos search [--q QUERY] [options]

Search objects in the workspace index.

options:
  --server URL        Compos server URL
  --workspace ID      workspace id
  --q QUERY           Search query text
  --limit N           Maximum results (default: 20)
  --filter EXPR       Filter expression (e.g., type=contract,state=active)
  --sort EXPR         Sort expression (e.g., updated_at:desc)
  --offset N          Offset for pagination
  --attributes LIST   Comma-separated attributes to retrieve

examples:
  compos search --q "contract"
  compos search --q "contract" --filter "type=contract" --sort "updated_at:desc"
  compos search --filter "state=active" --limit 50
`)
}
