package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

const webMaxErrorMessageBytes = 1 << 10

//go:embed all:webdist
var webDistFS embed.FS

type inertiaPage struct {
	Component string         `json:"component"`
	Props     map[string]any `json:"props"`
	URL       string         `json:"url"`
	Version   string         `json:"version"`
}

type webManifestEntry struct {
	File    string   `json:"file"`
	CSS     []string `json:"css"`
	IsEntry bool     `json:"isEntry"`
}

func runWeb(args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	_ = journal
	flags := flag.NewFlagSet("web", flag.ContinueOnError)
	flags.SetOutput(deps.stderr)
	listen := flags.String("listen", "127.0.0.1:8787", "local web listen address")
	noOpen := flags.Bool("no-open", false, "do not open a browser")
	enableWrites := flags.Bool("enable-writes", false, "allow browser form submissions")
	server := flags.String("server", "", "Compos server URL")
	workspace := flags.String("workspace", "", "workspace id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	session, err := sessionForCommand(deps, *server, *workspace)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	assets, err := fs.Sub(webDistFS, "webdist")
	if err != nil {
		return err
	}
	handler := newWebHandlerWithOptions(deps, session, assets, journal, *enableWrites)
	localURL := "http://" + listener.Addr().String()
	if !*noOpen {
		if err := openWebBrowser(localURL); err != nil {
			_, _ = fmt.Fprintf(deps.stdout, "Open this URL in your browser: %s\n", localURL)
		}
	}
	_, _ = fmt.Fprintf(deps.stdout, "compos web listening on %s\n", localURL)
	serverInstance := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	err = serverInstance.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func openWebBrowser(target string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", target)
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		command = exec.Command("xdg-open", target)
	}
	if browser := strings.TrimSpace(os.Getenv("BROWSER")); browser != "" {
		parts := strings.Fields(browser)
		if len(parts) > 0 {
			command = exec.Command(parts[0], append(parts[1:], target)...)
		}
	}
	if err := command.Start(); err != nil {
		return fmt.Errorf("open web console: %w", err)
	}
	return nil
}

// newWebHandler is retained for callers that need the read-only default.
func newWebHandler(deps runtimeDeps, session cliSession, _ webManifestEntry, assets fs.FS) http.Handler {
	return newWebHandlerWithOptions(deps, session, assets, ledger.NewMemoryJournal(), false)
}

func newWebHandlerWithOptions(deps runtimeDeps, session cliSession, assets fs.FS, journal ledger.CommandAttemptJournal, enableWrites bool) http.Handler {
	mux := http.NewServeMux()
	_ = assets // Presentations are deliberately script-free; embedded browser assets are not served.
	csrf, err := newWebCSRFToken()
	if err != nil {
		panic(err)
	}
	app := webSSR{deps: deps, session: session, journal: journal, writes: enableWrites, csrf: csrf}
	mux.HandleFunc("/", app.serveHTTP)
	return localHostOnlyHandler(mux)
}

func isAllowedLocalWebHost(hostport string) bool {
	host := hostport
	if splitHost, _, err := net.SplitHostPort(hostport); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	return strings.EqualFold(host, "localhost") || host == "127.0.0.1" || host == "::1"
}

func localHostOnlyHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
		if !isAllowedLocalWebHost(r.Host) || !isAllowedLocalRemote(r.RemoteAddr) {
			http.Error(w, "local access only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isAllowedLocalRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func renderDashboardPage(w http.ResponseWriter, r *http.Request, deps runtimeDeps, session cliSession, manifest webManifestEntry) {
	props := map[string]any{
		"session": map[string]any{
			"server":       session.Server,
			"workspace_id": session.WorkspaceID,
			"principal_id": session.PrincipalID,
		},
		"commandSurface": map[string]any{"workspace_id": session.WorkspaceID, "nouns": []any{}},
		"objects":        map[string]any{"data": []any{}},
		"errors":         map[string]string{},
	}
	if surface, err := webGetJSON(r.Context(), deps, session, fmt.Sprintf("%s/api/v1/workspaces/%s/command-surface", trimSlash(session.Server), url.PathEscape(session.WorkspaceID))); err == nil {
		props["commandSurface"] = surface
	} else {
		props["errors"] = appendWebError(props["errors"], "command_surface", err)
	}
	if objects, err := webGetJSON(r.Context(), deps, session, fmt.Sprintf("%s/api/v1/workspaces/%s/objects", trimSlash(session.Server), url.PathEscape(session.WorkspaceID))); err == nil {
		props["objects"] = objects
	} else {
		props["errors"] = appendWebError(props["errors"], "objects", err)
	}
	page := inertiaPage{
		Component: "Dashboard",
		Props:     props,
		URL:       r.URL.RequestURI(),
		Version:   version,
	}
	writeInertiaPage(w, r, page, manifest)
}

func appendWebError(value any, key string, err error) map[string]string {
	result := map[string]string{}
	if existing, ok := value.(map[string]string); ok {
		for existingKey, existingValue := range existing {
			result[existingKey] = existingValue
		}
	}
	result[key] = err.Error()
	return result
}

func webGetJSON(ctx context.Context, deps runtimeDeps, session cliSession, endpoint string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	setBearerCredential(req, session.Credential)
	resp, err := deps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := readHTTPBody(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := truncateBodyForMessage(body, webMaxErrorMessageBytes)
		return nil, fmt.Errorf("server read failed: HTTP %d: %s", resp.StatusCode, message)
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("server read returned trailing JSON data")
	}
	if result == nil {
		return nil, fmt.Errorf("server read returned null JSON object")
	}
	return result, nil
}

func writeInertiaPage(w http.ResponseWriter, r *http.Request, page inertiaPage, manifest webManifestEntry) {
	if strings.EqualFold(r.Header.Get("X-Inertia"), "true") {
		w.Header().Set("X-Inertia", "true")
		w.Header().Set("Vary", "X-Inertia")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
		return
	}
	body, _ := json.Marshal(page)
	var cssLinks strings.Builder
	for _, css := range manifest.CSS {
		_, _ = fmt.Fprintf(&cssLinks, `<link rel="stylesheet" href="/%s">`, html.EscapeString(css))
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Compos Console</title>
%s
<script type="module" src="/%s"></script>
</head>
<body>
<script data-page="app" type="application/json">%s</script>
<div id="app"></div>
</body>
</html>`, cssLinks.String(), html.EscapeString(manifest.File), string(body))
}

func loadWebManifest() (webManifestEntry, error) {
	body, err := webDistFS.ReadFile("webdist/.vite/manifest.json")
	if err != nil {
		return webManifestEntry{}, err
	}
	var manifest map[string]webManifestEntry
	if err := json.Unmarshal(body, &manifest); err != nil {
		return webManifestEntry{}, err
	}
	for _, entry := range manifest {
		if entry.IsEntry && entry.File != "" {
			return entry, nil
		}
	}
	if len(manifest) == 1 {
		for _, entry := range manifest {
			if entry.File != "" {
				return entry, nil
			}
		}
	}
	return webManifestEntry{}, fmt.Errorf("web manifest missing app entry")
}
