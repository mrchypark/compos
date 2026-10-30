package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

const webFormMaxBytes = 64 << 10

type webSSR struct {
	deps    runtimeDeps
	session cliSession
	journal ledger.CommandAttemptJournal
	writes  bool
	csrf    string
}
type webNoun struct {
	ID, Key, Singular, Plural string
	Schema                    map[string]any
}

type webCommandReceipt struct {
	CommandID string `json:"command_id"`
	Outcome   string `json:"outcome"`
	Aggregate *struct {
		Type    string `json:"type"`
		ID      string `json:"id"`
		Version int64  `json:"version"`
	} `json:"aggregate"`
}

func newWebCSRFToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (a webSSR) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	parts, ok := splitWebPath(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 0 {
		if r.Method == http.MethodPost {
			http.NotFound(w, r)
			return
		}
		surface, err := a.surface(r.Context())
		if err != nil {
			a.fallback(w, "Console unavailable")
			return
		}
		a.index(w, surface)
		return
	}
	if parts[0] != "objects" {
		http.NotFound(w, r)
		return
	}
	parts = parts[1:]
	if r.Method == http.MethodPost {
		a.post(w, r, parts)
		return
	}
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	surface, err := a.surface(r.Context())
	if err != nil {
		a.fallback(w, "Console unavailable")
		return
	}
	noun, ok := surface[parts[0]]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if _, selected := r.URL.Query()["template"]; selected {
		http.Error(w, "template selection is retired", http.StatusGone)
		return
	}
	switch len(parts) {
	case 1:
		a.list(w, r, noun)
	case 2:
		if parts[1] == "new" {
			if !a.writes {
				http.NotFound(w, r)
				return
			}
			a.form(w, r, noun, nil, false)
		} else {
			a.detail(w, r, noun, parts[1])
		}
	case 3:
		if parts[2] == "edit" {
			if !a.writes {
				http.NotFound(w, r)
				return
			}
			obj, err := a.object(r.Context(), parts[1])
			if err != nil || str(obj["object_type_key"]) != noun.Key {
				http.NotFound(w, r)
				return
			}
			a.form(w, r, noun, obj, true)
		} else {
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}
func splitWebPath(path string) ([]string, bool) {
	if path == "" || path == "/" {
		return nil, true
	}
	if !strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return nil, false
	}
	rawParts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	parts := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		if raw == "" {
			return nil, false
		}
		part, err := url.PathUnescape(raw)
		if err != nil || part == "" || part == "." || part == ".." {
			return nil, false
		}
		parts = append(parts, part)
	}
	return parts, true
}
func (a webSSR) surface(ctx context.Context) (map[string]webNoun, error) {
	raw, err := webGetJSON(ctx, a.deps, a.session, fmt.Sprintf("%s/api/v1/workspaces/%s/command-surface", trimSlash(a.session.Server), url.PathEscape(a.session.WorkspaceID)))
	if err != nil {
		return nil, err
	}
	out := map[string]webNoun{}
	nouns, _ := raw["nouns"].([]any)
	for _, v := range nouns {
		m, _ := v.(map[string]any)
		key, _ := m["key"].(string)
		schema, _ := m["field_schema"].(map[string]any)
		if key != "" {
			out[key] = webNoun{ID: str(m["object_type_id"]), Key: key, Singular: str(m["singular"]), Plural: str(m["plural"]), Schema: schema}
		}
	}
	return out, nil
}
func str(v any) string { s, _ := v.(string); return s }

func positiveJSONInt64(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	value, err := n.Int64()
	return value, err == nil && value > 0
}
func (a webSSR) object(ctx context.Context, id string) (map[string]any, error) {
	raw, err := webGetJSON(ctx, a.deps, a.session, fmt.Sprintf("%s/api/v1/workspaces/%s/objects/%s", trimSlash(a.session.Server), url.PathEscape(a.session.WorkspaceID), url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	v, _ := raw["data"].(map[string]any)
	if v == nil {
		return nil, fmt.Errorf("missing object")
	}
	return v, nil
}
func (a webSSR) list(w http.ResponseWriter, r *http.Request, n webNoun) {
	raw, err := webGetJSON(r.Context(), a.deps, a.session, fmt.Sprintf("%s/api/v1/workspaces/%s/objects?object_type=%s", trimSlash(a.session.Server), url.PathEscape(a.session.WorkspaceID), url.QueryEscape(n.Key)))
	if err != nil {
		a.fallback(w, "Objects unavailable")
		return
	}
	rows, _ := raw["data"].([]any)
	a.page(w, n.Plural, func(b *strings.Builder) {
		if a.writes {
			fmt.Fprintf(b, `<p><a href="/objects/%s/new">Create %s</a></p>`, html.EscapeString(webPathSegment(n.Key)), html.EscapeString(n.Singular))
		}
		b.WriteString("<ul>")
		for _, row := range rows {
			obj, _ := row.(map[string]any)
			id := str(obj["id"])
			fmt.Fprintf(b, `<li><a href="/objects/%s/%s">%s</a></li>`, html.EscapeString(webPathSegment(n.Key)), html.EscapeString(webPathSegment(id)), html.EscapeString(str(obj["title"])))
		}
		b.WriteString("</ul>")
	})
}
func (a webSSR) detail(w http.ResponseWriter, r *http.Request, n webNoun, id string) {
	obj, err := a.object(r.Context(), id)
	if err != nil || str(obj["object_type_key"]) != n.Key {
		http.NotFound(w, r)
		return
	}
	a.page(w, str(obj["title"]), func(b *strings.Builder) {
		fmt.Fprintf(b, "<h1>%s</h1><dl>", html.EscapeString(str(obj["title"])))
		valueKeys := make([]string, 0, len(values(obj)))
		for key := range values(obj) {
			valueKeys = append(valueKeys, key)
		}
		sort.Strings(valueKeys)
		for _, key := range valueKeys {
			value := values(obj)[key]
			fmt.Fprintf(b, "<dt>%s</dt><dd>%s</dd>", html.EscapeString(key), html.EscapeString(fmt.Sprint(value)))
		}
		b.WriteString("</dl>")
		if a.writes {
			fmt.Fprintf(b, `<p><a href="/objects/%s/%s/edit">Edit</a></p>`, html.EscapeString(webPathSegment(n.Key)), html.EscapeString(webPathSegment(id)))
		}
	})
}
func (a webSSR) form(w http.ResponseWriter, r *http.Request, n webNoun, obj map[string]any, edit bool) {
	title := "Create " + n.Singular
	action := "/objects/" + webPathSegment(n.Key)
	if edit {
		title = "Edit " + str(obj["title"])
		action += "/" + webPathSegment(str(obj["id"]))
	}
	a.page(w, title, func(b *strings.Builder) {
		fmt.Fprintf(b, `<form method="post" action="%s"><input type="hidden" name="_csrf" value="%s"><label for="/title">Title</label><input id="/title" name="/title" type="text" value="%s">`, html.EscapeString(action), html.EscapeString(a.csrf), html.EscapeString(str(obj["title"])))
		if formSchemaSupported(n.Schema) {
			b.WriteString(`<input type="hidden" name="_compos_form_mode" value="native">`)
			renderNativeFields(b, n.Schema, obj)
			b.WriteString(`<button type="submit">Submit</button>`)
		}
		b.WriteString(`</form>`)
	})
}
func (a webSSR) index(w http.ResponseWriter, nouns map[string]webNoun) {
	a.page(w, "Compos", func(b *strings.Builder) {
		b.WriteString("<ul>")
		keys := make([]string, 0, len(nouns))
		for key := range nouns {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			n := nouns[key]
			fmt.Fprintf(b, `<li><a href="/objects/%s">%s</a></li>`, html.EscapeString(webPathSegment(n.Key)), html.EscapeString(n.Plural))
		}
		b.WriteString("</ul>")
	})
}
func (a webSSR) page(w http.ResponseWriter, title string, body func(*strings.Builder)) {
	var b strings.Builder
	b.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">")
	fmt.Fprintf(&b, "<title>%s</title></head><body><main>", html.EscapeString(title))
	body(&b)
	b.WriteString("</main></body></html>")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}
func (a webSSR) fallback(w http.ResponseWriter, message string) {
	a.page(w, "Compos", func(b *strings.Builder) { a.fallbackBody(b, message) })
}
func (a webSSR) fallbackBody(b *strings.Builder, message string) {
	fmt.Fprintf(b, "<p>%s</p>", html.EscapeString(message))
}

func (a webSSR) post(w http.ResponseWriter, r *http.Request, p []string) {
	if !a.writes {
		http.NotFound(w, r)
		return
	}
	if len(p) != 1 && len(p) != 2 {
		http.NotFound(w, r)
		return
	}
	if err := a.checkPost(r); err != nil {
		http.Error(w, "invalid form submission", http.StatusForbidden)
		return
	}
	nouns, err := a.surface(r.Context())
	if err != nil {
		a.fallback(w, "Console unavailable")
		return
	}
	n, ok := nouns[p[0]]
	if !ok {
		http.NotFound(w, r)
		return
	}
	fields, err := decodeWebForm(r, n.Schema)
	if err != nil {
		http.Error(w, "invalid form submission", http.StatusBadRequest)
		return
	}
	if fields["_csrf"] != a.csrf {
		http.Error(w, "invalid form submission", http.StatusForbidden)
		return
	}
	nativeMode, hasNativeMode := fields["_compos_form_mode"].(string)
	if !hasNativeMode || nativeMode != "native" || !formSchemaSupported(n.Schema) {
		http.Error(w, "invalid form submission", http.StatusBadRequest)
		return
	}
	delete(fields, "_csrf")
	delete(fields, "_compos_form_mode")
	var command commandEnvelope
	expectedReceiptVersion := int64(1)
	opts := mutationOptions{server: &a.session.Server, workspace: &a.session.WorkspaceID, actor: &a.session.PrincipalID, idempotencyKey: ptr("web_" + a.deps.newID("idem"))}
	if len(p) == 1 {
		title, ok := fields["/title"].(string)
		if !ok || title == "" {
			http.Error(w, "title is required", 400)
			return
		}
		command = baseCommand(a.deps, opts, "object.create", "object_instance", nil)
		command.Payload = map[string]any{"object_type_key": n.Key, "title": title, "values": pointerValues(fields)}
	} else {
		obj, err := a.object(r.Context(), p[1])
		if err != nil || str(obj["id"]) != p[1] || str(obj["object_type_key"]) != n.Key {
			http.NotFound(w, r)
			return
		}
		values, _ := obj["values"].(map[string]any)
		for k, v := range pointerValues(fields) {
			values[k] = v
		}
		id := str(obj["id"])
		version, ok := positiveJSONInt64(obj["version"])
		if !ok {
			http.Error(w, "invalid object version", http.StatusBadGateway)
			return
		}
		if version == 1<<63-1 {
			http.Error(w, "invalid object version", http.StatusBadGateway)
			return
		}
		expectedReceiptVersion = version + 1
		title := str(obj["title"])
		if submittedTitle, ok := fields["/title"].(string); ok {
			title = submittedTitle
		}
		command = baseCommand(a.deps, opts, "object.update", "object_instance", &id)
		command.Preconditions = []map[string]any{{"aggregate_type": "object_instance", "aggregate_id": id, "expected_version": version}}
		command.Payload = map[string]any{"title": title, "values": values}
	}
	receiptBody, err := submitCommandReceipt(r.Context(), a.journal, a.deps, opts, command)
	if err != nil {
		http.Error(w, "command rejected", http.StatusConflict)
		return
	}
	requestedID := ""
	if len(p) == 2 {
		requestedID = p[1]
	}
	receipt, err := validateWebCommandReceipt(receiptBody, command, requestedID, expectedReceiptVersion)
	if err != nil {
		http.Error(w, "invalid command receipt", http.StatusBadGateway)
		return
	}
	if err := a.revalidateRedirectTarget(r.Context(), n, receipt); err != nil {
		http.Error(w, "command applied but confirmation read failed", http.StatusBadGateway)
		return
	}
	target := "/objects/" + webPathSegment(n.Key)
	if len(p) == 2 {
		target += "/" + webPathSegment(p[1])
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func validateWebCommandReceipt(body []byte, command commandEnvelope, requestedID string, expectedVersion int64) (webCommandReceipt, error) {
	var receipt webCommandReceipt
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return receipt, fmt.Errorf("receipt has trailing JSON data")
	}
	if receipt.Outcome != "applied" && receipt.Outcome != "duplicate" {
		return receipt, fmt.Errorf("receipt outcome is not successful")
	}
	if receipt.CommandID != command.CommandID {
		return receipt, fmt.Errorf("receipt command_id mismatch")
	}
	if receipt.Aggregate == nil || receipt.Aggregate.Type != "object_instance" || receipt.Aggregate.ID == "" || receipt.Aggregate.Version <= 0 {
		return receipt, fmt.Errorf("receipt aggregate is invalid")
	}
	if requestedID != "" && receipt.Aggregate.ID != requestedID {
		return receipt, fmt.Errorf("receipt aggregate id mismatch")
	}
	if receipt.Aggregate.Version != expectedVersion {
		return receipt, fmt.Errorf("receipt aggregate version mismatch")
	}
	return receipt, nil
}

func (a webSSR) revalidateRedirectTarget(ctx context.Context, n webNoun, receipt webCommandReceipt) error {
	obj, err := a.object(ctx, receipt.Aggregate.ID)
	if err != nil {
		return err
	}
	version, ok := positiveJSONInt64(obj["version"])
	if str(obj["id"]) != receipt.Aggregate.ID || str(obj["object_type_key"]) != n.Key || !ok || version < receipt.Aggregate.Version {
		return fmt.Errorf("revalidated object does not match receipt")
	}
	return nil
}

func ptr(s string) *string { return &s }
func (a webSSR) checkPost(r *http.Request) error {
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		return fmt.Errorf("content type")
	}
	if r.Header.Get("Sec-Fetch-Site") != "" && r.Header.Get("Sec-Fetch-Site") != "same-origin" && r.Header.Get("Sec-Fetch-Site") != "none" {
		return fmt.Errorf("fetch site")
	}
	origin := "http://" + r.Host
	if v := r.Header.Get("Origin"); v != "" && v != origin {
		return fmt.Errorf("origin")
	}
	if v := r.Header.Get("Referer"); v != "" {
		u, e := url.Parse(v)
		if e != nil || u.Scheme+"://"+u.Host != origin {
			return fmt.Errorf("referer")
		}
	}
	return nil
}

func decodeWebForm(r *http.Request, schema map[string]any) (map[string]any, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, webFormMaxBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("utf8")
	}
	q, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	props, _ := schema["properties"].(map[string]any)
	required := stringSet(schema["required"])
	for name, vs := range q {
		if len(vs) != 1 {
			return nil, fmt.Errorf("duplicate")
		}
		if name == "_csrf" {
			out[name] = vs[0]
			continue
		}
		if name == "_compos_form_mode" {
			out[name] = vs[0]
			continue
		}
		if name == "/title" {
			out[name] = vs[0]
			continue
		}
		if strings.HasPrefix(name, "_compos_present_/") {
			key := strings.TrimPrefix(name, "_compos_present_/")
			spec, _ := props[key].(map[string]any)
			if spec == nil || strings.Contains(key, "/") || !scalarSchemaSupported(spec, required[key]) || str(spec["type"]) != "boolean" || vs[0] != "1" {
				return nil, fmt.Errorf("presence")
			}
			out[name] = true
			continue
		}
		if strings.HasPrefix(name, "_") || !strings.HasPrefix(name, "/") {
			return nil, fmt.Errorf("reserved")
		}
		key := strings.TrimPrefix(name, "/")
		spec, _ := props[key].(map[string]any)
		if spec == nil || strings.Contains(key, "/") || !scalarSchemaSupported(spec, required[key]) {
			return nil, fmt.Errorf("unknown")
		}
		v, ok := decodeScalar(vs[0], spec)
		if !ok {
			return nil, fmt.Errorf("unsupported")
		}
		out[name] = v
	}
	for key, raw := range props {
		spec, _ := raw.(map[string]any)
		if str(spec["type"]) == "boolean" && out["_compos_present_/"+key] == true {
			if _, submitted := out["/"+key]; !submitted {
				out["/"+key] = false
			}
		}
	}
	for key := range required {
		if _, ok := out["/"+key]; !ok {
			return nil, fmt.Errorf("required")
		}
	}
	return out, nil
}

func formSchemaSupported(schema map[string]any) bool {
	props, _ := schema["properties"].(map[string]any)
	required := stringSet(schema["required"])
	for key, raw := range props {
		spec, ok := raw.(map[string]any)
		if !ok || !scalarSchemaSupported(spec, required[key]) {
			return false
		}
	}
	return true
}

func scalarSchemaSupported(spec map[string]any, required bool) bool {
	typ, _ := spec["type"].(string)
	if typ == "boolean" {
		return true
	}
	return typ == "string" || typ == "integer" || typ == "number"
}
func stringSet(v any) map[string]bool {
	out := map[string]bool{}
	for _, x := range asSlice(v) {
		if s, ok := x.(string); ok {
			out[s] = true
		}
	}
	return out
}
func asSlice(v any) []any { x, _ := v.([]any); return x }
func decodeScalar(raw string, s map[string]any) (any, bool) {
	typ, _ := s["type"].(string)
	switch typ {
	case "string":
		return raw, true
	case "integer":
		v, e := strconv.ParseInt(raw, 10, 64)
		return v, e == nil
	case "number":
		v, e := strconv.ParseFloat(raw, 64)
		return v, e == nil
	case "boolean":
		if raw == "true" || raw == "on" {
			return true, true
		}
		if raw == "false" {
			return false, true
		}
	}
	return nil, false
}
func pointerValues(f map[string]any) map[string]any {
	o := map[string]any{}
	for k, v := range f {
		if k != "/title" && !strings.HasPrefix(k, "_compos_present_/") {
			o[strings.TrimPrefix(k, "/")] = v
		}
	}
	return o
}

func values(obj map[string]any) map[string]any {
	values, _ := obj["values"].(map[string]any)
	return values
}

func renderNativeFields(b *strings.Builder, schema map[string]any, obj map[string]any) {
	props, _ := schema["properties"].(map[string]any)
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	current := values(obj)
	for _, key := range keys {
		spec, _ := props[key].(map[string]any)
		typ := str(spec["type"])
		if typ == "boolean" {
			fmt.Fprintf(b, `<input type="hidden" name="_compos_present_/%s" value="1"><label>%s<input name="/%s" type="checkbox" value="true"%s></label>`, html.EscapeString(key), html.EscapeString(key), html.EscapeString(key), checked(current[key]))
			continue
		}
		inputType := "text"
		if typ == "integer" || typ == "number" {
			inputType = "number"
		}
		fmt.Fprintf(b, `<label>%s<input name="/%s" type="%s" value="%s"></label>`, html.EscapeString(key), html.EscapeString(key), inputType, html.EscapeString(fmt.Sprint(current[key])))
	}
}
func checked(v any) string {
	if b, _ := v.(bool); b {
		return " checked"
	}
	return ""
}

func webPathSegment(value string) string {
	return url.PathEscape(value)
}
