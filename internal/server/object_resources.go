package server

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
)

const (
	maxRecordScopeIDs   = 199
	maxRecordScopeBytes = 65536
)

// parseRecordScope accepts only Exposure's parenthesized disjunction of id equality terms.
func parseRecordScope(filter string) ([]string, error) {
	if len(filter) > maxRecordScopeBytes {
		return nil, errors.New("filter exceeds 65536 bytes")
	}
	first, rest, err := parseRecordScopeGroup(filter)
	if err != nil {
		return nil, err
	}
	if rest == "" {
		return first, nil
	}
	if !strings.HasPrefix(rest, "&&") {
		return nil, errors.New("unsupported filter")
	}
	second, rest, err := parseRecordScopeGroup(strings.TrimSpace(rest[2:]))
	if err != nil || rest != "" || len(first)+len(second) > maxRecordScopeIDs+1 {
		return nil, errors.New("unsupported filter")
	}
	if len(first) == 0 {
		return []string{}, nil
	}
	allowed := make(map[string]struct{}, len(second))
	for _, id := range second {
		allowed[id] = struct{}{}
	}
	intersection := make([]string, 0, len(first))
	for _, id := range first {
		if _, ok := allowed[id]; ok {
			intersection = append(intersection, id)
		}
	}
	return intersection, nil
}

func parseRecordScopeGroup(filter string) ([]string, string, error) {
	filter = strings.TrimSpace(filter)
	if len(filter) < 3 || filter[0] != '(' {
		return nil, "", errors.New("unsupported filter")
	}
	inString, escaped, close := false, false, -1
	for i := 1; i < len(filter); i++ {
		if inString {
			if escaped {
				escaped = false
			} else if filter[i] == '\\' {
				escaped = true
			} else if filter[i] == '"' {
				inString = false
			}
			continue
		}
		if filter[i] == '"' {
			inString = true
		} else if filter[i] == ')' {
			close = i
			break
		}
	}
	if close < 0 || inString || escaped {
		return nil, "", errors.New("unsupported filter")
	}
	group, rest := filter[:close+1], strings.TrimSpace(filter[close+1:])
	if group == "(1 = 0)" {
		return []string{}, rest, nil
	}
	inner := strings.TrimSpace(group[1 : len(group)-1])
	ids := make([]string, 0, 4)
	for {
		inner = strings.TrimSpace(inner)
		if !strings.HasPrefix(inner, "id") {
			return nil, "", errors.New("unsupported filter")
		}
		inner = strings.TrimSpace(inner[2:])
		if !strings.HasPrefix(inner, "=") {
			return nil, "", errors.New("unsupported filter")
		}
		inner = strings.TrimSpace(inner[1:])
		if len(inner) == 0 || inner[0] != '"' {
			return nil, "", errors.New("unsupported filter")
		}
		end := 1
		for end < len(inner) {
			if inner[end] == '\\' {
				end += 2
				continue
			}
			if inner[end] == '"' {
				break
			}
			end++
		}
		if end >= len(inner) {
			return nil, "", errors.New("unsupported filter")
		}
		id, err := strconv.Unquote(inner[:end+1])
		if err != nil || id == "" {
			return nil, "", errors.New("unsupported filter")
		}
		ids = append(ids, id)
		if len(ids) > maxRecordScopeIDs {
			return nil, "", fmt.Errorf("filter exceeds %d IDs", maxRecordScopeIDs)
		}
		inner = strings.TrimSpace(inner[end+1:])
		if inner == "" {
			return ids, rest, nil
		}
		if !strings.HasPrefix(inner, "||") {
			return nil, "", errors.New("unsupported filter")
		}
		inner = inner[2:]
	}
}

func (s *Server) listTypedObjects(w http.ResponseWriter, r *http.Request) {
	filters := r.URL.Query()["filter"]
	if len(filters) != 1 {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "exactly one filter is required")
		return
	}
	ids, err := parseRecordScope(filters[0])
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, err.Error())
		return
	}
	workspace, objectTypeID := r.PathValue("workspace_id"), r.PathValue("object_type_id")
	limit, offset := boundedInt(r, "limit", 50, 1, 100), boundedInt(r, "offset", 0, 0, 1<<30)
	query := `SELECT i.id,i.title,i.payload_json,i.state_key,i.object_type_schema_version,i.version,i.created_at,i.updated_at,t.key
		FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id
		WHERE i.workspace_id=? AND t.id=? AND t.archived_at IS NULL AND i.archived_at IS NULL`
	args := []any{workspace, objectTypeID}
	if len(ids) == 0 {
		query += ` AND 1=0`
	} else {
		query += ` AND i.id IN (` + strings.TrimRight(strings.Repeat("?,", len(ids)), ",") + `)`
		for _, id := range ids {
			args = append(args, id)
		}
	}
	query += ` ORDER BY i.updated_at DESC,i.id LIMIT ? OFFSET ?`
	args = append(args, limit+1, offset)
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, title, payload, typeKey, created, updated string
		var state sql.NullString
		var schemaVersion, version int64
		if err := rows.Scan(&id, &title, &payload, &state, &schemaVersion, &version, &created, &updated, &typeKey); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"id": id, "object_type_id": objectTypeID, "object_type_key": typeKey, "object_type_schema_version": schemaVersion, "title": title, "values": raw(payload), "state_key": nullableString(state.String), "version": version, "created_at": created, "updated_at": updated})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	var next any
	if len(data) > limit {
		data = data[:limit]
		next = offset + limit
	}
	write(w, http.StatusOK, map[string]any{"data": data, "page": map[string]any{"next_cursor": next}, "consistency": consistency("canonical_stale_bounded")})
}

type typedObjectBody struct {
	Title           string         `json:"title"`
	Values          map[string]any `json:"values"`
	IdempotencyKey  string         `json:"idempotency_key"`
	ExpectedVersion int64          `json:"expected_version"`
}

func (s *Server) createTypedObject(w http.ResponseWriter, r *http.Request) {
	var body typedObjectBody
	if !decodeTypedObjectBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Title) == "" || strings.TrimSpace(body.IdempotencyKey) == "" {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "title and idempotency_key are required")
		return
	}
	workspace, typeID := r.PathValue("workspace_id"), r.PathValue("object_type_id")
	var typeKey string
	if err := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT key FROM object_types WHERE workspace_id=? AND id=? AND archived_at IS NULL`, workspace, typeID).Scan(&typeKey); errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "object type not found")
		return
	} else if err != nil {
		dbError(w, err)
		return
	}
	payloadValue := map[string]any{"object_type_key": typeKey, "title": body.Title}
	if body.Values != nil {
		payloadValue["values"] = body.Values
	}
	payload, _ := json.Marshal(payloadValue)
	s.executeTypedObjectCommand(w, r, kernel.Command{IdempotencyKey: body.IdempotencyKey, WorkspaceID: workspace, CommandType: "object.create", Target: kernel.Target{AggregateType: "object_instance"}, Preconditions: []kernel.Precondition{}, Payload: payload, Causation: kernel.Causation{Source: "api"}})
}

func (s *Server) updateTypedObject(w http.ResponseWriter, r *http.Request) {
	var body typedObjectBody
	if !decodeTypedObjectBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.IdempotencyKey) == "" || body.ExpectedVersion < 1 || (strings.TrimSpace(body.Title) == "" && body.Values == nil) {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "idempotency_key, positive expected_version, and title or values are required")
		return
	}
	payload := map[string]any{}
	if body.Title != "" {
		payload["title"] = body.Title
	}
	if body.Values != nil {
		payload["values"] = body.Values
	}
	s.typedObjectCommand(w, r, body, "object.update", payload)
}

func (s *Server) archiveTypedObject(w http.ResponseWriter, r *http.Request) {
	var body typedObjectBody
	if !decodeTypedObjectBody(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.IdempotencyKey) == "" || body.ExpectedVersion < 1 {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "idempotency_key and positive expected_version are required")
		return
	}
	s.typedObjectCommand(w, r, body, "object.delete", map[string]any{"hard": false})
}

func decodeTypedObjectBody(w http.ResponseWriter, r *http.Request, body *typedObjectBody) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || rejectDuplicateJSONKeys(raw) != nil {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "invalid JSON request")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(body); err != nil {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "invalid JSON request")
		return false
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "invalid JSON request")
		return false
	}
	return true
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := checkJSONKeys(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON request")
	}
	return nil
}

func checkJSONKeys(decoder *json.Decoder, depth int) error {
	if depth > 100 {
		return errors.New("JSON nesting exceeds 100 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if depth == 0 {
				key = strings.ToLower(key)
			}
			if _, exists := keys[key]; exists {
				return errors.New("duplicate or case-colliding JSON key")
			}
			keys[key] = struct{}{}
			if err := checkJSONKeys(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := checkJSONKeys(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("invalid JSON structure")
	}
}

func (s *Server) typedObjectCommand(w http.ResponseWriter, r *http.Request, body typedObjectBody, commandType string, payloadValue any) {
	workspace, typeID, objectID := r.PathValue("workspace_id"), r.PathValue("object_type_id"), r.PathValue("object_id")
	var exists int
	if err := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT 1 FROM object_instances WHERE workspace_id=? AND id=? AND object_type_id=?`, workspace, objectID, typeID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "object not found")
		return
	} else if err != nil {
		dbError(w, err)
		return
	}
	payload, _ := json.Marshal(payloadValue)
	s.executeTypedObjectCommand(w, r, kernel.Command{IdempotencyKey: body.IdempotencyKey, WorkspaceID: workspace, CommandType: commandType, Target: kernel.Target{AggregateType: "object_instance", AggregateID: &objectID}, Preconditions: []kernel.Precondition{{AggregateType: "object_instance", AggregateID: objectID, ExpectedVersion: body.ExpectedVersion}}, Payload: payload, Causation: kernel.Causation{Source: "api"}})
}

func (s *Server) executeTypedObjectCommand(w http.ResponseWriter, r *http.Request, command kernel.Command) {
	// Match the ledger scope without delimiter ambiguity or cross-principal ID collisions.
	scope, _ := json.Marshal([]string{command.WorkspaceID, principal(r), command.IdempotencyKey})
	command.CommandID = fmt.Sprintf("cmd_typed_%x", sha256.Sum256(scope))
	receipt, err := s.kernel.Execute(r.Context(), principal(r), command)
	if err != nil {
		commandError(w, err)
		return
	}
	s.wakeWorkflow()
	write(w, http.StatusOK, map[string]any{"command_id": receipt.CommandID, "outcome": receipt.Outcome, "aggregate": receipt.Aggregate, "projection": map[string]any{"workspace_id": receipt.WorkspaceID, "projection_sequence": receipt.ProjectionSequence}})
}
