package server

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/mrchypark/compos/internal/store"
)

// listDefinitions returns immutable, editor-selectable Task, Function, and Workflow revisions.
func (s *Server) listDefinitions(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	if kind != "" && kind != "task" && kind != "function" && kind != "workflow" {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "kind must be task, function, or workflow")
		return
	}
	limit := boundedInt(r, "limit", 50, 1, 100)
	offset := boundedInt(r, "offset", 0, 0, 1<<30)
	query := `SELECT id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id FROM spec_revisions WHERE workspace_id=? AND kind IN ('task','function','workflow')`
	args := []any{workspace}
	if kind != "" {
		query += ` AND kind=?`
		args = append(args, kind)
	}
	if familyID := strings.TrimSpace(r.URL.Query().Get("family_id")); familyID != "" {
		query += ` AND family_id=?`
		args = append(args, familyID)
	}
	query += ` ORDER BY family_id,revision DESC,id LIMIT ? OFFSET ?`
	args = append(args, limit+1, offset)
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		item, err := scanDefinition(rows)
		if err != nil {
			dbError(w, err)
			return
		}
		data = append(data, item)
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	var next any
	if len(data) > limit {
		next = offset + limit
		data = data[:limit]
	}
	write(w, http.StatusOK, map[string]any{"data": data, "page": map[string]any{"next_cursor": next}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) readDefinition(w http.ResponseWriter, r *http.Request) {
	row := s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id FROM spec_revisions WHERE workspace_id=? AND id=? AND kind IN ('task','function','workflow')`, r.PathValue("workspace_id"), r.PathValue("revision_id"))
	item, err := scanDefinition(row)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "definition revision not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, http.StatusOK, map[string]any{"data": item, "consistency": consistency("canonical_stale_bounded")})
}

type definitionScanner interface {
	Scan(...any) error
}

func scanDefinition(row definitionScanner) (map[string]any, error) {
	var id, familyID, kind, payload, digest, createdAt, creator string
	var revision int64
	if err := row.Scan(&id, &familyID, &revision, &kind, &payload, &digest, &createdAt, &creator); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "family_id": familyID, "revision": revision, "kind": kind, "spec": raw(payload), "digest": digest, "created_at": createdAt, "created_by_principal_id": creator}, nil
}

// listObjectRelations returns persisted relation edges around one existing object.
func (s *Server) listObjectRelations(w http.ResponseWriter, r *http.Request) {
	workspace, objectID := r.PathValue("workspace_id"), r.PathValue("object_id")
	if _, err := s.object(r.Context(), workspace, objectID); errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "object not found")
		return
	} else if err != nil {
		dbError(w, err)
		return
	}
	direction := r.URL.Query().Get("direction")
	if direction == "" {
		direction = "both"
	}
	if direction != "both" && direction != "outgoing" && direction != "incoming" {
		problem(w, http.StatusBadRequest, "invalid_request", "validation", false, "direction must be incoming, outgoing, or both")
		return
	}
	limit := boundedInt(r, "limit", 50, 1, 100)
	offset := boundedInt(r, "offset", 0, 0, 1<<30)
	query := `SELECT r.id,r.relation_definition_id,d.key,r.from_object_instance_id,r.to_object_instance_id,r.version,r.created_at,r.updated_at,
		CASE WHEN r.from_object_instance_id=? THEN 'outgoing' ELSE 'incoming' END,
		CASE WHEN r.from_object_instance_id=? THEN r.to_object_instance_id ELSE r.from_object_instance_id END,
		i.title,t.key,i.version
		FROM object_relations r
		JOIN relation_definitions d ON d.workspace_id=r.workspace_id AND d.id=r.relation_definition_id AND d.archived_at IS NULL
		JOIN object_instances i ON i.workspace_id=r.workspace_id AND i.id=CASE WHEN r.from_object_instance_id=? THEN r.to_object_instance_id ELSE r.from_object_instance_id END AND i.archived_at IS NULL
		JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id
		WHERE r.workspace_id=? AND (r.from_object_instance_id=? OR r.to_object_instance_id=?)`
	args := []any{objectID, objectID, objectID, workspace, objectID, objectID}
	if direction == "outgoing" {
		query += ` AND r.from_object_instance_id=?`
		args = append(args, objectID)
	} else if direction == "incoming" {
		query += ` AND r.to_object_instance_id=?`
		args = append(args, objectID)
	}
	if definitionID := strings.TrimSpace(r.URL.Query().Get("relation_definition_id")); definitionID != "" {
		query += ` AND r.relation_definition_id=?`
		args = append(args, definitionID)
	}
	query += ` ORDER BY r.created_at DESC,r.id LIMIT ? OFFSET ?`
	args = append(args, limit+1, offset)
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, definitionID, key, from, to, createdAt, updatedAt, edgeDirection, relatedID, relatedTitle, relatedType string
		var version, relatedVersion int64
		if err := rows.Scan(&id, &definitionID, &key, &from, &to, &version, &createdAt, &updatedAt, &edgeDirection, &relatedID, &relatedTitle, &relatedType, &relatedVersion); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"id": id, "relation_definition_id": definitionID, "relation_key": key, "from_object_id": from, "to_object_id": to, "direction": edgeDirection, "version": version, "created_at": createdAt, "updated_at": updatedAt, "related_object": map[string]any{"id": relatedID, "title": relatedTitle, "object_type_key": relatedType, "version": relatedVersion}})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	var next any
	if len(data) > limit {
		next = offset + limit
		data = data[:limit]
	}
	write(w, http.StatusOK, map[string]any{"data": data, "page": map[string]any{"next_cursor": next}, "consistency": consistency("canonical_stale_bounded")})
}
