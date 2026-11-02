package server

import (
	"database/sql"
	"net/http"

	"github.com/mrchypark/compos/internal/store"
)

func (s *Server) listDomainEvents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT id,stream_id,spec_revision_id,participant_kind,participant_id,principal_id,payload_json,created_at FROM journal_records WHERE workspace_id=? AND kind='domain.event.emitted' ORDER BY created_at,id LIMIT 1000`, r.PathValue("workspace_id"))
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, streamID, principalID, payload, createdAt string
		var specID, participantKind, participantID sql.NullString
		if err := rows.Scan(&id, &streamID, &specID, &participantKind, &participantID, &principalID, &payload, &createdAt); err != nil {
			dbError(w, err)
			return
		}
		item := map[string]any{"id": id, "stream_id": streamID, "event_revision_id": nullString(specID), "principal_id": principalID, "payload": raw(payload), "created_at": createdAt}
		if participantKind.Valid {
			item["participant"] = map[string]any{"kind": participantKind.String, "id": participantID.String}
		}
		data = append(data, item)
	}
	write(w, http.StatusOK, map[string]any{"data": data})
}
