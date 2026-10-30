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

func (s *Server) listKnowledgeProposals(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT p.stream_id,p.participant_kind,p.participant_id,p.principal_id,p.payload_json,p.created_at,(SELECT t.kind FROM journal_records t WHERE t.workspace_id=p.workspace_id AND t.stream_id=p.stream_id AND t.kind IN ('knowledge.proposal.accepted','knowledge.proposal.rejected') ORDER BY t.stream_sequence DESC LIMIT 1),(SELECT t.payload_json FROM journal_records t WHERE t.workspace_id=p.workspace_id AND t.stream_id=p.stream_id AND t.kind IN ('knowledge.proposal.accepted','knowledge.proposal.rejected') ORDER BY t.stream_sequence DESC LIMIT 1) FROM journal_records p WHERE p.workspace_id=? AND p.kind='knowledge.proposed' ORDER BY p.created_at,p.stream_id LIMIT 1000`, r.PathValue("workspace_id"))
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var streamID, principalID, payload, createdAt string
		var participantKind, participantID, terminalKind, reviewPayload sql.NullString
		if err := rows.Scan(&streamID, &participantKind, &participantID, &principalID, &payload, &createdAt, &terminalKind, &reviewPayload); err != nil {
			dbError(w, err)
			return
		}
		status := "proposed"
		if terminalKind.String == "knowledge.proposal.accepted" {
			status = "accepted"
		} else if terminalKind.Valid {
			status = "rejected"
		}
		item := map[string]any{"id": streamID, "status": status, "principal_id": principalID, "proposal": raw(payload), "created_at": createdAt}
		if participantKind.Valid {
			item["participant"] = map[string]any{"kind": participantKind.String, "id": participantID.String}
		}
		if reviewPayload.Valid {
			item["review"] = raw(reviewPayload.String)
		}
		data = append(data, item)
	}
	write(w, http.StatusOK, map[string]any{"data": data})
}
