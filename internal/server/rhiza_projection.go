package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mrchypark/compos/internal/knowledge"
	"github.com/mrchypark/compos/internal/rhizaprojection"
	"github.com/mrchypark/compos/internal/store"
)

const graphProjectionBatchSize = 1_000

type knowledgeProjectionEvent struct {
	WorkspaceID string
	Sequence    int64
	BundleID    string
	ConceptID   string
}

func (s *Server) runGraphProjectionReconciler(ctx context.Context) {
	defer close(s.graphProjectionDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	initial := true
	for {
		s.reconcileGraphProjection(ctx, initial)
		initial = false
		select {
		case <-ctx.Done():
			return
		case <-s.graphProjectionWake:
		case <-ticker.C:
		}
	}
}

func (s *Server) reconcileGraphProjection(parent context.Context, verifyDigest bool) {
	if s.graphProjection == nil {
		return
	}
	s.graphProjectionSync.Lock()
	defer s.graphProjectionSync.Unlock()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()

	heads, err := knowledge.HeadSequences(ctx, s.store)
	if err != nil {
		s.finishGraphProjectionReconcile(heads, nil, err)
		return
	}
	cursors, digest, version, stateErr := s.graphProjection.State(ctx)
	rebuild := stateErr != nil || version != rhizaprojection.SchemaVersion
	for workspaceID, cursor := range cursors {
		if cursor > heads[workspaceID] {
			rebuild = true
		}
	}
	current := sequencesCurrent(heads, cursors)
	if verifyDigest && current && !rebuild {
		_, _, canonicalDigest, snapshotErr := knowledge.SnapshotState(ctx, s.store)
		if snapshotErr != nil {
			s.finishGraphProjectionReconcile(heads, cursors, snapshotErr)
			return
		}
		rebuild = digest != canonicalDigest
	}
	if rebuild {
		s.rebuildGraphProjection(ctx)
		return
	}
	if current {
		s.finishGraphProjectionReconcile(heads, cursors, nil)
		return
	}

	_, snapshotHeads, snapshotDigest, err := knowledge.SnapshotState(ctx, s.store)
	if err != nil {
		s.finishGraphProjectionReconcile(heads, cursors, err)
		return
	}
	events, err := s.pendingKnowledgeEvents(ctx, cursors, snapshotHeads, graphProjectionBatchSize)
	if err != nil {
		s.finishGraphProjectionReconcile(snapshotHeads, cursors, err)
		return
	}
	if len(events) == 0 {
		s.rebuildGraphProjection(ctx)
		return
	}
	for _, event := range events {
		records, err := knowledge.ConceptSnapshot(ctx, s.store, event.WorkspaceID, event.BundleID, event.ConceptID)
		if err == nil {
			err = s.graphProjection.ApplyConcept(ctx, event.WorkspaceID, event.BundleID, event.ConceptID, records, event.Sequence, snapshotDigest)
		}
		if err != nil {
			s.finishGraphProjectionReconcile(snapshotHeads, cursors, err)
			return
		}
		cursors[event.WorkspaceID] = event.Sequence
	}
	s.finishGraphProjectionReconcile(snapshotHeads, cursors, nil)
	if !sequencesCurrent(snapshotHeads, cursors) {
		s.wakeGraphProjection()
	}
}

func (s *Server) rebuildGraphProjection(ctx context.Context) {
	s.graphProjectionMu.Lock()
	s.graphProjectionRebuilding = true
	s.graphProjectionRebuilds++
	s.graphProjectionErr = "rebuilding"
	s.graphProjectionMu.Unlock()
	records, heads, digest, err := knowledge.SnapshotState(ctx, s.store)
	if err == nil {
		err = s.graphProjection.Rebuild(ctx, records, heads, digest)
	}
	s.graphProjectionMu.Lock()
	s.graphProjectionRebuilding = false
	s.graphProjectionMu.Unlock()
	s.finishGraphProjectionReconcile(heads, heads, err)
}

func (s *Server) pendingKnowledgeEvents(ctx context.Context, cursors, heads map[string]int64, limit int) ([]knowledgeProjectionEvent, error) {
	workspaces := make([]string, 0, len(heads))
	for workspaceID := range heads {
		workspaces = append(workspaces, workspaceID)
	}
	sort.Strings(workspaces)
	events := make([]knowledgeProjectionEvent, 0, limit)
	for _, workspaceID := range workspaces {
		if cursors[workspaceID] >= heads[workspaceID] || len(events) == limit {
			continue
		}
		rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT projection_sequence,payload_json FROM projection_events WHERE workspace_id=? AND aggregate_type='knowledge_document' AND projection_sequence>? AND projection_sequence<=? ORDER BY projection_sequence LIMIT ?`, workspaceID, cursors[workspaceID], heads[workspaceID], limit-len(events))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var event knowledgeProjectionEvent
			var payloadJSON string
			event.WorkspaceID = workspaceID
			if err := rows.Scan(&event.Sequence, &payloadJSON); err != nil {
				rows.Close()
				return nil, err
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				rows.Close()
				return nil, err
			}
			event.BundleID, _ = payload["bundle_id"].(string)
			event.ConceptID, _ = payload["concept_id"].(string)
			if event.BundleID == "" || event.ConceptID == "" {
				rows.Close()
				return nil, fmt.Errorf("knowledge projection event %d is missing bundle_id or concept_id", event.Sequence)
			}
			events = append(events, event)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func sequencesCurrent(heads, cursors map[string]int64) bool {
	for workspaceID, head := range heads {
		if cursors[workspaceID] < head {
			return false
		}
	}
	return true
}

func (s *Server) finishGraphProjectionReconcile(heads, cursors map[string]int64, err error) {
	s.graphProjectionMu.Lock()
	defer s.graphProjectionMu.Unlock()
	if heads != nil {
		for workspaceID, head := range heads {
			if head > s.graphProjectionHeads[workspaceID] {
				s.graphProjectionHeads[workspaceID] = head
			}
		}
	}
	if cursors != nil {
		for workspaceID, cursor := range cursors {
			if cursor > s.graphProjectionCursors[workspaceID] {
				s.graphProjectionCursors[workspaceID] = cursor
			}
		}
	}
	if err != nil {
		s.graphProjectionErr = err.Error()
		return
	}
	s.graphProjectionErr = ""
	s.graphProjectionLastSuccess = time.Now().UTC().Format(time.RFC3339Nano)
}

func (s *Server) markGraphProjectionHead(workspaceID string, sequence int64) {
	if s.graphProjection == nil {
		return
	}
	s.graphProjectionMu.Lock()
	if sequence > s.graphProjectionHeads[workspaceID] {
		s.graphProjectionHeads[workspaceID] = sequence
	}
	s.graphProjectionMu.Unlock()
}

func (s *Server) wakeGraphProjection() {
	if s.graphProjectionWake == nil {
		return
	}
	select {
	case s.graphProjectionWake <- struct{}{}:
	default:
	}
}
