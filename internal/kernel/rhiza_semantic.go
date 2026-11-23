package kernel

import (
	"context"
	"encoding/json"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (e *Executor) loadSpecRhiza(ctx context.Context, workspace, id, kind string, statements *[]rhiza.SQLStatement) (map[string]any, error) {
	var payload string
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM spec_revisions WHERE workspace_id=? AND id=? AND kind=?`, workspace, id, kind).Scan(&payload); err != nil {
		return nil, reject("precondition_failed", kind+" spec revision does not exist")
	}
	one := int64(1)
	*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM spec_revisions WHERE workspace_id=? AND id=? AND kind=? AND payload_json=?`, Args: []any{workspace, id, kind, payload}, WantRows: true, ExpectedReturnedRows: &one})
	var spec map[string]any
	if err := json.Unmarshal([]byte(payload), &spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func (e *Executor) emitDomainEventRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	eventID, _ := payload["event_revision_id"].(string)
	spec, err := e.loadSpecRhiza(ctx, c.WorkspaceID, eventID, "event", &statements)
	if err != nil {
		return Receipt{}, err
	}
	data, _ := payload["data"].(map[string]any)
	payloadSchema, _ := spec["payload_schema"].(map[string]any)
	if err := validateValues(data, payloadSchema, "payload.data"); err != nil {
		return Receipt{}, err
	}
	streamID := aggregateID("dev", c.CommandID)
	eventPayload := map[string]any{"event_revision_id": eventID, "data": data}
	if subject, ok := payload["subject"].(map[string]any); ok {
		eventPayload["subject"] = subject
	}
	effect := makeEffect(streamID, "domain_event", 1, "domain.event.emitted", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "domain.event.emitted", specRevision: eventID, participant: commandParticipant(c, principal), payload: eventPayload}}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) journalStatementsRhiza(ctx context.Context, principal string, c Command, facts []journalFact, ts string) ([]rhiza.SQLStatement, error) {
	sequences := map[string]int64{}
	statements := make([]rhiza.SQLStatement, 0, len(facts))
	one := int64(1)
	for _, fact := range facts {
		sequence, ok := sequences[fact.streamID]
		if !ok {
			if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COALESCE(MAX(stream_sequence),0) FROM journal_records WHERE workspace_id=? AND stream_id=?`, c.WorkspaceID, fact.streamID).Scan(&sequence); err != nil {
				return nil, err
			}
		}
		sequence++
		sequences[fact.streamID] = sequence
		var participantKind, participantID any
		if fact.participant != nil {
			participantKind, participantID = fact.participant.Type, fact.participant.ID
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO journal_records(id,workspace_id,stream_id,stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,causation_id,command_id,payload_json,created_at) VALUES(?,?,?,?,?,NULLIF(?,''),?,?,?,?,NULLIF(?,''),?,?,?)`, Args: []any{newID("jnl"), c.WorkspaceID, fact.streamID, sequence, fact.kind, fact.specRevision, participantKind, participantID, principal, nullableDB(fact.correlationID), fact.causationID, c.CommandID, canonical(fact.payload), ts}, ExpectedRowsAffected: &one})
	}
	return statements, nil
}
