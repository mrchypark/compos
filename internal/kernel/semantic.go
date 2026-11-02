package kernel

import (
	"context"

	"github.com/mrchypark/compos/internal/store"
)

func emitDomainEvent(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any) (effect, error) {
	eventID, _ := p["event_revision_id"].(string)
	spec, err := loadSpec(ctx, tx, c.WorkspaceID, eventID, "event")
	if err != nil {
		return effect{}, err
	}
	data, _ := p["data"].(map[string]any)
	payloadSchema, _ := spec["payload_schema"].(map[string]any)
	if err := validateValues(data, payloadSchema, "payload.data"); err != nil {
		return effect{}, err
	}
	streamID := aggregateID("dev", c.CommandID)
	payload := map[string]any{"event_revision_id": eventID, "data": data}
	if subject, ok := p["subject"].(map[string]any); ok {
		payload["subject"] = subject
	}
	eff := makeEffect(streamID, "domain_event", 1, "domain.event.emitted", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "domain.event.emitted", specRevision: eventID, participant: commandParticipant(c, principal), payload: payload}}
	return eff, nil
}

func functionEventFacts(ctx context.Context, tx store.Tx, workspace, streamID, invocationID, objectID string, participant *Actor, functionSpec, output map[string]any) ([]journalFact, error) {
	emits, _ := functionSpec["emits"].([]any)
	facts := make([]journalFact, 0, len(emits))
	for _, raw := range emits {
		eventID := raw.(string)
		spec, err := loadSpec(ctx, tx, workspace, eventID, "event")
		if err != nil {
			return nil, err
		}
		payloadSchema, _ := spec["payload_schema"].(map[string]any)
		if err := validateValues(output, payloadSchema, "function output event payload"); err != nil {
			return nil, err
		}
		subject := map[string]any{"work_stream_id": streamID}
		if objectID != "" {
			subject["object_id"] = objectID
		}
		payload := map[string]any{"event_revision_id": eventID, "data": output, "subject": subject}
		facts = append(facts, journalFact{streamID: streamID, kind: "domain.event.emitted", specRevision: eventID, participant: participant, correlationID: invocationID, causationID: invocationID, payload: payload})
	}
	return facts, nil
}
