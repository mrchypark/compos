package kernel

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/knowledge"
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

func proposeKnowledge(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any) (effect, error) {
	sources, _ := p["sources"].([]any)
	if len(sources) == 0 || len(sources) > 256 {
		return effect{}, reject("schema_mismatch", "sources must contain 1 to 256 accepted Knowledge references")
	}
	pinnedSources := make([]any, 0, len(sources))
	for _, raw := range sources {
		source, ok := raw.(map[string]any)
		if !ok || onlyKeys("knowledge proposal source", source, "bundle_id", "concept_id") != nil {
			return effect{}, reject("schema_mismatch", "proposal sources must contain bundle_id and concept_id")
		}
		bundleID, _ := source["bundle_id"].(string)
		conceptID, _ := source["concept_id"].(string)
		var contentHash, sourceMarkdown string
		if strings.TrimSpace(bundleID) == "" || strings.TrimSpace(conceptID) == "" || tx.QueryRowContext(ctx, `SELECT content_hash,source_markdown FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, bundleID, conceptID).Scan(&contentHash, &sourceMarkdown) != nil {
			return effect{}, reject("precondition_failed", "every proposal source must reference accepted Knowledge in the workspace")
		}
		pinnedSources = append(pinnedSources, map[string]any{"bundle_id": bundleID, "concept_id": conceptID, "revision_id": knowledge.RevisionID(sourceMarkdown), "content_hash": contentHash})
	}
	proposal, _ := p["proposal"].(map[string]any)
	if err := validateOntologySpec(proposal); err != nil {
		return effect{}, err
	}
	streamID := aggregateID("kpr", c.CommandID)
	payload := map[string]any{"sources": pinnedSources, "proposal": proposal}
	eff := makeEffect(streamID, "knowledge_proposal", 1, "knowledge.proposed", payload)
	eff.journal = []journalFact{{streamID: streamID, kind: "knowledge.proposed", participant: commandParticipant(c, principal), payload: payload}}
	return eff, nil
}

func reviewKnowledge(ctx context.Context, tx store.Tx, principal string, c Command, p map[string]any, now time.Time) (effect, error) {
	if commandParticipant(c, principal).Type != "human" {
		return effect{}, reject("forbidden", "Knowledge proposal review requires a human participant")
	}
	streamID := *c.Target.AggregateID
	var proposedJSON string
	if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='knowledge.proposed' ORDER BY stream_sequence LIMIT 1`, c.WorkspaceID, streamID).Scan(&proposedJSON); err != nil {
		return effect{}, reject("precondition_failed", "Knowledge proposal does not exist")
	}
	var reviewed int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('knowledge.proposal.accepted','knowledge.proposal.rejected')`, c.WorkspaceID, streamID).Scan(&reviewed); err != nil {
		return effect{}, err
	}
	if reviewed != 0 {
		return effect{}, reject("precondition_failed", "Knowledge proposal is already reviewed")
	}
	decision, _ := p["decision"].(string)
	payload := map[string]any{"decision": decision}
	if reason, _ := p["reason"].(string); strings.TrimSpace(reason) != "" {
		payload["reason"] = reason
	}
	kind, event := "knowledge.proposal.rejected", "knowledge.proposal.rejected"
	fact := journalFact{streamID: streamID, kind: kind, participant: commandParticipant(c, principal), payload: payload}
	if decision == "accept" {
		family, _ := p["family_id"].(string)
		revision, ok := integer(p["revision"])
		if strings.TrimSpace(family) == "" || !ok || revision < 1 {
			return effect{}, reject("schema_mismatch", "accepted proposals require family_id and a positive revision")
		}
		var proposed map[string]any
		if err := json.Unmarshal([]byte(proposedJSON), &proposed); err != nil {
			return effect{}, err
		}
		proposal, _ := proposed["proposal"].(map[string]any)
		for _, raw := range anySlice(proposed["sources"]) {
			source := raw.(map[string]any)
			pinnedRevision, usesSourceBytes := source["revision_id"]
			if !usesSourceBytes { // proposals created before source-byte revision IDs
				pinnedRevision = source["content_hash"]
			}
			var currentHash, currentSource string
			err := tx.QueryRowContext(ctx, `SELECT content_hash,source_markdown FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, source["bundle_id"], source["concept_id"]).Scan(&currentHash, &currentSource)
			currentRevision := any(currentHash)
			if usesSourceBytes {
				currentRevision = knowledge.RevisionID(currentSource)
			}
			if err != nil || currentRevision != pinnedRevision {
				return effect{}, reject("precondition_failed", "Knowledge proposal source changed after proposal creation")
			}
		}
		if err := validateOntologySpec(proposal); err != nil {
			return effect{}, err
		}
		specID := aggregateID("spc", c.CommandID)
		digest := digestJSON([]byte(canonical(map[string]any{"family_id": family, "revision": revision, "kind": "ontology", "spec": proposal})))
		_, err := tx.ExecContext(ctx, `INSERT INTO spec_revisions(id,workspace_id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id) VALUES(?,?,?,?,?,?,?,?,?)`, specID, c.WorkspaceID, family, revision, "ontology", canonical(proposal), digest, timestamp(now), principal)
		if err != nil {
			return effect{}, mapSQLError(err)
		}
		payload["ontology_revision_id"], payload["family_id"], payload["revision"] = specID, family, revision
		kind, event = "knowledge.proposal.accepted", "knowledge.proposal.accepted"
		fact.kind, fact.specRevision, fact.payload = kind, specID, payload
	}
	eff := makeEffect(streamID, "knowledge_proposal", 2, event, payload)
	eff.journal = []journalFact{fact}
	return eff, nil
}
