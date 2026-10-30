package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/mrchypark/compos/internal/knowledge"
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

func (e *Executor) proposeKnowledgeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	sources, _ := payload["sources"].([]any)
	if len(sources) == 0 || len(sources) > 256 {
		return Receipt{}, reject("schema_mismatch", "sources must contain 1 to 256 accepted Knowledge references")
	}
	pinned := make([]any, 0, len(sources))
	one := int64(1)
	for _, raw := range sources {
		source, ok := raw.(map[string]any)
		if !ok || onlyKeys("knowledge proposal source", source, "bundle_id", "concept_id") != nil {
			return Receipt{}, reject("schema_mismatch", "proposal sources must contain bundle_id and concept_id")
		}
		bundleID, _ := source["bundle_id"].(string)
		conceptID, _ := source["concept_id"].(string)
		var contentHash, sourceMarkdown string
		if strings.TrimSpace(bundleID) == "" || strings.TrimSpace(conceptID) == "" || e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT content_hash,source_markdown FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, bundleID, conceptID).Scan(&contentHash, &sourceMarkdown) != nil {
			return Receipt{}, reject("precondition_failed", "every proposal source must reference accepted Knowledge in the workspace")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=? AND content_hash=? AND source_markdown=?`, Args: []any{c.WorkspaceID, bundleID, conceptID, contentHash, sourceMarkdown}, WantRows: true, ExpectedReturnedRows: &one})
		pinned = append(pinned, map[string]any{"bundle_id": bundleID, "concept_id": conceptID, "revision_id": knowledge.RevisionID(sourceMarkdown), "content_hash": contentHash})
	}
	proposal, _ := payload["proposal"].(map[string]any)
	if err := validateOntologySpec(proposal); err != nil {
		return Receipt{}, err
	}
	streamID := aggregateID("kpr", c.CommandID)
	eventPayload := map[string]any{"sources": pinned, "proposal": proposal}
	effect := makeEffect(streamID, "knowledge_proposal", 1, "knowledge.proposed", eventPayload)
	effect.journal = []journalFact{{streamID: streamID, kind: "knowledge.proposed", participant: commandParticipant(c, principal), payload: eventPayload}}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) reviewKnowledgeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	participant := commandParticipant(c, principal)
	if participant.Type != "human" {
		return Receipt{}, reject("forbidden", "Knowledge proposal review requires a human participant")
	}
	streamID := *c.Target.AggregateID
	var proposalID, proposedJSON string
	var proposalSequence int64
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,stream_sequence,payload_json FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='knowledge.proposed' ORDER BY stream_sequence LIMIT 1`, c.WorkspaceID, streamID).Scan(&proposalID, &proposalSequence, &proposedJSON); err != nil {
		return Receipt{}, reject("precondition_failed", "Knowledge proposal does not exist")
	}
	one, zero := int64(1), int64(0)
	statements = append(statements,
		rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND kind='knowledge.proposed' AND payload_json=?`, Args: []any{c.WorkspaceID, proposalID, streamID, proposalSequence, proposedJSON}, WantRows: true, ExpectedReturnedRows: &one},
		rhiza.SQLStatement{SQL: `SELECT id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('knowledge.proposal.accepted','knowledge.proposal.rejected')`, Args: []any{c.WorkspaceID, streamID}, WantRows: true, ExpectedReturnedRows: &zero},
	)
	decision, _ := payload["decision"].(string)
	eventPayload := map[string]any{"decision": decision}
	if reason, _ := payload["reason"].(string); strings.TrimSpace(reason) != "" {
		eventPayload["reason"] = reason
	}
	kind, event := "knowledge.proposal.rejected", "knowledge.proposal.rejected"
	fact := journalFact{streamID: streamID, kind: kind, participant: participant, payload: eventPayload}
	if decision == "accept" {
		family, _ := payload["family_id"].(string)
		revision, ok := integer(payload["revision"])
		if strings.TrimSpace(family) == "" || !ok || revision < 1 {
			return Receipt{}, reject("schema_mismatch", "accepted proposals require family_id and a positive revision")
		}
		var proposed map[string]any
		if err := json.Unmarshal([]byte(proposedJSON), &proposed); err != nil {
			return Receipt{}, err
		}
		ontology, _ := proposed["proposal"].(map[string]any)
		for _, raw := range anySlice(proposed["sources"]) {
			source := raw.(map[string]any)
			pinnedRevision, sourceBytes := source["revision_id"]
			if !sourceBytes {
				pinnedRevision = source["content_hash"]
			}
			var currentHash, currentSource string
			if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT content_hash,source_markdown FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, source["bundle_id"], source["concept_id"]).Scan(&currentHash, &currentSource); err != nil {
				return Receipt{}, reject("precondition_failed", "Knowledge proposal source changed after proposal creation")
			}
			currentRevision := any(currentHash)
			if sourceBytes {
				currentRevision = knowledge.RevisionID(currentSource)
			}
			if currentRevision != pinnedRevision {
				return Receipt{}, reject("precondition_failed", "Knowledge proposal source changed after proposal creation")
			}
			statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=? AND content_hash=? AND source_markdown=?`, Args: []any{c.WorkspaceID, source["bundle_id"], source["concept_id"], currentHash, currentSource}, WantRows: true, ExpectedReturnedRows: &one})
		}
		if err := validateOntologySpec(ontology); err != nil {
			return Receipt{}, err
		}
		specID, ts := aggregateID("spc", c.CommandID), timestamp(e.now().UTC())
		digest := digestJSON([]byte(canonical(map[string]any{"family_id": family, "revision": revision, "kind": "ontology", "spec": ontology})))
		statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO spec_revisions(id,workspace_id,family_id,revision,kind,payload_json,digest,created_at,created_by_principal_id) VALUES(?,?,?,?,?,?,?,?,?)`, Args: []any{specID, c.WorkspaceID, family, revision, "ontology", canonical(ontology), digest, ts, principal}, ExpectedRowsAffected: &one})
		eventPayload["ontology_revision_id"], eventPayload["family_id"], eventPayload["revision"] = specID, family, revision
		kind, event = "knowledge.proposal.accepted", "knowledge.proposal.accepted"
		fact.kind, fact.specRevision, fact.payload = kind, specID, eventPayload
	}
	effect := makeEffect(streamID, "knowledge_proposal", 2, event, eventPayload)
	effect.journal = []journalFact{fact}
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, timestamp(e.now().UTC()))
}

func (e *Executor) acceptKnowledgeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	bundleID := strings.TrimSpace(payload["bundle_id"].(string))
	if !validBundleID(bundleID) {
		return Receipt{}, reject("validation_failed", "payload.bundle_id must be a non-empty identifier of at most 128 characters")
	}
	document, err := knowledge.Parse(payload["path"].(string), payload["source_markdown"].(string))
	if err != nil {
		return Receipt{}, reject("validation_failed", err.Error())
	}
	type grounding struct{ entityID, predicate, mapping string }
	groundings := make([]grounding, 0, len(document.Groundings))
	one, zero := int64(1), int64(0)
	for _, item := range document.Groundings {
		var entityID string
		if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id FROM object_types WHERE workspace_id=? AND key=? AND archived_at IS NULL`, c.WorkspaceID, item.EntityDefinitionKey).Scan(&entityID); err != nil {
			return Receipt{}, reject("validation_failed", "grounding EntityDefinition does not exist in the workspace")
		}
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM object_types WHERE workspace_id=? AND id=? AND key=? AND archived_at IS NULL`, Args: []any{c.WorkspaceID, entityID, item.EntityDefinitionKey}, WantRows: true, ExpectedReturnedRows: &one})
		groundings = append(groundings, grounding{entityID, item.Predicate, canonical(item.Mapping)})
	}
	eventPayload := map[string]any{"workspace_id": c.WorkspaceID, "bundle_id": bundleID, "concept_id": document.ConceptID, "concept_type": document.ConceptType, "title": document.Title, "description": document.Description, "resource": document.Resource, "body_markdown": document.BodyMarkdown, "revision_id": document.ContentHash, "content_hash": document.ContentHash, "operation": "accepted"}
	var existingHash string
	err = e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT content_hash FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, bundleID, document.ConceptID).Scan(&existingHash)
	ts := timestamp(e.now().UTC())
	if err == nil && existingHash == document.ContentHash {
		statements = append(statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=? AND content_hash=?`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID, existingHash}, WantRows: true, ExpectedReturnedRows: &one})
	} else {
		if err == nil {
			statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE knowledge_documents SET concept_type=?,title=NULLIF(?,''),description=NULLIF(?,''),resource=NULLIF(?,''),frontmatter_json=?,body_markdown=?,source_markdown=?,content_hash=?,updated_at=?,updated_by_principal_id=? WHERE workspace_id=? AND bundle_id=? AND concept_id=? AND content_hash=?`, Args: []any{document.ConceptType, document.Title, document.Description, document.Resource, document.FrontmatterJSON, document.BodyMarkdown, document.SourceMarkdown, document.ContentHash, ts, principal, c.WorkspaceID, bundleID, document.ConceptID, existingHash}, ExpectedRowsAffected: &one})
		} else if errors.Is(err, sql.ErrNoRows) {
			statements = append(statements,
				rhiza.SQLStatement{SQL: `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID}, WantRows: true, ExpectedReturnedRows: &zero},
				rhiza.SQLStatement{SQL: `INSERT INTO knowledge_documents(workspace_id,bundle_id,concept_id,concept_type,title,description,resource,frontmatter_json,body_markdown,source_markdown,content_hash,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,NULLIF(?,''),NULLIF(?,''),NULLIF(?,''),?,?,?,?,?,?,?,?)`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID, document.ConceptType, document.Title, document.Description, document.Resource, document.FrontmatterJSON, document.BodyMarkdown, document.SourceMarkdown, document.ContentHash, ts, ts, principal, principal}, ExpectedRowsAffected: &one},
			)
		} else {
			return Receipt{}, err
		}
		statements = append(statements,
			rhiza.SQLStatement{SQL: `DELETE FROM knowledge_relations WHERE workspace_id=? AND bundle_id=? AND source_concept_id=?`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID}},
			rhiza.SQLStatement{SQL: `DELETE FROM ontology_groundings WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID}},
		)
		for _, relation := range document.Relations {
			external := 0
			if relation.TargetIsExternal {
				external = 1
			}
			metadata := relation.Metadata
			if metadata == nil {
				metadata = map[string]any{}
			}
			statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO knowledge_relations(workspace_id,bundle_id,source_concept_id,kind,predicate,target_ref,target_is_external,cardinality,metadata_json) VALUES(?,?,?,?,?,?,?,NULLIF(?,''),?)`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID, relation.Kind, relation.Predicate, relation.TargetRef, external, relation.Cardinality, canonical(metadata)}, ExpectedRowsAffected: &one})
		}
		for _, item := range groundings {
			statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO ontology_groundings VALUES(?,?,?,?,?,?,?,?)`, Args: []any{c.WorkspaceID, bundleID, document.ConceptID, item.entityID, item.predicate, item.mapping, ts, principal}, ExpectedRowsAffected: &one})
		}
	}
	effect := makeEffect(bundleID+":"+document.ConceptID, "knowledge_document", 1, "knowledge.accepted", eventPayload)
	return e.commitRhiza(ctx, principal, actor, c, statements, effect, ts)
}

func (e *Executor) removeKnowledgeRhiza(ctx context.Context, principal string, actor Actor, c Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	bundleID := strings.TrimSpace(payload["bundle_id"].(string))
	if !validBundleID(bundleID) {
		return Receipt{}, reject("validation_failed", "payload.bundle_id must be a non-empty identifier of at most 128 characters")
	}
	conceptID, err := knowledge.NormalizeConceptID(strings.TrimSpace(payload["concept_id"].(string)) + ".md")
	if err != nil {
		return Receipt{}, reject("validation_failed", "payload.concept_id is invalid")
	}
	var contentHash string
	if err := e.store.ReadRow(ctx, store.ReadLinearizable, `SELECT content_hash FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, c.WorkspaceID, bundleID, conceptID).Scan(&contentHash); err != nil {
		return Receipt{}, reject("validation_failed", "knowledge document does not exist")
	}
	one := int64(1)
	statements = append(statements, rhiza.SQLStatement{SQL: `DELETE FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=? AND content_hash=?`, Args: []any{c.WorkspaceID, bundleID, conceptID, contentHash}, ExpectedRowsAffected: &one})
	eventPayload := map[string]any{"workspace_id": c.WorkspaceID, "bundle_id": bundleID, "concept_id": conceptID, "content_hash": contentHash, "tombstone_id": c.CommandID, "operation": "removed"}
	effect := makeEffect(bundleID+":"+conceptID, "knowledge_document", 1, "knowledge.removed", eventPayload)
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
