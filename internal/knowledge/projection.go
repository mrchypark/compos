package knowledge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/mrchypark/compos/internal/store"
)

type ProjectionRecord struct {
	Kind                string         `json:"kind"`
	WorkspaceID         string         `json:"workspace_id"`
	BundleID            string         `json:"bundle_id"`
	ConceptID           string         `json:"concept_id"`
	ContentHash         string         `json:"content_hash"`
	ConceptType         string         `json:"concept_type,omitempty"`
	Title               string         `json:"title,omitempty"`
	Description         string         `json:"description,omitempty"`
	BodyMarkdown        string         `json:"body_markdown,omitempty"`
	RelationKind        string         `json:"relation_kind,omitempty"`
	Predicate           string         `json:"predicate,omitempty"`
	TargetRef           string         `json:"target_ref,omitempty"`
	TargetIsExternal    bool           `json:"target_is_external,omitempty"`
	Cardinality         string         `json:"cardinality,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
	EntityDefinitionID  string         `json:"entity_definition_id,omitempty"`
	EntityDefinitionKey string         `json:"entity_definition_key,omitempty"`
	Mapping             map[string]any `json:"mapping,omitempty"`
}

type projectionReader struct {
	store       *store.Store
	consistency store.ReadConsistency
}

func (r projectionReader) ReadRow(ctx context.Context, query string, args ...any) store.Row {
	return r.store.ReadRow(ctx, r.consistency, query, args...)
}

func (r projectionReader) ReadRows(ctx context.Context, query string, args ...any) (store.Rows, error) {
	return r.store.ReadRows(ctx, r.consistency, query, args...)
}

func Snapshot(ctx context.Context, db *store.Store, workspaceID string) ([]ProjectionRecord, error) {
	return snapshot(ctx, projectionReader{store: db, consistency: store.ReadLocal}, workspaceID, "", "")
}

func SnapshotAll(ctx context.Context, db *store.Store) ([]ProjectionRecord, error) {
	return snapshotAll(ctx, projectionReader{store: db, consistency: store.ReadLocal})
}

// SnapshotState reads projection data and every workspace cursor from one
// materialized state. Rhiza retries if the state advances between queries.
func SnapshotState(ctx context.Context, db *store.Store) ([]ProjectionRecord, map[string]int64, string, error) {
	var records []ProjectionRecord
	var sequences map[string]int64
	_, err := db.ReadSnapshot(ctx, store.ReadLocal, func(reader store.SnapshotReader) error {
		var err error
		records, err = snapshotAll(ctx, reader)
		if err != nil {
			return err
		}
		sequences, err = headSequences(ctx, reader)
		return err
	})
	if err != nil {
		return nil, nil, "", err
	}
	return records, sequences, ProjectionDigest(records), nil
}

// HeadSequences returns the durable knowledge-projection head for each workspace.
// Other aggregate events intentionally do not make the Knowledge projection stale.
func HeadSequences(ctx context.Context, db *store.Store) (map[string]int64, error) {
	return headSequences(ctx, projectionReader{store: db, consistency: store.ReadLocal})
}

func headSequences(ctx context.Context, db store.SnapshotReader) (map[string]int64, error) {
	rows, err := db.ReadRows(ctx, `SELECT w.id,COALESCE(MAX(e.projection_sequence),0) FROM workspaces w LEFT JOIN projection_events e ON e.workspace_id=w.id AND e.aggregate_type='knowledge_document' GROUP BY w.id ORDER BY w.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sequences := map[string]int64{}
	for rows.Next() {
		var workspaceID string
		var sequence int64
		if err := rows.Scan(&workspaceID, &sequence); err != nil {
			return nil, err
		}
		sequences[workspaceID] = sequence
	}
	return sequences, rows.Err()
}

func snapshotAll(ctx context.Context, db store.SnapshotReader) ([]ProjectionRecord, error) {
	rows, err := db.ReadRows(ctx, `SELECT id FROM workspaces ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var workspaces []string
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			rows.Close()
			return nil, err
		}
		workspaces = append(workspaces, workspaceID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	all := []ProjectionRecord{}
	for _, workspaceID := range workspaces {
		records, err := snapshot(ctx, db, workspaceID, "", "")
		if err != nil {
			return nil, err
		}
		all = append(all, records...)
	}
	return all, nil
}

func ConceptSnapshot(ctx context.Context, db *store.Store, workspaceID, bundleID, conceptID string) ([]ProjectionRecord, error) {
	return snapshot(ctx, projectionReader{store: db, consistency: store.ReadLocal}, workspaceID, bundleID, conceptID)
}

func ProjectionDigest(records []ProjectionRecord) string {
	raw, _ := json.Marshal(records)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func snapshot(ctx context.Context, db store.SnapshotReader, workspaceID, bundleID, conceptID string) ([]ProjectionRecord, error) {
	filter := ""
	args := []any{workspaceID}
	if bundleID != "" {
		filter += ` AND bundle_id=?`
		args = append(args, bundleID)
	}
	if conceptID != "" {
		filter += ` AND concept_id=?`
		args = append(args, conceptID)
	}
	rows, err := db.ReadRows(ctx, `SELECT bundle_id,concept_id,content_hash,concept_type,COALESCE(title,''),COALESCE(description,''),body_markdown FROM knowledge_documents WHERE workspace_id=?`+filter+` ORDER BY bundle_id,concept_id`, args...)
	if err != nil {
		return nil, err
	}
	records := []ProjectionRecord{}
	for rows.Next() {
		record := ProjectionRecord{Kind: "concept", WorkspaceID: workspaceID}
		if err := rows.Scan(&record.BundleID, &record.ConceptID, &record.ContentHash, &record.ConceptType, &record.Title, &record.Description, &record.BodyMarkdown); err != nil {
			rows.Close()
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	filter = ""
	args = []any{workspaceID}
	if bundleID != "" {
		filter += ` AND r.bundle_id=?`
		args = append(args, bundleID)
	}
	if conceptID != "" {
		filter += ` AND r.source_concept_id=?`
		args = append(args, conceptID)
	}
	rows, err = db.ReadRows(ctx, `SELECT r.bundle_id,r.source_concept_id,d.content_hash,r.kind,r.predicate,r.target_ref,r.target_is_external,COALESCE(r.cardinality,''),r.metadata_json FROM knowledge_relations r JOIN knowledge_documents d ON d.workspace_id=r.workspace_id AND d.bundle_id=r.bundle_id AND d.concept_id=r.source_concept_id WHERE r.workspace_id=?`+filter+` ORDER BY r.bundle_id,r.source_concept_id,r.kind,r.predicate,r.target_ref`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		record := ProjectionRecord{Kind: "relation", WorkspaceID: workspaceID}
		var external int
		var metadata string
		if err := rows.Scan(&record.BundleID, &record.ConceptID, &record.ContentHash, &record.RelationKind, &record.Predicate, &record.TargetRef, &external, &record.Cardinality, &metadata); err != nil {
			rows.Close()
			return nil, err
		}
		record.TargetIsExternal = external == 1
		_ = json.Unmarshal([]byte(metadata), &record.Metadata)
		records = append(records, record)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	filter = ""
	args = []any{workspaceID}
	if bundleID != "" {
		filter += ` AND g.bundle_id=?`
		args = append(args, bundleID)
	}
	if conceptID != "" {
		filter += ` AND g.concept_id=?`
		args = append(args, conceptID)
	}
	rows, err = db.ReadRows(ctx, `SELECT g.bundle_id,g.concept_id,d.content_hash,g.predicate,g.entity_definition_id,t.key,g.mapping_json FROM ontology_groundings g JOIN knowledge_documents d ON d.workspace_id=g.workspace_id AND d.bundle_id=g.bundle_id AND d.concept_id=g.concept_id JOIN object_types t ON t.workspace_id=g.workspace_id AND t.id=g.entity_definition_id WHERE g.workspace_id=?`+filter+` ORDER BY g.bundle_id,g.concept_id,g.predicate,g.entity_definition_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		record := ProjectionRecord{Kind: "grounding", WorkspaceID: workspaceID}
		var mapping string
		if err := rows.Scan(&record.BundleID, &record.ConceptID, &record.ContentHash, &record.Predicate, &record.EntityDefinitionID, &record.EntityDefinitionKey, &mapping); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(mapping), &record.Mapping)
		records = append(records, record)
	}
	return records, rows.Err()
}
