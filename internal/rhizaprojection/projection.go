package rhizaprojection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/mrchypark/compos/internal/knowledge"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

var (
	ErrTraversalLimit  = errors.New("ontology traversal exceeded its server resource budget")
	ErrStaleProjection = errors.New("Rhiza graph projection has not reached the required sequence")
	ErrConceptNotFound = errors.New("ontology Concept not found in Rhiza graph projection")
)

const (
	SchemaVersion         = 3
	MaxTraversalRows      = 10_000
	MaxTraversalExpansion = 100_000
)

type TraversalNode struct {
	ConceptID     string
	Depth         int
	FromConceptID *string
	Predicate     *string
	Cardinality   *string
}

type ReachabilityResult struct {
	ConceptIDs           []string
	ProjectionSequence   int64
	ProjectionGeneration string
}

type Projection struct{ store *store.Store }

func Open(s *store.Store) (*Projection, error) {
	if !s.GraphAvailable() {
		return nil, errors.New("embedded Rhiza graph is unavailable")
	}
	return &Projection{store: s}, nil
}

func (p *Projection) Close() error { return nil }

func (p *Projection) Rebuild(ctx context.Context, records []knowledge.ProjectionRecord, sequences map[string]int64, digest string) error {
	for _, edgeType := range []string{"ComposRelation", "ComposGrounding"} {
		result, err := p.store.GraphQuery(ctx, fmt.Sprintf(`MATCH ()-[r:%s]->() RETURN r LIMIT 1`, edgeType), nil)
		if err == nil && len(result.Rows) != 0 {
			if err := p.exec(ctx, "rebuild-clear-"+edgeType+":"+digest, fmt.Sprintf(`MATCH ()-[r:%s]->() DELETE r`, edgeType), nil); err != nil {
				return err
			}
		}
	}
	for _, label := range []string{"ComposConcept", "ComposEntityDefinition", "ComposProjectionState"} {
		if err := p.exec(ctx, "rebuild-clear-"+label+":"+digest, fmt.Sprintf(`MATCH (n:%s) DELETE n`, label), nil); err != nil {
			return err
		}
	}
	groups := map[string][]knowledge.ProjectionRecord{}
	for _, record := range records {
		key := conceptKey(record.WorkspaceID, record.BundleID, record.ConceptID)
		groups[key] = append(groups[key], record)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		first := group[0]
		if err := p.ApplyConcept(ctx, first.WorkspaceID, first.BundleID, first.ConceptID, group, sequences[first.WorkspaceID], digest); err != nil {
			return err
		}
	}
	workspaces := make([]string, 0, len(sequences))
	for workspaceID := range sequences {
		workspaces = append(workspaces, workspaceID)
	}
	sort.Strings(workspaces)
	for _, workspaceID := range workspaces {
		if err := p.writeWorkspaceState(ctx, workspaceID, sequences[workspaceID], digest); err != nil {
			return err
		}
	}
	return p.writeGlobalState(ctx, digest)
}

func (p *Projection) ApplyConcept(ctx context.Context, workspaceID, bundleID, conceptID string, records []knowledge.ProjectionRecord, sequence int64, digest string) error {
	key := conceptKey(workspaceID, bundleID, conceptID)
	for _, edgeType := range []string{"ComposRelation", "ComposGrounding"} {
		result, queryErr := p.store.GraphQuery(ctx, fmt.Sprintf(`MATCH (s:ComposConcept {projection_key: $key})-[r:%s]->() RETURN r LIMIT 1`, edgeType), map[string]any{"key": key})
		if queryErr == nil && len(result.Rows) != 0 {
			if err := p.exec(ctx, key+":edges:"+edgeType+":"+strconv.FormatInt(sequence, 10), fmt.Sprintf(`MATCH (s:ComposConcept {projection_key: $key})-[r:%s]->() DELETE r`, edgeType), map[string]any{"key": key}); err != nil {
				return err
			}
		}
	}
	var concept *knowledge.ProjectionRecord
	for i := range records {
		if records[i].Kind == "concept" {
			concept = &records[i]
			break
		}
	}
	if concept == nil {
		if err := p.upsertConcept(ctx, knowledge.ProjectionRecord{WorkspaceID: workspaceID, BundleID: bundleID, ConceptID: conceptID}, true, sequence); err != nil {
			return err
		}
	} else if err := p.upsertConcept(ctx, *concept, false, sequence); err != nil {
		return err
	}
	for i, record := range records {
		switch record.Kind {
		case "relation":
			if record.RelationKind != "ontology" || record.TargetIsExternal {
				continue
			}
			targetKey := conceptKey(workspaceID, bundleID, record.TargetRef)
			if err := p.upsertConcept(ctx, knowledge.ProjectionRecord{WorkspaceID: workspaceID, BundleID: bundleID, ConceptID: record.TargetRef}, true, sequence); err != nil {
				return err
			}
			if err := p.exec(ctx, key+":relation:"+strconv.Itoa(i)+":"+strconv.FormatInt(sequence, 10), `MATCH (s:ComposConcept {projection_key: $source}), (t:ComposConcept {projection_key: $target}) CREATE (s)-[:ComposRelation {predicate: $predicate, cardinality: $cardinality}]->(t)`, map[string]any{"source": key, "target": targetKey, "predicate": record.Predicate, "cardinality": record.Cardinality}); err != nil {
				return err
			}
		case "grounding":
			entityKey := projectionKey("entity", workspaceID, record.EntityDefinitionID)
			if err := p.upsertEntity(ctx, entityKey, record, sequence); err != nil {
				return err
			}
			mapping, _ := json.Marshal(record.Mapping)
			if err := p.exec(ctx, key+":grounding:"+strconv.Itoa(i)+":"+strconv.FormatInt(sequence, 10), `MATCH (s:ComposConcept {projection_key: $source}), (t:ComposEntityDefinition {projection_key: $target}) CREATE (s)-[:ComposGrounding {predicate: $predicate, mapping_json: $mapping_json}]->(t)`, map[string]any{"source": key, "target": entityKey, "predicate": record.Predicate, "mapping_json": string(mapping)}); err != nil {
				return err
			}
		}
	}
	if err := p.writeGlobalState(ctx, digest); err != nil {
		return err
	}
	return p.writeWorkspaceState(ctx, workspaceID, sequence, digest)
}

func (p *Projection) Reachable(ctx context.Context, workspaceID, bundleID, conceptID string, depth, maxResults, maxScannedEdges int, minSequence int64) (ReachabilityResult, error) {
	sequences, digest, version, slot, err := p.readState(ctx)
	if err != nil {
		return ReachabilityResult{}, err
	}
	sequence, found := sequences[workspaceID]
	if !found || version != SchemaVersion || sequence < minSequence {
		return ReachabilityResult{}, ErrStaleProjection
	}
	// Read the durable head, not the HTTP server's cached head: another server
	// may have committed a Knowledge change without waking this reconciler.
	var head int64
	row := p.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COALESCE(MAX(projection_sequence),0) FROM projection_events WHERE workspace_id=? AND aggregate_type='knowledge_document'`, workspaceID)
	if err := row.Scan(&head); err != nil {
		return ReachabilityResult{}, err
	}
	if head != sequence || row.Version().AppliedSlot != slot {
		return ReachabilityResult{}, ErrStaleProjection
	}
	result, err := p.store.GraphReachable(ctx, rhiza.GraphReachableRequest{
		StartLabel: "ComposConcept", StartProperty: "projection_key", StartValue: conceptKey(workspaceID, bundleID, conceptID),
		EdgeType: "ComposRelation", NodeLabel: "ComposConcept", NodeFilters: map[string]any{"workspace_id": workspaceID, "bundle_id": bundleID},
		ResultProperty: "concept_id", MaxDepth: uint32(depth), MaxResults: uint(maxResults), MaxScannedEdges: uint(maxScannedEdges), MaxBytes: rhiza.MaxGraphReachableBytes,
		RequireAppliedSlot: &slot,
	})
	if err != nil {
		if errors.Is(err, rhiza.ErrGraphResourceLimit) {
			return ReachabilityResult{}, ErrTraversalLimit
		}
		if errors.Is(err, rhiza.ErrReadVersionMismatch) {
			return ReachabilityResult{}, ErrStaleProjection
		}
		return ReachabilityResult{}, err
	}
	if !result.StartFound {
		return ReachabilityResult{}, ErrConceptNotFound
	}
	ids := make([]string, len(result.Nodes))
	for i, node := range result.Nodes {
		id, ok := node.Value.(string)
		if !ok || id == "" {
			return ReachabilityResult{}, errors.New("Rhiza graph projection returned an invalid Concept ID")
		}
		ids[i] = id
	}
	sort.Strings(ids)
	return ReachabilityResult{ConceptIDs: ids, ProjectionSequence: sequence, ProjectionGeneration: digest}, nil
}

func (p *Projection) Cursor(ctx context.Context, workspaceID string) (int64, string, error) {
	sequences, digest, _, err := p.State(ctx)
	return sequences[workspaceID], digest, err
}

func (p *Projection) State(ctx context.Context) (map[string]int64, string, int, error) {
	sequences, digest, version, _, err := p.readState(ctx)
	return sequences, digest, version, err
}

func (p *Projection) readState(ctx context.Context) (map[string]int64, string, int, uint64, error) {
	result, err := p.store.GraphQuery(ctx, `MATCH (s:ComposProjectionState) RETURN s.scope AS scope, s.workspace_id AS workspace_id, s.sequence AS sequence, s.digest AS digest, s.version AS version`, nil)
	if err != nil {
		return nil, "", 0, 0, err
	}
	sequences := map[string]int64{}
	var digest string
	var version int
	for _, row := range result.Rows {
		values := rowMap(result.Columns, row)
		if stringValue(values["scope"]) == "global" {
			digest = stringValue(values["digest"])
			version = int(int64Value(values["version"]))
		} else if workspaceID := stringValue(values["workspace_id"]); workspaceID != "" {
			sequences[workspaceID] = int64Value(values["sequence"])
		}
	}
	if digest == "" {
		return sequences, "", version, result.AppliedSlot, ErrStaleProjection
	}
	return sequences, digest, version, result.AppliedSlot, nil
}

func (p *Projection) upsertConcept(ctx context.Context, record knowledge.ProjectionRecord, placeholder bool, sequence int64) error {
	key := conceptKey(record.WorkspaceID, record.BundleID, record.ConceptID)
	args := map[string]any{"key": key, "workspace_id": record.WorkspaceID, "bundle_id": record.BundleID, "concept_id": record.ConceptID, "type": record.ConceptType, "title": record.Title, "description": record.Description, "body": record.BodyMarkdown, "content_hash": record.ContentHash, "placeholder": placeholder}
	found, err := p.nodeExists(ctx, "ComposConcept", key)
	if err != nil {
		return err
	}
	if !found {
		return p.exec(ctx, key+":create:"+strconv.FormatInt(sequence, 10), `CREATE (:ComposProjection:ComposConcept {projection_key: $key, workspace_id: $workspace_id, bundle_id: $bundle_id, concept_id: $concept_id, type: $type, title: $title, description: $description, body: $body, content_hash: $content_hash, placeholder: $placeholder})`, args)
	}
	properties := []string{"workspace_id", "bundle_id", "concept_id", "type", "title", "description", "body", "content_hash", "placeholder"}
	for _, property := range properties {
		if err := p.exec(ctx, key+":update:"+property+":"+strconv.FormatInt(sequence, 10), fmt.Sprintf(`MATCH (n:ComposConcept {projection_key: $key}) SET n.%s = $value`, property), map[string]any{"key": key, "value": args[property]}); err != nil {
			return err
		}
	}
	return nil
}

func (p *Projection) upsertEntity(ctx context.Context, key string, record knowledge.ProjectionRecord, sequence int64) error {
	found, err := p.nodeExists(ctx, "ComposEntityDefinition", key)
	if err != nil || found {
		return err
	}
	return p.exec(ctx, key+":create:"+strconv.FormatInt(sequence, 10), `CREATE (:ComposProjection:ComposEntityDefinition {projection_key: $key, workspace_id: $workspace_id, id: $id, key: $entity_key})`, map[string]any{"key": key, "workspace_id": record.WorkspaceID, "id": record.EntityDefinitionID, "entity_key": record.EntityDefinitionKey})
}

func (p *Projection) writeWorkspaceState(ctx context.Context, workspaceID string, sequence int64, digest string) error {
	key := projectionKey("state", workspaceID)
	found, err := p.nodeExists(ctx, "ComposProjectionState", key)
	if err != nil {
		return err
	}
	args := map[string]any{"key": key, "workspace_id": workspaceID, "sequence": sequence, "digest": digest, "version": SchemaVersion}
	if !found {
		return p.exec(ctx, key+":create:"+strconv.FormatInt(sequence, 10)+":"+digest, `CREATE (:ComposProjection:ComposProjectionState {projection_key: $key, scope: 'workspace', workspace_id: $workspace_id, sequence: $sequence, digest: $digest, version: $version})`, args)
	}
	if err := p.exec(ctx, key+":delete:"+strconv.FormatInt(sequence, 10)+":"+digest, `MATCH (s:ComposProjectionState {projection_key: $key}) DELETE s`, args); err != nil {
		return err
	}
	return p.exec(ctx, key+":create-after-delete:"+strconv.FormatInt(sequence, 10)+":"+digest, `CREATE (:ComposProjection:ComposProjectionState {projection_key: $key, scope: 'workspace', workspace_id: $workspace_id, sequence: $sequence, digest: $digest, version: $version})`, args)
}

func (p *Projection) writeGlobalState(ctx context.Context, digest string) error {
	key := projectionKey("state", "global")
	found, err := p.nodeExists(ctx, "ComposProjectionState", key)
	if err != nil {
		return err
	}
	args := map[string]any{"key": key, "digest": digest, "version": SchemaVersion}
	if !found {
		return p.exec(ctx, key+":create:"+digest, `CREATE (:ComposProjection:ComposProjectionState {projection_key: $key, scope: 'global', workspace_id: '', sequence: 0, digest: $digest, version: $version})`, args)
	}
	if err := p.exec(ctx, key+":delete:"+digest, `MATCH (s:ComposProjectionState {projection_key: $key}) DELETE s`, args); err != nil {
		return err
	}
	return p.exec(ctx, key+":create-after-delete:"+digest, `CREATE (:ComposProjection:ComposProjectionState {projection_key: $key, scope: 'global', workspace_id: '', sequence: 0, digest: $digest, version: $version})`, args)
}

func (p *Projection) nodeExists(ctx context.Context, label, key string) (bool, error) {
	result, err := p.store.GraphQuery(ctx, fmt.Sprintf(`MATCH (n:%s {projection_key: $key}) RETURN n.projection_key AS projection_key LIMIT 1`, label), map[string]any{"key": key})
	return len(result.Rows) != 0, err
}

func (p *Projection) exec(ctx context.Context, requestID, cypher string, args map[string]any) error {
	sum := sha256.Sum256([]byte(requestID))
	if err := p.store.GraphExecute(ctx, "compos-graph-"+hex.EncodeToString(sum[:16]), cypher, args); err != nil {
		return fmt.Errorf("project graph %q: %w", requestID, err)
	}
	return nil
}

func conceptKey(workspaceID, bundleID, conceptID string) string {
	return projectionKey("concept", workspaceID, bundleID, conceptID)
}

func projectionKey(parts ...string) string {
	encoded, _ := json.Marshal(parts)
	return string(encoded)
}

func rowMap(columns []string, row []any) map[string]any {
	values := make(map[string]any, len(columns))
	for i, column := range columns {
		if i < len(row) {
			values[column] = row[i]
		}
	}
	return values
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func int64Value(value any) int64 {
	switch number := value.(type) {
	case int64:
		return number
	case int:
		return int64(number)
	case float64:
		return int64(number)
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		return parsed
	}
}
