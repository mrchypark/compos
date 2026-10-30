package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/knowledge"
	rhizaprojection "github.com/mrchypark/compos/internal/rhizaprojection"
	"github.com/mrchypark/compos/internal/store"
)

func (s *Server) listKnowledge(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	bundle := strings.TrimSpace(r.URL.Query().Get("bundle_id"))
	conceptType := strings.TrimSpace(r.URL.Query().Get("type"))
	if strings.TrimSpace(r.URL.Query().Get("q")) != "" {
		problem(w, 400, "invalid_request", "validation", false, "Knowledge full-text search uses POST /api/v1/workspaces/{workspace_id}/search")
		return
	}
	limit := boundedInt(r, "limit", 50, 1, 100)
	offset := boundedInt(r, "offset", 0, 0, 1<<30)
	query := `SELECT d.bundle_id,d.concept_id,d.concept_type,d.title,d.description,d.resource,d.content_hash,d.updated_at FROM knowledge_documents d`
	args := []any{workspace}
	query += ` WHERE d.workspace_id=?`
	if bundle != "" {
		query += ` AND d.bundle_id=?`
		args = append(args, bundle)
	}
	if conceptType != "" {
		query += ` AND d.concept_type=?`
		args = append(args, conceptType)
	}
	query += ` ORDER BY d.bundle_id,d.concept_id`
	query += ` LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var bundleID, conceptID, kind, hash, updated string
		var title, description, resource sql.NullString
		if err := rows.Scan(&bundleID, &conceptID, &kind, &title, &description, &resource, &hash, &updated); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"bundle_id": bundleID, "concept_id": conceptID, "type": kind, "title": nullString(title), "description": nullString(description), "resource": nullString(resource), "content_hash": hash, "updated_at": updated})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": data, "page": map[string]any{"limit": limit, "offset": offset}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) readKnowledge(w http.ResponseWriter, r *http.Request) {
	conceptID, err := knowledge.NormalizeConceptID(r.PathValue("concept_id") + ".md")
	if err != nil {
		problem(w, 400, "invalid_request", "validation", false, "concept_id is invalid")
		return
	}
	workspace, bundle := r.PathValue("workspace_id"), r.PathValue("bundle_id")
	format := r.URL.Query().Get("format")
	if format != "" && format != "source" {
		problem(w, 400, "invalid_request", "validation", false, "format must be source")
		return
	}
	if format == "source" {
		var source string
		revisionID := strings.TrimSpace(r.URL.Query().Get("revision_id"))
		if revisionID == "" {
			err = s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT source_markdown FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, workspace, bundle, conceptID).Scan(&source)
		} else {
			source, err = s.historicalKnowledgeSource(r.Context(), workspace, bundle+":"+conceptID, revisionID)
		}
		if errors.Is(err, sql.ErrNoRows) {
			problem(w, 404, "not_found", "request", false, "knowledge document not found")
			return
		}
		if err != nil {
			dbError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(source))
		return
	}

	var kind, frontmatter, body, source, hash, created, updated, createdBy, updatedBy string
	var title, description, resource sql.NullString
	err = s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT concept_type,title,description,resource,frontmatter_json,body_markdown,source_markdown,content_hash,created_at,updated_at,created_by_principal_id,updated_by_principal_id FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, workspace, bundle, conceptID).Scan(&kind, &title, &description, &resource, &frontmatter, &body, &source, &hash, &created, &updated, &createdBy, &updatedBy)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, 404, "not_found", "request", false, "knowledge document not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	relations, err := s.knowledgeRelations(r, workspace, bundle, conceptID)
	if err != nil {
		dbError(w, err)
		return
	}
	groundings, err := s.groundings(r, workspace, bundle, conceptID, "")
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": map[string]any{
		"bundle_id": bundle, "concept_id": conceptID, "type": kind, "title": nullString(title), "description": nullString(description), "resource": nullString(resource),
		"frontmatter": raw(frontmatter), "body_markdown": body, "revision_id": knowledge.RevisionID(source), "content_hash": hash, "relations": relations, "groundings": groundings,
		"created_at": created, "updated_at": updated, "created_by_principal_id": createdBy, "updated_by_principal_id": updatedBy,
	}, "consistency": consistency("canonical_linearizable")})
}

func (s *Server) historicalKnowledgeSource(ctx context.Context, workspace, aggregateID, revisionID string) (string, error) {
	rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT json_extract(payload_json,'$.source_markdown') FROM command_ledger
		WHERE workspace_id=? AND command_type='knowledge.accept' AND json_extract(result_json,'$.aggregate.id')=? ORDER BY created_at,command_id`, workspace, aggregateID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			return "", err
		}
		if knowledge.RevisionID(source) == revisionID {
			return source, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "", sql.ErrNoRows
}

func (s *Server) knowledgeRelations(r *http.Request, workspace, bundle, conceptID string) ([]any, error) {
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT kind,predicate,target_ref,target_is_external,cardinality,metadata_json FROM knowledge_relations WHERE workspace_id=? AND bundle_id=? AND source_concept_id=? ORDER BY kind,predicate,target_ref`, workspace, bundle, conceptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var kind, predicate, target, metadata string
		var external int
		var cardinality sql.NullString
		if err := rows.Scan(&kind, &predicate, &target, &external, &cardinality, &metadata); err != nil {
			return nil, err
		}
		data = append(data, map[string]any{"kind": kind, "predicate": predicate, "target_ref": target, "target_is_external": external == 1, "cardinality": nullString(cardinality), "metadata": raw(metadata)})
	}
	return data, rows.Err()
}

func (s *Server) traverseOntology(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	workspace := r.PathValue("workspace_id")
	bundle := strings.TrimSpace(r.URL.Query().Get("bundle_id"))
	conceptID, err := knowledge.NormalizeConceptID(strings.TrimSpace(r.URL.Query().Get("concept_id")) + ".md")
	if bundle == "" || err != nil {
		problem(w, 400, "invalid_request", "validation", false, "bundle_id and a valid concept_id are required")
		return
	}
	direction := r.URL.Query().Get("direction")
	if direction == "" {
		direction = "out"
	}
	if direction != "out" && direction != "in" && direction != "both" {
		problem(w, 400, "invalid_request", "validation", false, "direction must be out, in, or both")
		return
	}
	depth := 1
	if rawDepth := r.URL.Query().Get("depth"); rawDepth != "" {
		depth, err = strconv.Atoi(rawDepth)
		if err != nil || depth < 1 || depth > 8 {
			problem(w, 400, "invalid_request", "validation", false, "depth must be between 1 and 8")
			return
		}
	}
	var exists int
	if err := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, workspace, bundle, conceptID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		problem(w, 404, "not_found", "request", false, "ontology Concept not found")
		return
	} else if err != nil {
		dbError(w, err)
		return
	}
	nodes, err := s.sqliteTraversal(ctx, workspace, bundle, conceptID, direction, depth)
	if err != nil {
		if errors.Is(err, rhizaprojection.ErrTraversalLimit) || errors.Is(err, context.DeadlineExceeded) {
			problem(w, 422, "resource_limit_exceeded", "request", false, err.Error())
			return
		}
		dbError(w, err)
		return
	}
	data := make([]any, 0, len(nodes))
	for _, node := range nodes {
		data = append(data, map[string]any{"concept_id": node.ConceptID, "depth": node.Depth, "from_concept_id": node.FromConceptID, "predicate": node.Predicate, "cardinality": node.Cardinality})
	}
	write(w, 200, map[string]any{"data": data, "query": map[string]any{"bundle_id": bundle, "concept_id": conceptID, "direction": direction, "depth": depth}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) reachableOntologyNodeIDs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	workspace := r.PathValue("workspace_id")
	bundle := strings.TrimSpace(r.URL.Query().Get("bundle_id"))
	conceptID, err := knowledge.NormalizeConceptID(strings.TrimSpace(r.URL.Query().Get("concept_id")) + ".md")
	if bundle == "" || err != nil {
		problem(w, 400, "invalid_request", "validation", false, "bundle_id and a valid concept_id are required")
		return
	}
	depth, err := strictQueryInt(r, "depth", 1, 1, 8)
	if err != nil {
		problem(w, 400, "invalid_request", "validation", false, err.Error())
		return
	}
	maxResults, err := strictQueryInt(r, "max_results", rhizaprojection.MaxTraversalRows, 1, rhizaprojection.MaxTraversalRows)
	if err != nil {
		problem(w, 400, "invalid_request", "validation", false, err.Error())
		return
	}
	maxScannedEdges, err := strictQueryInt(r, "max_scanned_edges", rhizaprojection.MaxTraversalExpansion, 1, rhizaprojection.MaxTraversalExpansion)
	if err != nil {
		problem(w, 400, "invalid_request", "validation", false, err.Error())
		return
	}
	var minSequence int64
	if raw := strings.TrimSpace(r.URL.Query().Get("min_projection_sequence")); raw != "" {
		minSequence, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || minSequence < 0 {
			problem(w, 400, "invalid_request", "validation", false, "min_projection_sequence must be a non-negative integer")
			return
		}
	}
	var exists int
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT 1 FROM knowledge_documents WHERE workspace_id=? AND bundle_id=? AND concept_id=?`, workspace, bundle, conceptID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		problem(w, 404, "not_found", "request", false, "ontology Concept not found")
		return
	} else if err != nil {
		dbError(w, err)
		return
	}
	if s.graphProjection == nil {
		problem(w, 503, "graph_unavailable", "projection", true, "embedded Rhiza graph reachability is not available")
		return
	}
	// Do not wait behind a multi-command rebuild or expose its intermediate
	// state. Reachable also fences changes between watermark and traversal reads.
	if !s.graphProjectionSync.TryRLock() {
		problem(w, 503, "stale_projection", "projection", true, "Rhiza graph projection is updating")
		return
	}
	defer s.graphProjectionSync.RUnlock()
	if !s.graphProjectionReady(workspace) {
		problem(w, 503, "stale_projection", "projection", true, "Rhiza graph projection has not reached the canonical head")
		return
	}
	result, err := s.graphProjection.Reachable(ctx, workspace, bundle, conceptID, depth, maxResults, maxScannedEdges, minSequence)
	if err != nil {
		switch {
		case errors.Is(err, rhizaprojection.ErrStaleProjection):
			problem(w, 503, "stale_projection", "projection", true, err.Error())
		case errors.Is(err, rhizaprojection.ErrTraversalLimit):
			problem(w, 422, "resource_limit_exceeded", "request", false, err.Error())
		case errors.Is(err, context.DeadlineExceeded):
			problem(w, 504, "deadline_exceeded", "request", true, err.Error())
		case errors.Is(err, rhizaprojection.ErrConceptNotFound):
			problem(w, 503, "stale_projection", "projection", true, err.Error())
		default:
			dbError(w, err)
		}
		return
	}
	write(w, 200, map[string]any{
		"data":        map[string]any{"concept_ids": result.ConceptIDs},
		"query":       map[string]any{"bundle_id": bundle, "concept_id": conceptID, "depth": depth, "max_results": maxResults, "max_scanned_edges": maxScannedEdges},
		"consistency": map[string]any{"mode": "eventual_bounded", "source": "rhiza_graph", "projection_sequence": result.ProjectionSequence, "projection_generation": result.ProjectionGeneration},
	})
}

func strictQueryInt(r *http.Request, key string, defaultValue, minValue, maxValue int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minValue || value > maxValue {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue)
	}
	return value, nil
}

func (s *Server) sqliteTraversal(ctx context.Context, workspaceID, bundleID, conceptID, direction string, depth int) ([]rhizaprojection.TraversalNode, error) {
	type edge struct{ source, target, predicate, cardinality string }
	rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT source_concept_id,target_ref,predicate,COALESCE(cardinality,'') FROM knowledge_relations WHERE workspace_id=? AND bundle_id=? AND kind='ontology' AND target_is_external=0 ORDER BY source_concept_id,target_ref,predicate LIMIT ?`, workspaceID, bundleID, rhizaprojection.MaxTraversalExpansion+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	edges := make([]edge, 0)
	for rows.Next() {
		var item edge
		if err := rows.Scan(&item.source, &item.target, &item.predicate, &item.cardinality); err != nil {
			return nil, err
		}
		edges = append(edges, item)
		if len(edges) > rhizaprojection.MaxTraversalExpansion {
			return nil, rhizaprojection.ErrTraversalLimit
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	adjacency := map[string][]edge{}
	for _, item := range edges {
		if direction != "in" {
			adjacency[item.source] = append(adjacency[item.source], item)
		}
		if direction != "out" {
			adjacency[item.target] = append(adjacency[item.target], edge{source: item.target, target: item.source, predicate: item.predicate, cardinality: item.cardinality})
		}
	}
	type path struct {
		node    string
		depth   int
		visited map[string]bool
	}
	result := []rhizaprojection.TraversalNode{{ConceptID: conceptID, Depth: 0}}
	frontier := []path{{node: conceptID, visited: map[string]bool{conceptID: true}}}
	expansions := 0
	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := frontier[0]
		frontier = frontier[1:]
		if current.depth == depth {
			continue
		}
		for _, item := range adjacency[current.node] {
			expansions++
			if expansions > rhizaprojection.MaxTraversalExpansion || len(result) >= rhizaprojection.MaxTraversalRows {
				return nil, rhizaprojection.ErrTraversalLimit
			}
			if current.visited[item.target] {
				continue
			}
			from, predicate, cardinality := current.node, item.predicate, item.cardinality
			result = append(result, rhizaprojection.TraversalNode{ConceptID: item.target, Depth: current.depth + 1, FromConceptID: &from, Predicate: &predicate, Cardinality: &cardinality})
			visited := make(map[string]bool, len(current.visited)+1)
			for node := range current.visited {
				visited[node] = true
			}
			visited[item.target] = true
			frontier = append(frontier, path{node: item.target, depth: current.depth + 1, visited: visited})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Depth != result[j].Depth {
			return result[i].Depth < result[j].Depth
		}
		if result[i].ConceptID != result[j].ConceptID {
			return result[i].ConceptID < result[j].ConceptID
		}
		if pointerString(result[i].FromConceptID) != pointerString(result[j].FromConceptID) {
			return pointerString(result[i].FromConceptID) < pointerString(result[j].FromConceptID)
		}
		return pointerString(result[i].Predicate) < pointerString(result[j].Predicate)
	})
	return result, nil
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Server) listGroundings(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	bundle := strings.TrimSpace(r.URL.Query().Get("bundle_id"))
	conceptID := strings.TrimSpace(r.URL.Query().Get("concept_id"))
	entityID := strings.TrimSpace(r.URL.Query().Get("entity_definition_id"))
	if conceptID != "" {
		var err error
		conceptID, err = knowledge.NormalizeConceptID(conceptID + ".md")
		if err != nil {
			problem(w, 400, "invalid_request", "validation", false, "concept_id is invalid")
			return
		}
	}
	data, err := s.groundings(r, workspace, bundle, conceptID, entityID)
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": data, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) groundings(r *http.Request, workspace, bundle, conceptID, entityID string) ([]any, error) {
	query := `SELECT g.bundle_id,g.concept_id,g.entity_definition_id,t.key,g.predicate,g.mapping_json,g.created_at,g.created_by_principal_id FROM ontology_groundings g JOIN object_types t ON t.workspace_id=g.workspace_id AND t.id=g.entity_definition_id WHERE g.workspace_id=?`
	args := []any{workspace}
	if bundle != "" {
		query += ` AND g.bundle_id=?`
		args = append(args, bundle)
	}
	if conceptID != "" {
		query += ` AND g.concept_id=?`
		args = append(args, conceptID)
	}
	if entityID != "" {
		query += ` AND g.entity_definition_id=?`
		args = append(args, entityID)
	}
	query += ` ORDER BY g.bundle_id,g.concept_id,g.entity_definition_id,g.predicate`
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var bundleID, concept, id, key, predicate, mapping, created, createdBy string
		if err := rows.Scan(&bundleID, &concept, &id, &key, &predicate, &mapping, &created, &createdBy); err != nil {
			return nil, err
		}
		data = append(data, map[string]any{"bundle_id": bundleID, "concept_id": concept, "entity_definition_id": id, "entity_definition_key": key, "predicate": predicate, "mapping": raw(mapping), "created_at": created, "created_by_principal_id": createdBy})
	}
	return data, rows.Err()
}
