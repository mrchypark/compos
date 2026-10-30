package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/compos/internal/kernel"
	rhizaprojection "github.com/mrchypark/compos/internal/rhizaprojection"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

type Server struct {
	store                       *store.Store
	kernel                      *kernel.Executor
	mux                         *http.ServeMux
	graphProjection             *rhizaprojection.Projection
	graphProjectionSet          bool
	graphProjectionMu           sync.RWMutex
	graphProjectionSync         sync.RWMutex
	graphProjectionErr          string
	graphProjectionHeads        map[string]int64
	graphProjectionCursors      map[string]int64
	graphProjectionRebuilding   bool
	graphProjectionLastSuccess  string
	graphProjectionRebuilds     int64
	graphProjectionWake         chan struct{}
	graphProjectionCancel       context.CancelFunc
	graphProjectionDone         chan struct{}
	conductorURL                string
	conductorToken              string
	conductorCallbackToken      string
	conductorClient             *http.Client
	conductorWake               chan struct{}
	conductorCancel             context.CancelFunc
	conductorDone               chan struct{}
	workflowWake                chan struct{}
	workflowCancel              context.CancelFunc
	workflowDone                chan struct{}
	searchURL                   string
	searchIndex                 string
	searchKey                   string
	searchClient                *http.Client
	searchSyncerToken           string
	searchSyncerWorkspaceID     string
	auth                        AuthConfig
	authClient                  *http.Client
	oauth2IntrospectionEndpoint string
	oauth2UserInfoEndpoint      string
}

func New(s *store.Store) *Server {
	server := &Server{
		store: s, kernel: kernel.NewStoreExecutor(s), mux: http.NewServeMux(),
		searchClient: &http.Client{Timeout: 10 * time.Second}, auth: AuthConfig{Mode: AuthModeNone},
		authClient: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }},
	}
	server.routes()
	server.enableWorkflowScheduler()
	if s.GraphAvailable() {
		server.enableRhizaGraph()
	}
	return server
}

func (server *Server) enableRhizaGraph() {
	server.graphProjectionSet = true
	projection, err := rhizaprojection.Open(server.store)
	if err != nil {
		server.graphProjectionErr = err.Error()
		return
	}
	server.graphProjection = projection
	server.graphProjectionErr = "reconciling"
	server.graphProjectionHeads = map[string]int64{}
	server.graphProjectionCursors = map[string]int64{}
	server.graphProjectionWake = make(chan struct{}, 1)
	server.graphProjectionDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	server.graphProjectionCancel = cancel
	go server.runGraphProjectionReconciler(ctx)
}

func (s *Server) Close() error {
	if s.workflowCancel != nil {
		s.workflowCancel()
		<-s.workflowDone
	}
	if s.conductorCancel != nil {
		s.conductorCancel()
		<-s.conductorDone
	}
	if s.graphProjectionCancel != nil {
		s.graphProjectionCancel()
		<-s.graphProjectionDone
	}
	if s.graphProjection != nil {
		return s.graphProjection.Close()
	}
	return nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r, ok := s.authenticateRequest(w, r)
	if !ok {
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/v1/health", s.health)
	s.mux.HandleFunc("POST /api/v1/auth/rauthy", s.authRauthy)
	s.mux.HandleFunc("POST /api/v1/auth/pocketbase", s.authPocketBase)
	s.mux.HandleFunc("GET /api/v1/auth/session", s.authSession)
	s.mux.HandleFunc("POST /api/v1/auth/logout", s.authLogout)
	s.mux.HandleFunc("POST /api/v1/auth/identity", s.authIdentity)
	s.mux.HandleFunc("GET /api/v1/search-syncer/workspace", s.searchSyncerWorkspace)
	s.mux.HandleFunc("POST /api/v1/workspaces", s.createWorkspace)
	s.mux.HandleFunc("GET /api/v1/kernel/command-registry", func(w http.ResponseWriter, r *http.Request) {
		write(w, 200, map[string]any{"commands": kernel.Registry()})
	})
	s.mux.HandleFunc("POST /api/v1/kernel/commands", s.submitCommand)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/command-surface", s.commandSurface)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/definitions", s.listDefinitions)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/definitions/{revision_id}", s.readDefinition)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/objects/{object_id}/relations", s.listObjectRelations)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/actor-capabilities", s.listCapabilities)
	s.mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/actor-capabilities", s.grantCapability)
	s.mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/adapter/inbound-events", s.appendAdapterEvent)
	s.mux.HandleFunc("POST /internal/v1/conductor/callbacks", s.appendConductorCallback)
	s.mux.HandleFunc("GET /v1/connector/objects/{object_key}/schema", s.conductorObjectSchema)
	s.mux.HandleFunc("GET /v1/connector/objects/{object_key}/records/{record_id}", s.conductorObjectRecord)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/objects/{object_id}", s.readObject)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/objects", s.listObjects)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/objects", s.listTypedObjects)
	s.mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/objects", s.createTypedObject)
	s.mux.HandleFunc("PATCH /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/objects/{object_id}", s.updateTypedObject)
	s.mux.HandleFunc("DELETE /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/objects/{object_id}", s.archiveTypedObject)
	s.mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/objects", s.batchObjects)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/work", s.listHumanWork)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/work/{stream_id}", s.readWork)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/events", s.listDomainEvents)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/knowledge/proposals", s.listKnowledgeProposals)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/command-ledger", s.listLedger)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/command-ledger/{command_id}", s.getLedger)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/object-types/{object_type_id}/schema-revisions", s.schemaRevisions)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/projection/events", s.projectionEvents)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/projection/snapshot", s.projectionSnapshot)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/knowledge", s.listKnowledge)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/knowledge/{bundle_id}/{concept_id...}", s.readKnowledge)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/ontology/traverse", s.traverseOntology)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/ontology/reachable-node-ids", s.reachableOntologyNodeIDs)
	s.mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/ontology/groundings", s.listGroundings)
	s.mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/search", s.search)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Health(r.Context()); err != nil {
		problem(w, 503, "storage_unavailable", "storage", true, err.Error())
		return
	}
	s.graphProjectionMu.RLock()
	status := "disabled"
	if s.graphProjectionSet {
		status = "degraded"
		if s.graphProjectionRebuilding {
			status = "rebuilding"
		} else if s.graphProjection != nil && s.graphProjectionErr == "" && sequencesCurrent(s.graphProjectionHeads, s.graphProjectionCursors) {
			status = "ready"
		}
	}
	lags := map[string]int64{}
	for workspaceID, head := range s.graphProjectionHeads {
		lags[workspaceID] = head - s.graphProjectionCursors[workspaceID]
	}
	projectionStatus := map[string]any{
		"enabled": s.graphProjectionSet, "status": status, "healthy": status == "ready", "error": nullableString(s.graphProjectionErr),
		"production_approved": false, "request_reads": status == "ready", "search_backend": map[bool]string{true: "meilisearch", false: "unavailable"}[s.searchURL != ""], "traversal_backend": "sqlite",
		"reachable_node_ids_backend": map[bool]string{true: "rhiza_graph", false: "unavailable"}[status == "ready"],
		"sqlite_heads":               cloneSequences(s.graphProjectionHeads), "graph_cursors": cloneSequences(s.graphProjectionCursors), "lag_events": lags,
		"last_success_at": nullableString(s.graphProjectionLastSuccess),
		"rebuild_count":   s.graphProjectionRebuilds, "projection_version": rhizaprojection.SchemaVersion,
	}
	s.graphProjectionMu.RUnlock()
	write(w, 200, map[string]any{"status": "ok", "storage": map[string]any{"kind": s.store.Kind(), "cluster_id": s.store.ClusterID(), "ready": s.store.Ready()}, "knowledge_projection": projectionStatus})
}

func (s *Server) createWorkspace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name           string `json:"name"`
		Key            string `json:"key"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if !decode(w, r, &body) {
		return
	}
	body.Name = strings.TrimSpace(body.Name)
	body.Key = asciiLower(strings.TrimSpace(body.Key))
	if body.Name == "" || body.IdempotencyKey == "" {
		problem(w, 400, "invalid_request", "validation", false, "name and idempotency_key are required")
		return
	}
	if body.Key == "" {
		body.Key = slug(body.Name)
	}
	principal := principal(r)
	payload, _ := json.Marshal(body)
	hash := sha(payload)
	var response any
	created := false
	var err error
	if s.store.Kind() == "rhiza" {
		response, created, err = s.createWorkspaceRhiza(r.Context(), principal, body.Name, body.Key, body.IdempotencyKey, string(payload), hash)
	} else {
		err = s.store.Transact(r.Context(), "workspace:"+principal+":"+body.IdempotencyKey, func(tx store.Tx) error {
			var storedHash, result string
			err := tx.QueryRow(`SELECT request_hash,result_json FROM workspace_creation_requests WHERE authenticated_principal_id=? AND idempotency_key=?`, principal, body.IdempotencyKey).Scan(&storedHash, &result)
			if err == nil {
				if storedHash != hash {
					return errors.New("idempotency conflict")
				}
				return json.Unmarshal([]byte(result), &response)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			var existingID string
			if err = tx.QueryRow(`SELECT id FROM workspaces WHERE key=?`, body.Key).Scan(&existingID); err == nil {
				return errors.New("workspace key conflict")
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			id := "wks_" + uuid.NewString()
			now := time.Now().UTC().Format(time.RFC3339Nano)
			if _, err = tx.Exec(`INSERT INTO workspaces VALUES(?,?,?,1,?,?)`, id, body.Key, body.Name, now, now); err != nil {
				if strings.Contains(err.Error(), "workspaces.key") {
					return errors.New("workspace key conflict")
				}
				return err
			}
			if _, err = tx.Exec(`INSERT INTO workspace_memberships VALUES(?,?,'owner',?)`, id, principal, now); err != nil {
				return err
			}
			if err = provisionBuiltins(r.Context(), tx, id, body.Name, principal, now); err != nil {
				return err
			}
			response = map[string]any{"data": map[string]any{
				"id": id, "workspace_id": id, "key": body.Key, "name": body.Name, "version": 1,
				"creator_principal_id": principal, "owner_principal_id": principal,
				"creator_person_object_instance_id":    "obj_creator_" + id,
				"workspace_company_object_instance_id": "obj_company_" + id,
				"works_for_relation_definition_id":     "rdf_builtin_works_for_" + id,
				"works_for_object_relation_id":         "rel_creator_company_" + id,
				"created_at":                           now, "created": true, "status": "created",
			}}
			resultJSON, _ := json.Marshal(response)
			_, err = tx.Exec(`INSERT INTO workspace_creation_requests VALUES(?,?,?,?,?,?,?)`, principal, body.IdempotencyKey, hash, string(payload), string(resultJSON), id, now)
			created = err == nil
			return err
		})
	}
	if err != nil {
		if err.Error() == "idempotency conflict" {
			problem(w, 409, "idempotency_conflict", "conflict", false, "idempotency key was already used")
			return
		}
		if err.Error() == "workspace key conflict" {
			problem(w, 409, "workspace_key_conflict", "conflict", false, "workspace key is already in use")
			return
		}
		problem(w, 500, "internal_error", "storage", true, err.Error())
		return
	}
	if created {
		write(w, 201, response)
	} else {
		data := response.(map[string]any)["data"].(map[string]any)
		data["created"], data["status"] = false, "duplicate"
		write(w, 200, response)
	}
}

func (s *Server) createWorkspaceRhiza(ctx context.Context, principal, name, key, idempotencyKey, requestJSON, requestHash string) (any, bool, error) {
	readRequest := func() (any, string, error) {
		var storedHash, result string
		err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT request_hash,result_json FROM workspace_creation_requests WHERE authenticated_principal_id=? AND idempotency_key=?`, principal, idempotencyKey).Scan(&storedHash, &result)
		if err != nil {
			return nil, "", err
		}
		var response any
		if err := json.Unmarshal([]byte(result), &response); err != nil {
			return nil, "", err
		}
		return response, storedHash, nil
	}
	if response, storedHash, err := readRequest(); err == nil {
		if storedHash != requestHash {
			return nil, false, errors.New("idempotency conflict")
		}
		return response, false, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	var existingID string
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id FROM workspaces WHERE key=?`, key).Scan(&existingID); err == nil {
		return nil, false, errors.New("workspace key conflict")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	id := "wks_" + uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	response := map[string]any{"data": map[string]any{
		"id": id, "workspace_id": id, "key": key, "name": name, "version": 1,
		"creator_principal_id": principal, "owner_principal_id": principal,
		"creator_person_object_instance_id":    "obj_creator_" + id,
		"workspace_company_object_instance_id": "obj_company_" + id,
		"works_for_relation_definition_id":     "rdf_builtin_works_for_" + id,
		"works_for_object_relation_id":         "rel_creator_company_" + id,
		"created_at":                           now, "created": true, "status": "created",
	}}
	resultJSON, _ := json.Marshal(response)
	zero, one := int64(0), int64(1)
	statements := []rhiza.SQLStatement{
		{SQL: `SELECT authenticated_principal_id FROM workspace_creation_requests WHERE authenticated_principal_id=? AND idempotency_key=?`, Args: []any{principal, idempotencyKey}, WantRows: true, ExpectedReturnedRows: &zero},
		{SQL: `SELECT id FROM workspaces WHERE key=?`, Args: []any{key}, WantRows: true, ExpectedReturnedRows: &zero},
		{SQL: `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES(?,?,?,1,?,?)`, Args: []any{id, key, name, now, now}, ExpectedRowsAffected: &one},
		{SQL: `INSERT INTO workspace_memberships(workspace_id,principal_id,role,created_at) VALUES(?,?,'owner',?)`, Args: []any{id, principal, now}, ExpectedRowsAffected: &one},
	}
	builtins, err := provisionBuiltinStatements(ctx, id, name, principal, now)
	if err != nil {
		return nil, false, err
	}
	statements = append(statements, builtins...)
	statements = append(statements, rhiza.SQLStatement{SQL: `INSERT INTO workspace_creation_requests(authenticated_principal_id,idempotency_key,request_hash,request_payload_json,result_json,workspace_id,created_at) VALUES(?,?,?,?,?,?,?)`, Args: []any{principal, idempotencyKey, requestHash, requestJSON, string(resultJSON), id, now}, ExpectedRowsAffected: &one})
	applyErr := s.store.ApplyStatements(ctx, "workspace:"+uuid.NewString(), statements)
	if applyErr == nil {
		return response, true, nil
	}
	if existing, storedHash, err := readRequest(); err == nil {
		if storedHash != requestHash {
			return nil, false, errors.New("idempotency conflict")
		}
		return existing, false, nil
	}
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id FROM workspaces WHERE key=?`, key).Scan(&existingID); err == nil {
		return nil, false, errors.New("workspace key conflict")
	}
	return nil, false, applyErr
}

type contextExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type statementCollector struct {
	one        int64
	statements []rhiza.SQLStatement
}

func (c *statementCollector) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	c.statements = append(c.statements, rhiza.SQLStatement{SQL: query, Args: args, ExpectedRowsAffected: &c.one})
	return affectedRows(1), nil
}

type affectedRows int64

func (r affectedRows) LastInsertId() (int64, error) { return 0, nil }
func (r affectedRows) RowsAffected() (int64, error) { return int64(r), nil }

func provisionBuiltinStatements(ctx context.Context, workspace, workspaceName, principal, ts string) ([]rhiza.SQLStatement, error) {
	collector := &statementCollector{one: 1}
	err := provisionBuiltins(ctx, collector, workspace, workspaceName, principal, ts)
	return collector.statements, err
}

func provisionBuiltins(ctx context.Context, tx contextExecer, workspace, workspaceName, principal, ts string) error {
	peopleID := "otp_builtin_person_" + workspace
	companiesID := "otp_builtin_company_" + workspace
	relationID := "rdf_builtin_works_for_" + workspace
	personID := "obj_creator_" + workspace
	companyID := "obj_company_" + workspace
	edgeID := "rel_creator_company_" + workspace
	peopleSchema := canonical(map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{"display_name": map[string]any{"type": "string"}, "email": map[string]any{"type": "string", "format": "email"}, "phone": map[string]any{"type": "string"}}})
	companySchema := canonical(map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []string{"name"}})
	link := map[string]any{"id": relationID, "key": "works_for", "from_object_type_id": peopleID, "to_object_type_id": companiesID, "cardinality": "many_to_one", "required": false}
	relationSchema := canonical(map[string]any{"links": []any{link}})
	for _, item := range []struct{ id, key, singular, plural, schema string }{{peopleID, "people", "person", "people", peopleSchema}, {companiesID, "companies", "company", "companies", companySchema}} {
		digest := schemaRevisionDigest(1, item.schema, relationSchema, "")
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_types(id,workspace_id,key,singular,plural,schema_version,field_schema_json,relation_schema_json,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,1,?,?,1,?,?,?,?)`, item.id, workspace, item.key, item.singular, item.plural, item.schema, relationSchema, ts, ts, "system", "system"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_type_schema_revisions(workspace_id,object_type_id,schema_version,field_schema_json,relation_schema_json,digest,created_at,created_by_principal_id) VALUES(?,?,1,?,?,?,?,?)`, workspace, item.id, item.schema, relationSchema, digest, ts, "system"); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO relation_definitions(id,workspace_id,key,from_object_type_id,to_object_type_id,cardinality,required,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,'many_to_one',0,1,?,?,?,?)`, relationID, workspace, "works_for", peopleID, companiesID, ts, ts, "system", "system"); err != nil {
		return err
	}
	personValues := canonical(map[string]any{"display_name": principal})
	companyValues := canonical(map[string]any{"name": workspaceName})
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_instances(id,workspace_id,object_type_id,object_type_schema_version,title,payload_json,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,1,?,?,?,?)`, personID, workspace, peopleID, 1, principal, personValues, ts, ts, principal, principal); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_instances(id,workspace_id,object_type_id,object_type_schema_version,title,payload_json,version,created_at,updated_at,created_by_principal_id,updated_by_principal_id) VALUES(?,?,?,?,?,?,1,?,?,?,?)`, companyID, workspace, companiesID, 1, workspaceName, companyValues, ts, ts, principal, principal); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO principal_person_bindings VALUES(?,?,?,?,?,?)`, workspace, principal, personID, "workspace_creator", principal, ts); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO object_relations VALUES(?,?,?,?,?,1,?,?,?,?)`, edgeID, workspace, relationID, personID, companyID, ts, ts, principal, principal); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO relation_cardinality_slots VALUES(?,?,?,?,?,?,?)`, "slot_"+edgeID, workspace, relationID, edgeID, "from", personID, ts); err != nil {
		return err
	}
	for _, row := range []struct {
		typeID, objectID, field, kind string
		value                         any
	}{{peopleID, personID, "display_name", "string", principal}, {companiesID, companyID, "name", "string", workspaceName}} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO object_field_index(workspace_id,object_type_id,object_instance_id,field_name,value_kind,value_text) VALUES(?,?,?,?,?,?)`, workspace, row.typeID, row.objectID, row.field, row.kind, row.value); err != nil {
			return err
		}
	}
	return nil
}

func schemaRevisionDigest(version int64, field, relation, state string) string {
	var fieldValue, relationValue any
	_ = json.Unmarshal([]byte(field), &fieldValue)
	_ = json.Unmarshal([]byte(relation), &relationValue)
	var stateValue any = nil
	if state != "" {
		stateValue = state
	}
	return sha([]byte(canonical(map[string]any{"schema_version": version, "field_schema": fieldValue, "relation_schema": relationValue, "state_machine_id": stateValue})))
}

func (s *Server) submitCommand(w http.ResponseWriter, r *http.Request) {
	var c kernel.Command
	if !decodeLimit(w, r, &c, 8<<20) {
		return
	}
	if c.CommandType == "function.record_result" || c.CommandType == "agent.record_result" || c.CommandType == "workflow.complete" {
		problem(w, http.StatusForbidden, "forbidden", "authz", false, "internal runtime completion commands are not accepted on the public command endpoint")
		return
	}
	receipt, err := s.kernel.Execute(r.Context(), principal(r), c)
	if err != nil {
		commandError(w, err)
		return
	}
	if c.CommandType == "knowledge.accept" || c.CommandType == "knowledge.remove" {
		if receipt.ProjectionSequence != nil {
			s.markGraphProjectionHead(c.WorkspaceID, *receipt.ProjectionSequence)
		}
		s.wakeGraphProjection()
	}
	if c.CommandType == "function.invoke" {
		s.wakeConductor()
	}
	s.wakeWorkflow()
	response := map[string]any{"command_id": receipt.CommandID, "outcome": receipt.Outcome, "aggregate": receipt.Aggregate, "projection": map[string]any{"workspace_id": receipt.WorkspaceID, "projection_sequence": receipt.ProjectionSequence}}
	if len(receipt.ClientRefs) != 0 {
		response["client_refs"] = receipt.ClientRefs
	}
	write(w, 200, response)
}

func (s *Server) graphProjectionReady(workspaceID string) bool {
	s.graphProjectionMu.RLock()
	defer s.graphProjectionMu.RUnlock()
	return s.graphProjection != nil && s.graphProjectionErr == "" && !s.graphProjectionRebuilding && s.graphProjectionCursors[workspaceID] >= s.graphProjectionHeads[workspaceID]
}

func cloneSequences(source map[string]int64) map[string]int64 {
	copy := make(map[string]int64, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func (s *Server) commandSurface(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT id,key,singular,plural,schema_version,field_schema_json,relation_schema_json,state_machine_id,version FROM object_types WHERE workspace_id=? AND archived_at IS NULL ORDER BY key`, workspace)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	nouns := []any{}
	for rows.Next() {
		var id, key, singular, plural, field, relation string
		var schemaVersion, version int64
		var state sql.NullString
		if err := rows.Scan(&id, &key, &singular, &plural, &schemaVersion, &field, &relation, &state, &version); err != nil {
			dbError(w, err)
			return
		}
		relations, err := s.surfaceRelations(r.Context(), workspace, id)
		if err != nil {
			dbError(w, err)
			return
		}
		commands := []string{"create", "get"}
		var fieldSchema map[string]any
		_ = json.Unmarshal([]byte(field), &fieldSchema)
		if _, hasStateMachine := fieldSchema["state_machine"]; state.Valid || hasStateMachine {
			commands = append(commands, "move")
		}
		if len(relations) != 0 {
			commands = append(commands, "link")
		}
		nouns = append(nouns, map[string]any{"id": id, "key": key, "singular": singular, "plural": plural, "kind": "object_type", "object_type_id": id, "schema_version": schemaVersion, "field_schema": raw(field), "relation_schema": raw(relation), "state_machine_id": nullString(state), "version": version, "commands": commands, "relations": relations})
	}
	write(w, 200, map[string]any{"workspace_id": workspace, "nouns": nouns, "commands": kernel.Registry()})
}

func (s *Server) surfaceRelations(ctx context.Context, workspace, objectTypeID string) ([]any, error) {
	rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT id,key,version,from_object_type_id,to_object_type_id,cardinality FROM relation_definitions WHERE workspace_id=? AND archived_at IS NULL AND (from_object_type_id=? OR to_object_type_id=?) ORDER BY key,id`, workspace, objectTypeID, objectTypeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []any{}
	for rows.Next() {
		var id, key, from, to, cardinality string
		var version int64
		if err := rows.Scan(&id, &key, &version, &from, &to, &cardinality); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "key": key, "version": version, "from_object_type_id": from, "to_object_type_id": to, "cardinality": cardinality})
	}
	return out, rows.Err()
}

func (s *Server) readObject(w http.ResponseWriter, r *http.Request) {
	data, err := s.object(r.Context(), r.PathValue("workspace_id"), r.PathValue("object_id"))
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, 404, "not_found", "request", false, "object not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": data, "consistency": consistency("canonical_linearizable")})
}
func (s *Server) object(ctx context.Context, workspace, id string) (map[string]any, error) {
	var typeID, typeKey, title, payload, state string
	var schemaVersion, version int64
	var stateValue sql.NullString
	var created, updated string
	err := s.store.ReadRow(ctx, store.ReadLocal, `SELECT i.object_type_id,t.key,i.title,i.payload_json,i.state_key,i.object_type_schema_version,i.version,i.created_at,i.updated_at FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.id=? AND i.archived_at IS NULL`, workspace, id).Scan(&typeID, &typeKey, &title, &payload, &stateValue, &schemaVersion, &version, &created, &updated)
	state = stateValue.String
	return map[string]any{"id": id, "object_type_id": typeID, "object_type_key": typeKey, "object_type_schema_version": schemaVersion, "title": title, "values": raw(payload), "state_key": nullableString(state), "version": version, "created_at": created, "updated_at": updated}, err
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	query := `SELECT i.id FROM object_instances i JOIN object_types t ON t.workspace_id=i.workspace_id AND t.id=i.object_type_id WHERE i.workspace_id=? AND i.archived_at IS NULL`
	args := []any{workspace}
	if key := r.URL.Query().Get("object_type"); key != "" {
		query += ` AND t.key=?`
		args = append(args, key)
	}
	query += ` ORDER BY i.updated_at DESC,i.id`
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, query, args...)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		item, err := s.object(r.Context(), workspace, id)
		if err != nil {
			dbError(w, err)
			return
		}
		data = append(data, item)
	}
	write(w, 200, map[string]any{"data": data, "page": map[string]any{"next_cursor": nil}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) batchObjects(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ObjectIDs []string `json:"object_ids"`
	}
	if !decode(w, r, &body) {
		return
	}
	if len(body.ObjectIDs) > 100 {
		problem(w, 400, "invalid_request", "validation", false, "object_ids must contain at most 100 IDs")
		return
	}
	data := []any{}
	for _, id := range body.ObjectIDs {
		item, err := s.object(r.Context(), r.PathValue("workspace_id"), id)
		if err == nil {
			data = append(data, item)
		} else if !errors.Is(err, sql.ErrNoRows) {
			dbError(w, err)
			return
		}
	}
	write(w, 200, map[string]any{"data": data, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) listHumanWork(w http.ResponseWriter, r *http.Request) {
	limit := boundedInt(r, "limit", 50, 1, 100)
	offset := boundedInt(r, "offset", 0, 0, 1<<30)
	// ponytail: replay the journal directly; materialize an inbox only when measured queue latency requires it.
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `
		SELECT route.stream_id,route.stream_sequence,route.payload_json,route.created_at,
		       opened.spec_revision_id,opened.principal_id,opened.payload_json,opened.created_at,
		       spec.family_id,spec.revision,spec.payload_json,spec.digest,
		       (SELECT MAX(j.stream_sequence) FROM journal_records j WHERE j.workspace_id=route.workspace_id AND j.stream_id=route.stream_id),
		       (SELECT j.created_at FROM journal_records j WHERE j.workspace_id=route.workspace_id AND j.stream_id=route.stream_id ORDER BY j.stream_sequence DESC LIMIT 1),
		       EXISTS(SELECT 1 FROM journal_records started WHERE started.workspace_id=route.workspace_id AND started.stream_id=route.stream_id AND started.kind='function.requested'
		         AND NOT EXISTS(SELECT 1 FROM journal_records ended WHERE ended.workspace_id=started.workspace_id AND ended.stream_id=started.stream_id AND ended.correlation_id=started.correlation_id AND ended.kind IN ('function.succeeded','function.failed'))),
		       EXISTS(SELECT 1 FROM journal_records started WHERE started.workspace_id=route.workspace_id AND started.stream_id=route.stream_id AND started.kind='workflow.started'
		         AND NOT EXISTS(SELECT 1 FROM journal_records ended WHERE ended.workspace_id=started.workspace_id AND ended.stream_id=started.stream_id AND ended.correlation_id=started.correlation_id AND ended.kind IN ('workflow.completed','workflow.failed'))),
		       EXISTS(SELECT 1 FROM journal_records due WHERE due.workspace_id=route.workspace_id AND due.stream_id=route.stream_id AND due.kind='work.deadline.reached')
		FROM journal_records route
		JOIN journal_records opened ON opened.workspace_id=route.workspace_id AND opened.stream_id=route.stream_id AND opened.kind='work.opened'
		JOIN spec_revisions spec ON spec.workspace_id=opened.workspace_id AND spec.id=opened.spec_revision_id
		WHERE route.workspace_id=? AND route.kind='work.routed'
		  AND route.participant_kind='human' AND route.participant_id=?
		  AND route.stream_sequence=(SELECT MAX(r2.stream_sequence) FROM journal_records r2 WHERE r2.workspace_id=route.workspace_id AND r2.stream_id=route.stream_id AND r2.kind='work.routed' AND COALESCE(json_extract(r2.payload_json,'$.workflow_route_id'),'')=COALESCE(json_extract(route.payload_json,'$.workflow_route_id'),''))
		  AND NOT EXISTS (SELECT 1 FROM journal_records terminal WHERE terminal.workspace_id=route.workspace_id AND terminal.stream_id=route.stream_id AND terminal.kind IN ('work.satisfied','work.cancelled'))
		  AND NOT EXISTS (SELECT 1 FROM journal_records submitted WHERE submitted.workspace_id=route.workspace_id AND submitted.stream_id=route.stream_id AND submitted.kind='work.submitted' AND submitted.stream_sequence>route.stream_sequence AND COALESCE(json_extract(submitted.payload_json,'$.workflow_route_id'),'')=COALESCE(json_extract(route.payload_json,'$.workflow_route_id'),''))
		ORDER BY route.created_at,route.stream_id LIMIT ? OFFSET ?`, r.PathValue("workspace_id"), principal(r), limit, offset)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, routePayload, assignedAt, taskID, owner, openedPayload, openedAt, family, taskPayload, digest, updatedAt string
		var assignedSequence, revision, version int64
		var activeFunctions, activeWorkflows, deadlineReached bool
		if err := rows.Scan(&id, &assignedSequence, &routePayload, &assignedAt, &taskID, &owner, &openedPayload, &openedAt, &family, &revision, &taskPayload, &digest, &version, &updatedAt, &activeFunctions, &activeWorkflows, &deadlineReached); err != nil {
			dbError(w, err)
			return
		}
		route, _ := raw(routePayload).(map[string]any)
		opened, _ := raw(openedPayload).(map[string]any)
		item := map[string]any{
			"id": id, "version": version, "status": projectedWorkStatus(false, false, activeFunctions, activeWorkflows, false, true),
			"task":               map[string]any{"revision_id": taskID, "family_id": family, "revision": revision, "digest": digest, "spec": raw(taskPayload)},
			"owner_principal_id": owner, "assignee": map[string]any{"kind": "human", "id": principal(r)}, "context": opened["context"],
			"deadline_at": opened["deadline_at"], "deadline_reached": deadlineReached,
			"assigned_sequence": assignedSequence, "assigned_at": assignedAt, "opened_at": openedAt, "updated_at": updatedAt,
		}
		if reason, ok := route["reason"]; ok {
			item["reason"] = reason
		}
		if stepID, ok := route["workflow_step_id"]; ok {
			item["workflow_step_id"] = stepID
		}
		if routeID, ok := route["workflow_route_id"]; ok {
			item["workflow_route_id"] = routeID
		}
		data = append(data, item)
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": data, "page": map[string]any{"limit": limit, "offset": offset}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) readWork(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT stream_sequence,kind,spec_revision_id,participant_kind,participant_id,principal_id,correlation_id,causation_id,command_id,payload_json,created_at FROM journal_records WHERE workspace_id=? AND stream_id=? ORDER BY stream_sequence`, r.PathValue("workspace_id"), r.PathValue("stream_id"))
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	facts, submissions, assignments, handoffs := []any{}, []any{}, []any{}, []any{}
	var assignee, contextValue any
	var completion, cancellation, deadlineAt any
	var deadlineReached bool
	var deadlineReachedAt any
	var taskRevisionID, ownerPrincipalID, openedAt, updatedAt string
	var version int64
	active, activeAgents, activeWorkflows := map[string]bool{}, map[string]bool{}, map[string]bool{}
	executions, executionOrder := map[string]map[string]any{}, []string{}
	completed, cancelled, awaitingReview := false, false, false
	for rows.Next() {
		var sequence int64
		var kind, principalID, commandID, payload, createdAt string
		var specID, participantKind, participantID, correlationID, causationID sql.NullString
		if err := rows.Scan(&sequence, &kind, &specID, &participantKind, &participantID, &principalID, &correlationID, &causationID, &commandID, &payload, &createdAt); err != nil {
			dbError(w, err)
			return
		}
		payloadValue, _ := raw(payload).(map[string]any)
		fact := map[string]any{"sequence": sequence, "kind": kind, "spec_revision_id": nullString(specID), "principal_id": principalID, "correlation_id": nullString(correlationID), "causation_id": nullString(causationID), "command_id": commandID, "payload": payloadValue, "created_at": createdAt}
		if participantKind.Valid {
			fact["participant"] = map[string]any{"kind": participantKind.String, "id": participantID.String}
		}
		facts = append(facts, fact)
		version, updatedAt = sequence, createdAt
		switch kind {
		case "work.opened":
			taskRevisionID, ownerPrincipalID, openedAt = specID.String, principalID, createdAt
			contextValue = payloadValue["context"]
			deadlineAt = payloadValue["deadline_at"]
		case "work.deadline.reached":
			deadlineReached, deadlineReachedAt = true, createdAt
		case "work.routed":
			next := fact["participant"]
			assignment := map[string]any{"sequence": sequence, "participant": next, "assigned_at": createdAt}
			if reason, ok := payloadValue["reason"]; ok {
				assignment["reason"] = reason
			}
			assignments = append(assignments, assignment)
			if previous, ok := assignee.(map[string]any); ok {
				current, _ := next.(map[string]any)
				if previous["kind"] != current["kind"] || previous["id"] != current["id"] {
					handoff := map[string]any{"sequence": sequence, "from": previous, "to": current, "handed_off_at": createdAt}
					if reason, ok := payloadValue["reason"]; ok {
						handoff["reason"] = reason
					}
					handoffs = append(handoffs, handoff)
				}
			}
			assignee = next
			awaitingReview = false
		case "work.submitted":
			submissions = append(submissions, map[string]any{"sequence": sequence, "participant": fact["participant"], "submission": payloadValue["submission"], "created_at": createdAt})
			awaitingReview = true
		case "function.requested":
			active[correlationID.String] = true
			if _, exists := executions[correlationID.String]; !exists {
				executions[correlationID.String] = map[string]any{"id": correlationID.String, "kind": "function", "function_revision_id": specID.String, "participant": fact["participant"], "status": "running", "input": payloadValue["input"], "started_at": createdAt}
				executionOrder = append(executionOrder, correlationID.String)
			}
			awaitingReview = false
		case "function.succeeded", "function.failed":
			delete(active, correlationID.String)
			if execution := executions[correlationID.String]; execution != nil {
				execution["status"], execution["output"], execution["ended_at"] = strings.TrimPrefix(kind, "function."), payloadValue["output"], createdAt
				if detail, ok := payloadValue["error_detail"]; ok {
					execution["error_detail"] = detail
				}
			}
		case "agent.requested":
			activeAgents[correlationID.String] = true
			executions[correlationID.String] = map[string]any{"id": correlationID.String, "kind": "agent", "agent_revision_id": specID.String, "participant": fact["participant"], "status": "running", "input": payloadValue["input"], "started_at": createdAt}
			executionOrder = append(executionOrder, correlationID.String)
			awaitingReview = false
		case "agent.succeeded", "agent.failed":
			delete(activeAgents, correlationID.String)
			if execution := executions[correlationID.String]; execution != nil {
				execution["status"], execution["output"], execution["ended_at"] = strings.TrimPrefix(kind, "agent."), payloadValue["output"], createdAt
				if detail, ok := payloadValue["error_detail"]; ok {
					execution["error_detail"] = detail
				}
			}
		case "workflow.started":
			activeWorkflows[correlationID.String] = true
			if execution := executions[correlationID.String]; execution != nil {
				execution["kind"], execution["workflow_revision_id"] = "workflow", specID.String
			}
		case "workflow.completed", "workflow.failed":
			delete(activeWorkflows, correlationID.String)
		case "work.satisfied":
			completed = true
			completion = payloadValue
			awaitingReview = false
		case "work.cancelled":
			cancelled = true
			cancellation = payloadValue
			awaitingReview = false
		}
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	if len(facts) == 0 {
		problem(w, 404, "not_found", "request", false, "work stream not found")
		return
	}
	var taskFamily, taskKind, taskPayload, taskDigest string
	var taskRevision int64
	if err := s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT family_id,revision,kind,payload_json,digest FROM spec_revisions WHERE workspace_id=? AND id=?`, r.PathValue("workspace_id"), taskRevisionID).Scan(&taskFamily, &taskRevision, &taskKind, &taskPayload, &taskDigest); err != nil {
		dbError(w, err)
		return
	}
	activeIDs := []string{}
	for id := range active {
		activeIDs = append(activeIDs, id)
	}
	sort.Strings(activeIDs)
	activeAgentIDs := []string{}
	for id := range activeAgents {
		activeAgentIDs = append(activeAgentIDs, id)
	}
	sort.Strings(activeAgentIDs)
	activeWorkflowIDs := []string{}
	for id := range activeWorkflows {
		activeWorkflowIDs = append(activeWorkflowIDs, id)
	}
	sort.Strings(activeWorkflowIDs)
	status := projectedWorkStatus(cancelled, completed, len(active) != 0 || len(activeAgents) != 0, len(activeWorkflows) != 0, awaitingReview, assignee != nil)
	executionValues := make([]any, 0, len(executionOrder))
	for _, id := range executionOrder {
		executionValues = append(executionValues, executions[id])
	}
	write(w, 200, map[string]any{"data": map[string]any{
		"id": r.PathValue("stream_id"), "version": version, "status": status,
		"task":               map[string]any{"revision_id": taskRevisionID, "family_id": taskFamily, "revision": taskRevision, "kind": taskKind, "digest": taskDigest, "spec": raw(taskPayload)},
		"owner_principal_id": ownerPrincipalID, "assignee": assignee, "context": contextValue,
		"awaiting_review": awaitingReview, "active_function_ids": activeIDs, "active_agent_ids": activeAgentIDs, "active_workflow_ids": activeWorkflowIDs,
		"assignments": assignments, "handoffs": handoffs, "submissions": submissions, "executions": executionValues,
		"completion": completion, "cancellation": cancellation, "deadline_at": deadlineAt, "deadline_reached": deadlineReached, "deadline_reached_at": deadlineReachedAt,
		"opened_at": openedAt, "updated_at": updatedAt, "facts": facts,
	}})
}

func projectedWorkStatus(cancelled, completed, activeFunctions, activeWorkflows, awaitingReview, assigned bool) string {
	switch {
	case cancelled:
		return "cancelled"
	case completed:
		return "completed"
	case activeFunctions || activeWorkflows:
		return "running"
	case awaitingReview:
		return "awaiting_review"
	case assigned:
		return "assigned"
	}
	return "open"
}

func (s *Server) listLedger(w http.ResponseWriter, r *http.Request) {
	limit := boundedInt(r, "limit", 50, 1, 100)
	offset := boundedInt(r, "offset", 0, 0, 1<<30)
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT command_id,authenticated_principal_id,idempotency_key,command_type,outcome,result_json,effective_at,created_at FROM command_ledger WHERE workspace_id=? ORDER BY created_at DESC,command_id LIMIT ? OFFSET ?`, r.PathValue("workspace_id"), limit, offset)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, principal, key, commandType, outcome, result, effective, created string
		if err := rows.Scan(&id, &principal, &key, &commandType, &outcome, &result, &effective, &created); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"command_id": id, "authenticated_principal_id": principal, "idempotency_key": key, "command_type": commandType, "outcome": outcome, "result": raw(result), "effective_at": effective, "created_at": created})
	}
	var total int
	_ = s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT COUNT(*) FROM command_ledger WHERE workspace_id=?`, r.PathValue("workspace_id")).Scan(&total)
	write(w, 200, map[string]any{"data": data, "page": map[string]any{"total": total, "limit": limit, "offset": offset}, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) getLedger(w http.ResponseWriter, r *http.Request) {
	var result string
	err := s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT result_json FROM command_ledger WHERE workspace_id=? AND command_id=?`, r.PathValue("workspace_id"), r.PathValue("command_id")).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, 404, "not_found", "request", false, "command ledger entry not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 200, map[string]any{"data": raw(result)})
}

func (s *Server) schemaRevisions(w http.ResponseWriter, r *http.Request) {
	limit := boundedInt(r, "limit", 50, 1, 100)
	before := int64(1 << 62)
	if raw := r.URL.Query().Get("before"); raw != "" {
		before, _ = strconv.ParseInt(raw, 10, 64)
	}
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT schema_version,field_schema_json,relation_schema_json,state_machine_id,digest,created_at,created_by_principal_id FROM object_type_schema_revisions WHERE workspace_id=? AND object_type_id=? AND schema_version<? ORDER BY schema_version DESC LIMIT ?`, r.PathValue("workspace_id"), r.PathValue("object_type_id"), before, limit+1)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var version int64
		var field, relation, digest, created, creator string
		var state sql.NullString
		if err := rows.Scan(&version, &field, &relation, &state, &digest, &created, &creator); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"schema_version": version, "field_schema": raw(field), "relation_schema": raw(relation), "state_machine_id": nullString(state), "digest": digest, "created_at": created, "created_by_principal_id": creator})
	}
	var next any = nil
	if len(data) > limit {
		last := data[limit-1].(map[string]any)
		next = last["schema_version"]
		data = data[:limit]
	}
	write(w, 200, map[string]any{"data": data, "page": map[string]any{"next_cursor": next}})
}

func (s *Server) projectionEvents(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit := boundedInt(r, "limit", 100, 1, 1000)
	workspace := r.PathValue("workspace_id")
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT p.id,p.command_id,p.projection_sequence,p.aggregate_type,p.aggregate_id,p.aggregate_version,p.payload_hash,p.payload_json,p.created_at,a.event_type FROM projection_events p JOIN activities a ON a.workspace_id=p.workspace_id AND a.command_id=p.command_id AND a.aggregate_type=p.aggregate_type AND a.aggregate_id=p.aggregate_id WHERE p.workspace_id=? AND p.projection_sequence>? ORDER BY p.projection_sequence LIMIT ?`, workspace, after, limit)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, commandID, kind, aggregateID, hash, payload, created, eventType string
		var sequence, version int64
		if err := rows.Scan(&id, &commandID, &sequence, &kind, &aggregateID, &version, &hash, &payload, &created, &eventType); err != nil {
			dbError(w, err)
			return
		}
		data = append(data, map[string]any{"id": id, "workspace_id": workspace, "command_id": commandID, "projection_sequence": sequence, "event_type": eventType, "aggregate_type": kind, "aggregate_id": aggregateID, "aggregate_version": version, "payload_hash": hash, "payload": raw(payload), "created_at": created})
	}
	write(w, 200, map[string]any{"events": data, "consistency": consistency("canonical_stale_bounded")})
}

func (s *Server) projectionSnapshot(w http.ResponseWriter, r *http.Request) {
	workspace := r.PathValue("workspace_id")
	var sequence int64
	_ = s.store.ReadRow(r.Context(), store.ReadLocal, `SELECT COALESCE(MAX(projection_sequence),0) FROM projection_events WHERE workspace_id=?`, workspace).Scan(&sequence)
	objects, err := s.snapshotObjects(r.Context(), workspace)
	if err != nil {
		dbError(w, err)
		return
	}
	knowledge, err := s.snapshotKnowledge(r.Context(), workspace)
	if err != nil {
		dbError(w, err)
		return
	}
	data := map[string]any{"objects": objects, "knowledge": knowledge}
	created := time.Now().UTC().Format(time.RFC3339Nano)
	snapshot := map[string]any{
		"snapshot_id": "snap_" + workspace + "_" + strconv.FormatInt(sequence, 10), "workspace_id": workspace,
		"projection_sequence": sequence, "created_at": created, "signature": nil, "signing_key_id": nil,
		"schema_version": 2, "data_hash": sha([]byte(canonical(data))), "objects": objects, "knowledge": knowledge,
	}
	write(w, 200, map[string]any{"snapshot": snapshot, "consistency": consistency("canonical_linearizable")})
}

func (s *Server) snapshotObjects(ctx context.Context, workspace string) ([]any, error) {
	rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT id FROM object_instances WHERE workspace_id=? AND archived_at IS NULL ORDER BY id`, workspace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	data := make([]any, 0, len(ids))
	for _, id := range ids {
		item, err := s.object(ctx, workspace, id)
		if err != nil {
			return nil, err
		}
		item["workspace_id"] = workspace
		data = append(data, item)
	}
	return data, nil
}

func (s *Server) snapshotKnowledge(ctx context.Context, workspace string) ([]any, error) {
	rows, err := s.store.ReadRows(ctx, store.ReadLocal, `SELECT bundle_id,concept_id,concept_type,title,description,resource,body_markdown,content_hash,created_at,updated_at FROM knowledge_documents WHERE workspace_id=? ORDER BY bundle_id,concept_id`, workspace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var bundle, concept, conceptType, body, hash, created, updated string
		var title, description, resource sql.NullString
		if err := rows.Scan(&bundle, &concept, &conceptType, &title, &description, &resource, &body, &hash, &created, &updated); err != nil {
			return nil, err
		}
		data = append(data, map[string]any{
			"id": bundle + ":" + concept, "workspace_id": workspace, "bundle_id": bundle, "concept_id": concept,
			"concept_type": conceptType, "title": nullString(title), "description": nullString(description),
			"resource": nullString(resource), "body_markdown": body, "content_hash": hash, "created_at": created, "updated_at": updated,
		})
	}
	return data, rows.Err()
}

func (s *Server) appendAdapterEvent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AdapterID      string `json:"adapter_id"`
		InboundEventID string `json:"inbound_event_id"`
		Payload        any    `json:"payload"`
	}
	if !decode(w, r, &body) {
		return
	}
	body.AdapterID = strings.TrimSpace(body.AdapterID)
	body.InboundEventID = strings.TrimSpace(body.InboundEventID)
	if body.AdapterID == "" || body.InboundEventID == "" || len(body.AdapterID) > 128 || len(body.InboundEventID) > 128 {
		problem(w, 400, "invalid_request", "validation", false, "adapter_id and inbound_event_id must be 1 to 128 bytes")
		return
	}
	id := "ain_" + uuid.NewString()
	payload := canonical(body.Payload)
	payloadHash := sha([]byte(payload))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	created := false
	var err error
	if s.store.Kind() == "rhiza" {
		id, now, created, err = s.appendAdapterEventRhiza(r.Context(), id, r.PathValue("workspace_id"), body.AdapterID, body.InboundEventID, payloadHash, payload, now)
	} else {
		err = s.store.Transact(r.Context(), "adapter-inbound:"+r.PathValue("workspace_id")+":"+body.AdapterID+":"+body.InboundEventID+":"+payloadHash, func(tx store.Tx) error {
			var existingID, existingHash, receivedAt string
			err := tx.QueryRow(`SELECT id,payload_hash,received_at FROM adapter_inbound_events WHERE workspace_id=? AND adapter_id=? AND inbound_event_id=?`, r.PathValue("workspace_id"), body.AdapterID, body.InboundEventID).Scan(&existingID, &existingHash, &receivedAt)
			if err == nil {
				if existingHash != payloadHash {
					return errors.New("adapter event idempotency conflict")
				}
				id, now = existingID, receivedAt
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			_, err = tx.Exec(`INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, id, r.PathValue("workspace_id"), body.AdapterID, body.InboundEventID, payloadHash, payload, now)
			created = err == nil
			return err
		})
	}
	if err != nil {
		if err.Error() == "adapter event idempotency conflict" {
			problem(w, 409, "idempotency_conflict", "conflict", false, "inbound event id was already used with different content")
			return
		}
		dbError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	write(w, status, map[string]any{"data": map[string]any{"id": id, "workspace_id": r.PathValue("workspace_id"), "adapter_id": body.AdapterID, "inbound_event_id": body.InboundEventID, "status": "appended", "received_at": now}})
}

func (s *Server) appendAdapterEventRhiza(ctx context.Context, id, workspaceID, adapterID, inboundEventID, payloadHash, payload, receivedAt string) (string, string, bool, error) {
	readExisting := func() (string, string, string, error) {
		var existingID, existingHash, existingAt string
		err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT id,payload_hash,received_at FROM adapter_inbound_events WHERE workspace_id=? AND adapter_id=? AND inbound_event_id=?`, workspaceID, adapterID, inboundEventID).Scan(&existingID, &existingHash, &existingAt)
		return existingID, existingHash, existingAt, err
	}
	if existingID, existingHash, existingAt, err := readExisting(); err == nil {
		if existingHash != payloadHash {
			return "", "", false, errors.New("adapter event idempotency conflict")
		}
		return existingID, existingAt, false, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", "", false, err
	}
	one := int64(1)
	err := s.store.ApplyStatements(ctx, "adapter-inbound:"+uuid.NewString(), []rhiza.SQLStatement{{
		SQL: `INSERT OR IGNORE INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, Args: []any{id, workspaceID, adapterID, inboundEventID, payloadHash, payload, receivedAt}, ExpectedRowsAffected: &one,
	}})
	if err != nil && !errors.Is(err, store.ErrMutationPreconditionFailed) {
		return "", "", false, err
	}
	existingID, existingHash, existingAt, readErr := readExisting()
	if readErr != nil {
		return "", "", false, readErr
	}
	if existingHash != payloadHash {
		return "", "", false, errors.New("adapter event idempotency conflict")
	}
	return existingID, existingAt, err == nil && existingID == id, nil
}

func (s *Server) listCapabilities(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	rows, err := s.store.ReadRows(r.Context(), store.ReadLocal, `SELECT id,principal_id,actor_type,actor_id,command_type,causation_source,relation_definition_id,idempotency_key_prefix,max_automation_recursion_depth FROM actor_capabilities WHERE workspace_id=? ORDER BY id`, r.PathValue("workspace_id"))
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	data := []any{}
	for rows.Next() {
		var id, principal, actorType, actorID, commandType, source, relation, prefix string
		var max sql.NullInt64
		_ = rows.Scan(&id, &principal, &actorType, &actorID, &commandType, &source, &relation, &prefix, &max)
		data = append(data, map[string]any{"id": id, "principal_id": principal, "requested_actor": map[string]any{"type": actorType, "id": actorID}, "command_type": commandType, "causation_source": source, "relation_definition_id": nullableString(relation), "idempotency_key_prefix": prefix, "max_automation_recursion_depth": nullInt(max)})
	}
	write(w, 200, map[string]any{"data": data})
}

func (s *Server) grantCapability(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	var body struct {
		PrincipalID                 string       `json:"principal_id"`
		RequestedActor              kernel.Actor `json:"requested_actor"`
		CommandType                 string       `json:"command_type"`
		CausationSource             string       `json:"causation_source"`
		RelationDefinitionID        *string      `json:"relation_definition_id"`
		IdempotencyKeyPrefix        string       `json:"idempotency_key_prefix"`
		MaxAutomationRecursionDepth *int64       `json:"max_automation_recursion_depth"`
	}
	if !decode(w, r, &body) {
		return
	}
	relation := ""
	if body.RelationDefinitionID != nil {
		relation = *body.RelationDefinitionID
	}
	id := "cap_" + uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	err := s.store.Exec(r.Context(), "capability:"+id, `INSERT INTO actor_capabilities VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, id, r.PathValue("workspace_id"), body.PrincipalID, body.RequestedActor.Type, body.RequestedActor.ID, body.CommandType, body.CausationSource, relation, body.IdempotencyKeyPrefix, body.MaxAutomationRecursionDepth, now, now)
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, 201, map[string]any{"data": map[string]any{"id": id}})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	return decodeLimit(w, r, v, 1<<20)
}
func decodeLimit(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		problem(w, 400, "invalid_request", "validation", false, "invalid JSON request")
		return false
	}
	return true
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code, category string, retryable bool, message string) {
	problemDetails(w, status, code, category, retryable, message, nil)
}
func problemDetails(w http.ResponseWriter, status int, code, category string, retryable bool, message string, details *kernel.ChangeErrorDetails) {
	errorBody := map[string]any{"code": code, "category": category, "retryable": retryable, "message": message}
	if details != nil {
		errorBody["details"] = details
	}
	write(w, status, map[string]any{"error": errorBody})
}
func dbError(w http.ResponseWriter, err error) {
	problem(w, 500, "internal_error", "storage", true, err.Error())
}
func commandError(w http.ResponseWriter, err error) {
	var e *kernel.CommandError
	if !errors.As(err, &e) {
		dbError(w, err)
		return
	}
	status := 400
	category := "validation"
	switch e.Outcome {
	case "idempotency_conflict", "precondition_failed":
		status = 409
		category = "conflict"
	case "forbidden":
		status = 403
		category = "authz"
	case "schema_mismatch":
		status = 422
		category = "schema"
	case "unsupported_storage":
		status = http.StatusNotImplemented
		category = "storage"
	case "quorum_unavailable":
		status = http.StatusServiceUnavailable
		category = "storage"
	}
	problemDetails(w, status, e.Outcome, category, e.Retryable, e.Message, e.Details)
}
func principal(r *http.Request) string {
	if value, ok := r.Context().Value(principalContextKey{}).(string); ok && value != "" {
		return value
	}
	return "dev"
}
func raw(s string) any       { var v any; _ = json.Unmarshal([]byte(s), &v); return v }
func canonical(v any) string { b, _ := json.Marshal(v); return string(b) }
func sha(raw []byte) string  { return fmt.Sprintf("sha256:%x", kernelHash(raw)) }
func kernelHash(raw []byte) [32]byte {
	var v any
	if json.Unmarshal(raw, &v) == nil {
		raw = []byte(canonical(v))
	}
	return sha256Sum(raw)
}
func sha256Sum(raw []byte) [32]byte { return sha256.Sum256(raw) }
func asciiLower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
func slug(s string) string {
	s = asciiLower(strings.TrimSpace(s))
	var b strings.Builder
	dash := false
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			dash = false
		} else if !dash && b.Len() > 0 {
			b.WriteByte('-')
			dash = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}
func consistency(mode string) map[string]any { return map[string]any{"mode": mode, "source": "kernel"} }
func boundedInt(r *http.Request, key string, def, min, max int) int {
	value, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil || value < min || value > max {
		return def
	}
	return value
}
func nullString(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}
func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func nullInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
