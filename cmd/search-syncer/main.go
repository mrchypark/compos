package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/meilisearch/meilisearch-go"
	"github.com/mrchypark/compos/internal/korean"
)

type Config struct {
	ComposEndpoint  string
	WorkspaceID     string
	MeilisearchURL  string
	IndexUID        string
	PrimaryKey      string
	IntervalSeconds int
	BatchSize       int
	APIToken        string
	MeilisearchKey  string
}

type SyncState struct {
	LastSequence  int64
	SnapshotReady bool
	WorkspaceID   string
}

type Object struct {
	ID            string                 `json:"id"`
	WorkspaceID   string                 `json:"workspace_id"`
	ObjectTypeKey string                 `json:"object_type_key"`
	Title         string                 `json:"title"`
	StateKey      *string                `json:"state_key"`
	Payload       map[string]interface{} `json:"values"`
	Version       int64                  `json:"version"`
	CreatedAt     string                 `json:"created_at"`
	UpdatedAt     string                 `json:"updated_at"`
}

type ProjectionSnapshot struct {
	SnapshotID         string              `json:"snapshot_id"`
	WorkspaceID        string              `json:"workspace_id"`
	ProjectionSequence int64               `json:"projection_sequence"`
	CreatedAt          string              `json:"created_at"`
	Signature          *string             `json:"signature"`
	SigningKeyID       *string             `json:"signing_key_id"`
	SchemaVersion      int                 `json:"schema_version"`
	DataHash           string              `json:"data_hash"`
	Objects            []Object            `json:"objects"`
	Knowledge          []KnowledgeDocument `json:"knowledge"`
}

type KnowledgeDocument struct {
	ID           string  `json:"id"`
	WorkspaceID  string  `json:"workspace_id"`
	BundleID     string  `json:"bundle_id"`
	ConceptID    string  `json:"concept_id"`
	ConceptType  string  `json:"concept_type"`
	Title        *string `json:"title"`
	Description  *string `json:"description"`
	Resource     *string `json:"resource"`
	BodyMarkdown string  `json:"body_markdown"`
	ContentHash  string  `json:"content_hash"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type ProjectionEvent struct {
	ID                 string      `json:"id"`
	WorkspaceID        string      `json:"workspace_id"`
	CommandID          string      `json:"command_id"`
	ProjectionSequence int64       `json:"projection_sequence"`
	SnapshotID         *string     `json:"snapshot_id"`
	AggregateType      string      `json:"aggregate_type"`
	AggregateID        string      `json:"aggregate_id"`
	AggregateVersion   int64       `json:"aggregate_version"`
	EventType          string      `json:"event_type"`
	PayloadHash        string      `json:"payload_hash"`
	Payload            interface{} `json:"payload"`
	CreatedAt          string      `json:"created_at"`
}

type ProjectionEventsResponse struct {
	Events []ProjectionEvent `json:"events"`
}

func main() {
	cfg := Config{
		ComposEndpoint:  getEnv("COMPOS_ENDPOINT", "http://compos:7878"),
		WorkspaceID:     getEnv("COMPOS_WORKSPACE_ID", ""),
		MeilisearchURL:  getEnv("MEILISEARCH_URL", "http://meilisearch:7700"),
		IndexUID:        getEnv("MEILISEARCH_INDEX_UID", "compos-objects"),
		PrimaryKey:      getEnv("MEILISEARCH_PRIMARY_KEY", "id"),
		IntervalSeconds: getEnvInt("SYNC_INTERVAL_SECONDS", 5),
		BatchSize:       getEnvInt("SYNC_BATCH_SIZE", 100),
		APIToken:        getEnv("COMPOS_API_TOKEN", ""),
		MeilisearchKey:  getEnv("MEILISEARCH_API_KEY", ""),
	}

	if cfg.WorkspaceID == "" && cfg.APIToken == "" {
		fmt.Fprintln(os.Stderr, "COMPOS_API_TOKEN is required when COMPOS_WORKSPACE_ID is empty")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("syncer starting: compos=%s workspace=%s meilisearch=%s index=%s\n",
		cfg.ComposEndpoint, cfg.WorkspaceID, cfg.MeilisearchURL, cfg.IndexUID)

	msClient := newMeilisearchClient(cfg)
	httpClient := &http.Client{Timeout: 30 * time.Second}

	state := SyncState{}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("syncer shutting down")
			return
		default:
		}

		if err := syncOnce(ctx, httpClient, msClient, cfg, &state); err != nil {
			fmt.Fprintf(os.Stderr, "sync error: %v\n", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(cfg.IntervalSeconds) * time.Second):
		}
	}
}

func newMeilisearchClient(cfg Config) meilisearch.ServiceManager {
	if cfg.MeilisearchKey != "" {
		return meilisearch.New(cfg.MeilisearchURL, meilisearch.WithAPIKey(cfg.MeilisearchKey))
	}
	return meilisearch.New(cfg.MeilisearchURL)
}

func syncOnce(ctx context.Context, httpClient *http.Client, msClient meilisearch.ServiceManager, cfg Config, state *SyncState) error {
	previousWorkspaceID := ""
	workspaceChanged := false
	if cfg.WorkspaceID == "" {
		workspaceID, err := discoverWorkspace(ctx, httpClient, cfg)
		if err != nil {
			return err
		}
		previousWorkspaceID = state.WorkspaceID
		workspaceChanged = previousWorkspaceID != workspaceID
		cfg.WorkspaceID = workspaceID
	}
	created, err := ensureIndex(ctx, msClient, cfg)
	if err != nil {
		return fmt.Errorf("ensure index: %w", err)
	}
	if workspaceChanged {
		if previousWorkspaceID != "" {
			if err := clearWorkspaceSearchProjection(msClient, cfg.IndexUID, previousWorkspaceID); err != nil {
				return err
			}
		}
		state.WorkspaceID = cfg.WorkspaceID
		state.LastSequence = 0
		state.SnapshotReady = false
	}
	if created {
		state.LastSequence = 0
		state.SnapshotReady = false
	}
	if !state.SnapshotReady {
		if !created {
			if err := updateIndexSettings(msClient, cfg); err != nil {
				return err
			}
		}
		fmt.Println("mode: snapshot sync")
		return syncFromSnapshot(ctx, httpClient, msClient, cfg, state)
	}
	fmt.Printf("mode: event sync (last_sequence=%d)\n", state.LastSequence)
	return syncFromEvents(ctx, httpClient, msClient, cfg, state)
}

func discoverWorkspace(ctx context.Context, httpClient *http.Client, cfg Config) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cfg.ComposEndpoint, "/")+"/api/v1/search-syncer/workspace", nil)
	if err != nil {
		return "", fmt.Errorf("create workspace discovery request: %w", err)
	}
	if cfg.APIToken == "" {
		return "", errors.New("workspace discovery requires an API token")
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("workspace discovery request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("workspace discovery failed with status %d: %s", resp.StatusCode, string(body))
	}
	var response struct {
		WorkspaceID string `json:"workspace_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("decode workspace discovery: %w", err)
	}
	if !pathSafeWorkspaceID(response.WorkspaceID) {
		return "", errors.New("workspace discovery returned an invalid workspace_id")
	}
	return response.WorkspaceID, nil
}

func pathSafeWorkspaceID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func ensureIndex(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config) (bool, error) {
	if _, err := msClient.GetIndexWithContext(ctx, cfg.IndexUID); err != nil {
		var meiliErr *meilisearch.Error
		if !errors.As(err, &meiliErr) || meiliErr.StatusCode != http.StatusNotFound {
			return false, fmt.Errorf("read index: %w", err)
		}
		task, err := msClient.CreateIndex(&meilisearch.IndexConfig{Uid: cfg.IndexUID, PrimaryKey: cfg.PrimaryKey})
		if err != nil {
			return false, fmt.Errorf("create index: %w", err)
		}
		if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
			return false, fmt.Errorf("wait for index creation: %w", err)
		}

		if err := updateIndexSettings(msClient, cfg); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func updateIndexSettings(msClient meilisearch.ServiceManager, cfg Config) error {
	task, err := msClient.Index(cfg.IndexUID).UpdateSettings(&meilisearch.Settings{
		SearchableAttributes: []string{"name", "name_search", "name_mixed", "title", "description", "body_markdown", "concept_id", "type"},
		FilterableAttributes: []string{"workspace_id", "type", "state", "concept_type", "bundle_id", "updated_at"},
		SortableAttributes:   []string{"updated_at", "title"},
	})
	if err != nil {
		return fmt.Errorf("update index settings: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
		return fmt.Errorf("wait for index settings: %w", err)
	}
	return nil
}

func syncFromSnapshot(ctx context.Context, httpClient *http.Client, msClient meilisearch.ServiceManager, cfg Config, state *SyncState) error {
	fmt.Println("syncing from snapshot...")

	url := fmt.Sprintf("%s/api/v1/workspaces/%s/projection/snapshot", cfg.ComposEndpoint, cfg.WorkspaceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create snapshot request: %w", err)
	}
	if cfg.APIToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("snapshot request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("snapshot request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Snapshot ProjectionSnapshot `json:"snapshot"`
	}
	respBody, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(respBody, &response); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	snapshot := response.Snapshot

	fmt.Printf("snapshot received: %d objects, sequence=%d\n", len(snapshot.Objects), snapshot.ProjectionSequence)

	if err := clearWorkspaceSearchProjection(msClient, cfg.IndexUID, cfg.WorkspaceID); err != nil {
		return err
	}

	if err := indexDocuments(ctx, msClient, cfg, snapshot.Objects); err != nil {
		return fmt.Errorf("index snapshot documents: %w", err)
	}
	if err := indexKnowledgeDocuments(ctx, msClient, cfg, snapshot.Knowledge); err != nil {
		return fmt.Errorf("index Knowledge documents: %w", err)
	}

	state.LastSequence = snapshot.ProjectionSequence
	state.SnapshotReady = true
	fmt.Printf("snapshot sync complete, sequence=%d\n", state.LastSequence)
	return nil
}

func clearWorkspaceSearchProjection(msClient meilisearch.ServiceManager, indexUID, workspaceID string) error {
	deleteTask, err := msClient.Index(indexUID).DeleteDocumentsByFilter(`workspace_id = `+strconv.Quote(workspaceID), nil)
	if err != nil {
		return fmt.Errorf("clear workspace search projection: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, deleteTask.TaskUID); err != nil {
		return fmt.Errorf("wait for workspace search projection clear: %w", err)
	}
	return nil
}

func syncFromEvents(ctx context.Context, httpClient *http.Client, msClient meilisearch.ServiceManager, cfg Config, state *SyncState) error {
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/projection/events?after=%d&limit=%d",
		cfg.ComposEndpoint, cfg.WorkspaceID, state.LastSequence, cfg.BatchSize)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create events request: %w", err)
	}
	if cfg.APIToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("events request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		fmt.Println("cursor too old, rebuilding from snapshot")
		state.LastSequence = 0
		state.SnapshotReady = false
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("events request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var eventsResp ProjectionEventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&eventsResp); err != nil {
		return fmt.Errorf("decode events: %w", err)
	}

	if len(eventsResp.Events) == 0 {
		fmt.Println("no new events")
		return nil
	}

	fmt.Printf("received %d events\n", len(eventsResp.Events))

	for _, event := range eventsResp.Events {
		if err := processEvent(ctx, msClient, cfg, event); err != nil {
			return fmt.Errorf("process event %s: %w", event.ID, err)
		}
		state.LastSequence = event.ProjectionSequence
	}

	return nil
}

func processEvent(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, event ProjectionEvent) error {
	switch event.AggregateType {
	case "object_instance":
		return processObjectEvent(ctx, msClient, cfg, event)
	case "knowledge_document":
		return processKnowledgeEvent(ctx, msClient, cfg, event)
	default:
		return nil
	}
}

func processKnowledgeEvent(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, event ProjectionEvent) error {
	payload, ok := event.Payload.(map[string]interface{})
	if !ok {
		return nil
	}
	operation, _ := payload["operation"].(string)
	if operation == "removed" {
		return deleteDocument(ctx, msClient, cfg, knowledgeSearchID(event.WorkspaceID, event.AggregateID))
	}
	if operation != "accepted" {
		return nil
	}
	document := KnowledgeDocument{ID: event.AggregateID, WorkspaceID: event.WorkspaceID, UpdatedAt: event.CreatedAt, CreatedAt: event.CreatedAt}
	document.BundleID, _ = payload["bundle_id"].(string)
	document.ConceptID, _ = payload["concept_id"].(string)
	document.ConceptType, _ = payload["concept_type"].(string)
	document.BodyMarkdown, _ = payload["body_markdown"].(string)
	document.ContentHash, _ = payload["content_hash"].(string)
	document.Title = optionalString(payload["title"])
	document.Description = optionalString(payload["description"])
	document.Resource = optionalString(payload["resource"])
	return indexKnowledgeDocuments(ctx, msClient, cfg, []KnowledgeDocument{document})
}

func processObjectEvent(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, event ProjectionEvent) error {
	payload, ok := event.Payload.(map[string]interface{})
	if !ok {
		return nil
	}

	switch event.EventType {
	case "object.created", "object.updated":
		obj := Object{
			ID:          event.AggregateID,
			WorkspaceID: event.WorkspaceID,
			Version:     event.AggregateVersion,
			CreatedAt:   event.CreatedAt,
			UpdatedAt:   event.CreatedAt,
		}
		if ot, ok := payload["object_type_key"].(string); ok {
			obj.ObjectTypeKey = ot
		}
		if title, ok := payload["title"].(string); ok {
			obj.Title = title
		}
		if stateKey, ok := payload["state_key"].(string); ok {
			obj.StateKey = &stateKey
		}
		if values, ok := payload["values"].(map[string]interface{}); ok {
			obj.Payload = values
		}
		if createdAt, ok := payload["created_at"].(string); ok {
			obj.CreatedAt = createdAt
		}
		if updatedAt, ok := payload["updated_at"].(string); ok {
			obj.UpdatedAt = updatedAt
		}

		return indexDocuments(ctx, msClient, cfg, []Object{obj})

	case "object.moved":
		if stateKey, ok := payload["state_key"].(string); ok {
			return patchDocument(ctx, msClient, cfg, objectSearchID(event.WorkspaceID, event.AggregateID), map[string]interface{}{
				"state":      stateKey,
				"updated_at": event.CreatedAt,
				"version":    event.AggregateVersion,
			})
		}

	case "object.deleted":
		return deleteDocument(ctx, msClient, cfg, objectSearchID(event.WorkspaceID, event.AggregateID))
	}

	return nil
}

func indexDocuments(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, objects []Object) error {
	if len(objects) == 0 {
		return nil
	}

	docs := make([]map[string]interface{}, len(objects))
	for i, obj := range objects {
		doc := map[string]interface{}{
			"id":           objectSearchID(obj.WorkspaceID, obj.ID),
			"object_id":    obj.ID,
			"workspace_id": obj.WorkspaceID,
			"type":         obj.ObjectTypeKey,
			"title":        obj.Title,
			"version":      obj.Version,
			"created_at":   obj.CreatedAt,
			"updated_at":   obj.UpdatedAt,
		}
		if obj.StateKey != nil {
			doc["state"] = *obj.StateKey
		}
		if obj.Payload != nil {
			for k, v := range obj.Payload {
				doc["payload_"+k] = v
			}
		}

		// Korean search normalization
		if obj.Title != "" {
			doc["name_chosung"] = korean.ExtractChosung(obj.Title)
			doc["name_jamo"] = korean.DecomposeJamo(obj.Title)
			doc["name_search"] = korean.SearchFields(obj.Title)
			doc["name_mixed"] = korean.MixedSearchFields(obj.Title)
		}

		docs[i] = doc
	}

	task, err := msClient.Index(cfg.IndexUID).AddDocuments(docs, nil)
	if err != nil {
		return fmt.Errorf("add documents: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
		return fmt.Errorf("wait for document indexing: %w", err)
	}

	return nil
}

func indexKnowledgeDocuments(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, knowledge []KnowledgeDocument) error {
	if len(knowledge) == 0 {
		return nil
	}
	docs := make([]map[string]interface{}, 0, len(knowledge))
	for _, item := range knowledge {
		doc := map[string]interface{}{
			"id": knowledgeSearchID(item.WorkspaceID, item.ID), "knowledge_id": item.ID, "workspace_id": item.WorkspaceID, "type": "knowledge", "bundle_id": item.BundleID,
			"concept_id": item.ConceptID, "concept_type": item.ConceptType, "body_markdown": item.BodyMarkdown,
			"content_hash": item.ContentHash, "created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
		}
		if item.Title != nil {
			doc["title"] = *item.Title
			doc["name_search"] = korean.SearchFields(*item.Title)
			doc["name_mixed"] = korean.MixedSearchFields(*item.Title)
		}
		if item.Description != nil {
			doc["description"] = *item.Description
		}
		if item.Resource != nil {
			doc["resource"] = *item.Resource
		}
		docs = append(docs, doc)
	}
	task, err := msClient.Index(cfg.IndexUID).AddDocuments(docs, nil)
	if err != nil {
		return fmt.Errorf("add Knowledge documents: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
		return fmt.Errorf("wait for Knowledge indexing: %w", err)
	}
	return nil
}

func knowledgeSearchID(workspaceID, canonicalID string) string {
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + canonicalID))
	return "knw_" + hex.EncodeToString(sum[:])
}

func objectSearchID(workspaceID, canonicalID string) string {
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + canonicalID))
	return "obj_" + hex.EncodeToString(sum[:])
}

func optionalString(value any) *string {
	text, ok := value.(string)
	if !ok || text == "" {
		return nil
	}
	return &text
}

func patchDocument(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, docID string, update map[string]interface{}) error {
	update["id"] = docID
	task, err := msClient.Index(cfg.IndexUID).UpdateDocuments([]map[string]interface{}{update}, nil)
	if err != nil {
		return fmt.Errorf("update document: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
		return fmt.Errorf("wait for document update: %w", err)
	}

	return nil
}

func deleteDocument(ctx context.Context, msClient meilisearch.ServiceManager, cfg Config, docID string) error {
	task, err := msClient.Index(cfg.IndexUID).DeleteDocument(docID, nil)
	if err != nil {
		return fmt.Errorf("delete document: %w", err)
	}
	if err := waitTask(msClient.WaitForTask, task.TaskUID); err != nil {
		return fmt.Errorf("wait for document deletion: %w", err)
	}

	return nil
}

func waitTask(wait func(int64, time.Duration) (*meilisearch.Task, error), uid int64) error {
	task, err := wait(uid, 100*time.Millisecond)
	if err != nil {
		return err
	}
	if task.Status != meilisearch.TaskStatusSucceeded {
		if task.Error.Code != "" {
			return fmt.Errorf("Meilisearch task %d ended with status %s (%s)", uid, task.Status, task.Error.Code)
		}
		return fmt.Errorf("Meilisearch task %d ended with status %s", uid, task.Status)
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}
