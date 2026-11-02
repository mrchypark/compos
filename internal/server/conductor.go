package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mrchypark/compos/internal/kernel"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func (s *Server) EnableConductor(baseURL, token, callbackToken string) error {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("conductor URL must be an absolute HTTP(S) URL")
	}
	s.conductorURL = strings.TrimRight(parsed.String(), "/")
	s.conductorToken = token
	s.conductorCallbackToken = callbackToken
	s.conductorClient = &http.Client{Timeout: 30 * time.Second}
	s.conductorWake = make(chan struct{}, 1)
	s.conductorDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s.conductorCancel = cancel
	go s.runConductorDispatcher(ctx)
	return nil
}

type conductorCallback struct {
	CallbackID         string          `json:"callback_id"`
	WorkspaceID        string          `json:"workspace_id"`
	ExecutionRequestID string          `json:"execution_request_id"`
	ExecutionID        string          `json:"execution_id"`
	ProviderKey        string          `json:"provider_key"`
	ProviderRunRef     string          `json:"provider_run_ref"`
	HandlerVersion     string          `json:"handler_version"`
	RuntimeStatus      string          `json:"runtime_status"`
	AttemptCount       int             `json:"attempt_count"`
	Result             json.RawMessage `json:"result"`
	ErrorClass         *string         `json:"error_class"`
	ErrorDetail        *string         `json:"error_detail"`
	TaskID             string          `json:"task_id,omitempty"`
	WorkflowInstanceID string          `json:"workflow_instance_id,omitempty"`
	CorrelationID      string          `json:"correlation_id,omitempty"`
	CausationID        string          `json:"causation_id,omitempty"`
	OccurredAt         string          `json:"occurred_at"`
}

func (s *Server) appendConductorCallback(w http.ResponseWriter, r *http.Request) {
	if s.conductorCallbackToken == "" {
		problem(w, http.StatusServiceUnavailable, "conductor_callback_unavailable", "configuration", true, "Conductor callback token is not configured")
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.conductorCallbackToken)) != 1 {
		problem(w, http.StatusUnauthorized, "unauthorized", "authn", false, "invalid Conductor callback token")
		return
	}
	var callback conductorCallback
	if !decodeLimit(w, r, &callback, 8<<20) {
		return
	}
	if strings.TrimSpace(callback.CallbackID) == "" || strings.TrimSpace(callback.WorkspaceID) == "" || strings.TrimSpace(callback.ExecutionRequestID) == "" || strings.TrimSpace(callback.ExecutionID) == "" || strings.TrimSpace(callback.ProviderKey) == "" || strings.TrimSpace(callback.ProviderRunRef) == "" || strings.TrimSpace(callback.HandlerVersion) == "" || callback.AttemptCount < 1 || !map[string]bool{"succeeded": true, "failed": true, "cancelled": true}[callback.RuntimeStatus] {
		problem(w, http.StatusBadRequest, "validation_failed", "request", false, "callback, workspace, execution request, execution, provider, positive attempt, and terminal status are required")
		return
	}
	if _, err := time.Parse(time.RFC3339Nano, callback.OccurredAt); err != nil {
		problem(w, http.StatusBadRequest, "validation_failed", "request", false, "occurred_at must be RFC3339")
		return
	}
	streamID, principalID, requestKind, err := s.conductorInvocation(r.Context(), callback)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "function invocation does not exist")
		return
	}
	if errors.Is(err, errConductorIdentity) {
		problem(w, http.StatusConflict, "execution_identity_conflict", "conflict", false, err.Error())
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	payload, err := canonicalConductorCallback(callback)
	if err != nil {
		problem(w, http.StatusBadRequest, "validation_failed", "request", false, "result must be valid JSON")
		return
	}
	hash := sha([]byte(payload))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	id := "ain_" + uuid.NewString()
	created := false
	if s.store.Kind() == "rhiza" {
		id, now, created, err = s.appendAdapterEventRhiza(r.Context(), id, callback.WorkspaceID, "conductor", callback.CallbackID, hash, payload, now)
	} else {
		err = s.store.Exec(r.Context(), "conductor-inbound:"+callback.WorkspaceID+":"+callback.CallbackID, `INSERT INTO adapter_inbound_events VALUES(?,?,?,?,?,?,'appended',?)`, id, callback.WorkspaceID, "conductor", callback.CallbackID, hash, payload, now)
		if err != nil {
			var existingID, existingHash, existingAt string
			lookupErr := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT id,payload_hash,received_at FROM adapter_inbound_events WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=?`, callback.WorkspaceID, callback.CallbackID).Scan(&existingID, &existingHash, &existingAt)
			if lookupErr == nil && existingHash == hash {
				id, now, err = existingID, existingAt, nil
			} else if lookupErr == nil {
				err = errors.New("adapter event idempotency conflict")
			}
		} else {
			created = true
		}
	}
	if err != nil {
		if err.Error() == "adapter event idempotency conflict" {
			problem(w, http.StatusConflict, "idempotency_conflict", "conflict", false, "callback id was already used for different content")
			return
		}
		dbError(w, err)
		return
	}
	if !s.reduceConductorCallback(w, r, callback, streamID, principalID, requestKind) {
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	write(w, status, map[string]any{"data": map[string]any{"id": id, "workspace_id": callback.WorkspaceID, "adapter_id": "conductor", "inbound_event_id": callback.CallbackID, "stream_id": streamID, "status": "reduced", "received_at": now}})
}

func canonicalConductorCallback(callback conductorCallback) (string, error) {
	if len(callback.Result) == 0 {
		return canonical(callback), nil
	}
	decoder := json.NewDecoder(bytes.NewReader(callback.Result))
	decoder.UseNumber()
	var result any
	if err := decoder.Decode(&result); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	callback.Result = normalized
	return canonical(callback), nil
}

func (s *Server) conductorInvocation(ctx context.Context, callback conductorCallback) (streamID, principalID, requestKind string, err error) {
	var handlerKey, handlerVersion string
	err = s.store.ReadRow(ctx, store.ReadLinearizable, `
		SELECT j.stream_id,j.principal_id,j.kind,json_extract(o.payload_json,'$._compos.handler_key'),json_extract(o.payload_json,'$._compos.handler_version')
		FROM journal_records j
		JOIN outbox_events o ON o.workspace_id=j.workspace_id AND o.command_id=j.command_id
		WHERE j.workspace_id=? AND j.correlation_id=? AND j.kind IN ('function.requested','agent.requested')
		  AND (j.kind='agent.requested' OR json_extract(j.payload_json,'$.binding_kind') IN ('connector','native'))
		  AND json_extract(o.payload_json,'$._compos.execution_request_id')=?
		ORDER BY j.stream_sequence LIMIT 1`, callback.WorkspaceID, callback.ExecutionRequestID, callback.ExecutionRequestID).Scan(&streamID, &principalID, &requestKind, &handlerKey, &handlerVersion)
	if err == nil && (callback.ProviderKey != handlerKey || callback.HandlerVersion != handlerVersion) {
		err = errConductorIdentity
	}
	return
}

var errConductorIdentity = errors.New("callback does not match the pinned handler identity")
var errConductorTerminalConflict = errors.New("callback conflicts with the existing terminal result")

func (s *Server) reduceConductorCallback(w http.ResponseWriter, r *http.Request, callback conductorCallback, streamID, principalID, requestKind string) bool {
	status, err := s.reduceConductorCallbackCore(r.Context(), callback, streamID, principalID, requestKind)
	if err != nil {
		if errors.Is(err, errConductorTerminalConflict) {
			_ = s.rejectConductorCallback(r.Context(), callback)
			problem(w, http.StatusConflict, "terminal_result_conflict", "conflict", false, err.Error())
			return false
		}
		var commandErr *kernel.CommandError
		if errors.As(err, &commandErr) {
			if !commandErr.Retryable {
				_ = s.rejectConductorCallback(r.Context(), callback)
			}
			commandError(w, err)
		} else {
			dbError(w, err)
		}
		return false
	}
	if status == "rejected" {
		write(w, http.StatusOK, map[string]any{"data": map[string]any{"workspace_id": callback.WorkspaceID, "adapter_id": "conductor", "inbound_event_id": callback.CallbackID, "status": "rejected"}})
		return false
	}
	s.wakeWorkflow()
	return true
}

func (s *Server) reduceConductorCallbackCore(ctx context.Context, callback conductorCallback, streamID, principalID, requestKind string) (string, error) {
	var status string
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT status FROM adapter_inbound_events WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=?`, callback.WorkspaceID, callback.CallbackID).Scan(&status); err != nil {
		return "", err
	}
	if status == "reduced" {
		return status, nil
	}
	if status == "rejected" {
		return status, nil
	}
	var workTerminal int
	if err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind IN ('work.satisfied','work.cancelled')`, callback.WorkspaceID, streamID).Scan(&workTerminal); err != nil {
		return "", err
	}
	if workTerminal != 0 {
		return "", &kernel.CommandError{Outcome: "precondition_failed", Message: "work stream is already terminal"}
	}
	output := map[string]any{}
	runtimeStatus := callback.RuntimeStatus
	errorClass, errorDetail := stringValue(callback.ErrorClass), stringValue(callback.ErrorDetail)
	if len(callback.Result) != 0 && string(callback.Result) != "null" {
		if err := json.Unmarshal(callback.Result, &output); err != nil {
			runtimeStatus, errorClass, errorDetail = "failed", "output_contract_mismatch", "Conductor result must be a JSON object"
		}
	} else if runtimeStatus == "succeeded" {
		runtimeStatus, errorClass, errorDetail = "failed", "output_contract_mismatch", "Conductor result must be a JSON object"
	}
	if converged, err := s.convergeTerminalCallback(ctx, callback, requestKind, output, runtimeStatus, errorClass, errorDetail); err != nil || converged {
		if err != nil {
			return "", err
		}
		return "reduced", nil
	}
	payload := map[string]any{
		"invocation_id": callback.ExecutionRequestID, "callback_id": callback.CallbackID,
		"runtime_status": runtimeStatus, "output": output, "adapter_inbound_event_id": callback.CallbackID,
		"execution_id": callback.ExecutionID, "provider_key": callback.ProviderKey, "provider_run_ref": callback.ProviderRunRef, "handler_version": callback.HandlerVersion, "attempt_count": callback.AttemptCount,
	}
	if errorClass != "" {
		payload["error_class"] = errorClass
	}
	if errorDetail != "" {
		payload["error_detail"] = errorDetail
	}
	digest := strings.TrimPrefix(sha([]byte(callback.WorkspaceID+":"+callback.CallbackID)), "sha256:")[:32]
	eventID := callback.CallbackID
	commandType := "function.record_result"
	if requestKind == "agent.requested" {
		commandType = "agent.record_result"
	}
	_, err := s.kernel.Execute(ctx, principalID, kernel.Command{
		CommandID: "cmd_conductor_" + digest, IdempotencyKey: "conductor:" + callback.CallbackID,
		WorkspaceID: callback.WorkspaceID, CommandType: commandType,
		Target:        kernel.Target{AggregateType: "work_stream", AggregateID: &streamID},
		Preconditions: []kernel.Precondition{}, Payload: json.RawMessage(canonical(payload)),
		Causation: kernel.Causation{Source: "adapter_reducer", EventID: &eventID},
	})
	if err != nil {
		return "", err
	}
	return "reduced", nil
}

func (s *Server) convergeTerminalCallback(ctx context.Context, callback conductorCallback, requestKind string, output map[string]any, runtimeStatus, errorClass, errorDetail string) (bool, error) {
	kinds := "'function.succeeded','function.failed'"
	if requestKind == "agent.requested" {
		kinds = "'agent.succeeded','agent.failed'"
	}
	var raw string
	err := s.store.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM journal_records WHERE workspace_id=? AND correlation_id=? AND kind IN (`+kinds+`) ORDER BY stream_sequence LIMIT 1`, callback.WorkspaceID, callback.ExecutionRequestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var terminal map[string]any
	if err := json.Unmarshal([]byte(raw), &terminal); err != nil {
		return false, err
	}
	digest := callbackResultDigest(runtimeStatus, output, errorClass, errorDetail)
	same := stringValueAny(terminal["result_digest"]) == digest
	if stringValueAny(terminal["result_digest"]) == "" {
		same = terminal["runtime_status"] == runtimeStatus && canonical(terminal["output"]) == canonical(output) && stringValueAny(terminal["error_class"]) == errorClass && stringValueAny(terminal["error_detail"]) == errorDetail
	}
	if !same {
		return false, errConductorTerminalConflict
	}
	err = s.store.Exec(ctx, "conductor-converged:"+callback.WorkspaceID+":"+callback.CallbackID, `UPDATE adapter_inbound_events SET status='reduced' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, callback.WorkspaceID, callback.CallbackID)
	return err == nil, err
}

func callbackResultDigest(runtimeStatus string, output map[string]any, errorClass, errorDetail string) string {
	return sha([]byte(canonical(map[string]any{"runtime_status": runtimeStatus, "output": output, "error_class": errorClass, "error_detail": errorDetail})))
}

func (s *Server) rejectConductorCallback(ctx context.Context, callback conductorCallback) error {
	return s.store.Exec(ctx, "conductor-rejected:"+callback.WorkspaceID+":"+callback.CallbackID, `UPDATE adapter_inbound_events SET status='rejected' WHERE workspace_id=? AND adapter_id='conductor' AND inbound_event_id=? AND status='appended'`, callback.WorkspaceID, callback.CallbackID)
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func stringValueAny(value any) string {
	text, _ := value.(string)
	return text
}

func (s *Server) authorizeConductor(w http.ResponseWriter, r *http.Request) (string, bool) {
	if s.conductorToken == "" {
		problem(w, http.StatusServiceUnavailable, "conductor_unavailable", "configuration", true, "Conductor service token is not configured")
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.conductorToken)) != 1 {
		problem(w, http.StatusUnauthorized, "unauthorized", "authn", false, "invalid Conductor service token")
		return "", false
	}
	workspaceID := strings.TrimSpace(r.Header.Get("X-Conductor-Workspace-Id"))
	if workspaceID == "" {
		problem(w, http.StatusBadRequest, "validation_failed", "request", false, "X-Conductor-Workspace-Id is required")
		return "", false
	}
	return workspaceID, true
}

func (s *Server) conductorObjectRecord(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := s.authorizeConductor(w, r)
	if !ok {
		return
	}
	item, err := s.object(r.Context(), workspaceID, r.PathValue("record_id"))
	if errors.Is(err, sql.ErrNoRows) || err == nil && item["object_type_key"] != r.PathValue("object_key") {
		problem(w, http.StatusNotFound, "not_found", "request", false, "record not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, http.StatusOK, item)
}

func (s *Server) conductorObjectSchema(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := s.authorizeConductor(w, r)
	if !ok {
		return
	}
	var id, fieldSchema, relationSchema string
	var schemaVersion int64
	var stateMachine sql.NullString
	err := s.store.ReadRow(r.Context(), store.ReadLinearizable, `SELECT id,schema_version,field_schema_json,relation_schema_json,state_machine_id FROM object_types WHERE workspace_id=? AND key=? AND archived_at IS NULL`, workspaceID, r.PathValue("object_key")).Scan(&id, &schemaVersion, &fieldSchema, &relationSchema, &stateMachine)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, http.StatusNotFound, "not_found", "request", false, "object schema not found")
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	write(w, http.StatusOK, map[string]any{"id": id, "key": r.PathValue("object_key"), "schema_version": schemaVersion, "field_schema": raw(fieldSchema), "relation_schema": raw(relationSchema), "state_machine_id": nullString(stateMachine)})
}

func (s *Server) wakeConductor() {
	if s.conductorWake == nil {
		return
	}
	select {
	case s.conductorWake <- struct{}{}:
	default:
	}
}

func (s *Server) runConductorDispatcher(ctx context.Context) {
	defer close(s.conductorDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.reducePendingConductorCallbacks(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("conductor callback reduction failed: %v", err)
		}
		if err := s.dispatchConductorOutbox(ctx); err != nil && !errors.Is(err, context.Canceled) {
			// ponytail: one-process poller; add durable attempt scheduling when multi-node delivery is required.
			log.Printf("conductor outbox delivery failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.conductorWake:
		}
	}
}

func (s *Server) reducePendingConductorCallbacks(ctx context.Context) error {
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `SELECT payload_json FROM adapter_inbound_events WHERE adapter_id='conductor' AND status='appended' ORDER BY received_at LIMIT 100`)
	if err != nil {
		return err
	}
	var callbacks []conductorCallback
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return err
		}
		var callback conductorCallback
		if err := json.Unmarshal([]byte(payload), &callback); err != nil {
			rows.Close()
			return err
		}
		callbacks = append(callbacks, callback)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, callback := range callbacks {
		streamID, principalID, requestKind, err := s.conductorInvocation(ctx, callback)
		if errors.Is(err, sql.ErrNoRows) {
			if err := s.rejectConductorCallback(ctx, callback); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if _, err := s.reduceConductorCallbackCore(ctx, callback, streamID, principalID, requestKind); err != nil {
			var commandErr *kernel.CommandError
			if errors.As(err, &commandErr) && !commandErr.Retryable {
				if err := s.rejectConductorCallback(ctx, callback); err != nil {
					return err
				}
				continue
			}
			return err
		}
		s.wakeWorkflow()
	}
	return nil
}

type conductorOutboxEvent struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
}

type conductorAcknowledgement struct {
	Accepted bool `json:"accepted"`
	Events   []struct {
		EventID            json.RawMessage `json:"event_id"`
		ExecutionID        string          `json:"execution_id"`
		ExecutionRequestID string          `json:"execution_request_id"`
		Status             string          `json:"status"`
	} `json:"events"`
}

func (s *Server) dispatchConductorOutbox(ctx context.Context) error {
	if s.conductorURL == "" {
		return nil
	}
	// Rhiza accepts at most 64 statements per replicated SQL request; one publish
	// update is issued for each delivered event.
	rows, err := s.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id,command_id,event_type,payload_json FROM outbox_events WHERE status='pending' AND json_type(payload_json,'$._compos.execution_request_id')='text' ORDER BY created_at,id LIMIT 64`)
	if err != nil {
		return err
	}
	var events []conductorOutboxEvent
	for rows.Next() {
		var event conductorOutboxEvent
		var data string
		if err := rows.Scan(&event.ID, &event.IdempotencyKey, &event.Name, &data); err != nil {
			rows.Close()
			return err
		}
		event.Data = json.RawMessage(data)
		events = append(events, event)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.conductorURL+"/v1/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.conductorToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.conductorToken)
	}
	response, err := s.conductorClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("conductor returned %s: %s", response.Status, strings.TrimSpace(string(detail)))
	}
	if err := validateConductorAcknowledgement(response.Body, events); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if s.store.Kind() == "rhiza" {
		statements := make([]rhiza.SQLStatement, 0, len(events))
		for _, event := range events {
			statements = append(statements, rhiza.SQLStatement{SQL: `UPDATE outbox_events SET status='published',updated_at=? WHERE id=? AND status='pending'`, Args: []any{now, event.ID}})
		}
		return s.store.ApplyStatements(ctx, "conductor-published:"+uuid.NewString(), statements)
	}
	return s.store.Transact(ctx, "conductor-published:"+events[0].ID, func(tx store.Tx) error {
		for _, event := range events {
			if _, err := tx.ExecContext(ctx, `UPDATE outbox_events SET status='published',updated_at=? WHERE id=? AND status='pending'`, now, event.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func validateConductorAcknowledgement(body io.Reader, events []conductorOutboxEvent) error {
	const acknowledgementLimit = 1 << 20
	payload, err := io.ReadAll(io.LimitReader(body, acknowledgementLimit+1))
	if err != nil {
		return fmt.Errorf("read conductor acknowledgment: %w", err)
	}
	if len(payload) > acknowledgementLimit {
		return errors.New("conductor acknowledgment exceeds 1 MiB")
	}
	var acknowledgement conductorAcknowledgement
	if err := json.Unmarshal(payload, &acknowledgement); err != nil {
		return fmt.Errorf("decode conductor acknowledgment: %w", err)
	}
	if !acknowledgement.Accepted {
		return errors.New("conductor did not accept event batch")
	}
	expected := make(map[string]string, len(events))
	for _, event := range events {
		var data struct {
			Compos struct {
				ExecutionRequestID string `json:"execution_request_id"`
			} `json:"_compos"`
		}
		if err := json.Unmarshal(event.Data, &data); err != nil || data.Compos.ExecutionRequestID == "" {
			return fmt.Errorf("invalid conductor event %s", event.ID)
		}
		if _, exists := expected[data.Compos.ExecutionRequestID]; exists {
			return fmt.Errorf("duplicate conductor invocation %s", data.Compos.ExecutionRequestID)
		}
		expected[data.Compos.ExecutionRequestID] = event.ID
	}
	for _, event := range acknowledgement.Events {
		if event.ExecutionRequestID == "" || event.ExecutionID == "" || !conductorExecutionStatus(event.Status) {
			return errors.New("invalid conductor event acknowledgment")
		}
		expectedEventID, ok := expected[event.ExecutionRequestID]
		if !ok {
			return errors.New("foreign or duplicate conductor event acknowledgment")
		}
		if len(event.EventID) != 0 {
			var eventID string
			if json.Unmarshal(event.EventID, &eventID) != nil || eventID != expectedEventID {
				return errors.New("conductor event acknowledgment has mismatched event_id")
			}
		}
		delete(expected, event.ExecutionRequestID)
	}
	if len(expected) != 0 {
		return errors.New("missing conductor event acknowledgment")
	}
	return nil
}

func conductorExecutionStatus(status string) bool {
	return map[string]bool{"queued": true, "running": true, "retry_wait": true, "succeeded": true, "failed": true, "timed_out": true, "cancelled": true}[status]
}
