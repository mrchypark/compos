package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func committedEventFromReceipt(attemptID ledger.AttemptID, expectedCommandID, expectedWorkspaceID string, body []byte, committedAt time.Time) (ledger.AttemptCommitted, error) {
	var receipt struct {
		CommandID          string  `json:"command_id"`
		Outcome            string  `json:"outcome"`
		WorkspaceID        *string `json:"workspace_id"`
		ProjectionSequence *int64  `json:"projection_sequence"`
		Aggregate          *struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Version int64  `json:"version"`
		} `json:"aggregate"`
		Projection *struct {
			WorkspaceID        string `json:"workspace_id"`
			ProjectionSequence *int64 `json:"projection_sequence"`
		} `json:"projection"`
	}
	if err := json.Unmarshal(body, &receipt); err != nil {
		return ledger.AttemptCommitted{}, err
	}
	if strings.TrimSpace(receipt.CommandID) == "" {
		return ledger.AttemptCommitted{}, fmt.Errorf("missing command_id")
	}
	if receipt.CommandID != expectedCommandID {
		return ledger.AttemptCommitted{}, fmt.Errorf("command_id %q does not match submitted command %q", receipt.CommandID, expectedCommandID)
	}
	if receipt.Outcome != "applied" && receipt.Outcome != "duplicate" {
		return ledger.AttemptCommitted{}, fmt.Errorf("outcome %q is not a successful command receipt", receipt.Outcome)
	}
	var aggregateType string
	var aggregateID string
	var aggregateVersion *int64
	if receipt.Aggregate != nil {
		aggregateType = strings.TrimSpace(receipt.Aggregate.Type)
		aggregateID = strings.TrimSpace(receipt.Aggregate.ID)
		aggregateVersion = &receipt.Aggregate.Version
	}
	var projectionSequence *int64
	var workspaceID string
	if receipt.WorkspaceID != nil {
		workspaceID = strings.TrimSpace(*receipt.WorkspaceID)
	}
	if receipt.ProjectionSequence != nil {
		projectionSequence = receipt.ProjectionSequence
	}
	if receipt.Projection != nil {
		projectionWorkspaceID := strings.TrimSpace(receipt.Projection.WorkspaceID)
		if workspaceID != "" && projectionWorkspaceID != "" && workspaceID != projectionWorkspaceID {
			return ledger.AttemptCommitted{}, fmt.Errorf("workspace_id differs between receipt and projection")
		}
		if workspaceID == "" {
			workspaceID = projectionWorkspaceID
		}
		if projectionSequence != nil && receipt.Projection.ProjectionSequence != nil && *projectionSequence != *receipt.Projection.ProjectionSequence {
			return ledger.AttemptCommitted{}, fmt.Errorf("projection_sequence differs between receipt and projection")
		}
		if projectionSequence == nil {
			projectionSequence = receipt.Projection.ProjectionSequence
		}
	}
	if workspaceID == "" {
		return ledger.AttemptCommitted{}, fmt.Errorf("missing workspace_id")
	}
	if workspaceID != expectedWorkspaceID {
		return ledger.AttemptCommitted{}, fmt.Errorf("workspace_id %q does not match submitted workspace %q", workspaceID, expectedWorkspaceID)
	}
	return ledger.AttemptCommitted{
		AttemptID:          attemptID,
		CommandID:          receipt.CommandID,
		WorkspaceID:        workspaceID,
		AggregateType:      aggregateType,
		AggregateID:        aggregateID,
		AggregateVersion:   aggregateVersion,
		ProjectionSequence: projectionSequence,
		CommittedAt:        committedAt,
	}, nil
}

func commandFromLedgerEvent(event ledger.AttemptAppended, attemptID ledger.AttemptID) (commandEnvelope, error) {
	if event.AttemptID != attemptID {
		return commandEnvelope{}, fmt.Errorf("ledger event attempt_id mismatch")
	}
	if strings.TrimSpace(event.Server) == "" || strings.TrimSpace(event.PrincipalID) == "" {
		return commandEnvelope{}, fmt.Errorf("ledger event is missing server or principal pin")
	}
	if len(event.CommandIntent) == 0 {
		return commandEnvelope{}, fmt.Errorf("ledger event is missing command_intent")
	}
	var command commandEnvelope
	decoder := json.NewDecoder(bytes.NewReader(event.CommandIntent))
	decoder.UseNumber()
	if err := decoder.Decode(&command); err != nil {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is invalid: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent must contain one JSON object")
	}
	commandHash, err := jsonHash(command)
	if err != nil || event.CommandHash == "" || commandHash != event.CommandHash {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent command_hash mismatch")
	}
	if command.CommandID == "" {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing command_id")
	}
	if command.WorkspaceID == "" {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing workspace_id")
	}
	if command.IdempotencyKey == "" {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing idempotency_key")
	}
	if command.CommandType == "" {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing command_type")
	}
	if len(command.RequestedActor) == 0 {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing requested_actor")
	}
	if len(command.Target) == 0 {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing target")
	}
	if command.Payload == nil {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent is missing payload")
	}
	if event.WorkspaceID == "" || event.WorkspaceID != command.WorkspaceID {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent workspace_id mismatch")
	}
	if event.CommandType == "" || event.CommandType != command.CommandType {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent command_type mismatch")
	}
	if event.IdempotencyKey == "" || event.IdempotencyKey != command.IdempotencyKey {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent idempotency_key mismatch")
	}
	if command.Causation["attempt_id"] != string(attemptID) {
		return commandEnvelope{}, fmt.Errorf("command-log command_intent attempt_id mismatch")
	}
	payloadHash, err := jsonHash(command.Payload)
	if err != nil || event.PayloadHash == "" || payloadHash != event.PayloadHash {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent payload_hash mismatch")
	}
	targetHash, err := jsonHash(command.Target)
	if err != nil || event.TargetHash == "" || targetHash != event.TargetHash {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent target_hash mismatch")
	}
	causationSource, _ := command.Causation["source"].(string)
	actorContextHash, err := jsonHash(map[string]any{
		"workspace_id":               command.WorkspaceID,
		"authenticated_principal_id": event.PrincipalID,
		"requested_actor":            command.RequestedActor,
		"causation_source":           causationSource,
	})
	if err != nil || causationSource == "" || event.ActorContextHash == "" || actorContextHash != event.ActorContextHash {
		return commandEnvelope{}, fmt.Errorf("ledger command_intent actor_context_hash mismatch")
	}
	return command, nil
}
