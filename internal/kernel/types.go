package kernel

import (
	"encoding/json"
	"fmt"
)

type Actor struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type Target struct {
	AggregateType string  `json:"aggregate_type"`
	AggregateID   *string `json:"aggregate_id"`
}

type Precondition struct {
	AggregateType   string `json:"aggregate_type"`
	AggregateID     string `json:"aggregate_id"`
	ExpectedVersion int64  `json:"expected_version"`
}

type Causation struct {
	Source     string          `json:"source"`
	AttemptID  *string         `json:"attempt_id"`
	EventID    *string         `json:"event_id"`
	Automation json.RawMessage `json:"automation,omitempty"`
}

type Command struct {
	CommandID      string          `json:"command_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	WorkspaceID    string          `json:"workspace_id"`
	RequestedActor *Actor          `json:"requested_actor"`
	CommandType    string          `json:"command_type"`
	Target         Target          `json:"target"`
	Preconditions  []Precondition  `json:"preconditions"`
	Payload        json.RawMessage `json:"payload"`
	Causation      Causation       `json:"causation"`
}

type Aggregate struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

type Receipt struct {
	CommandID          string                      `json:"command_id"`
	Outcome            string                      `json:"outcome"`
	Aggregate          *Aggregate                  `json:"aggregate"`
	AggregateVersion   *int64                      `json:"aggregate_version"`
	WorkspaceID        *string                     `json:"workspace_id"`
	ProjectionSequence *int64                      `json:"projection_sequence"`
	ClientRefs         map[string]WorkChangeResult `json:"client_refs,omitempty"`
}

// WorkChangeResult is the durable server-assigned identity for one temporary
// client reference in a work.submit.changes command.
type WorkChangeResult struct {
	ID              string         `json:"id"`
	Version         int64          `json:"version"`
	GeneratedValues map[string]any `json:"generated_values,omitempty"`
}

type ChangeErrorDetails struct {
	ChangeIndex *int   `json:"change_index,omitempty"`
	ClientRef   string `json:"client_ref,omitempty"`
	BindingID   string `json:"binding_id,omitempty"`
	Field       string `json:"field,omitempty"`
	Code        string `json:"code,omitempty"`
}

type CommandError struct {
	Outcome   string
	Message   string
	Retryable bool
	Details   *ChangeErrorDetails
}

func (e *CommandError) Error() string { return fmt.Sprintf("%s: %s", e.Outcome, e.Message) }

func reject(outcome, message string) error {
	return &CommandError{Outcome: outcome, Message: message}
}
