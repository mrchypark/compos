package ledger

import (
	"context"
	"encoding/json"
	"time"
)

type AttemptID string

type CommandAttemptJournal interface {
	AppendAttempt(ctx context.Context, event AttemptAppended) (AttemptID, error)
	AppendSubmitted(ctx context.Context, event AttemptSubmitted) error
	AppendRejected(ctx context.Context, event AttemptRejected) error
	AppendCommitted(ctx context.Context, event AttemptCommitted) error
}

type CommandAttemptReader interface {
	EventRecords(ctx context.Context, filter EventFilter) ([]EventRecord, error)
}

type EventFilter struct {
	AttemptID AttemptID
	Limit     int
}

type EventRecord struct {
	Key   string `json:"key"`
	Event Event  `json:"event"`
}

type AttemptAppended struct {
	AttemptID        AttemptID
	Server           string
	PrincipalID      string
	WorkspaceID      string
	ActorContextHash string
	CommandType      string
	IdempotencyKey   string
	CommandHash      string
	TargetHash       string
	PayloadHash      string
	CommandIntent    json.RawMessage
	CreatedAt        time.Time
}

type AttemptSubmitted struct {
	AttemptID   AttemptID
	CommandID   string
	SubmittedAt time.Time
}

type AttemptRejected struct {
	AttemptID  AttemptID
	Code       string
	Message    string
	RejectedAt time.Time
}

type AttemptCommitted struct {
	AttemptID          AttemptID `json:"attempt_id"`
	CommandID          string    `json:"command_id"`
	WorkspaceID        string    `json:"workspace_id"`
	CommandStream      string    `json:"command_stream"`
	CommandPosition    string    `json:"command_position"`
	AggregateType      string    `json:"aggregate_type,omitempty"`
	AggregateID        string    `json:"aggregate_id,omitempty"`
	AggregateVersion   *int64    `json:"aggregate_version,omitempty"`
	ProjectionSequence *int64    `json:"projection_sequence,omitempty"`
	CommittedAt        time.Time `json:"committed_at"`
}
