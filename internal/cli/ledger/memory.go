package ledger

import (
	"context"
	"fmt"
	"sync"
)

type EventKind string

const (
	EventAttemptAppended  EventKind = "attempt_appended"
	EventAttemptSubmitted EventKind = "attempt_submitted"
	EventAttemptRejected  EventKind = "attempt_rejected"
	EventAttemptCommitted EventKind = "attempt_committed"
)

type Event struct {
	Kind      EventKind
	AttemptID AttemptID
	Appended  *AttemptAppended
	Submitted *AttemptSubmitted
	Rejected  *AttemptRejected
	Committed *AttemptCommitted
}

type AttemptStatus string

const (
	AttemptStatusAppended  AttemptStatus = "appended"
	AttemptStatusSubmitted AttemptStatus = "submitted"
	AttemptStatusRejected  AttemptStatus = "rejected"
	AttemptStatusCommitted AttemptStatus = "committed"
)

type MemoryJournal struct {
	mu     sync.Mutex
	events []Event
}

func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{} }

func (j *MemoryJournal) AppendAttempt(_ context.Context, event AttemptAppended) (AttemptID, error) {
	if event.AttemptID == "" {
		return "", fmt.Errorf("attempt id is required")
	}
	return event.AttemptID, j.append(Event{Kind: EventAttemptAppended, AttemptID: event.AttemptID, Appended: &event})
}

func (j *MemoryJournal) AppendSubmitted(_ context.Context, event AttemptSubmitted) error {
	return j.append(Event{Kind: EventAttemptSubmitted, AttemptID: event.AttemptID, Submitted: &event})
}

func (j *MemoryJournal) AppendRejected(_ context.Context, event AttemptRejected) error {
	return j.append(Event{Kind: EventAttemptRejected, AttemptID: event.AttemptID, Rejected: &event})
}

func (j *MemoryJournal) AppendCommitted(_ context.Context, event AttemptCommitted) error {
	return j.append(Event{Kind: EventAttemptCommitted, AttemptID: event.AttemptID, Committed: &event})
}

func (j *MemoryJournal) Events() []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	events := make([]Event, len(j.events))
	copy(events, j.events)
	return events
}

func (j *MemoryJournal) EventRecords(_ context.Context, filter EventFilter) ([]EventRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	records := make([]EventRecord, 0, len(j.events))
	for i, event := range j.events {
		if filter.AttemptID != "" && event.AttemptID != filter.AttemptID {
			continue
		}
		records = append(records, EventRecord{Key: fmt.Sprintf("memory/%020d/%s/%s", i, event.AttemptID, event.Kind), Event: event})
		if filter.Limit > 0 && len(records) >= filter.Limit {
			break
		}
	}
	return records, nil
}

func (j *MemoryJournal) Status(attemptID AttemptID) (AttemptStatus, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var status AttemptStatus
	found := false
	for _, event := range j.events {
		if event.AttemptID != attemptID {
			continue
		}
		found = true
		switch event.Kind {
		case EventAttemptAppended:
			status = AttemptStatusAppended
		case EventAttemptSubmitted:
			status = AttemptStatusSubmitted
		case EventAttemptRejected:
			status = AttemptStatusRejected
		case EventAttemptCommitted:
			status = AttemptStatusCommitted
		}
	}
	return status, found
}

func (j *MemoryJournal) append(event Event) error {
	if event.AttemptID == "" {
		return fmt.Errorf("attempt id is required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, event)
	return nil
}
