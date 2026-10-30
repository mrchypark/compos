package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func runLedger(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: compos ledger retry")
	}
	switch args[0] {
	case "list":
		return runLedgerList(ctx, args[1:], journal, deps)
	case "retry":
		return runLedgerRetry(ctx, args[1:], journal, deps)
	default:
		return fmt.Errorf("unknown ledger command %q", args[0])
	}
}

type ledgerAttemptState struct {
	appended     ledger.AttemptAppended
	committed    ledger.AttemptCommitted
	status       ledger.AttemptStatus
	statusAt     time.Time
	rejectedCode string
}

type ledgerListItem struct {
	AttemptID   string `json:"attempt_id"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	Server      string `json:"server"`
	WorkspaceID string `json:"workspace_id"`
	CommandType string `json:"command_type"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	Retryable   bool   `json:"retryable"`
	createdAt   time.Time
}

func ledgerAttemptStates(records []ledger.EventRecord) map[ledger.AttemptID]ledgerAttemptState {
	byAttemptID := map[ledger.AttemptID]ledgerAttemptState{}
	for _, record := range records {
		current := byAttemptID[record.Event.AttemptID]
		switch record.Event.Kind {
		case ledger.EventAttemptAppended:
			if record.Event.Appended != nil {
				current.appended = *record.Event.Appended
				current.updateStatus(ledger.AttemptStatusAppended, record.Event.Appended.CreatedAt)
			}
		case ledger.EventAttemptSubmitted:
			at := time.Time{}
			if record.Event.Submitted != nil {
				at = record.Event.Submitted.SubmittedAt
			}
			current.updateStatus(ledger.AttemptStatusSubmitted, at)
		case ledger.EventAttemptRejected:
			if record.Event.Rejected != nil {
				current.rejectedCode = record.Event.Rejected.Code
				current.updateStatus(ledger.AttemptStatusRejected, record.Event.Rejected.RejectedAt)
			} else {
				current.updateStatus(ledger.AttemptStatusRejected, time.Time{})
			}
		case ledger.EventAttemptCommitted:
			if record.Event.Committed != nil {
				current.committed = *record.Event.Committed
				current.updateStatus(ledger.AttemptStatusCommitted, record.Event.Committed.CommittedAt)
			} else {
				current.updateStatus(ledger.AttemptStatusCommitted, time.Time{})
			}
		}
		byAttemptID[record.Event.AttemptID] = current
	}
	return byAttemptID
}

func (s *ledgerAttemptState) updateStatus(status ledger.AttemptStatus, at time.Time) {
	if s.status == "" ||
		at.After(s.statusAt) ||
		(at.Equal(s.statusAt) && ledgerAttemptStatusRank(status) >= ledgerAttemptStatusRank(s.status)) {
		s.status = status
		s.statusAt = at
	}
}

func ledgerAttemptStatusRank(status ledger.AttemptStatus) int {
	switch status {
	case ledger.AttemptStatusAppended:
		return 1
	case ledger.AttemptStatusSubmitted:
		return 2
	case ledger.AttemptStatusRejected:
		return 3
	case ledger.AttemptStatusCommitted:
		return 4
	default:
		return 0
	}
}

func openLedgerJournal(journal ledger.CommandAttemptJournal, journalPath string) (any, func(), error) {
	if journalPath == "" {
		return journal, nil, nil
	}
	opened, err := ledger.NewBboltJournal(journalPath)
	if err != nil {
		return nil, nil, err
	}
	return opened, func() { opened.Close() }, nil
}

func openLedgerReader(journal ledger.CommandAttemptJournal, journalPath string) (ledger.CommandAttemptReader, func(), error) {
	if journalPath == "" {
		reader, ok := journal.(ledger.CommandAttemptReader)
		if !ok {
			return nil, nil, fmt.Errorf("request ledger does not support reading")
		}
		return reader, nil, nil
	}
	opened, err := ledger.OpenBboltReader(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return ledger.NewMemoryJournal(), nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return opened, func() { _ = opened.Close() }, nil
}

func runLedgerList(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("ledger list", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	journalPath := fs.String("journal", "", "request ledger path")
	statusFilter := fs.String("status", "", "unsent, unknown, rejected, or committed")
	workspaceFilter := fs.String("workspace", "", "workspace id")
	limit := fs.Int("limit", 20, "maximum number of attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit <= 0 {
		return fmt.Errorf("ledger list --limit must be positive")
	}
	if !validLedgerDisplayStatus(*statusFilter) {
		return fmt.Errorf("ledger list --status must be unsent, unknown, rejected, or committed")
	}
	reader, closeReader, err := openLedgerReader(journal, *journalPath)
	if err != nil {
		return err
	}
	if closeReader != nil {
		defer closeReader()
	}
	records, err := reader.EventRecords(ctx, ledger.EventFilter{})
	if err != nil {
		return err
	}
	attempts := make([]ledgerListItem, 0)
	for attemptID, current := range ledgerAttemptStates(records) {
		if current.appended.AttemptID == "" {
			continue
		}
		item := ledgerListItem{
			AttemptID:   string(attemptID),
			CreatedAt:   current.appended.CreatedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:   current.statusAt.UTC().Format(time.RFC3339Nano),
			Server:      current.appended.Server,
			WorkspaceID: current.appended.WorkspaceID,
			CommandType: current.appended.CommandType,
			createdAt:   current.appended.CreatedAt,
		}
		item.Status, item.Reason, item.Retryable = ledgerDisplay(current)
		if item.Retryable && (strings.TrimSpace(current.appended.Server) == "" || strings.TrimSpace(current.appended.PrincipalID) == "") {
			item.Retryable = false
			item.Reason = "missing_scope"
		}
		if strings.TrimSpace(*statusFilter) != "" && item.Status != strings.TrimSpace(*statusFilter) {
			continue
		}
		if strings.TrimSpace(*workspaceFilter) != "" && item.WorkspaceID != strings.TrimSpace(*workspaceFilter) {
			continue
		}
		attempts = append(attempts, item)
	}
	sort.Slice(attempts, func(i, j int) bool {
		if attempts[i].createdAt.Equal(attempts[j].createdAt) {
			return attempts[i].AttemptID < attempts[j].AttemptID
		}
		return attempts[i].createdAt.After(attempts[j].createdAt)
	})
	if len(attempts) > *limit {
		attempts = attempts[:*limit]
	}
	return writeJSONLine(deps.stdout, map[string]any{"attempts": attempts})
}

func validLedgerDisplayStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "", "unsent", "unknown", "rejected", "committed":
		return true
	default:
		return false
	}
}

func ledgerDisplay(current ledgerAttemptState) (status, reason string, retryable bool) {
	if current.committed.AttemptID != "" {
		return "committed", "", false
	}
	switch current.status {
	case ledger.AttemptStatusAppended:
		return "unsent", "", true
	case ledger.AttemptStatusSubmitted:
		return "unknown", "delivery_unknown", true
	case ledger.AttemptStatusRejected:
		if ambiguousRejectedCode(current.rejectedCode) || current.rejectedCode == "receipt_invalid" {
			return "unknown", safeLedgerFailureCode(current.rejectedCode), true
		}
		return "rejected", safeLedgerFailureCode(current.rejectedCode), false
	case ledger.AttemptStatusCommitted:
		return "committed", "", false
	default:
		return "unknown", "ledger_incomplete", false
	}
}

func safeLedgerFailureCode(code string) string {
	if code == "network_error" || code == "read_error" || code == "receipt_invalid" {
		return code
	}
	if status, err := strconv.Atoi(code); err == nil && status >= 100 && status <= 599 {
		return code
	}
	return "server_rejected"
}

func runLedgerRetry(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("ledger retry", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	journalPath := fs.String("journal", "", "request ledger path")
	attemptIDFilter := fs.String("attempt", "", "attempt id to retry")
	limit := fs.Int("limit", 1, "maximum ambiguous attempts to retry")
	server := fs.String("server", "", "Compos server URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*attemptIDFilter) == "" && *limit <= 0 {
		return fmt.Errorf("ledger retry requires --attempt when --limit is not positive")
	}
	source, closeJournal, err := openLedgerJournal(journal, *journalPath)
	if err != nil {
		return err
	}
	if closeJournal != nil {
		defer closeJournal()
	}
	reader, ok := source.(ledger.CommandAttemptReader)
	if !ok {
		return fmt.Errorf("request ledger does not support retry")
	}
	retryJournal, ok := source.(ledger.CommandAttemptJournal)
	if !ok {
		return fmt.Errorf("request ledger does not support retry writes")
	}
	records, err := reader.EventRecords(ctx, ledger.EventFilter{})
	if err != nil {
		return err
	}
	byAttemptID := ledgerAttemptStates(records)
	attemptIDs := make([]string, 0, len(byAttemptID))
	for attemptID, current := range byAttemptID {
		if current.appended.AttemptID == "" || current.committed.AttemptID != "" {
			continue
		}
		if strings.TrimSpace(*attemptIDFilter) != "" && string(attemptID) != strings.TrimSpace(*attemptIDFilter) {
			continue
		}
		_, _, retryable := ledgerDisplay(current)
		if !retryable {
			continue
		}
		attemptIDs = append(attemptIDs, string(attemptID))
	}
	sort.Slice(attemptIDs, func(i, j int) bool {
		left := byAttemptID[ledger.AttemptID(attemptIDs[i])]
		right := byAttemptID[ledger.AttemptID(attemptIDs[j])]
		if left.appended.CreatedAt.Equal(right.appended.CreatedAt) {
			return attemptIDs[i] < attemptIDs[j]
		}
		return left.appended.CreatedAt.Before(right.appended.CreatedAt)
	})
	if strings.TrimSpace(*attemptIDFilter) != "" && len(attemptIDs) == 0 {
		return fmt.Errorf("ledger retry %s is not retryable", strings.TrimSpace(*attemptIDFilter))
	}
	retried := 0
	for _, id := range attemptIDs {
		if *limit > 0 && retried >= *limit {
			break
		}
		attemptID := ledger.AttemptID(id)
		current := byAttemptID[attemptID]
		command, err := commandFromLedgerEvent(current.appended, attemptID)
		if err != nil {
			return err
		}
		session, err := pinnedLedgerSession(deps, *server, current.appended, command)
		if err != nil {
			return err
		}
		if err := submitCommandWithAttempts(ctx, retryJournal, deps, session, attemptID, command, true); err != nil {
			return err
		}
		retried++
	}
	return writeJSONLine(deps.stdout, map[string]any{"retried": retried})
}

func pinnedLedgerSession(deps runtimeDeps, serverOverride string, appended ledger.AttemptAppended, command commandEnvelope) (cliSession, error) {
	if strings.TrimSpace(appended.Server) == "" || strings.TrimSpace(appended.PrincipalID) == "" {
		return cliSession{}, fmt.Errorf("ledger attempt %s has no server or principal pin", appended.AttemptID)
	}
	pinnedServer, err := canonicalLedgerServer(appended.Server)
	if err != nil {
		return cliSession{}, fmt.Errorf("ledger attempt %s has invalid server pin: %w", appended.AttemptID, err)
	}
	if strings.TrimSpace(serverOverride) != "" {
		override, err := canonicalLedgerServer(serverOverride)
		if err != nil {
			return cliSession{}, err
		}
		if override != pinnedServer {
			return cliSession{}, fmt.Errorf("ledger attempt %s is pinned to a different server", appended.AttemptID)
		}
	}
	session, err := loadSession(deps)
	if err != nil {
		return cliSession{}, err
	}
	sessionServer, err := canonicalLedgerServer(session.Server)
	if err != nil {
		return cliSession{}, fmt.Errorf("session server is invalid: %w", err)
	}
	if sessionServer != pinnedServer || session.PrincipalID != appended.PrincipalID || session.WorkspaceID != appended.WorkspaceID || command.WorkspaceID != appended.WorkspaceID {
		return cliSession{}, fmt.Errorf("ledger attempt %s does not match the active session", appended.AttemptID)
	}
	if err := validateUserActorMatchesSession(command, session); err != nil {
		return cliSession{}, err
	}
	session.Server = pinnedServer
	return session, nil
}

func ambiguousRejectedCode(code string) bool {
	return code == "network_error" || code == "read_error" || retryableHTTPStatusCode(code)
}
