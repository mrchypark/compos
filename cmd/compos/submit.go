package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

const maxHTTPResponseBodyBytes = 16 << 20
const maxHTTPErrorMessageBytes = 4 << 10
const idempotencyRetryHeader = "X-Compos-Idempotency-Retry"
const commandSubmitMaxAttempts = 2
const commandSubmitRetryBackoff = 50 * time.Millisecond

type mutationOptions struct {
	server         *string
	workspace      *string
	actor          *string
	idempotencyKey *string
}

func commonMutationFlags(fs *flag.FlagSet) mutationOptions {
	return mutationOptions{
		server:         fs.String("server", "", "Compos server URL"),
		workspace:      fs.String("workspace", "", "workspace id"),
		actor:          fs.String("actor", "", "requested actor id"),
		idempotencyKey: fs.String("idempotency-key", "", "idempotency key"),
	}
}

type commandEnvelope struct {
	CommandID      string           `json:"command_id"`
	WorkspaceID    string           `json:"workspace_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	RequestedActor map[string]any   `json:"requested_actor"`
	CommandType    string           `json:"command_type"`
	Target         map[string]any   `json:"target"`
	Preconditions  []map[string]any `json:"preconditions"`
	Payload        map[string]any   `json:"payload"`
	Causation      map[string]any   `json:"causation"`
}

func baseCommand(deps runtimeDeps, opts mutationOptions, commandType, aggregateType string, aggregateID *string) commandEnvelope {
	idempotencyKey := *opts.idempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = deps.newID("idem")
	}
	target := map[string]any{
		"aggregate_type": aggregateType,
		"aggregate_id":   nil,
	}
	if aggregateID != nil && *aggregateID != "" {
		target["aggregate_id"] = *aggregateID
	}
	actor := *opts.actor
	return commandEnvelope{
		CommandID:      deps.newID("cmd"),
		WorkspaceID:    *opts.workspace,
		IdempotencyKey: idempotencyKey,
		RequestedActor: map[string]any{"type": "user", "id": actor},
		CommandType:    commandType,
		Target:         target,
		Preconditions:  []map[string]any{},
		Payload:        map[string]any{},
		Causation:      map[string]any{"source": "cli", "attempt_id": nil, "event_id": nil},
	}
}

func submitCommand(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps, opts mutationOptions, command commandEnvelope) error {
	_, err := submitCommandReceipt(ctx, journal, deps, opts, command)
	return err
}

func submitCommandReceipt(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps, opts mutationOptions, command commandEnvelope) ([]byte, error) {
	session, err := sessionForCommand(deps, *opts.server, command.WorkspaceID)
	if err != nil {
		return nil, err
	}
	ledgerServer, err := canonicalLedgerServer(session.Server)
	if err != nil {
		return nil, err
	}
	principalID := strings.TrimSpace(session.PrincipalID)
	if principalID == "" {
		return nil, fmt.Errorf("session principal is required")
	}
	if command.WorkspaceID == "" {
		command.WorkspaceID = session.WorkspaceID
	}
	if actor, _ := command.RequestedActor["id"].(string); actor == "" {
		command.RequestedActor["id"] = session.PrincipalID
	}
	if err := validateUserActorMatchesSession(command, session); err != nil {
		return nil, err
	}
	attemptID := ledger.AttemptID(deps.newID("attempt"))
	command.Causation["attempt_id"] = string(attemptID)
	payloadHash, err := jsonHash(command.Payload)
	if err != nil {
		return nil, err
	}
	targetHash, err := jsonHash(command.Target)
	if err != nil {
		return nil, err
	}
	actorContextHash, err := jsonHash(map[string]any{
		"workspace_id":               command.WorkspaceID,
		"authenticated_principal_id": session.PrincipalID,
		"requested_actor":            command.RequestedActor,
		"causation_source":           command.Causation["source"],
	})
	if err != nil {
		return nil, err
	}
	commandHash, err := jsonHash(command)
	if err != nil {
		return nil, err
	}
	commandIntent, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	appended := ledger.AttemptAppended{
		AttemptID:        attemptID,
		Server:           ledgerServer,
		PrincipalID:      principalID,
		WorkspaceID:      command.WorkspaceID,
		ActorContextHash: actorContextHash,
		CommandType:      command.CommandType,
		IdempotencyKey:   command.IdempotencyKey,
		CommandHash:      commandHash,
		TargetHash:       targetHash,
		PayloadHash:      payloadHash,
		CommandIntent:    commandIntent,
		CreatedAt:        deps.now(),
	}
	attemptID, err = journal.AppendAttempt(ctx, appended)
	if err != nil {
		return nil, fmt.Errorf("append local ledger command intent before ingest sync: %w", err)
	}
	return submitCommandWithAttemptsReceipt(ctx, journal, deps, session, attemptID, command, false)
}

func submitCommandWithAttempts(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps, session cliSession, attemptID ledger.AttemptID, command commandEnvelope, retryReplay bool) error {
	_, err := submitCommandWithAttemptsReceipt(ctx, journal, deps, session, attemptID, command, retryReplay)
	return err
}

func submitCommandWithAttemptsReceipt(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps, session cliSession, attemptID ledger.AttemptID, command commandEnvelope, retryReplay bool) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= commandSubmitMaxAttempts; attempt++ {
		receipt, err, retryable := submitCommandAttemptOnce(ctx, journal, deps, session, attemptID, command, retryReplay || attempt > 1)
		if err == nil {
			return receipt, nil
		}
		lastErr = err
		if !retryable || attempt == commandSubmitMaxAttempts {
			return nil, err
		}
		if _, warnErr := fmt.Fprintf(deps.stderr, "warning: retrying command %s with attempt %s and idempotency key %s after transient submit failure: %v\n", command.CommandID, attemptID, command.IdempotencyKey, err); warnErr != nil {
			return nil, warnErr
		}
		if err := deps.sleep(ctx, commandSubmitRetryBackoff); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func submitCommandAttemptOnce(ctx context.Context, journal ledger.CommandAttemptJournal, deps runtimeDeps, session cliSession, attemptID ledger.AttemptID, command commandEnvelope, retryHeader bool) ([]byte, error, bool) {
	body, err := json.Marshal(command)
	if err != nil {
		return nil, err, false
	}
	endpointURL := trimSlash(session.Server) + "/api/v1/kernel/commands"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return nil, err, false
	}
	req.Header.Set("Content-Type", "application/json")
	setBearerCredential(req, session.Credential)
	if retryHeader {
		req.Header.Set(idempotencyRetryHeader, string(attemptID))
	}
	if err := journal.AppendSubmitted(ctx, ledger.AttemptSubmitted{
		AttemptID:   attemptID,
		CommandID:   command.CommandID,
		SubmittedAt: deps.now(),
	}); err != nil {
		return nil, err, false
	}
	resp, err := deps.client.Do(req)
	if err != nil {
		_ = journal.AppendRejected(ctx, ledger.AttemptRejected{
			AttemptID:  attemptID,
			Code:       "network_error",
			Message:    err.Error(),
			RejectedAt: deps.now(),
		})
		return nil, err, true
	}
	defer resp.Body.Close()
	respBody, readErr := readHTTPBody(resp.Body)
	if readErr != nil {
		_ = journal.AppendRejected(ctx, ledger.AttemptRejected{
			AttemptID:  attemptID,
			Code:       "read_error",
			Message:    readErr.Error(),
			RejectedAt: deps.now(),
		})
		return nil, readErr, true
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		outcome := commandHTTPFailure(resp.StatusCode, respBody)
		_ = journal.AppendRejected(ctx, ledger.AttemptRejected{
			AttemptID:  attemptID,
			Code:       strconv.Itoa(resp.StatusCode),
			Message:    outcome.Message,
			RejectedAt: deps.now(),
		})
		return nil, fmt.Errorf("server rejected command: HTTP %d: %s", resp.StatusCode, outcome.Message), outcome.Retryable
	}
	committed, err := committedEventFromReceipt(attemptID, command.CommandID, command.WorkspaceID, respBody, deps.now())
	if err != nil {
		_ = journal.AppendRejected(ctx, ledger.AttemptRejected{
			AttemptID:  attemptID,
			Code:       "receipt_invalid",
			Message:    err.Error(),
			RejectedAt: deps.now(),
		})
		return nil, fmt.Errorf("server receipt is invalid: %w", err), false
	}
	if err := journal.AppendCommitted(ctx, committed); err != nil {
		_, _ = fmt.Fprintf(deps.stderr, "warning: command %s committed but local ledger commit logging failed: %v\n", committed.CommandID, err)
	}
	_, err = fmt.Fprintln(deps.stdout, string(respBody))
	return respBody, err, false
}

func validateUserActorMatchesSession(command commandEnvelope, session cliSession) error {
	if actorType, _ := command.RequestedActor["type"].(string); actorType != "user" {
		return nil
	}
	actorID, _ := command.RequestedActor["id"].(string)
	if actorID == session.PrincipalID {
		return nil
	}
	return fmt.Errorf("requested user actor %q cannot be submitted with session principal %q", actorID, session.PrincipalID)
}

func getJSONWithAuth(ctx context.Context, deps runtimeDeps, url string, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	setBearerCredential(req, token)
	resp, err := deps.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := readHTTPBody(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server read failed: HTTP %d: %s", resp.StatusCode, truncateBodyForMessage(body, maxHTTPErrorMessageBytes))
	}
	_, err = fmt.Fprintln(deps.stdout, string(body))
	return err
}

func writeJSONLine(out io.Writer, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(body))
	return err
}
