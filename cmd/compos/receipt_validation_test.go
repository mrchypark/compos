package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func TestCommittedEventFromReceiptRejectsInvalidSuccessfulHTTPResponses(t *testing.T) {
	for name, body := range map[string]string{
		"malformed":              `not-json`,
		"missing_command_id":     `{"outcome":"applied"}`,
		"missing_workspace":      `{"command_id":"cmd_1","outcome":"applied"}`,
		"null_workspace":         `{"command_id":"cmd_1","outcome":"applied","workspace_id":null,"projection":{"workspace_id":null}}`,
		"empty_workspace":        `{"command_id":"cmd_1","outcome":"applied","workspace_id":""}`,
		"wrong_command_id":       `{"command_id":"cmd_other","outcome":"applied"}`,
		"non_exact_command_id":   `{"command_id":" cmd_1 ","outcome":"applied"}`,
		"wrong_workspace":        `{"command_id":"cmd_1","outcome":"applied","projection":{"workspace_id":"wks_other"}}`,
		"inconsistent_workspace": `{"command_id":"cmd_1","outcome":"applied","workspace_id":"wks_1","projection":{"workspace_id":"wks_other"}}`,
		"unsupported_outcome":    `{"command_id":"cmd_1","outcome":"accepted"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := committedEventFromReceipt("attempt_1", "cmd_1", "wks_1", []byte(body), time.Unix(10, 0).UTC())
			if err == nil {
				t.Fatalf("expected invalid receipt error")
			}
		})
	}
}

func TestCommittedEventFromReceiptAcceptsKernelOutcomes(t *testing.T) {
	for _, outcome := range []string{"applied", "duplicate"} {
		t.Run(outcome, func(t *testing.T) {
			committed, err := committedEventFromReceipt("attempt_1", "cmd_1", "wks_1", []byte(`{"command_id":"cmd_1","outcome":"`+outcome+`","projection":{"workspace_id":"wks_1"}}`), time.Unix(10, 0).UTC())
			if err != nil || committed.CommandID != "cmd_1" {
				t.Fatalf("receipt=%#v err=%v", committed, err)
			}
		})
	}
}

func TestCommittedEventFromReceiptAcceptsCanonicalKernelReceipt(t *testing.T) {
	committed, err := committedEventFromReceipt("attempt_1", "cmd_1", "wks_1", []byte(`{"command_id":"cmd_1","outcome":"applied","workspace_id":"wks_1","projection_sequence":7}`), time.Unix(10, 0).UTC())
	if err != nil || committed.WorkspaceID != "wks_1" || committed.ProjectionSequence == nil || *committed.ProjectionSequence != 7 {
		t.Fatalf("receipt=%#v err=%v", committed, err)
	}
}

func TestReceiptInvalidErrorDoesNotClaimCommit(t *testing.T) {
	_, err := committedEventFromReceipt("attempt_1", "cmd_1", "wks_1", []byte(`{"command_id":"cmd_1","outcome":"failed"}`), time.Unix(10, 0).UTC())
	if err == nil || !strings.Contains(err.Error(), "not a successful") {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestCommandFromLedgerEventRejectsModifiedIntent(t *testing.T) {
	valid := `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`
	base := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "https://api.example.test", PrincipalID: "usr_1", WorkspaceID: "wks_1", CommandType: "object.create", CommandIntent: json.RawMessage(valid)}
	fillLedgerIntentHashes(t, &base)
	if _, err := commandFromLedgerEvent(base, "attempt_1"); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	for name, intent := range map[string]string{
		"payload":       `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Changed"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"target":        `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"other"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"key":           `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_other","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"actor":         `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_other"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"preconditions": `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[{"aggregate_type":"object_instance","aggregate_id":"obj_1","expected_version":1}],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"command_id":    `{"command_id":"cmd_other","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"cli","attempt_id":"attempt_1"}}`,
		"causation":     `{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"title":"Acme"},"causation":{"source":"other","attempt_id":"attempt_1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			event := base
			event.CommandIntent = json.RawMessage(intent)
			if _, err := commandFromLedgerEvent(event, "attempt_1"); err == nil {
				t.Fatalf("modified %s intent was accepted", name)
			}
		})
	}
}

func TestCommandFromLedgerEventPreservesLargeJSONInteger(t *testing.T) {
	intent := json.RawMessage(`{"command_id":"cmd_1","workspace_id":"wks_1","idempotency_key":"idem_1","requested_actor":{"type":"user","id":"usr_1"},"command_type":"object.create","target":{"aggregate_type":"object_instance"},"preconditions":[],"payload":{"amount":9007199254740993},"causation":{"source":"cli","attempt_id":"attempt_1"}}`)
	var command commandEnvelope
	decoder := json.NewDecoder(bytes.NewReader(intent))
	decoder.UseNumber()
	if err := decoder.Decode(&command); err != nil {
		t.Fatal(err)
	}
	payloadHash, err := jsonHash(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	targetHash, err := jsonHash(command.Target)
	if err != nil {
		t.Fatal(err)
	}
	actorHash, err := jsonHash(map[string]any{"workspace_id": "wks_1", "authenticated_principal_id": "usr_1", "requested_actor": command.RequestedActor, "causation_source": "cli"})
	if err != nil {
		t.Fatal(err)
	}
	commandHash, err := jsonHash(command)
	if err != nil {
		t.Fatal(err)
	}
	event := ledger.AttemptAppended{AttemptID: "attempt_1", Server: "https://api.example.test", PrincipalID: "usr_1", WorkspaceID: "wks_1", CommandType: "object.create", IdempotencyKey: "idem_1", CommandHash: commandHash, TargetHash: targetHash, PayloadHash: payloadHash, ActorContextHash: actorHash, CommandIntent: intent}
	decoded, err := commandFromLedgerEvent(event, "attempt_1")
	if err != nil {
		t.Fatal(err)
	}
	if amount, ok := decoded.Payload["amount"].(json.Number); !ok || amount.String() != "9007199254740993" {
		t.Fatalf("amount=%#v", decoded.Payload["amount"])
	}
}
