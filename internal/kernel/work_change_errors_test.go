package kernel

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestWorkChangeErrorAddsAddressableDetails(t *testing.T) {
	change := workChange{BindingID: "items", ClientRef: "item-draft-1"}
	err := workChangeError(reject("schema_mismatch", "payload.values.quantity is below minimum"), 2, change, "")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("error=%T", err)
	}
	if commandErr.Outcome != "schema_mismatch" || commandErr.Details == nil || *commandErr.Details.ChangeIndex != 2 || commandErr.Details.ClientRef != "item-draft-1" || commandErr.Details.BindingID != "items" || commandErr.Details.Field != "quantity" || commandErr.Details.Code != "schema_mismatch" {
		t.Fatalf("error=%+v details=%+v", commandErr, commandErr.Details)
	}
}

func TestWorkChangeErrorDoesNotExposeUnknownFailure(t *testing.T) {
	err := workChangeError(errors.New("UNIQUE constraint failed: private_table.secret"), 0, workChange{BindingID: "quote", ClientRef: "draft-quote"}, "amount")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("error=%T", err)
	}
	if commandErr.Outcome != "quorum_unavailable" || !commandErr.Retryable || commandErr.Message != "work change could not be processed; retry" || strings.Contains(commandErr.Message, "private_table") {
		t.Fatalf("public error=%+v", commandErr)
	}
	if commandErr.Details == nil || commandErr.Details.Field != "amount" || commandErr.Details.Code != "quorum_unavailable" {
		t.Fatalf("details=%+v", commandErr.Details)
	}
}

func TestReceiptClientRefsRoundTrip(t *testing.T) {
	receipt := Receipt{CommandID: "cmd", ClientRefs: map[string]WorkChangeResult{"item-1": {ID: "obj_1", Version: 1, GeneratedValues: map[string]any{"number": "Q-1"}}}}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Receipt
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.ClientRefs["item-1"]; got.ID != "obj_1" || got.Version != 1 || got.GeneratedValues["number"] != "Q-1" {
		t.Fatalf("client refs=%+v", decoded.ClientRefs)
	}
}
