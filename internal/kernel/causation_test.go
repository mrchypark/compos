package kernel

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCausationUsesAttemptIdentity(t *testing.T) {
	var causation Causation
	if err := json.Unmarshal([]byte(`{"source":"cli","attempt_id":"attempt_1"}`), &causation); err != nil {
		t.Fatal(err)
	}
	if causation.AttemptID == nil || *causation.AttemptID != "attempt_1" {
		t.Fatalf("attempt identity lost: %+v", causation)
	}
	encoded, err := json.Marshal(causation)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"attempt_id":"attempt_1"`) || strings.Contains(string(encoded), "audit_id") {
		t.Fatalf("unexpected causation contract: %s", encoded)
	}
	var legacy Causation
	if err := json.Unmarshal([]byte(`{"source":"cli","audit_id":"audit_1"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.AttemptID != nil {
		t.Fatal("legacy audit_id must not alias attempt_id")
	}
}
