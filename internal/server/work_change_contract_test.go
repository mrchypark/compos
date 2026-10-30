package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/compos/internal/kernel"
)

func TestCommandErrorIncludesStructuredWorkChangeDetails(t *testing.T) {
	index := 3
	w := httptest.NewRecorder()
	commandError(w, &kernel.CommandError{Outcome: "schema_mismatch", Message: "payload.values.quantity is below minimum", Details: &kernel.ChangeErrorDetails{ChangeIndex: &index, ClientRef: "item-3", BindingID: "items", Field: "quantity", Code: "schema_mismatch"}})
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response struct {
		Error struct {
			Code     string                     `json:"code"`
			Category string                     `json:"category"`
			Details  *kernel.ChangeErrorDetails `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != "schema_mismatch" || response.Error.Category != "schema" || response.Error.Details == nil || *response.Error.Details.ChangeIndex != 3 || response.Error.Details.ClientRef != "item-3" || response.Error.Details.Field != "quantity" {
		t.Fatalf("response=%s", w.Body.String())
	}
}

func TestCommandErrorWithoutDetailsKeepsExistingErrorShape(t *testing.T) {
	w := httptest.NewRecorder()
	commandError(w, &kernel.CommandError{Outcome: "forbidden", Message: "no"})
	var response map[string]map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if _, exists := response["error"]["details"]; exists || response["error"]["code"] != "forbidden" || response["error"]["category"] != "authz" {
		t.Fatalf("response=%s", w.Body.String())
	}
}
