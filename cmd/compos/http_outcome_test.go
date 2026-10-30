package main

import (
	"net/http"
	"testing"
)

func TestCommandHTTPFailureClassifiesRetryableAndTerminalResponses(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      []byte
		retryable bool
		terminal  bool
	}{
		{
			name:      "retryable status",
			status:    http.StatusBadGateway,
			body:      []byte(`temporary`),
			retryable: true,
			terminal:  false,
		},
		{
			name:      "retryable conflict body",
			status:    http.StatusConflict,
			body:      []byte(`{"error":{"retryable":true}}`),
			retryable: true,
			terminal:  false,
		},
		{
			name:      "terminal stale conflict",
			status:    http.StatusConflict,
			body:      []byte(`{"error":{"category":"conflict","retryable":false}}`),
			retryable: false,
			terminal:  true,
		},
		{
			name:      "command log forbidden is not terminal",
			status:    http.StatusForbidden,
			body:      []byte(`{"error":{"category":"command_log","retryable":false}}`),
			retryable: false,
			terminal:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome := commandHTTPFailure(tc.status, tc.body)
			if outcome.Retryable != tc.retryable || outcome.Terminal != tc.terminal {
				t.Fatalf("got retryable=%v terminal=%v", outcome.Retryable, outcome.Terminal)
			}
		})
	}
}
