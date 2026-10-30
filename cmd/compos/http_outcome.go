package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type commandHTTPFailureOutcome struct {
	Message   string
	Retryable bool
	Terminal  bool
}

func commandHTTPFailure(status int, body []byte) commandHTTPFailureOutcome {
	message := truncateBodyForMessage(body, maxHTTPErrorMessageBytes)
	retryable := retryableHTTPStatus(status)
	var parsed struct {
		Error struct {
			Category  string `json:"category"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &parsed)
	if parsed.Error.Retryable {
		retryable = true
	}
	return commandHTTPFailureOutcome{
		Message:   message,
		Retryable: retryable,
		Terminal:  !retryable && terminalCommandHTTPStatus(status, parsed.Error.Category, body),
	}
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		(status >= 500 && status <= 599)
}

func retryableHTTPStatusCode(code string) bool {
	status, err := strconv.Atoi(strings.TrimSpace(code))
	return err == nil && retryableHTTPStatus(status)
}

func terminalCommandHTTPStatus(status int, category string, body []byte) bool {
	switch status {
	case http.StatusBadRequest, http.StatusConflict, http.StatusUnprocessableEntity:
		return true
	case http.StatusForbidden:
		return category != "command_log" && !bytes.Contains(body, []byte(`"category":"command_log"`))
	default:
		return false
	}
}
