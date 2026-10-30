package kernel

import (
	"errors"
	"strings"
)

// workChangeError adds UI-addressable context without changing the command
// outcome. Unknown storage/runtime errors become a safe retryable response:
// raw SQL and adapter failures are not public API text or false input errors.
func workChangeError(err error, index int, change workChange, field string) error {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		commandErr = &CommandError{Outcome: "quorum_unavailable", Message: "work change could not be processed; retry", Retryable: true}
	}
	copyErr := *commandErr
	details := ChangeErrorDetails{}
	if commandErr.Details != nil {
		details = *commandErr.Details
	}
	details.ChangeIndex = &index
	if details.ClientRef == "" {
		details.ClientRef = change.ClientRef
	}
	if details.BindingID == "" {
		details.BindingID = change.BindingID
	}
	if field != "" {
		details.Field = field
	} else if details.Field == "" {
		details.Field = workChangeErrorField(copyErr.Message)
	}
	if details.Code == "" {
		details.Code = copyErr.Outcome
	}
	copyErr.Details = &details
	return &copyErr
}

func workChangeErrorField(message string) string {
	const values = "payload.values."
	if rest, ok := strings.CutPrefix(message, values); ok {
		if field, _, _ := strings.Cut(rest, " "); field != "" {
			return field
		}
	}
	if strings.HasPrefix(message, "payload.title") {
		return "title"
	}
	return ""
}
