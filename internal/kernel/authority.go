package kernel

import (
	"context"
	"errors"
	"strings"

	"github.com/mrchypark/compos/internal/store"
)

// CommandAuthorizer is supplied by the trusted external integration, never by HTTP headers.
// It must approve the complete command, including workspace, actor, payload and causation.
type CommandAuthorizer func(context.Context, string, Command) (Actor, error)
type commandAuthorizerKey struct{}

// ErrAuthorizationDenied means an explicit policy denial. Other external errors
// (including cancellation and missing configuration) remain retryable.
var ErrAuthorizationDenied = errors.New("external authorization denied")

func authorityUnavailable() error {
	return &CommandError{Outcome: "external_authority_unavailable", Message: "external authorization is unavailable; retry", Retryable: true}
}

func authorize(ctx context.Context, _ store.Tx, principal string, c Command) (Actor, error) {
	check, _ := ctx.Value(commandAuthorizerKey{}).(CommandAuthorizer)
	if check == nil {
		return Actor{}, authorityUnavailable()
	}
	actor, err := check(ctx, principal, c)
	if err != nil {
		if errors.Is(err, ErrAuthorizationDenied) {
			return Actor{}, reject("forbidden", "external command authorization denied")
		}
		return Actor{}, authorityUnavailable()
	}
	if strings.TrimSpace(principal) == "" || strings.TrimSpace(actor.ID) == "" || (actor.Type != "user" && actor.Type != "service" && actor.Type != "automation") {
		return Actor{}, reject("forbidden", "external authorization returned an invalid identity")
	}
	if actor.Type == "user" && actor.ID != principal {
		return Actor{}, reject("forbidden", "external user actor does not match principal")
	}
	requested := Actor{Type: "user", ID: principal}
	if c.RequestedActor != nil {
		requested = *c.RequestedActor
	}
	if requested != actor {
		return Actor{}, reject("forbidden", "requested actor does not match external authorization")
	}
	return actor, nil
}
