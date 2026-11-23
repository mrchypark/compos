package kernel

import "strings"

func resolveActor(principal string, c Command) (Actor, error) {
	actor := Actor{Type: "user", ID: principal}
	if c.RequestedActor != nil {
		actor = *c.RequestedActor
	}
	if strings.TrimSpace(actor.Type) == "" || strings.TrimSpace(actor.ID) == "" {
		return Actor{}, reject("validation_failed", "requested actor requires a non-empty type and id")
	}
	return actor, nil
}
