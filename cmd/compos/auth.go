package main

import (
	"fmt"
	"net/http"
	"strings"
)

func loadSession(deps runtimeDeps) (cliSession, error) {
	if deps.identityProvider == nil {
		return cliSession{}, fmt.Errorf("external identity integration is not configured")
	}
	session, err := deps.identityProvider()
	if err != nil {
		return cliSession{}, fmt.Errorf("external identity integration unavailable: %w", err)
	}
	if strings.TrimSpace(session.Server) == "" || strings.TrimSpace(session.PrincipalID) == "" || strings.TrimSpace(session.Credential) == "" {
		return cliSession{}, fmt.Errorf("external identity integration returned incomplete credentials")
	}
	return session, nil
}

func sessionForCommand(deps runtimeDeps, server, workspace string) (cliSession, error) {
	session, err := loadSession(deps)
	if err != nil {
		return cliSession{}, err
	}
	if strings.TrimSpace(server) != "" {
		session.Server = server
	}
	if strings.TrimSpace(workspace) != "" {
		session.WorkspaceID = workspace
	}
	return session, nil
}

type cliSession struct {
	Server      string `json:"server"`
	WorkspaceID string `json:"workspace_id"`
	PrincipalID string `json:"principal_id"`
	Credential  string `json:"credential"`
}

func setBearerCredential(req *http.Request, credential string) {
	if credential = strings.TrimSpace(credential); credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
}
