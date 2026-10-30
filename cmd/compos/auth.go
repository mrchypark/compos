package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type cliSession struct {
	Server      string `json:"server"`
	WorkspaceID string `json:"workspace_id"`
	PrincipalID string `json:"principal_id"`
	Credential  string `json:"credential"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
}

type sessionResponse struct {
	Data struct {
		PrincipalID string `json:"principal_id"`
		WorkspaceID string `json:"workspace_id"`
		Session     struct {
			Token     string `json:"token"`
			ExpiresAt string `json:"expires_at"`
		} `json:"session"`
	} `json:"data"`
}

func runSetup(deps runtimeDeps) error {
	_, err := fmt.Fprintf(deps.stdout, "session: %s\nrequest ledger: %s\n", sessionPath(deps), defaultLedgerPath())
	return err
}

func runLogin(args []string, deps runtimeDeps) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	server := fs.String("server", "http://127.0.0.1:7878", "Compos server URL")
	workspace := fs.String("workspace", "", "workspace id")
	provider := fs.String("provider", "", "authentication provider: rauthy or pocketbase")
	accessToken := fs.String("access-token", "", "provider access token for one-shot session exchange")
	if err := fs.Parse(args); err != nil {
		return err
	}
	selectedProvider := strings.ToLower(strings.TrimSpace(*provider))
	token := strings.TrimSpace(*accessToken)
	if token == "" {
		return fmt.Errorf("--access-token is required")
	}
	if selectedProvider != "rauthy" && selectedProvider != "pocketbase" {
		return fmt.Errorf("--provider must be rauthy or pocketbase")
	}
	login, err := exchangeProviderSession(context.Background(), deps, trimSlash(*server), *workspace, selectedProvider, token)
	if err != nil {
		return err
	}
	selectedWorkspace := strings.TrimSpace(*workspace)
	if selectedWorkspace == "" {
		selectedWorkspace = login.Data.WorkspaceID
	}
	if selectedWorkspace == "" {
		return fmt.Errorf("workspace could not be selected automatically; rerun compos login --workspace W")
	}
	session := cliSession{
		Server: trimSlash(*server), WorkspaceID: selectedWorkspace, PrincipalID: login.Data.PrincipalID,
		Credential: login.Data.Session.Token, CreatedAt: deps.now().Format(time.RFC3339), ExpiresAt: login.Data.Session.ExpiresAt,
	}
	if err := saveSession(deps, session); err != nil {
		return err
	}
	_, err = fmt.Fprintf(deps.stdout, "logged in as %s for workspace %s\n", session.PrincipalID, session.WorkspaceID)
	return err
}

func exchangeProviderSession(ctx context.Context, deps runtimeDeps, server, workspaceID, provider, token string) (*sessionResponse, error) {
	tokenField := "access_token"
	if provider == "pocketbase" {
		tokenField = "token"
	}
	payload := map[string]any{tokenField: token}
	if strings.TrimSpace(workspaceID) != "" {
		payload["workspace_id"] = workspaceID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, trimSlash(server)+"/api/v1/auth/"+provider, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := deps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := readHTTPBody(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s login failed: HTTP %d: %s", provider, resp.StatusCode, truncateBodyForMessage(respBody, maxHTTPErrorMessageBytes))
	}
	var result sessionResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}
	if result.Data.PrincipalID == "" || result.Data.Session.Token == "" {
		return nil, fmt.Errorf("%s login response is incomplete", provider)
	}
	return &result, nil
}

func runLogout(deps runtimeDeps) error {
	path := sessionPath(deps)
	if session, err := loadSession(deps); err == nil && session.Credential != "" {
		if err := revokeRemoteSession(context.Background(), deps, session); err != nil {
			return fmt.Errorf("remote logout failed; local session was retained for retry: %w", err)
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_, err := fmt.Fprintln(deps.stdout, "logged out")
	return err
}

func runWhoami(deps runtimeDeps) error {
	session, err := loadSession(deps)
	if err != nil {
		return err
	}
	return writeJSONLine(deps.stdout, map[string]any{"server": session.Server, "workspace_id": session.WorkspaceID, "principal_id": session.PrincipalID, "created_at": session.CreatedAt})
}

func revokeRemoteSession(ctx context.Context, deps runtimeDeps, session cliSession) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, trimSlash(session.Server)+"/api/v1/auth/logout", nil)
	if err != nil {
		return err
	}
	setBearerCredential(req, session.Credential)
	resp, err := deps.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("remote logout failed: HTTP %d", resp.StatusCode)
	}
	return nil
}

func setBearerCredential(req *http.Request, credential string) {
	if credential = strings.TrimSpace(credential); credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
}
