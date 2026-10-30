package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
)

func runWorkspace(ctx context.Context, args []string, deps runtimeDeps) error {
	if len(args) == 0 {
		return errors.New(workspaceUsage())
	}
	switch args[0] {
	case "create":
		return runWorkspaceCreate(ctx, args[1:], deps)
	case "help":
		_, err := fmt.Fprintln(deps.stdout, workspaceUsage())
		return err
	default:
		return fmt.Errorf("unknown workspace command %q", args[0])
	}
}

func workspaceUsage() string {
	return "usage: compos workspace <create>\n\ncommands:\n  workspace create [--server URL] --name NAME [--key KEY] [--idempotency-key KEY]\n\nIf --idempotency-key is omitted, workspace create derives a stable key from --name and --key."
}

func runWorkspaceCreate(ctx context.Context, args []string, deps runtimeDeps) error {
	fs := flag.NewFlagSet("workspace create", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	server := fs.String("server", "", "Compos server URL")
	name := fs.String("name", "", "workspace name")
	key := fs.String("key", "", "workspace key")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	nameValue, err := requiredFlagValue(name, "name")
	if err != nil {
		return err
	}
	keyValue := canonicalWorkspaceKey(*key)
	idempotencyKeyValue := strings.TrimSpace(*idempotencyKey)
	if idempotencyKeyValue == "" {
		idempotencyKeyValue = stableWorkspaceCreateIdempotencyKey(nameValue, keyValue)
	}
	session, err := sessionForCommand(deps, *server, "")
	if err != nil {
		return err
	}
	payload := workspaceCreateRequest{
		Name:           nameValue,
		Key:            keyValue,
		IdempotencyKey: idempotencyKeyValue,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, trimSlash(session.Server)+"/api/v1/workspaces", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	setBearerCredential(req, session.Credential)
	resp, err := deps.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readHTTPBody(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workspace create failed: HTTP %d: %s", resp.StatusCode, truncateBodyForMessage(respBody, maxHTTPErrorMessageBytes))
	}
	_, err = fmt.Fprintln(deps.stdout, string(respBody))
	return err
}

type workspaceCreateRequest struct {
	Name           string `json:"name"`
	Key            string `json:"key,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
}

type workspaceCreateIntent struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

func stableWorkspaceCreateIdempotencyKey(name, key string) string {
	body, err := json.Marshal(workspaceCreateIntent{Name: name, Key: key})
	if err != nil {
		panic(fmt.Sprintf("workspace create intent should marshal: %v", err))
	}
	sum := sha256.Sum256(body)
	return "idem_" + hex.EncodeToString(sum[:])
}

func canonicalWorkspaceKey(key string) string {
	key = strings.TrimSpace(key)
	for i := 0; i < len(key); i++ {
		if key[i] < 'A' || key[i] > 'Z' {
			continue
		}
		out := []byte(key)
		for ; i < len(out); i++ {
			if out[i] >= 'A' && out[i] <= 'Z' {
				out[i] += 'a' - 'A'
			}
		}
		return string(out)
	}
	return key
}
