package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func runObjectTypeWordDefine(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	return runObjectTypeDefine(ctx, args, journal, deps)
}

func runObjectTypeWord(ctx context.Context, pluralKey string, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: compos <plural-object-type-key> <create|move|get|link|help>")
	}
	if args[0] == "help" || hasHelpFlag(args) {
		return runObjectTypeWordHelp(ctx, pluralKey, args[1:], deps)
	}
	noun, err := resolveObjectTypeWord(ctx, deps, pluralKey, args)
	if err != nil {
		return err
	}
	if !noun.supports(args[0]) {
		return fmt.Errorf("object type word %q does not support command %q", pluralKey, args[0])
	}
	switch args[0] {
	case "create":
		return runObjectCreate(ctx, args[1:], journal, deps, noun.Key)
	case "move":
		return runObjectMove(ctx, args[1:], journal, deps)
	case "get":
		return runObjectGet(ctx, args[1:], deps)
	case "link":
		return runRelationLink(ctx, args[1:], journal, deps)
	default:
		return fmt.Errorf("unknown object type word command %q for %q", args[0], pluralKey)
	}
}

func runObjectTypeWordHelp(ctx context.Context, pluralKey string, args []string, deps runtimeDeps) error {
	noun, err := resolveObjectTypeWord(ctx, deps, pluralKey, args)
	if err != nil {
		return err
	}
	return printObjectTypeWordHelp(deps.stdout, noun)
}

type commandSurface struct {
	WorkspaceID string        `json:"workspace_id"`
	Nouns       []surfaceNoun `json:"nouns"`
}

type surfaceNoun struct {
	Key            string            `json:"key"`
	Singular       string            `json:"singular"`
	Plural         string            `json:"plural"`
	Kind           string            `json:"kind"`
	ObjectTypeID   string            `json:"object_type_id"`
	Version        int64             `json:"version"`
	Commands       []string          `json:"commands"`
	FieldSchema    map[string]any    `json:"field_schema"`
	StateMachineID *string           `json:"state_machine_id"`
	Relations      []surfaceRelation `json:"relations"`
}

type surfaceRelation struct {
	ID               string `json:"id"`
	Key              string `json:"key"`
	Version          int64  `json:"version"`
	FromObjectTypeID string `json:"from_object_type_id"`
	ToObjectTypeID   string `json:"to_object_type_id"`
	Cardinality      string `json:"cardinality"`
}

func (noun surfaceNoun) supports(command string) bool {
	for _, supported := range noun.Commands {
		if supported == command {
			return true
		}
	}
	return false
}

func (noun surfaceNoun) validate() error {
	if strings.TrimSpace(noun.Key) == "" {
		return fmt.Errorf("command surface noun key is required")
	}
	if strings.TrimSpace(noun.Singular) == "" {
		return fmt.Errorf("command surface noun %q singular is required", noun.Key)
	}
	if strings.TrimSpace(noun.Plural) == "" {
		return fmt.Errorf("command surface noun %q plural is required", noun.Key)
	}
	return nil
}

type surfaceOptions struct {
	server    string
	workspace string
}

func surfaceOptionsFromArgs(args []string) (surfaceOptions, error) {
	opts := surfaceOptions{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, hasInlineValue := strings.Cut(arg, "=")
		switch name {
		case "--server":
			if hasInlineValue {
				opts.server = value
				continue
			}
			i++
			if i >= len(args) {
				return opts, fmt.Errorf("--server requires a value")
			}
			opts.server = args[i]
		case "--workspace":
			if hasInlineValue {
				opts.workspace = value
				continue
			}
			i++
			if i >= len(args) {
				return opts, fmt.Errorf("--workspace requires a value")
			}
			opts.workspace = args[i]
		case "--token":
			_ = value
			_ = hasInlineValue
			return opts, removedTokenFlagError()
		}
	}
	return opts, nil
}

func resolveObjectTypeWord(ctx context.Context, deps runtimeDeps, pluralKey string, args []string) (*surfaceNoun, error) {
	opts, err := surfaceOptionsFromArgs(args)
	if err != nil {
		return nil, err
	}
	session, err := sessionForCommand(deps, opts.server, opts.workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve object type word %q: %w", pluralKey, err)
	}
	surface, err := fetchCommandSurface(ctx, deps, session)
	if err != nil {
		return nil, err
	}
	for i := range surface.Nouns {
		if surface.Nouns[i].Kind == "object_type" && surface.Nouns[i].Key == pluralKey {
			if err := surface.Nouns[i].validate(); err != nil {
				return nil, err
			}
			return &surface.Nouns[i], nil
		}
	}
	return nil, fmt.Errorf("object type word %q is not defined in workspace %q", pluralKey, session.WorkspaceID)
}

func fetchCommandSurface(ctx context.Context, deps runtimeDeps, session cliSession) (*commandSurface, error) {
	endpoint := fmt.Sprintf("%s/api/v1/workspaces/%s/command-surface", trimSlash(session.Server), url.PathEscape(session.WorkspaceID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	setBearerCredential(req, session.Credential)
	resp, err := deps.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := readHTTPBody(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("command surface read failed: HTTP %d: %s", resp.StatusCode, truncateBodyForMessage(body, maxHTTPErrorMessageBytes))
	}
	var surface commandSurface
	if err := json.Unmarshal(body, &surface); err != nil {
		return nil, fmt.Errorf("command surface must be JSON: %w", err)
	}
	return &surface, nil
}

func printObjectTypeWordHelp(out io.Writer, noun *surfaceNoun) error {
	if noun == nil {
		return fmt.Errorf("object type word help requires a noun")
	}
	fieldSchema, err := json.MarshalIndent(noun.FieldSchema, "", "  ")
	if err != nil {
		return err
	}
	stateMachineID := "none"
	if noun.StateMachineID != nil && *noun.StateMachineID != "" {
		stateMachineID = *noun.StateMachineID
	}
	if _, err := fmt.Fprintf(out, `%s commands:
  %s create [--workspace W] --title TITLE [--values JSON]  create one %s
  %s get [--workspace W] --id OBJ  get one %s
  %s move [--workspace W] --id OBJ --version N --to-state STATE [--force]  move one %s
  %s link [--workspace W] --definition RDF --definition-version N --from OBJ --from-version N --to OBJ --to-version N  link one %s

definition:
  key: %s
  singular: %s
  plural: %s
  kind: %s
  object_type_id: %s
  version: %d
  state_machine_id: %s

field_schema:
%s
`, noun.Plural, noun.Key, noun.Singular, noun.Key, noun.Singular, noun.Key, noun.Singular, noun.Key, noun.Singular, noun.Key, noun.Singular, noun.Plural, noun.Kind, noun.ObjectTypeID, noun.Version, stateMachineID, indentLines(string(fieldSchema), "  ")); err != nil {
		return err
	}
	if len(noun.Relations) == 0 {
		_, err = fmt.Fprintln(out, "\nrelations: none")
		return err
	}
	if _, err := fmt.Fprintln(out, "\nrelations:"); err != nil {
		return err
	}
	for _, relation := range noun.Relations {
		if _, err := fmt.Fprintf(out, "  %s (%s) %s from=%s to=%s version=%d\n", relation.Key, relation.ID, relation.Cardinality, relation.FromObjectTypeID, relation.ToObjectTypeID, relation.Version); err != nil {
			return err
		}
	}
	return nil
}

func indentLines(value string, prefix string) string {
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}
