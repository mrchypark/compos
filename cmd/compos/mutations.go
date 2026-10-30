package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func requiredFlagValue(value *string, name string) (string, error) {
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return trimmed, nil
}

func enumFlagValue(value *string, name string, allowed ...string) (string, error) {
	trimmed := strings.TrimSpace(*value)
	for _, candidate := range allowed {
		if trimmed == candidate {
			return trimmed, nil
		}
	}
	return "", fmt.Errorf("%s must be %s", name, quotedChoiceList(allowed))
}

func quotedChoiceList(values []string) string {
	if len(values) == 0 {
		return ""
	}
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("%q", value)
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	if len(quoted) == 2 {
		return quoted[0] + " or " + quoted[1]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + ", or " + quoted[len(quoted)-1]
}

func runObjectCreate(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps, implicitObjectType string) error {
	fs := flag.NewFlagSet("object create", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	objectType := &implicitObjectType
	if implicitObjectType == "" {
		objectType = fs.String("type", "", "object type key")
	}
	title := fs.String("title", "", "object title")
	valuesRaw := fs.String("values", "{}", "object values JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	objectTypeValue, err := requiredFlagValue(objectType, "type")
	if err != nil {
		return err
	}
	titleValue, err := requiredFlagValue(title, "title")
	if err != nil {
		return err
	}
	values, err := parseJSONObject(*valuesRaw, "values")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "object.create", "object_instance", nil)
	command.Payload = map[string]any{
		"object_type_key": objectTypeValue,
		"title":           titleValue,
		"values":          values,
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runObjectUpdate(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("object update", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	id := fs.String("id", "", "object id")
	version := fs.Int64("version", 0, "expected object version")
	title := fs.String("title", "", "object title")
	valuesRaw := fs.String("values", "", "object values JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	idValue, err := requiredFlagValue(id, "id")
	if err != nil {
		return err
	}
	set := map[string]bool{}
	fs.Visit(func(flag *flag.Flag) { set[flag.Name] = true })
	if !set["version"] {
		return fmt.Errorf("version is required")
	}
	if *version <= 0 {
		return fmt.Errorf("version must be greater than zero")
	}
	if !set["title"] && !set["values"] {
		return fmt.Errorf("at least one of title or values is required")
	}
	payload := map[string]any{}
	if set["title"] {
		titleValue, err := requiredFlagValue(title, "title")
		if err != nil {
			return err
		}
		payload["title"] = titleValue
	}
	if set["values"] {
		values, err := parseJSONObject(*valuesRaw, "values")
		if err != nil {
			return err
		}
		payload["values"] = values
	}
	command := baseCommand(deps, opts, "object.update", "object_instance", &idValue)
	command.Preconditions = []map[string]any{
		{"aggregate_type": "object_instance", "aggregate_id": idValue, "expected_version": *version},
	}
	command.Payload = payload
	return submitCommand(ctx, journal, deps, opts, command)
}

func runObjectMove(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("object move", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	id := fs.String("id", "", "object id")
	toState := fs.String("to-state", "", "target state")
	version := fs.Int64("version", 0, "expected object version")
	reason := fs.String("reason", "", "transition reason")
	force := fs.Bool("force", false, "force state change even when transition is not allowed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	idValue, err := requiredFlagValue(id, "id")
	if err != nil {
		return err
	}
	toStateValue, err := requiredFlagValue(toState, "to-state")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "object.move", "object_instance", &idValue)
	command.Preconditions = []map[string]any{
		{"aggregate_type": "object_instance", "aggregate_id": idValue, "expected_version": *version},
	}
	command.Payload = map[string]any{
		"to_state": toStateValue,
	}
	if *reason != "" {
		command.Payload["reason"] = *reason
	}
	if *force {
		command.Payload["force"] = true
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runObjectGet(ctx context.Context, args []string, deps runtimeDeps) error {
	fs := flag.NewFlagSet("object get", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	server := fs.String("server", "", "Compos server URL")
	workspace := fs.String("workspace", "", "workspace id")
	id := fs.String("id", "", "object id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	idValue, err := requiredFlagValue(id, "id")
	if err != nil {
		return err
	}
	session, err := sessionForCommand(deps, *server, *workspace)
	if err != nil {
		return err
	}
	return getJSONWithAuth(ctx, deps, fmt.Sprintf("%s/api/v1/workspaces/%s/objects/%s", trimSlash(session.Server), url.PathEscape(session.WorkspaceID), url.PathEscape(idValue)), session.Credential)
}

func runObjectTypeDefine(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("object-type define", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	key := fs.String("key", "", "plural object type key")
	singular := fs.String("singular", "", "singular object type name")
	plural := fs.String("plural", "", "plural object type name")
	fieldSchemaRaw := fs.String("field-schema", "{}", "field schema JSON")
	stateMachineID := fs.String("state-machine-id", "", "state machine id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	keyValue, err := requiredFlagValue(key, "key")
	if err != nil {
		return err
	}
	singularValue, err := requiredFlagValue(singular, "singular")
	if err != nil {
		return err
	}
	pluralValue, err := requiredFlagValue(plural, "plural")
	if err != nil {
		return err
	}
	fieldSchema, err := parseJSONObject(*fieldSchemaRaw, "field-schema")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "object_type.define", "object_type", nil)
	command.Payload = map[string]any{
		"key":              keyValue,
		"singular":         singularValue,
		"plural":           pluralValue,
		"field_schema":     fieldSchema,
		"state_machine_id": nil,
	}
	if *stateMachineID != "" {
		command.Payload["state_machine_id"] = *stateMachineID
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runObjectTypeSchemaEvolve(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("object-type schema-evolve", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	id := fs.String("id", "", "object type id")
	version := fs.Int64("version", 0, "expected object type version")
	fieldSchemaRaw := fs.String("field-schema", "", "complete field schema JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	idValue, err := requiredFlagValue(id, "id")
	if err != nil {
		return err
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if !set["version"] {
		return fmt.Errorf("version is required")
	}
	if *version <= 0 {
		return fmt.Errorf("version must be greater than zero")
	}
	if !set["field-schema"] {
		return fmt.Errorf("field-schema is required")
	}
	fieldSchema, err := parseJSONObject(*fieldSchemaRaw, "field-schema")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "object_type.schema_evolve", "object_type", &idValue)
	command.Preconditions = []map[string]any{{"aggregate_type": "object_type", "aggregate_id": idValue, "expected_version": *version}}
	command.Payload = map[string]any{"field_schema": fieldSchema}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runRelation(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: compos relation <define|link|unlink>")
	}
	switch args[0] {
	case "define":
		return runRelationDefine(ctx, args[1:], journal, deps)
	case "link":
		return runRelationLink(ctx, args[1:], journal, deps)
	case "unlink":
		return runRelationUnlink(ctx, args[1:], journal, deps)
	default:
		return fmt.Errorf("usage: compos relation <define|link|unlink>")
	}
}

func runRelationDefine(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("relation define", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	key := fs.String("key", "", "relation definition key")
	fromObjectTypeID := fs.String("from-type", "", "from object type id")
	fromObjectTypeVersion := fs.Int64("from-type-version", 0, "expected from object type version")
	toObjectTypeID := fs.String("to-type", "", "to object type id")
	toObjectTypeVersion := fs.Int64("to-type-version", 0, "expected to object type version")
	cardinality := fs.String("cardinality", "", "relation cardinality")
	if err := fs.Parse(args); err != nil {
		return err
	}
	keyValue, err := requiredFlagValue(key, "key")
	if err != nil {
		return err
	}
	fromObjectTypeIDValue, err := requiredFlagValue(fromObjectTypeID, "from-type")
	if err != nil {
		return err
	}
	toObjectTypeIDValue, err := requiredFlagValue(toObjectTypeID, "to-type")
	if err != nil {
		return err
	}
	cardinalityValue, err := requiredFlagValue(cardinality, "cardinality")
	if err != nil {
		return err
	}
	cardinalityValue, err = enumFlagValue(cardinality, "cardinality", "one_to_one", "one_to_many", "many_to_one", "many_to_many")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "relation_definition.define", "relation_definition", nil)
	command.Preconditions = []map[string]any{
		{"aggregate_type": "object_type", "aggregate_id": fromObjectTypeIDValue, "expected_version": *fromObjectTypeVersion},
		{"aggregate_type": "object_type", "aggregate_id": toObjectTypeIDValue, "expected_version": *toObjectTypeVersion},
	}
	command.Payload = map[string]any{
		"key":                 keyValue,
		"from_object_type_id": fromObjectTypeIDValue,
		"to_object_type_id":   toObjectTypeIDValue,
		"cardinality":         cardinalityValue,
		"required":            false,
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runRelationLink(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("relation link", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	relationDefinitionID := fs.String("definition", "", "relation definition id")
	relationDefinitionVersion := fs.Int64("definition-version", 0, "expected relation definition version")
	fromObjectID := fs.String("from", "", "from object instance id")
	fromVersion := fs.Int64("from-version", 0, "expected from object version")
	toObjectID := fs.String("to", "", "to object instance id")
	toVersion := fs.Int64("to-version", 0, "expected to object version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	relationDefinitionIDValue, err := requiredFlagValue(relationDefinitionID, "definition")
	if err != nil {
		return err
	}
	fromObjectIDValue, err := requiredFlagValue(fromObjectID, "from")
	if err != nil {
		return err
	}
	toObjectIDValue, err := requiredFlagValue(toObjectID, "to")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "relation.link", "object_relation", nil)
	command.Preconditions = []map[string]any{
		{"aggregate_type": "relation_definition", "aggregate_id": relationDefinitionIDValue, "expected_version": *relationDefinitionVersion},
		{"aggregate_type": "object_instance", "aggregate_id": fromObjectIDValue, "expected_version": *fromVersion},
		{"aggregate_type": "object_instance", "aggregate_id": toObjectIDValue, "expected_version": *toVersion},
	}
	command.Payload = map[string]any{
		"relation_definition_id": relationDefinitionIDValue,
		"from_object_id":         fromObjectIDValue,
		"to_object_id":           toObjectIDValue,
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runRelationUnlink(ctx context.Context, args []string, journal ledger.CommandAttemptJournal, deps runtimeDeps) error {
	fs := flag.NewFlagSet("relation unlink", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	opts := commonMutationFlags(fs)
	relationID := fs.String("id", "", "object relation id")
	relationVersion := fs.Int64("version", 0, "expected relation version")
	relationDefinitionID := fs.String("definition", "", "relation definition id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	relationIDValue, err := requiredFlagValue(relationID, "id")
	if err != nil {
		return err
	}
	relationDefinitionIDValue, err := requiredFlagValue(relationDefinitionID, "definition")
	if err != nil {
		return err
	}
	command := baseCommand(deps, opts, "relation.unlink", "object_relation", &relationIDValue)
	command.Preconditions = []map[string]any{
		{"aggregate_type": "object_relation", "aggregate_id": relationIDValue, "expected_version": *relationVersion},
	}
	command.Payload = map[string]any{
		"relation_definition_id": relationDefinitionIDValue,
	}
	return submitCommand(ctx, journal, deps, opts, command)
}

func runObjectsRead(ctx context.Context, args []string, deps runtimeDeps) error {
	if len(args) < 1 || args[0] != "list" {
		return fmt.Errorf("usage: compos objects list")
	}
	fs := flag.NewFlagSet("objects list", flag.ContinueOnError)
	fs.SetOutput(deps.stderr)
	server := fs.String("server", "", "Compos server URL")
	workspace := fs.String("workspace", "", "workspace id")
	objectType := fs.String("type", "", "object type filter")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	session, err := sessionForCommand(deps, *server, *workspace)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/api/v1/workspaces/%s/objects", trimSlash(session.Server), url.PathEscape(session.WorkspaceID))
	if *objectType != "" {
		endpoint += "?object_type=" + url.QueryEscape(*objectType)
	}
	return getJSONWithAuth(ctx, deps, endpoint, session.Credential)
}
