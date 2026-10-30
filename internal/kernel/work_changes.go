package kernel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

// workChange is deliberately a small command envelope. Bindings decide which
// envelope is legal for a Work; this is not a general command batch.
type workChange struct {
	Index         int
	BindingID     string
	ClientRef     string
	CommandType   string
	Target        Target
	Preconditions []Precondition
	Payload       map[string]any
}

type sqlWorkChangeRow struct{ row *sql.Row }

func (r sqlWorkChangeRow) Scan(dest ...any) error   { return r.row.Scan(dest...) }
func (sqlWorkChangeRow) Version() store.ReadVersion { return store.ReadVersion{} }

func parseWorkChanges(raw any) ([]workChange, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok || len(items) > 32 {
		return nil, reject("schema_mismatch", "payload.changes must contain at most 32 changes")
	}
	changes := make([]workChange, 0, len(items))
	for i, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d] must be an object", i))
		}
		if err := onlyKeys(fmt.Sprintf("payload.changes[%d]", i), item, "binding_id", "client_ref", "command_type", "target", "preconditions", "payload"); err != nil {
			return nil, err
		}
		bindingID, _ := item["binding_id"].(string)
		commandType, _ := item["command_type"].(string)
		if strings.TrimSpace(bindingID) == "" || !map[string]bool{"object.create": true, "object.update": true, "relation.link": true, "relation.unlink": true}[commandType] {
			return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d] has an invalid binding_id or command_type", i))
		}
		payload, ok := item["payload"].(map[string]any)
		if !ok {
			return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d].payload must be an object", i))
		}
		clientRef, _ := item["client_ref"].(string)
		if _, exists := item["client_ref"]; exists && !workflowIdentifier(clientRef) {
			return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d].client_ref is invalid", i))
		}
		change := workChange{Index: i, BindingID: bindingID, ClientRef: clientRef, CommandType: commandType, Payload: payload}
		if rawTarget, exists := item["target"]; exists {
			target, ok := rawTarget.(map[string]any)
			if !ok {
				return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d].target must be an object", i))
			}
			if err := onlyKeys(fmt.Sprintf("payload.changes[%d].target", i), target, "aggregate_type", "aggregate_id"); err != nil {
				return nil, err
			}
			change.Target.AggregateType, _ = target["aggregate_type"].(string)
			if id, ok := target["aggregate_id"].(string); ok && id != "" {
				change.Target.AggregateID = &id
			}
		}
		if rawPreconditions, exists := item["preconditions"]; exists {
			encoded, err := json.Marshal(rawPreconditions)
			if err != nil || json.Unmarshal(encoded, &change.Preconditions) != nil {
				return nil, reject("schema_mismatch", fmt.Sprintf("payload.changes[%d].preconditions are invalid", i))
			}
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func childWorkChangeCommand(parent Command, index int, change workChange) (Command, error) {
	if change.Index >= 0 {
		index = change.Index
	}
	payload, err := json.Marshal(change.Payload)
	if err != nil {
		return Command{}, err
	}
	return Command{
		CommandID:      fmt.Sprintf("%s:change:%d", parent.CommandID, index),
		IdempotencyKey: fmt.Sprintf("%s:change:%d", parent.IdempotencyKey, index),
		WorkspaceID:    parent.WorkspaceID,
		RequestedActor: parent.RequestedActor,
		CommandType:    change.CommandType,
		Target:         change.Target,
		Preconditions:  change.Preconditions,
		Payload:        payload,
		Causation:      parent.Causation,
	}, nil
}

func (e *Executor) submitWorkChanges(ctx context.Context, tx store.Tx, principal string, actor Actor, parent Command, payloadHash string, payload map[string]any, now time.Time) (Receipt, error) {
	changes, err := parseWorkChanges(payload["changes"])
	if err != nil {
		return Receipt{}, err
	}
	if len(changes) == 0 {
		return Receipt{}, reject("schema_mismatch", "payload.changes must not be empty when provided")
	}
	references, err := planWorkReferences(parent, changes)
	if err != nil {
		return Receipt{}, err
	}
	changes = references.changes
	taskID, contextValue, err := workTask(ctx, tx, parent.WorkspaceID, *parent.Target.AggregateID)
	if err != nil {
		return Receipt{}, err
	}
	task, err := loadSpec(ctx, tx, parent.WorkspaceID, taskID, "task")
	if err != nil {
		return Receipt{}, err
	}
	stepID, _ := payload["workflow_step_id"].(string)
	routeID, _ := payload["workflow_route_id"].(string)
	if err := enforceWorkflowChangeBindings(ctx, tx, parent.WorkspaceID, *parent.Target.AggregateID, task, stepID, routeID, changes); err != nil {
		return Receipt{}, err
	}
	read := func(query string, args ...any) store.Row {
		return sqlWorkChangeRow{row: tx.QueryRowContext(ctx, query, args...)}
	}
	children := make([]Command, 0, len(changes))
	childChanges := make([]workChange, 0, len(changes))
	mutated := map[string]struct{}{}
	for i, change := range changes {
		if change.Target.AggregateID != nil {
			key := change.Target.AggregateType + ":" + *change.Target.AggregateID
			if _, duplicate := mutated[key]; duplicate {
				return Receipt{}, reject("validation_failed", "payload.changes cannot mutate the same existing aggregate twice")
			}
			mutated[key] = struct{}{}
		}
		if _, err := validateWorkChange(ctx, read, parent.WorkspaceID, *parent.Target.AggregateID, task, contextValue, change, references.objectsByID()); err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		child, err := childWorkChangeCommand(parent, i, change)
		if err != nil {
			return Receipt{}, err
		}
		if err := ValidateCommand(child); err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		if _, err := authorize(ctx, tx, principal, child); err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		if err := checkPreconditions(ctx, tx, child, references.objectsByID()); err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		children = append(children, child)
		childChanges = append(childChanges, change)
	}
	childEffects := make([]effect, 0, len(children))
	for i, child := range children {
		eff, err := e.apply(ctx, tx, principal, child, now)
		if err != nil {
			return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "")
		}
		childEffects = append(childEffects, eff)
	}
	submissionPayload := make(map[string]any, len(payload)-1)
	for key, value := range payload {
		if key != "changes" {
			submissionPayload[key] = value
		}
	}
	resolvedSubmission, err := resolveWorkSubmissionRefs(submissionPayload, references.objects)
	if err != nil {
		return Receipt{}, err
	}
	submissionPayload, _ = resolvedSubmission.(map[string]any)
	submissionCommand := parent
	submissionCommand.Payload, err = json.Marshal(submissionPayload)
	if err != nil {
		return Receipt{}, err
	}
	submission, err := e.apply(ctx, tx, principal, submissionCommand, now)
	if err != nil {
		return Receipt{}, err
	}
	sequence, err := nextProjectionSequence(ctx, tx, parent.WorkspaceID)
	if err != nil {
		return Receipt{}, err
	}
	finalSequence := sequence + int64(len(childEffects))
	version := submission.aggregate.Version
	receipt := Receipt{CommandID: parent.CommandID, Outcome: "applied", Aggregate: &submission.aggregate, AggregateVersion: &version, WorkspaceID: &parent.WorkspaceID, ProjectionSequence: &finalSequence}
	if receipt.ClientRefs, err = workChangeResults(ctx, tx, parent.WorkspaceID, references.objects); err != nil {
		return Receipt{}, err
	}
	if err := recordLedger(ctx, tx, principal, actor, parent, payloadHash, now, receipt); err != nil {
		return Receipt{}, err
	}
	for i, eff := range childEffects {
		if err := recordEffect(ctx, tx, parent.WorkspaceID, parent.CommandID, now, sequence+int64(i), eff); err != nil {
			return Receipt{}, err
		}
	}
	if err := recordEffect(ctx, tx, parent.WorkspaceID, parent.CommandID, now, finalSequence, submission); err != nil {
		return Receipt{}, err
	}
	if err := appendJournal(ctx, tx, principal, parent, submission.journal, timestamp(now)); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func (e *Executor) submitWorkChangesRhiza(ctx context.Context, principal string, actor Actor, parent Command, payload map[string]any, statements []rhiza.SQLStatement) (Receipt, error) {
	changes, err := parseWorkChanges(payload["changes"])
	if err != nil {
		return Receipt{}, err
	}
	if len(changes) == 0 {
		return Receipt{}, reject("schema_mismatch", "payload.changes must not be empty when provided")
	}
	references, err := planWorkReferences(parent, changes)
	if err != nil {
		return Receipt{}, err
	}
	changes = references.changes
	streamID := *parent.Target.AggregateID
	taskID, contextValue, err := e.workTaskRhiza(ctx, parent.WorkspaceID, streamID, &statements)
	if err != nil {
		return Receipt{}, err
	}
	task, err := e.loadSpecRhiza(ctx, parent.WorkspaceID, taskID, "task", &statements)
	if err != nil {
		return Receipt{}, err
	}
	stepID, _ := payload["workflow_step_id"].(string)
	routeID, _ := payload["workflow_route_id"].(string)
	if err := e.enforceWorkflowChangeBindingsRhiza(ctx, parent.WorkspaceID, streamID, task, stepID, routeID, changes, &statements); err != nil {
		return Receipt{}, err
	}
	read := func(query string, args ...any) store.Row {
		return e.store.ReadRow(ctx, store.ReadLinearizable, query, args...)
	}
	children := make([]Command, 0, len(changes))
	childChanges := make([]workChange, 0, len(changes))
	mutated := map[string]struct{}{}
	for i, change := range changes {
		if change.Target.AggregateID != nil {
			key := change.Target.AggregateType + ":" + *change.Target.AggregateID
			if _, duplicate := mutated[key]; duplicate {
				return Receipt{}, reject("validation_failed", "payload.changes cannot mutate the same existing aggregate twice")
			}
			mutated[key] = struct{}{}
		}
		bindingGuards, err := validateWorkChange(ctx, read, parent.WorkspaceID, streamID, task, contextValue, change, references.objectsByID())
		if err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		statements = append(statements, bindingGuards...)
		child, err := childWorkChangeCommand(parent, i, change)
		if err != nil {
			return Receipt{}, err
		}
		if err := ValidateCommand(child); err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		preparer := *e
		preparer.workObjects = references.objectsByID()
		_, childGuards, err := preparer.prepareRhiza(ctx, principal, child, change.Payload)
		if err != nil {
			return Receipt{}, workChangeError(err, change.Index, change, "")
		}
		// A child command is an internal plan only: it has no independent ledger
		// row, so its idempotency guard cannot establish a useful boundary.
		// Retain its authorization and aggregate precondition guards.
		childGuards = childGuards[1:]
		statements = append(statements, childGuards...)
		children = append(children, child)
		childChanges = append(childChanges, change)
	}
	type rhizaWorkChangePlan struct {
		statements []rhiza.SQLStatement
		effect     effect
	}
	plans := make([]rhizaWorkChangePlan, 0, len(children))
	collector := *e
	collector.workObjects = references.objectsByID()
	collector.workSequences = newWorkSequenceReservations()
	collector.rhizaCommit = func(callCtx context.Context, callPrincipal string, callActor Actor, child Command, childStatements []rhiza.SQLStatement, childEffect effect, ts string) (Receipt, error) {
		if len(childEffect.journal) != 0 {
			return Receipt{}, reject("validation_failed", "work changes cannot emit journal facts")
		}
		plans = append(plans, rhizaWorkChangePlan{statements: childStatements, effect: childEffect})
		return Receipt{CommandID: child.CommandID, Outcome: "applied"}, nil
	}
	for i, child := range children {
		var childPayload map[string]any
		if err := json.Unmarshal(child.Payload, &childPayload); err != nil {
			return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "payload")
		}
		switch child.CommandType {
		case "object.create":
			if _, err := collector.createObjectRhiza(ctx, principal, actor, child, childPayload, nil); err != nil {
				return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "")
			}
		case "object.update":
			if _, err := collector.updateObjectRhiza(ctx, principal, actor, child, childPayload, nil); err != nil {
				return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "")
			}
		case "relation.link":
			if _, err := collector.linkObjectRelationRhiza(ctx, principal, actor, child, childPayload, nil); err != nil {
				return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "")
			}
		case "relation.unlink":
			if _, err := collector.unlinkObjectRelationRhiza(ctx, principal, actor, child, childPayload, nil); err != nil {
				return Receipt{}, workChangeError(err, childChanges[i].Index, childChanges[i], "")
			}
		}
	}
	for _, plan := range plans {
		guards, _, err := splitRhizaWorkChangePlan(plan.statements)
		if err != nil {
			return Receipt{}, err
		}
		statements = append(statements, guards...)
	}
	collector.workSequences.appendStatements(&statements)
	// All these guards observe the same pre-submit state. Repeated bindings and
	// relation endpoints need one identical assertion, not one per child.
	seenGuards := map[string]bool{}
	uniqueGuards := make([]rhiza.SQLStatement, 0, len(statements))
	for _, statement := range statements {
		key := canonical(statement)
		if !seenGuards[key] {
			uniqueGuards = append(uniqueGuards, statement)
			seenGuards[key] = true
		}
	}
	statements = uniqueGuards
	for _, plan := range plans {
		_, mutations, err := splitRhizaWorkChangePlan(plan.statements)
		if err != nil {
			return Receipt{}, err
		}
		statements = append(statements, mutations...)
	}
	submissionPayload := make(map[string]any, len(payload)-1)
	for key, value := range payload {
		if key != "changes" {
			submissionPayload[key] = value
		}
	}
	resolvedSubmission, err := resolveWorkSubmissionRefs(submissionPayload, references.objects)
	if err != nil {
		return Receipt{}, err
	}
	submissionPayload, _ = resolvedSubmission.(map[string]any)
	committer := *e
	committer.rhizaSecondary = make([]effect, len(plans))
	committer.workResults = collector.workResults
	for i, plan := range plans {
		committer.rhizaSecondary[i] = plan.effect
	}
	return committer.submitWorkRhiza(ctx, principal, actor, parent, submissionPayload, statements)
}

func workChangeResults(ctx context.Context, tx store.Tx, workspace string, objects map[string]*workPendingObject) (map[string]WorkChangeResult, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	results := make(map[string]WorkChangeResult, len(objects))
	for ref, object := range objects {
		result := WorkChangeResult{ID: object.ID, Version: 1}
		rows, err := tx.QueryContext(ctx, `SELECT field_name,generated_value FROM generated_field_values WHERE workspace_id=? AND object_instance_id=? ORDER BY field_name`, workspace, object.ID)
		if err != nil {
			return nil, err
		}
		generated := map[string]any{}
		for rows.Next() {
			var field, value string
			if err := rows.Scan(&field, &value); err != nil {
				rows.Close()
				return nil, err
			}
			generated[field] = value
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if len(generated) > 0 {
			result.GeneratedValues = generated
		}
		results[ref] = result
	}
	return results, nil
}

func splitRhizaWorkChangePlan(statements []rhiza.SQLStatement) (guards, mutations []rhiza.SQLStatement, err error) {
	for _, statement := range statements {
		if len(statement.OutputRefs) != 0 {
			return nil, nil, reject("validation_failed", "Work change planner cannot reorder statement output references")
		}
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement.SQL)), "SELECT") {
			guards = append(guards, statement)
		} else {
			mutations = append(mutations, statement)
		}
	}
	return guards, mutations, nil
}

func enforceWorkflowChangeBindings(ctx context.Context, tx store.Tx, workspace, stream string, task map[string]any, stepID, routeID string, changes []workChange) error {
	if routeID == "" {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT correlation_id,spec_revision_id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='workflow.started' ORDER BY stream_sequence`, workspace, stream)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var invocationID, workflowID string
		if err := rows.Scan(&invocationID, &workflowID); err != nil {
			return err
		}
		workflow, err := loadSpec(ctx, tx, workspace, workflowID, "workflow")
		if err != nil {
			return err
		}
		contract, found := WorkflowRouteContractForSpec(workflow, invocationID, stepID, routeID)
		if !found {
			continue
		}
		return enforceWorkflowBindingIDs(contract.DataBindingIDs, task, changes)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return reject("precondition_failed", "workflow route definition is unavailable")
}

func (e *Executor) enforceWorkflowChangeBindingsRhiza(ctx context.Context, workspace, stream string, task map[string]any, stepID, routeID string, changes []workChange, statements *[]rhiza.SQLStatement) error {
	if routeID == "" {
		return nil
	}
	rows, err := e.store.ReadRows(ctx, store.ReadLinearizable, `SELECT id,stream_sequence,correlation_id,spec_revision_id FROM journal_records WHERE workspace_id=? AND stream_id=? AND kind='workflow.started' ORDER BY stream_sequence`, workspace, stream)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var recordID, invocationID, workflowID string
		var sequence int64
		if err := rows.Scan(&recordID, &sequence, &invocationID, &workflowID); err != nil {
			return err
		}
		workflow, err := e.loadSpecRhiza(ctx, workspace, workflowID, "workflow", statements)
		if err != nil {
			return err
		}
		contract, found := WorkflowRouteContractForSpec(workflow, invocationID, stepID, routeID)
		if !found {
			continue
		}
		one := int64(1)
		*statements = append(*statements, rhiza.SQLStatement{SQL: `SELECT 1 FROM journal_records WHERE workspace_id=? AND id=? AND stream_id=? AND stream_sequence=? AND correlation_id=? AND spec_revision_id=? AND kind='workflow.started'`, Args: []any{workspace, recordID, stream, sequence, invocationID, workflowID}, WantRows: true, ExpectedReturnedRows: &one})
		return enforceWorkflowBindingIDs(contract.DataBindingIDs, task, changes)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return reject("precondition_failed", "workflow route definition is unavailable")
}

func enforceWorkflowBindingIDs(allowed []string, task map[string]any, changes []workChange) error {
	if len(allowed) == 0 { // omitted route list inherits the Task bindings.
		return nil
	}
	taskBindings := map[string]struct{}{}
	for _, raw := range anySlice(task["data_bindings"]) {
		if binding, ok := raw.(map[string]any); ok {
			if id, ok := binding["id"].(string); ok {
				taskBindings[id] = struct{}{}
			}
		}
	}
	set := make(map[string]struct{}, len(allowed))
	for _, id := range allowed {
		if _, ok := taskBindings[id]; !ok {
			return reject("precondition_failed", "Workflow route names a binding absent from its Task")
		}
		set[id] = struct{}{}
	}
	for _, change := range changes {
		if _, ok := set[change.BindingID]; !ok {
			return reject("forbidden", "change is not allowed by the Workflow route")
		}
	}
	return nil
}

func appendRhizaSecondaryEffect(statements []rhiza.SQLStatement, parent Command, eff effect, ts string, sequenceStatement, offset int) []rhiza.SQLStatement {
	one := int64(1)
	payload := canonical(eff.payload)
	outboxEvent, outboxPayload := eff.event, payload
	if eff.outboxEvent != "" {
		outboxEvent, outboxPayload = eff.outboxEvent, canonical(eff.outboxPayload)
	}
	statements = append(statements,
		rhiza.SQLStatement{SQL: `INSERT INTO activities VALUES(?,?,?,?,?,?,?,?)`, Args: []any{newID("act"), parent.WorkspaceID, parent.CommandID, eff.aggregate.Type, eff.aggregate.ID, eff.event, payload, ts}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO outbox_events VALUES(?,?,?,?,?,'pending',?,?)`, Args: []any{newID("evt"), parent.WorkspaceID, parent.CommandID, outboxEvent, outboxPayload, ts, ts}, ExpectedRowsAffected: &one},
		rhiza.SQLStatement{SQL: `INSERT INTO projection_events(id,workspace_id,command_id,projection_sequence,aggregate_type,aggregate_id,aggregate_version,payload_hash,payload_json,created_at) VALUES(?,?,?,? + ` + strconv.Itoa(offset) + `,?,?,?,?,?,?)`, Args: []any{newID("prj"), parent.WorkspaceID, parent.CommandID, nil, eff.aggregate.Type, eff.aggregate.ID, eff.aggregate.Version, digestJSON([]byte(payload)), payload, ts}, ExpectedRowsAffected: &one, OutputRefs: []rhiza.SQLStatementOutputRef{{ArgIndex: 3, StatementIndex: sequenceStatement, ColumnName: "projection_sequence"}}},
	)
	return statements
}
