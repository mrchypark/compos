package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza"
)

const (
	LogicalImportFormat   = "compos-logical-v1"
	logicalSourceIdentity = "compos-go-sqlite-world-spec-journal-reactive-agent-v2"
	maxLogicalImportBytes = 256 << 20
)

// LogicalImport is the portable, application-only migration payload. It never
// includes Rhiza's replication state.
type LogicalImport struct {
	Format         string                 `json:"format"`
	Source         LogicalImportSource    `json:"source"`
	Tables         []LogicalImportTable   `json:"tables"`
	ExcludedTables []LogicalExcludedTable `json:"excluded_tables"`
}

type LogicalImportSource struct {
	RhizaVersion   string `json:"rhiza_version"`
	ClusterID      string `json:"cluster_id"`
	NodeID         string `json:"node_id"`
	ObjectProvider string `json:"object_provider"`
	ObjectBucket   string `json:"object_bucket"`
	ObjectPrefix   string `json:"object_prefix"`
	AppliedSlot    uint64 `json:"applied_slot"`
	SchemaIdentity string `json:"schema_identity"`
}

type LogicalImportTable struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

type LogicalExcludedTable struct {
	Name string `json:"name"`
	Rows uint64 `json:"rows"`
}

type LogicalTableSummary struct {
	Name   string `json:"name"`
	Rows   uint64 `json:"rows"`
	Digest string `json:"digest"`
}

type LogicalImportSummary struct {
	FileSHA256 string                `json:"file_sha256"`
	Tables     []LogicalTableSummary `json:"tables"`
	Digest     string                `json:"digest"`
}

var logicalImportOrder = []string{
	"workspaces",
	"object_types", "object_type_schema_revisions", "object_instances", "relation_definitions",
	"object_relations", "relation_cardinality_slots", "object_field_index", "field_sequence_counters",
	"generated_field_values", "reactive_rules", "reactive_rule_revisions",
	"adapter_inbound_events", "reactive_rule_evaluations", "spec_revisions",
	"command_ledger", "journal_records", "activities",
	"outbox_events", "projection_events", "workspace_creation_requests",
}

// DecodeLogicalImport validates the untrusted export before a target is opened
// for writes. json.Number preserves integer values through the import path.
func DecodeLogicalImport(data []byte) (*LogicalImport, string, error) {
	if len(data) == 0 || len(data) > maxLogicalImportBytes {
		return nil, "", fmt.Errorf("logical import must be between 1 and %d bytes", maxLogicalImportBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var input LogicalImport
	if err := decoder.Decode(&input); err != nil {
		return nil, "", fmt.Errorf("decode logical import: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, "", errors.New("logical import has trailing JSON values")
	}
	if err := validateLogicalImport(&input); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return &input, hex.EncodeToString(sum[:]), nil
}

func validateLogicalImport(input *LogicalImport) error {
	if input.Format != LogicalImportFormat {
		return fmt.Errorf("unsupported logical import format %q", input.Format)
	}
	if input.Source.RhizaVersion != "v0.12.0" {
		return fmt.Errorf("unsupported source Rhiza version %q", input.Source.RhizaVersion)
	}
	if !validLogicalRhizaIdentity(input.Source.ClusterID) || !validLogicalRhizaIdentity(input.Source.NodeID) {
		return errors.New("logical import source cluster_id and node_id must be plain identifiers")
	}
	if strings.TrimSpace(input.Source.ObjectProvider) == "" || strings.TrimSpace(input.Source.ObjectBucket) == "" {
		return errors.New("logical import source object-store provider/bucket are required")
	}
	if invalidLogicalObjectPrefix(input.Source.ObjectPrefix) {
		return errors.New("logical import source object-store prefix has traversal")
	}
	if input.Source.SchemaIdentity != logicalSourceIdentity {
		return fmt.Errorf("unsupported source schema identity %q", input.Source.SchemaIdentity)
	}
	allowed := make(map[string]struct{}, len(logicalImportOrder))
	for _, name := range logicalImportOrder {
		allowed[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(input.Tables))
	for index := range input.Tables {
		table := &input.Tables[index]
		if _, ok := allowed[table.Name]; !ok {
			return fmt.Errorf("logical import table %q is not an application table", table.Name)
		}
		if _, ok := seen[table.Name]; ok {
			return fmt.Errorf("logical import repeats table %q", table.Name)
		}
		seen[table.Name] = struct{}{}
		columns := make(map[string]struct{}, len(table.Columns))
		for _, column := range table.Columns {
			if column == "" {
				return fmt.Errorf("logical import table %q has an empty column", table.Name)
			}
			if _, ok := columns[column]; ok {
				return fmt.Errorf("logical import table %q repeats column %q", table.Name, column)
			}
			columns[column] = struct{}{}
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("logical import table %q has no columns", table.Name)
		}
		for rowIndex, row := range table.Rows {
			if len(row) != len(table.Columns) {
				return fmt.Errorf("logical import table %q row %d has %d values for %d columns", table.Name, rowIndex, len(row), len(table.Columns))
			}
			for columnIndex, value := range row {
				converted, err := logicalScalar(value)
				if err != nil {
					return fmt.Errorf("logical import table %q row %d column %q: %w", table.Name, rowIndex, table.Columns[columnIndex], err)
				}
				row[columnIndex] = converted
			}
		}
	}
	for _, name := range logicalImportOrder {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("logical import is missing application table %q", name)
		}
	}
	excluded := make(map[string]struct{}, len(input.ExcludedTables))
	for _, table := range input.ExcludedTables {
		if _, ok := excluded[table.Name]; ok {
			return fmt.Errorf("logical import repeats excluded table %q", table.Name)
		}
		excluded[table.Name] = struct{}{}
		if table.Name == "schema_metadata" {
			if table.Rows != 1 {
				return errors.New("logical import excludes schema_metadata with an invalid row count")
			}
			continue
		}
		if !retiredLogicalTable(table.Name) && !strings.HasPrefix(table.Name, "_rhiza_") && !strings.HasPrefix(table.Name, "sqlite_") {
			return fmt.Errorf("logical import excludes unsupported table %q", table.Name)
		}
	}
	if _, ok := excluded["object_type_templates"]; !ok {
		return errors.New("logical import must declare retired object_type_templates as excluded")
	}
	if _, ok := excluded["schema_metadata"]; !ok {
		return errors.New("logical import must declare source schema_metadata as excluded")
	}
	return nil
}

func retiredLogicalTable(name string) bool {
	switch name {
	case "object_type_templates", "knowledge_documents", "knowledge_relations", "ontology_groundings",
		"workspace_memberships", "auth_provider_identities", "auth_sessions", "actor_capabilities", "principal_person_bindings", "person_binding_claims":
		return true
	}
	return false
}

func logicalScalar(value any) (any, error) {
	switch value := value.(type) {
	case nil, string, bool, int64, float64:
		return value, nil
	case json.Number:
		if strings.ContainsAny(string(value), ".eE") {
			float, err := value.Float64()
			if err != nil {
				return nil, fmt.Errorf("invalid JSON number %q", value)
			}
			return float, nil
		}
		integer, err := value.Int64()
		if err != nil {
			return nil, fmt.Errorf("integer %q is outside int64", value)
		}
		return integer, nil
	default:
		return nil, fmt.Errorf("value must be a JSON scalar, got %T", value)
	}
}

// ImportLogical applies a validated logical export to a fresh Rhiza store.
// Import batches are deliberately replay-safe and do not reuse source request IDs.
func ImportLogical(ctx context.Context, target *Store, input *LogicalImport, fileSHA256 string) (LogicalImportSummary, error) {
	if target == nil || target.rhiza == nil {
		return LogicalImportSummary{}, errors.New("logical import requires a Rhiza target store")
	}
	if err := validateLogicalImport(input); err != nil {
		return LogicalImportSummary{}, err
	}
	if err := validateFileSHA256(fileSHA256); err != nil {
		return LogicalImportSummary{}, err
	}
	catalog, err := logicalCatalog(ctx, target)
	if err != nil {
		return LogicalImportSummary{}, err
	}
	if err := validateImportTables(input, catalog); err != nil {
		return LogicalImportSummary{}, err
	}
	if err := requireLogicalTargetEmpty(ctx, target); err != nil {
		return LogicalImportSummary{}, err
	}
	byName := make(map[string]*LogicalImportTable, len(input.Tables))
	for index := range input.Tables {
		byName[input.Tables[index].Name] = &input.Tables[index]
	}
	pins, err := logicalObjectSchemaPins(byName)
	if err != nil {
		return LogicalImportSummary{}, err
	}
	batches := make([]logicalImportBatch, 0)
	batch := 0
	for _, name := range logicalImportOrder {
		table := byName[name]
		rows, err := importRowsInOrder(table, catalog[name])
		if err != nil {
			return LogicalImportSummary{}, err
		}
		groups, err := logicalStatementGroups(name, table.Columns, rows, pins)
		if err != nil {
			return LogicalImportSummary{}, err
		}
		for start := 0; start < len(groups); {
			statements := make([]rhiza.SQLStatement, 0, 64)
			for start < len(groups) {
				candidate := append(append([]rhiza.SQLStatement(nil), statements...), groups[start]...)
				if err := rhiza.ValidateExecuteRequest(rhiza.ExecuteRequest{RequestID: compactSQLRequestID(logicalRequestID(fileSHA256, batch)), Statements: candidate}); err != nil {
					if len(statements) == 0 {
						return LogicalImportSummary{}, fmt.Errorf("validate logical import table %q group %d against Rhiza's %d-byte replicated mutation limit: %w", name, start, rhiza.MaxReplicatedMutationBytes, err)
					}
					break
				}
				statements = candidate
				start++
			}
			requestID := logicalRequestID(fileSHA256, batch)
			if err := rhiza.ValidateExecuteRequest(rhiza.ExecuteRequest{RequestID: compactSQLRequestID(requestID), Statements: statements}); err != nil {
				return LogicalImportSummary{}, fmt.Errorf("validate logical import batch %d for table %q: %w", batch, name, err)
			}
			batches = append(batches, logicalImportBatch{table: name, requestID: requestID, statements: statements})
			batch++
		}
	}
	for index, batch := range batches {
		if err := retryLogicalImportCommitUnknown(ctx, func() error {
			return target.ApplyStatements(ctx, batch.requestID, batch.statements)
		}); err != nil {
			return LogicalImportSummary{}, fmt.Errorf("apply logical import batch %d for table %q: %w", index, batch.table, err)
		}
	}
	return VerifyLogicalImport(ctx, target, input, fileSHA256)
}

func retryLogicalImportCommitUnknown(ctx context.Context, apply func() error) error {
	for attempt := 0; attempt < 3; attempt++ {
		err := apply()
		if !errors.Is(err, rhiza.ErrCommitUnknown) || attempt == 2 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}

type logicalObjectSchemaPin struct {
	workspace string
	typeID    string
	current   int64
	pinned    int64
}

func logicalObjectSchemaPins(tables map[string]*LogicalImportTable) (map[string]logicalObjectSchemaPin, error) {
	types, revisions, objects := tables["object_types"], tables["object_type_schema_revisions"], tables["object_instances"]
	current := make(map[string]int64, len(types.Rows))
	for _, row := range types.Rows {
		workspace, id, version, err := logicalTypeRow(types.Columns, row)
		if err != nil {
			return nil, err
		}
		current[workspace+"\x00"+id] = version
	}
	revisionSet := make(map[string]struct{}, len(revisions.Rows))
	for _, row := range revisions.Rows {
		workspace, id, version, err := logicalRevisionRow(revisions.Columns, row)
		if err != nil {
			return nil, err
		}
		revisionSet[workspace+"\x00"+id+"\x00"+strconv.FormatInt(version, 10)] = struct{}{}
	}
	pins := make(map[string]logicalObjectSchemaPin, len(objects.Rows))
	for _, row := range objects.Rows {
		workspace, id, typeID, pinned, err := logicalObjectRow(objects.Columns, row)
		if err != nil {
			return nil, err
		}
		key := workspace + "\x00" + typeID
		version, ok := current[key]
		if !ok {
			return nil, fmt.Errorf("object %q references missing object type %q", id, typeID)
		}
		if _, ok := revisionSet[key+"\x00"+strconv.FormatInt(pinned, 10)]; !ok {
			return nil, fmt.Errorf("object %q references missing schema revision %d", id, pinned)
		}
		pins[id] = logicalObjectSchemaPin{workspace: workspace, typeID: typeID, current: version, pinned: pinned}
	}
	return pins, nil
}

func logicalStatementGroups(table string, columns []string, rows [][]any, pins map[string]logicalObjectSchemaPin) ([][]rhiza.SQLStatement, error) {
	groups := make([][]rhiza.SQLStatement, 0, len(rows))
	if table != "object_instances" {
		for _, row := range rows {
			groups = append(groups, []rhiza.SQLStatement{insertStatement(table, columns, row)})
		}
		return groups, nil
	}
	idIndex := indexOf(columns, "id")
	if idIndex < 0 {
		return nil, errors.New("object_instances has no id")
	}
	one := int64(1)
	for _, row := range rows {
		id, ok := row[idIndex].(string)
		if !ok {
			return nil, errors.New("object_instances id is not text")
		}
		pin := pins[id]
		insert := insertStatement(table, columns, row)
		if pin.current == pin.pinned {
			groups = append(groups, []rhiza.SQLStatement{insert})
			continue
		}
		groups = append(groups, []rhiza.SQLStatement{
			{SQL: `UPDATE object_types SET schema_version=? WHERE workspace_id=? AND id=? AND schema_version=?`, Args: []any{pin.pinned, pin.workspace, pin.typeID, pin.current}, ExpectedRowsAffected: &one},
			insert,
			{SQL: `UPDATE object_types SET schema_version=? WHERE workspace_id=? AND id=? AND schema_version=?`, Args: []any{pin.current, pin.workspace, pin.typeID, pin.pinned}, ExpectedRowsAffected: &one},
		})
	}
	return groups, nil
}

func logicalTypeRow(columns []string, row []any) (string, string, int64, error) {
	return logicalSchemaRow(columns, row, "id", "schema_version")
}

func logicalRevisionRow(columns []string, row []any) (string, string, int64, error) {
	return logicalSchemaRow(columns, row, "object_type_id", "schema_version")
}

func logicalObjectRow(columns []string, row []any) (string, string, string, int64, error) {
	workspaceIndex, idIndex, typeIndex, pinIndex := indexOf(columns, "workspace_id"), indexOf(columns, "id"), indexOf(columns, "object_type_id"), indexOf(columns, "object_type_schema_version")
	if workspaceIndex < 0 || idIndex < 0 || typeIndex < 0 || pinIndex < 0 {
		return "", "", "", 0, errors.New("object_instances has incompatible columns")
	}
	workspace, workspaceOK := row[workspaceIndex].(string)
	id, idOK := row[idIndex].(string)
	typeID, typeOK := row[typeIndex].(string)
	pinned, pinOK := row[pinIndex].(int64)
	if !workspaceOK || !idOK || !typeOK || !pinOK {
		return "", "", "", 0, errors.New("object_instances schema pin fields have incompatible values")
	}
	return workspace, id, typeID, pinned, nil
}

func logicalSchemaRow(columns []string, row []any, idColumn, versionColumn string) (string, string, int64, error) {
	workspaceIndex, idIndex, versionIndex := indexOf(columns, "workspace_id"), indexOf(columns, idColumn), indexOf(columns, versionColumn)
	if workspaceIndex < 0 || idIndex < 0 || versionIndex < 0 {
		return "", "", 0, errors.New("object type schema has incompatible columns")
	}
	workspace, workspaceOK := row[workspaceIndex].(string)
	id, idOK := row[idIndex].(string)
	version, versionOK := row[versionIndex].(int64)
	if !workspaceOK || !idOK || !versionOK {
		return "", "", 0, errors.New("object type schema fields have incompatible values")
	}
	return workspace, id, version, nil
}

type logicalImportBatch struct {
	table      string
	requestID  string
	statements []rhiza.SQLStatement
}

// VerifyLogicalImport compares every row and scalar at one linearizable read
// snapshot. It is suitable for an already imported, quiescent target.
func VerifyLogicalImport(ctx context.Context, target *Store, input *LogicalImport, fileSHA256 string) (LogicalImportSummary, error) {
	if target == nil || target.rhiza == nil {
		return LogicalImportSummary{}, errors.New("logical verification requires a Rhiza target store")
	}
	if err := validateLogicalImport(input); err != nil {
		return LogicalImportSummary{}, err
	}
	if err := validateFileSHA256(fileSHA256); err != nil {
		return LogicalImportSummary{}, err
	}
	catalog, err := logicalCatalog(ctx, target)
	if err != nil {
		return LogicalImportSummary{}, err
	}
	if err := validateImportTables(input, catalog); err != nil {
		return LogicalImportSummary{}, err
	}
	byName := make(map[string]*LogicalImportTable, len(input.Tables))
	for index := range input.Tables {
		byName[input.Tables[index].Name] = &input.Tables[index]
	}
	summary := LogicalImportSummary{FileSHA256: fileSHA256}
	_, err = target.ReadSnapshot(ctx, ReadLinearizable, func(reader SnapshotReader) error {
		summary.Tables = summary.Tables[:0]
		for _, name := range logicalImportOrder {
			table, definition := byName[name], catalog[name]
			expected, err := importRowsInOrder(table, definition)
			if err != nil {
				return err
			}
			actual, err := readLogicalRows(ctx, reader, name, definition.Columns)
			if err != nil {
				return err
			}
			sort.Slice(expected, func(left, right int) bool { return logicalRowKey(expected[left]) < logicalRowKey(expected[right]) })
			if len(expected) != len(actual) {
				return fmt.Errorf("logical verification table %q has %d rows, want %d", name, len(actual), len(expected))
			}
			for index := range expected {
				if !logicalRowsEqual(expected[index], actual[index]) {
					return fmt.Errorf("logical verification table %q differs at ordered row %d", name, index)
				}
			}
			summary.Tables = append(summary.Tables, LogicalTableSummary{Name: name, Rows: uint64(len(actual)), Digest: logicalRowsDigest(actual)})
		}
		return nil
	})
	if err != nil {
		return LogicalImportSummary{}, err
	}
	summary.Digest = logicalSummaryDigest(summary.Tables)
	return summary, nil
}

type logicalTableDefinition struct {
	Columns []string
	PK      []int
}

func logicalCatalog(ctx context.Context, target *Store) (map[string]logicalTableDefinition, error) {
	catalog := make(map[string]logicalTableDefinition, len(logicalImportOrder))
	for _, name := range logicalImportOrder {
		rows, err := target.ReadRows(ctx, ReadLinearizable, "PRAGMA table_info("+quoteLogicalIdentifier(name)+")")
		if err != nil {
			return nil, fmt.Errorf("read target table %q definition: %w", name, err)
		}
		definition := logicalTableDefinition{}
		for rows.Next() {
			var cid, pk int64
			var column, columnType string
			var notNull any
			var defaultValue any
			if err := rows.Scan(&cid, &column, &columnType, &notNull, &defaultValue, &pk); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan target table %q definition: %w", name, err)
			}
			definition.Columns = append(definition.Columns, column)
			if pk > 0 {
				for len(definition.PK) < int(pk) {
					definition.PK = append(definition.PK, -1)
				}
				definition.PK[pk-1] = int(cid)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
		if len(definition.Columns) == 0 || len(definition.PK) == 0 {
			return nil, fmt.Errorf("target table %q has no compatible columns or primary key", name)
		}
		catalog[name] = definition
	}
	return catalog, nil
}

func validateImportTables(input *LogicalImport, catalog map[string]logicalTableDefinition) error {
	for _, table := range input.Tables {
		definition := catalog[table.Name]
		if len(table.Columns) != len(definition.Columns) {
			return fmt.Errorf("logical import table %q has incompatible columns", table.Name)
		}
		for index := range table.Columns {
			if table.Columns[index] != definition.Columns[index] {
				return fmt.Errorf("logical import table %q column %d is %q, want %q", table.Name, index, table.Columns[index], definition.Columns[index])
			}
		}
		seen := make(map[string]struct{}, len(table.Rows))
		for rowIndex, row := range table.Rows {
			key, err := logicalKey(row, definition.PK)
			if err != nil {
				return fmt.Errorf("logical import table %q row %d primary key: %w", table.Name, rowIndex, err)
			}
			if _, ok := seen[key]; ok {
				return fmt.Errorf("logical import table %q repeats a primary key", table.Name)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func requireLogicalTargetEmpty(ctx context.Context, target *Store) error {
	for _, name := range logicalImportOrder {
		var count int64
		if err := target.ReadRow(ctx, ReadLinearizable, "SELECT COUNT(*) FROM "+quoteLogicalIdentifier(name)).Scan(&count); err != nil {
			return fmt.Errorf("count target table %q: %w", name, err)
		}
		if count != 0 {
			return fmt.Errorf("logical import target table %q is not empty", name)
		}
	}
	return nil
}

func importRowsInOrder(table *LogicalImportTable, definition logicalTableDefinition) ([][]any, error) {
	rows := append([][]any(nil), table.Rows...)
	if table.Name != "reactive_rule_revisions" {
		sort.Slice(rows, func(left, right int) bool { return logicalRowKey(rows[left]) < logicalRowKey(rows[right]) })
		return rows, nil
	}
	supersedes := indexOf(table.Columns, "supersedes_revision_id")
	id := indexOf(table.Columns, "id")
	if supersedes < 0 || id < 0 {
		return nil, errors.New("reactive_rule_revisions has no id or supersedes_revision_id")
	}
	remaining := append([][]any(nil), rows...)
	resolved := make(map[string]struct{}, len(rows))
	ordered := make([][]any, 0, len(rows))
	for len(remaining) > 0 {
		progress := false
		sort.Slice(remaining, func(left, right int) bool { return logicalRowKey(remaining[left]) < logicalRowKey(remaining[right]) })
		next := remaining[:0]
		for _, row := range remaining {
			parent, ok := row[supersedes].(string)
			if row[supersedes] == nil || (ok && parent == "") {
				ordered, resolved[fmt.Sprint(row[id])] = append(ordered, row), struct{}{}
				progress = true
				continue
			}
			if _, ok := resolved[fmt.Sprint(row[supersedes])]; ok {
				ordered, resolved[fmt.Sprint(row[id])] = append(ordered, row), struct{}{}
				progress = true
				continue
			}
			next = append(next, row)
		}
		if !progress {
			return nil, errors.New("reactive_rule_revisions contains a missing or cyclic supersedes_revision_id")
		}
		remaining = next
	}
	return ordered, nil
}

func insertStatement(table string, columns []string, row []any) rhiza.SQLStatement {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for index, column := range columns {
		quoted[index], placeholders[index] = quoteLogicalIdentifier(column), "?"
	}
	one := int64(1)
	return rhiza.SQLStatement{SQL: "INSERT INTO " + quoteLogicalIdentifier(table) + " (" + strings.Join(quoted, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")", Args: row, ExpectedRowsAffected: &one}
}

func logicalRequestID(fileSHA256 string, batch int) string {
	return "imp-" + fileSHA256[:48] + "-" + strconv.Itoa(batch)
}

func validateFileSHA256(value string) error {
	if len(value) != sha256.Size*2 {
		return errors.New("logical import file SHA-256 is invalid")
	}
	_, err := hex.DecodeString(value)
	return err
}

func readLogicalRows(ctx context.Context, reader SnapshotReader, table string, columns []string) ([][]any, error) {
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = quoteLogicalIdentifier(column)
	}
	query := "SELECT " + strings.Join(quoted, ",") + " FROM " + quoteLogicalIdentifier(table) + " ORDER BY " + strings.Join(quoted, ",")
	rows, err := reader.ReadRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result [][]any
	for rows.Next() {
		row := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range row {
			destinations[index] = &row[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(left, right int) bool { return logicalRowKey(result[left]) < logicalRowKey(result[right]) })
	return result, nil
}

func logicalRowsEqual(left, right []any) bool { return logicalRowKey(left) == logicalRowKey(right) }

func logicalRowsDigest(rows [][]any) string {
	hash := sha256.New()
	for _, row := range rows {
		_, _ = hash.Write([]byte(logicalRowKey(row)))
		_, _ = hash.Write([]byte{'\n'})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func logicalSummaryDigest(tables []LogicalTableSummary) string {
	encoded, _ := json.Marshal(tables)
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func logicalKey(row []any, pk []int) (string, error) {
	key := make([]any, len(pk))
	for index, position := range pk {
		if position < 0 || position >= len(row) {
			return "", errors.New("invalid primary key position")
		}
		key[index] = row[position]
	}
	return logicalRowKey(key), nil
}

func logicalRowKey(row []any) string {
	encoded, _ := json.Marshal(row)
	return string(encoded)
}

func quoteLogicalIdentifier(identifier string) string {
	return `"` + identifier + `"`
}

func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}

func invalidLogicalObjectPrefix(prefix string) bool {
	for _, part := range strings.Split(strings.Trim(strings.TrimSpace(prefix), "/"), "/") {
		if part == "." || part == ".." {
			return true
		}
	}
	return false
}

func validLogicalRhizaIdentity(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}
