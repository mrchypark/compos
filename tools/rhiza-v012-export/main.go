// rhiza-v012-export copies a certified Rhiza v0.12 SQL state into a logical
// JSON document. It opens only an object-store read replica.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mrchypark/rhiza"
)

const (
	exportFormat = "compos-logical-v1"
	rhizaVersion = "v0.12.0"
	maxAttempts  = 3
)

type config struct {
	dataDir   string
	output    string
	provider  string
	bucket    string
	prefix    string
	objectDir string
	endpoint  string
	region    string
	insecure  bool
	clusterID string
	nodeID    string
	pageSize  int
}

type source struct {
	RhizaVersion   string `json:"rhiza_version"`
	ClusterID      string `json:"cluster_id"`
	NodeID         string `json:"node_id"`
	ObjectProvider string `json:"object_provider"`
	ObjectBucket   string `json:"object_bucket"`
	ObjectPrefix   string `json:"object_prefix"`
	AppliedSlot    uint64 `json:"applied_slot"`
	SchemaIdentity string `json:"schema_identity"`
}

type table struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

type excludedTable struct {
	Name string `json:"name"`
	Rows uint64 `json:"rows"`
}

type summary struct {
	SHA256         string
	Tables         int
	Rows           uint64
	ExcludedTables int
}

type catalogTable struct {
	name string
	sql  string
}

func main() {
	conf := parseFlags()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	result, err := export(ctx, conf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "export failed:", err)
		os.Exit(1)
	}
	// This is intentionally the only successful CLI output. It has no row data.
	fmt.Fprintf(os.Stderr, "export complete sha256=%s tables=%d rows=%d excluded_tables=%d\n", result.SHA256, result.Tables, result.Rows, result.ExcludedTables)
}

func parseFlags() config {
	var conf config
	flag.StringVar(&conf.dataDir, "data-dir", env("COMPOS_RHIZA_EXPORT_DATA_DIR", ""), "dedicated local read-replica data directory")
	flag.StringVar(&conf.output, "output", env("COMPOS_RHIZA_EXPORT_OUTPUT", ""), "new logical JSON output path")
	flag.StringVar(&conf.provider, "object-provider", env("COMPOS_RHIZA_OBJECT_PROVIDER", "gcs"), "object store provider: gcs or filesystem")
	flag.StringVar(&conf.bucket, "object-bucket", env("COMPOS_RHIZA_OBJECT_BUCKET", ""), "object store bucket")
	flag.StringVar(&conf.prefix, "object-prefix", env("COMPOS_RHIZA_OBJECT_PREFIX", ""), "object store prefix")
	flag.StringVar(&conf.objectDir, "object-dir", env("COMPOS_RHIZA_OBJECT_DIR", ""), "filesystem object store directory")
	flag.StringVar(&conf.endpoint, "object-endpoint", env("COMPOS_RHIZA_OBJECT_ENDPOINT", ""), "object store endpoint")
	flag.StringVar(&conf.region, "object-region", env("COMPOS_RHIZA_OBJECT_REGION", ""), "object store region")
	flag.BoolVar(&conf.insecure, "object-insecure", envBool("COMPOS_RHIZA_OBJECT_INSECURE"), "allow insecure object store transport")
	flag.StringVar(&conf.clusterID, "cluster-id", env("COMPOS_RHIZA_CLUSTER_ID", "compos"), "source Rhiza cluster ID")
	flag.StringVar(&conf.nodeID, "node-id", env("COMPOS_RHIZA_NODE_ID", "compos-0"), "source voter node ID")
	flag.IntVar(&conf.pageSize, "page-size", envInt("COMPOS_RHIZA_EXPORT_PAGE_SIZE", 500), "rows read per query page")
	flag.Parse()
	return conf
}

func export(ctx context.Context, conf config) (summary, error) {
	if err := validateConfig(conf); err != nil {
		return summary{}, err
	}
	replica, err := rhiza.OpenReadReplica(ctx, rhiza.ReplicaConfig{
		ClusterID: conf.clusterID,
		ReplicaID: conf.nodeID + "-logical-export",
		DataDir:   conf.dataDir,
		// Object-store mode verifies the certified archive. It does not join the
		// source cluster and deliberately receives no voter credentials.
		Members:          []rhiza.ReplicaMember{{ID: rhiza.NodeID(conf.nodeID)}},
		SyncInterval:     time.Hour,
		ObjStoreProvider: conf.provider,
		ObjStoreBucket:   conf.bucket,
		ObjStorePrefix:   conf.prefix,
		ObjStoreDir:      conf.objectDir,
		ObjStoreEndpoint: conf.endpoint,
		ObjStoreRegion:   conf.region,
		ObjStoreInsecure: conf.insecure,
	})
	if err != nil {
		return summary{}, fmt.Errorf("open read replica: %w", err)
	}
	defer replica.Close()

	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := replica.Sync(ctx); err != nil {
			return summary{}, fmt.Errorf("sync source: %w", err)
		}
		before := replica.Status()
		if !replica.Ready() || before.LagSlots != 0 || before.AppliedSlot != before.SourceTip {
			return summary{}, fmt.Errorf("read replica is not caught up: applied_slot=%d source_tip=%d lag_slots=%d", before.AppliedSlot, before.SourceTip, before.LagSlots)
		}
		result, err := exportAttempt(ctx, replica, conf, before)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, errSourceAdvanced) {
			return summary{}, err
		}
		last = err
	}
	return summary{}, fmt.Errorf("source advanced during export after %d attempts: %w", maxAttempts, last)
}

var errSourceAdvanced = errors.New("source advanced during export")

func exportAttempt(ctx context.Context, replica *rhiza.ReadReplica, conf config, before rhiza.ReplicaStatus) (_ summary, resultErr error) {
	state := snapshot{replica: replica, slot: before.AppliedSlot}
	catalog, err := state.catalog(ctx)
	if err != nil {
		return summary{}, err
	}
	identity, err := state.schemaIdentity(ctx, catalog)
	if err != nil {
		return summary{}, err
	}

	tmp, err := newOutput(conf.output)
	if err != nil {
		return summary{}, err
	}
	defer func() {
		if resultErr != nil {
			_ = os.Remove(tmp.Name())
		}
	}()
	hash := sha256.New()
	w := io.MultiWriter(tmp, hash)
	if _, err := io.WriteString(w, `{"format":`); err != nil {
		return summary{}, err
	}
	if err := writeJSON(w, exportFormat); err != nil {
		return summary{}, err
	}
	if _, err := io.WriteString(w, `,"source":`); err != nil {
		return summary{}, err
	}
	if err := writeJSON(w, source{RhizaVersion: rhizaVersion, ClusterID: conf.clusterID, NodeID: conf.nodeID, ObjectProvider: conf.provider, ObjectBucket: conf.bucket, ObjectPrefix: conf.prefix, AppliedSlot: before.AppliedSlot, SchemaIdentity: identity}); err != nil {
		return summary{}, err
	}
	if _, err := io.WriteString(w, `,"tables":[`); err != nil {
		return summary{}, err
	}

	var tables, rows uint64
	excluded := make([]excludedTable, 0)
	firstTable := true
	for _, entry := range catalog {
		if shouldSkip(entry.name) {
			if !strings.HasPrefix(entry.name, "_rhiza_") && !strings.HasPrefix(entry.name, "sqlite_") {
				count, err := state.count(ctx, entry.name)
				if err != nil {
					return summary{}, err
				}
				excluded = append(excluded, excludedTable{Name: entry.name, Rows: count})
			}
			continue
		}
		if isVirtualTable(entry.sql) {
			return summary{}, fmt.Errorf("virtual table %q is unsupported", entry.name)
		}
		columns, order, err := state.columns(ctx, entry.name)
		if err != nil {
			return summary{}, err
		}
		if !firstTable {
			if _, err := io.WriteString(w, ","); err != nil {
				return summary{}, err
			}
		}
		firstTable = false
		if err := writeTablePrefix(w, table{Name: entry.name, Columns: columns}); err != nil {
			return summary{}, err
		}
		count, err := state.rows(ctx, entry.name, columns, order, conf.pageSize, w)
		if err != nil {
			return summary{}, err
		}
		rows += count
		tables++
		if _, err := io.WriteString(w, `]}`); err != nil {
			return summary{}, err
		}
	}
	if _, err := io.WriteString(w, `],"excluded_tables":`); err != nil {
		return summary{}, err
	}
	if err := writeJSON(w, excluded); err != nil {
		return summary{}, err
	}
	if _, err := io.WriteString(w, "}"); err != nil {
		return summary{}, err
	}
	if err := tmp.Sync(); err != nil {
		return summary{}, err
	}
	if err := tmp.Close(); err != nil {
		return summary{}, err
	}

	final, err := state.query(ctx, "SELECT 1")
	if err != nil {
		return summary{}, err
	}
	if final.AppliedSlot != before.AppliedSlot {
		return summary{}, errSourceAdvanced
	}
	if err := replica.Sync(ctx); err != nil {
		return summary{}, fmt.Errorf("final source sync: %w", err)
	}
	after := replica.Status()
	if !replica.Ready() || after.LagSlots != 0 || after.AppliedSlot != after.SourceTip || after.AppliedSlot != before.AppliedSlot || after.SourceTip != before.SourceTip {
		return summary{}, errSourceAdvanced
	}
	if err := publishExclusive(tmp.Name(), conf.output); err != nil {
		return summary{}, err
	}
	return summary{SHA256: hex.EncodeToString(hash.Sum(nil)), Tables: int(tables), Rows: rows, ExcludedTables: len(excluded)}, nil
}

type snapshot struct {
	replica *rhiza.ReadReplica
	slot    uint64
}

func (s snapshot) query(ctx context.Context, sql string, args ...any) (rhiza.QueryResponse, error) {
	response, err := s.replica.Query(ctx, rhiza.QueryRequest{SQL: sql, Args: args, Consistency: rhiza.ConsistencyLocal})
	if err != nil {
		return rhiza.QueryResponse{}, err
	}
	if response.AppliedSlot != s.slot {
		return rhiza.QueryResponse{}, errSourceAdvanced
	}
	return response, nil
}

func (s snapshot) catalog(ctx context.Context) ([]catalogTable, error) {
	response, err := s.query(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name")
	if err != nil {
		return nil, err
	}
	catalog := make([]catalogTable, 0, len(response.Rows))
	for _, row := range response.Rows {
		if len(row) != 2 {
			return nil, errors.New("unexpected sqlite_master row")
		}
		name, ok := row[0].(string)
		if !ok || name == "" {
			return nil, errors.New("sqlite_master table name is invalid")
		}
		sql, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("sqlite_master SQL for table %q is invalid", name)
		}
		catalog = append(catalog, catalogTable{name: name, sql: sql})
	}
	return catalog, nil
}

func (s snapshot) schemaIdentity(ctx context.Context, catalog []catalogTable) (string, error) {
	for _, entry := range catalog {
		if entry.name != "schema_metadata" {
			continue
		}
		response, err := s.query(ctx, "SELECT identity FROM schema_metadata WHERE id = 1")
		if err != nil {
			return "", err
		}
		if len(response.Rows) == 0 {
			return "", nil
		}
		if len(response.Rows) != 1 || len(response.Rows[0]) != 1 {
			return "", errors.New("schema_metadata identity is invalid")
		}
		identity, ok := response.Rows[0][0].(string)
		if !ok {
			return "", errors.New("schema_metadata identity is not text")
		}
		return identity, nil
	}
	return "", nil
}

func (s snapshot) count(ctx context.Context, name string) (uint64, error) {
	response, err := s.query(ctx, "SELECT COUNT(*) FROM "+quoteIdentifier(name))
	if err != nil {
		return 0, err
	}
	if len(response.Rows) != 1 || len(response.Rows[0]) != 1 {
		return 0, fmt.Errorf("unexpected count for table %q", name)
	}
	value, ok := response.Rows[0][0].(int64)
	if !ok || value < 0 {
		return 0, fmt.Errorf("invalid count for table %q", name)
	}
	return uint64(value), nil
}

func (s snapshot) columns(ctx context.Context, name string) ([]string, []string, error) {
	response, err := s.query(ctx, "PRAGMA table_info("+quoteIdentifier(name)+")")
	if err != nil {
		return nil, nil, err
	}
	type column struct {
		name string
		pk   int64
	}
	found := make([]column, 0, len(response.Rows))
	for _, row := range response.Rows {
		if len(row) < 6 {
			return nil, nil, fmt.Errorf("unexpected schema for table %q", name)
		}
		columnName, ok := row[1].(string)
		if !ok || columnName == "" {
			return nil, nil, fmt.Errorf("invalid column in table %q", name)
		}
		pk, ok := row[5].(int64)
		if !ok || pk < 0 {
			return nil, nil, fmt.Errorf("invalid primary key in table %q", name)
		}
		found = append(found, column{name: columnName, pk: pk})
	}
	if len(found) == 0 {
		return nil, nil, fmt.Errorf("table %q has no columns", name)
	}
	columns := make([]string, len(found))
	primary := make([]column, 0, len(found))
	for index, column := range found {
		columns[index] = column.name
		if column.pk > 0 {
			primary = append(primary, column)
		}
	}
	if len(primary) > 0 {
		sort.Slice(primary, func(i, j int) bool { return primary[i].pk < primary[j].pk })
		order := make([]string, len(primary))
		for index, column := range primary {
			order[index] = column.name
		}
		return columns, order, nil
	}
	return columns, columns, nil
}

func (s snapshot) rows(ctx context.Context, name string, columns, order []string, pageSize int, w io.Writer) (uint64, error) {
	selectColumns := quoteIdentifiers(columns)
	orderColumns := quoteIdentifiers(order)
	var total uint64
	first := true
	for offset := 0; ; offset += pageSize {
		response, err := s.query(ctx, "SELECT "+selectColumns+" FROM "+quoteIdentifier(name)+" ORDER BY "+orderColumns+" LIMIT ? OFFSET ?", int64(pageSize), int64(offset))
		if err != nil {
			return 0, err
		}
		for _, row := range response.Rows {
			if len(row) != len(columns) {
				return 0, fmt.Errorf("row shape mismatch for table %q", name)
			}
			values, err := jsonRow(row)
			if err != nil {
				return 0, fmt.Errorf("table %q: %w", name, err)
			}
			encoded, err := json.Marshal(values)
			if err != nil {
				return 0, fmt.Errorf("encode table %q row: %w", name, err)
			}
			if !first {
				if _, err := io.WriteString(w, ","); err != nil {
					return 0, err
				}
			}
			first = false
			if _, err := w.Write(encoded); err != nil {
				return 0, err
			}
			total++
		}
		if len(response.Rows) < pageSize {
			return total, nil
		}
	}
}

func jsonRow(row []any) ([]any, error) {
	result := make([]any, len(row))
	for index, value := range row {
		switch value := value.(type) {
		case nil, string, bool, int64:
			result[index] = value
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("column %d has a non-finite number", index)
			}
			result[index] = value
		case []byte:
			return nil, fmt.Errorf("column %d is a BLOB", index)
		default:
			return nil, fmt.Errorf("column %d has unsupported type %T", index, value)
		}
	}
	return result, nil
}

func writeTablePrefix(w io.Writer, value table) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) < 1 || encoded[len(encoded)-1] != '}' {
		return errors.New("invalid table encoding")
	}
	_, err = w.Write(append(encoded[:len(encoded)-1], []byte(`,"rows":[`)...))
	return err
}

func writeJSON(w io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

func shouldSkip(name string) bool {
	switch name {
	case "schema_metadata", "object_type_templates", "knowledge_documents", "knowledge_relations", "ontology_groundings",
		"workspace_memberships", "auth_provider_identities", "auth_sessions", "actor_capabilities":
		return true
	}
	return strings.HasPrefix(name, "_rhiza_") || strings.HasPrefix(name, "sqlite_")
}

func isVirtualTable(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "CREATE VIRTUAL TABLE")
}

func quoteIdentifier(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func quoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = quoteIdentifier(name)
	}
	return strings.Join(quoted, ",")
}

func newOutput(output string) (*os.File, error) {
	if output == "" {
		return nil, errors.New("output is required")
	}
	dir := filepath.Dir(output)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("output directory %q must not be group or world accessible", dir)
	}
	if _, err := os.Lstat(output); err == nil {
		return nil, fmt.Errorf("output already exists: %q", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return os.CreateTemp(dir, ".rhiza-v012-export-*")
}

func publishExclusive(temp, output string) error {
	if err := os.Chmod(temp, 0o600); err != nil {
		return err
	}
	if err := os.Link(temp, output); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("output already exists: %q", output)
		}
		return fmt.Errorf("publish output exclusively: %w", err)
	}
	if err := os.Remove(temp); err != nil {
		_ = os.Remove(output)
		return err
	}
	return nil
}

func validateConfig(conf config) error {
	if conf.dataDir == "" || conf.clusterID == "" || conf.nodeID == "" {
		return errors.New("data-dir, cluster-id, and node-id are required")
	}
	if conf.pageSize < 1 || conf.pageSize > 5000 {
		return errors.New("page-size must be between 1 and 5000")
	}
	switch conf.provider {
	case "filesystem":
		if conf.objectDir == "" {
			return errors.New("object-dir is required for filesystem provider")
		}
	case "gcs":
		if conf.bucket == "" {
			return errors.New("object-bucket is required for gcs provider")
		}
	default:
		return fmt.Errorf("unsupported object provider %q", conf.provider)
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) bool {
	value, _ := strconvParseBool(os.Getenv(key))
	return value
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscan(value, &parsed); err != nil {
		return fallback
	}
	return parsed
}

func strconvParseBool(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	default:
		return false, fmt.Errorf("invalid boolean")
	}
}
