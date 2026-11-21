package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func TestExportFilesystemReplicaStableAndExact(t *testing.T) {
	storeDir := t.TempDir()
	seedSource(t, storeDir, false)

	outputDir := privateDir(t)
	first := filepath.Join(outputDir, "first.json")
	second := filepath.Join(outputDir, "second.json")
	conf := testConfig(storeDir, first)
	result, err := export(context.Background(), conf)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 2 || result.ExcludedTables != 9 || result.SHA256 == "" {
		t.Fatalf("unexpected summary: %#v", result)
	}
	info, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode=%#o, want 0600", info.Mode().Perm())
	}

	var document struct {
		Format string `json:"format"`
		Source source `json:"source"`
		Tables []struct {
			Name    string   `json:"name"`
			Columns []string `json:"columns"`
			Rows    [][]any  `json:"rows"`
		} `json:"tables"`
		Excluded []excludedTable `json:"excluded_tables"`
	}
	decodeJSON(t, first, &document)
	if document.Format != exportFormat || document.Source.RhizaVersion != rhizaVersion || document.Source.ClusterID != "source" || document.Source.NodeID != "n1" || document.Source.ObjectProvider != "filesystem" || document.Source.ObjectBucket != "" || document.Source.ObjectPrefix != "" || document.Source.SchemaIdentity != "old-schema" {
		t.Fatalf("unexpected header: %#v", document)
	}
	if len(document.Tables) != 1 || document.Tables[0].Name != "records" || len(document.Tables[0].Rows) != 2 {
		t.Fatalf("unexpected tables: %#v", document.Tables)
	}
	if got, ok := document.Tables[0].Rows[0][1].(json.Number); !ok || got.String() != "-9007199254740993" {
		t.Fatalf("exact integer lost: %#v", document.Tables[0].Rows[0][1])
	}
	if len(document.Excluded) != 9 {
		t.Fatalf("unexpected excluded tables: %#v", document.Excluded)
	}
	for _, excluded := range document.Excluded {
		want := uint64(1)
		if excluded.Name == "object_type_templates" {
			want = 2
		}
		if excluded.Rows != want {
			t.Fatalf("excluded count=%+v", excluded)
		}
	}
	if body, err := os.ReadFile(first); err != nil || bytes.Contains(body, []byte("retired-private-identity")) {
		t.Fatalf("export included retired identity values or could not be read: %v", err)
	}

	conf.output = second
	conf.dataDir = filepath.Join(t.TempDir(), "replica")
	if _, err := export(context.Background(), conf); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("same source produced different exports")
	}
}

func TestExportRejectsBlobWithoutPublishingOutput(t *testing.T) {
	storeDir := t.TempDir()
	seedSource(t, storeDir, true)
	output := filepath.Join(privateDir(t), "output.json")
	_, err := export(context.Background(), testConfig(storeDir, output))
	if err == nil || !strings.Contains(err.Error(), "BLOB") {
		t.Fatalf("error=%v, want BLOB rejection", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output exists or cannot be inspected: %v", statErr)
	}
}

func TestExportAttemptRejectsChangedAppliedSlotWithoutPublishingOutput(t *testing.T) {
	storeDir := t.TempDir()
	seedSource(t, storeDir, false)
	output := filepath.Join(privateDir(t), "output.json")
	conf := testConfig(storeDir, output)
	replica, err := rhiza.OpenReadReplica(context.Background(), rhiza.ReplicaConfig{
		ClusterID: conf.clusterID, ReplicaID: "test-export-attempt", DataDir: conf.dataDir,
		Members: []rhiza.ReplicaMember{{ID: rhiza.NodeID(conf.nodeID)}}, SyncInterval: time.Hour,
		ObjStoreProvider: conf.provider, ObjStoreDir: conf.objectDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	if err := replica.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := replica.Status()
	before.AppliedSlot++
	_, err = exportAttempt(context.Background(), replica, conf, before)
	if !errors.Is(err, errSourceAdvanced) {
		t.Fatalf("error=%v, want errSourceAdvanced", err)
	}
	if _, statErr := os.Lstat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output exists or cannot be inspected: %v", statErr)
	}
}

func seedSource(t *testing.T, storeDir string, blob bool) {
	t.Helper()
	ctx := context.Background()
	voter, err := rhiza.Open(ctx, rhiza.Config{
		ClusterID: "source", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = voter.Close() })
	exec := func(id, sql string, args ...any) {
		t.Helper()
		if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: id, SQL: sql, Args: args}); err != nil {
			t.Fatal(err)
		}
	}
	exec("metadata", "CREATE TABLE schema_metadata (id INTEGER PRIMARY KEY, identity TEXT)")
	exec("identity", "INSERT INTO schema_metadata VALUES (?, ?)", int64(1), "old-schema")
	exec("retired", "CREATE TABLE object_type_templates (id INTEGER PRIMARY KEY, body TEXT)")
	exec("retired-one", "INSERT INTO object_type_templates VALUES (?, ?)", int64(1), "first")
	exec("retired-two", "INSERT INTO object_type_templates VALUES (?, ?)", int64(2), "second")
	for _, table := range []string{"knowledge_documents", "knowledge_relations", "ontology_groundings"} {
		exec("retired-"+table, "CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, body TEXT)")
		exec("retired-row-"+table, "INSERT INTO "+table+" VALUES (?, ?)", int64(1), "retired Knowledge")
	}
	for _, table := range []string{"workspace_memberships", "auth_provider_identities", "auth_sessions", "actor_capabilities"} {
		exec("retired-"+table, "CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, body TEXT)")
		exec("retired-row-"+table, "INSERT INTO "+table+" VALUES (?, ?)", int64(1), "retired-private-identity")
	}
	exec("records", "CREATE TABLE records (id INTEGER PRIMARY KEY, exact INTEGER, name TEXT, enabled INTEGER, score REAL, note TEXT)")
	exec("record-two", "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)", int64(2), int64(9007199254740993), "second", int64(1), 1.25, nil)
	exec("record-one", "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)", int64(1), int64(-9007199254740993), "first", int64(0), 2.5, "note")
	if blob {
		exec("blobs", "CREATE TABLE blobs (id INTEGER PRIMARY KEY, value BLOB)")
		exec("blob", "INSERT INTO blobs VALUES (?, ?)", int64(1), []byte{1, 2})
	}
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
}

func testConfig(storeDir, output string) config {
	return config{
		dataDir: filepath.Join(filepath.Dir(output), "replica"), output: output,
		provider: "filesystem", objectDir: storeDir, clusterID: "source", nodeID: "n1", pageSize: 1,
	}
}

func decodeJSON(t *testing.T, path string, target any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
}

func privateDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}
