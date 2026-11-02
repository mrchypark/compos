package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/compos/internal/store"
)

func TestCredentialPinValidation(t *testing.T) {
	connector := func(pin map[string]any) map[string]any {
		return map[string]any{"name": "Send", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "binding": pin}
	}
	base := map[string]any{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage"}
	if err := validateSpec("function", connector(base)); err != nil {
		t.Fatalf("legacy connector binding: %v", err)
	}
	for _, pin := range []map[string]any{
		{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "credential_revision": 1},
		{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "connection_id": "conn_1", "credential_revision": -1},
		{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "connection_id": "conn_1", "credential_revision": 1.5},
		{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "connection_id": "conn_1", "credential_revision": int64(maxCredentialRevision + 1)},
	} {
		if err := validateSpec("function", connector(pin)); err == nil {
			t.Fatalf("invalid connector pin accepted: %#v", pin)
		}
	}
	if err := validateSpec("participant", map[string]any{"name": "Agent", "instructions": "Do work", "execution_policy": map[string]any{"handler_key": "agent.run", "handler_version": "1", "connection_id": "conn_1", "credential_revision": 7}}); err != nil {
		t.Fatalf("pinned agent policy: %v", err)
	}
}

func TestCredentialPinRejectsLossyJSONRevision(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"sqlite", "rhiza"} {
		t.Run(backend, func(t *testing.T) {
			var db *store.Store
			if backend == "rhiza" {
				db = openRhizaKernelStore(t, ctx, "credential-pin-raw")
			} else {
				var err error
				db, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				now := "2026-01-01T00:00:00Z"
				if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
					t.Fatal(err)
				}

			}
			defer db.Close()
			executor := testStoreExecutor(db)
			publish := func(id, kind, revision string) Command {
				spec := `{"name":"Send","input_schema":{"type":"object"},"output_schema":{"type":"object"},"binding":{"kind":"connector","handler_version":"1","provider":"slack","operation":"chat.postMessage","connection_id":"conn_1","credential_revision":` + revision + `}}`
				if kind == "participant" {
					spec = `{"name":"Agent","instructions":"Do work","execution_policy":{"handler_key":"agent.run","handler_version":"1","connection_id":"conn_1","credential_revision":` + revision + `}}`
				}
				c := command(id, id, "spec.publish", "spec_revision", nil, map[string]any{})
				c.Payload = json.RawMessage(`{"family_id":"` + id + `","revision":1,"kind":"` + kind + `","spec":` + spec + `}`)
				return c
			}
			publishDuplicatePin := func(id, kind, discarded string) Command {
				spec := `{"name":"Send","input_schema":{"type":"object"},"output_schema":{"type":"object"},"binding":` + discarded + `,"binding":{"kind":"connector","handler_version":"1","provider":"slack","operation":"chat.postMessage"}}`
				if kind == "participant" {
					spec = `{"name":"Agent","instructions":"Do work","execution_policy":` + discarded + `,"execution_policy":{"handler_key":"agent.run","handler_version":"1"}}`
				}
				c := command(id, id, "spec.publish", "spec_revision", nil, map[string]any{})
				c.Payload = json.RawMessage(`{"family_id":"` + id + `","revision":1,"kind":"` + kind + `","spec":` + spec + `}`)
				return c
			}
			for _, kind := range []string{"function", "participant"} {
				for i, revision := range []string{"0", "1.0", "1e0", "9007199254740992", "9007199254740993", "1.0000000000000001", "9007199254740990.5", `"1"`, "null", "true"} {
					_, err := executor.Execute(ctx, "dev", publish("credential_invalid_"+kind+"_"+string(rune('a'+i)), kind, revision))
					var commandErr *CommandError
					if !errors.As(err, &commandErr) || commandErr.Outcome != "schema_mismatch" {
						t.Fatalf("%s credential revision %s: expected schema_mismatch, got %v", kind, revision, err)
					}
				}
				receipt, err := executor.Execute(ctx, "dev", publish("credential_safe_"+kind, kind, "9007199254740991"))
				if err != nil {
					t.Fatalf("%s safe credential revision: %v", kind, err)
				}
				path := "$.binding.credential_revision"
				if kind == "participant" {
					path = "$.execution_policy.credential_revision"
				}
				var stored int64
				if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT json_extract(payload_json,?) FROM spec_revisions WHERE id=?`, path, receipt.Aggregate.ID).Scan(&stored); err != nil || stored != maxCredentialRevision {
					t.Fatalf("%s stored revision=%d err=%v", kind, stored, err)
				}
				for i, discarded := range []string{`{"credential_revision":1.5}`, "false"} {
					if _, err := executor.Execute(ctx, "dev", publishDuplicatePin("credential_duplicate_"+kind+"_"+string(rune('a'+i)), kind, discarded)); err != nil {
						t.Fatalf("%s duplicate pin should use the final binding: %v", kind, err)
					}
				}
			}
		})
	}
}

func TestCredentialPinExternalFunctionSnapshot(t *testing.T) {
	binding := map[string]any{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "connection_id": "conn_1", "credential_revision": 7}
	effect, err := requestExternalFunction("wrk_1", 2, "task_1", "fn_1", "connector", binding, map[string]any{"text": "hello"}, map[string]any{}, &Actor{Type: "human", ID: "dev"}, command("credential_function", "credential_function", "function.invoke", "work_stream", nil, map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	assertCredentialPinMap(t, effect.outboxPayload["_compos"].(map[string]any))
	assertCredentialPinMap(t, effect.journal[0].payload)
}

func TestCredentialPinSnapshotsFunctionAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"sqlite", "rhiza"} {
		t.Run(kind, func(t *testing.T) {
			var db *store.Store
			if kind == "rhiza" {
				db = openRhizaKernelStore(t, ctx, "credential-pin-function")
			} else {
				var err error
				db, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				now := "2026-01-01T00:00:00Z"
				if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
					t.Fatal(err)
				}

			}
			defer db.Close()
			executor := testStoreExecutor(db)
			task := execute(t, executor, "dev", command("credential_function_task", "credential_function_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "credential-function-task", "revision": 1, "kind": "task", "spec": map[string]any{"name": "Credential task", "goal": "Use pinned credential", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "completion": map[string]any{"kind": "submission_valid"}}}))
			function := execute(t, executor, "dev", command("credential_function", "credential_function", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "credential-function", "revision": 1, "kind": "function", "spec": map[string]any{"name": "Send", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "binding": map[string]any{"kind": "connector", "handler_version": "1", "provider": "slack", "operation": "chat.postMessage", "connection_id": "conn_1", "credential_revision": 7}}}))
			work := execute(t, executor, "dev", command("credential_function_work", "credential_function_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}}))
			workID := work.Aggregate.ID
			execute(t, executor, "dev", command("credential_function_route", "credential_function_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "human", "id": "dev"}}))
			invoke := command("credential_function_invoke", "credential_function_invoke", "function.invoke", "work_stream", &workID, map[string]any{"function_revision_id": function.Aggregate.ID, "input": map[string]any{}})
			execute(t, executor, "dev", invoke)
			var outboxBefore, journalBefore string
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM outbox_events WHERE command_id=?`, invoke.CommandID).Scan(&outboxBefore); err != nil {
				t.Fatal(err)
			}
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM journal_records WHERE command_id=? AND kind='function.requested'`, invoke.CommandID).Scan(&journalBefore); err != nil {
				t.Fatal(err)
			}
			if duplicate := execute(t, executor, "dev", invoke); duplicate.Outcome != "duplicate" {
				t.Fatalf("duplicate=%+v", duplicate)
			}
			var outboxAfter, journalAfter string
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM outbox_events WHERE command_id=?`, invoke.CommandID).Scan(&outboxAfter); err != nil || outboxBefore != outboxAfter {
				t.Fatalf("outbox changed err=%v before=%s after=%s", err, outboxBefore, outboxAfter)
			}
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM journal_records WHERE command_id=? AND kind='function.requested'`, invoke.CommandID).Scan(&journalAfter); err != nil || journalBefore != journalAfter {
				t.Fatalf("journal changed err=%v before=%s after=%s", err, journalBefore, journalAfter)
			}
			assertCredentialPinJSON(t, outboxBefore)
			assertCredentialPinJSON(t, journalBefore)
		})
	}
}

func TestCredentialPinSnapshotsAgentAcrossStores(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"sqlite", "rhiza"} {
		t.Run(kind, func(t *testing.T) {
			var db *store.Store
			if kind == "rhiza" {
				db = openRhizaKernelStore(t, ctx, "credential-pin-agent")
			} else {
				var err error
				db, err = store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "compos.sqlite")})
				if err != nil {
					t.Fatal(err)
				}
				now := "2026-01-01T00:00:00Z"
				if _, err := db.DB().Exec(`INSERT INTO workspaces VALUES('wks_1','one','One',1,?,?)`, now, now); err != nil {
					t.Fatal(err)
				}

			}
			defer db.Close()
			executor := testStoreExecutor(db)
			task := execute(t, executor, "dev", command("credential_task", "credential_task", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "credential-task", "revision": 1, "kind": "task", "spec": map[string]any{"name": "Credential task", "goal": "Use pinned credential", "input_schema": map[string]any{"type": "object"}, "output_schema": map[string]any{"type": "object"}, "completion": map[string]any{"kind": "submission_valid"}}}))
			agent := execute(t, executor, "dev", command("credential_agent", "credential_agent", "spec.publish", "spec_revision", nil, map[string]any{"family_id": "credential-agent", "revision": 1, "kind": "participant", "spec": map[string]any{"name": "Credential agent", "instructions": "Use the pinned connection", "execution_policy": map[string]any{"handler_key": "agent.run", "handler_version": "1", "connection_id": "conn_1", "credential_revision": 7}}}))
			work := execute(t, executor, "dev", command("credential_work", "credential_work", "work.open", "work_stream", nil, map[string]any{"task_revision_id": task.Aggregate.ID, "context": map[string]any{}}))
			workID := work.Aggregate.ID
			route := command("credential_route", "credential_route", "work.route", "work_stream", &workID, map[string]any{"participant": map[string]any{"kind": "agent", "id": agent.Aggregate.ID}})
			execute(t, executor, "dev", route)
			var outboxBefore, journal string
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM outbox_events WHERE command_id=?`, route.CommandID).Scan(&outboxBefore); err != nil {
				t.Fatal(err)
			}
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM journal_records WHERE command_id=? AND kind='agent.requested'`, route.CommandID).Scan(&journal); err != nil {
				t.Fatal(err)
			}
			duplicate := execute(t, executor, "dev", route)
			if duplicate.Outcome != "duplicate" {
				t.Fatalf("duplicate=%+v", duplicate)
			}
			var outboxAfter string
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM outbox_events WHERE command_id=?`, route.CommandID).Scan(&outboxAfter); err != nil || outboxBefore != outboxAfter {
				t.Fatalf("outbox changed err=%v before=%s after=%s", err, outboxBefore, outboxAfter)
			}
			assertCredentialPinJSON(t, outboxBefore)
			assertCredentialPinJSON(t, journal)
			var journalAfter string
			if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT payload_json FROM journal_records WHERE command_id=? AND kind='agent.requested'`, route.CommandID).Scan(&journalAfter); err != nil || journal != journalAfter {
				t.Fatalf("journal changed err=%v before=%s after=%s", err, journal, journalAfter)
			}
			var outbox map[string]any
			if err := json.Unmarshal([]byte(outboxBefore), &outbox); err != nil {
				t.Fatal(err)
			}
			agentDefinition, _ := outbox["agent_definition"].(map[string]any)
			executionPolicy, _ := agentDefinition["execution_policy"].(map[string]any)
			assertCredentialPinMap(t, executionPolicy)
		})
	}
}

func assertCredentialPinMap(t *testing.T, value map[string]any) {
	t.Helper()
	revision, ok := integer(value["credential_revision"])
	if value["connection_id"] != "conn_1" || !ok || revision != 7 {
		t.Fatalf("credential pin=%v", value)
	}
}

func assertCredentialPinJSON(t *testing.T, raw string) {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	if compos, ok := value["_compos"].(map[string]any); ok {
		value = compos
	}
	assertCredentialPinMap(t, value)
}
