package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

const crashQuery = `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES(?,?,?,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`

func TestReleaseCrashWriter(t *testing.T) {
	encoded := os.Getenv("COMPOS_CRASH_CONFIG")
	if encoded == "" {
		return
	}
	var config rhiza.Config
	if err := json.Unmarshal([]byte(encoded), &config); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := OpenRhiza(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Exec(ctx, "crash-write", crashQuery, "crash", "crash", "Acknowledged"); err != nil {
		t.Fatal(err)
	}
	// A dedicated pipe signals the acknowledged write, independent of log output.
	if _, err := os.NewFile(3, "ready").WriteString("ready\n"); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestReleaseSingleNodeCrashRecovery(t *testing.T) {
	for _, provider := range releaseProviders() {
		t.Run(provider, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			config := rhiza.Config{ClusterID: "release-crash", NodeID: "node-1", DataDir: t.TempDir(), PeerAddr: "127.0.0.1:0", ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck}
			configureReleaseProvider(t, ctx, provider, &config)
			encoded, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReleaseCrashWriter$", "-test.timeout=75s")
			cmd.Env = append(os.Environ(), "COMPOS_CRASH_CONFIG="+string(encoded))
			cmd.ExtraFiles = []*os.File{writer}
			cmd.Stderr = os.Stderr
			cmd.Stdout = os.Stderr
			if err := cmd.Start(); err != nil {
				writer.Close()
				t.Fatal(err)
			}
			writer.Close()
			running := true
			defer func() {
				if running {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			line, err := bufio.NewReader(reader).ReadString('\n')
			if err != nil || line != "ready\n" {
				t.Fatalf("writer did not acknowledge: %q: %v", line, err)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			running = false
			if err == nil {
				t.Fatal("writer exited without being killed")
			}
			config.DataDir = t.TempDir()
			recovered, err := OpenRhiza(ctx, config)
			if err != nil {
				t.Fatalf("open from archive after process kill: %v", err)
			}
			defer recovered.Close()
			var name string
			if err := recovered.ReadRow(ctx, ReadLinearizable, `SELECT name FROM workspaces WHERE id='crash'`).Scan(&name); err != nil || name != "Acknowledged" {
				t.Fatalf("recovered name=%q err=%v", name, err)
			}
			if err := recovered.Exec(ctx, "crash-write", crashQuery, "different", "different", "Different"); !errors.Is(err, rhiza.ErrRequestConflict) {
				t.Fatalf("changed request must conflict: %v", err)
			}
			if err := recovered.Exec(ctx, "crash-write", crashQuery, "crash", "crash", "Acknowledged"); err != nil {
				t.Fatalf("replay after crash: %v", err)
			}
			var count int
			if err := recovered.ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("workspace count=%d err=%v", count, err)
			}
		})
	}
}
