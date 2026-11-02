package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/compos/internal/cli/ledger"
)

func defaultLedgerPath() string {
	if value := strings.TrimSpace(os.Getenv("COMPOS_LEDGER_PATH")); value != "" {
		return value
	}
	return defaultDataPath("ledger.db", "compos-ledger.db")
}

func defaultDataPath(filename, fallback string) string {
	if runtime.GOOS == "windows" {
		if localAppData := strings.TrimSpace(os.Getenv("LocalAppData")); localAppData != "" {
			return filepath.Join(localAppData, "Compos", filename)
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return fallback
	}
	if runtime.GOOS == "windows" {
		return filepath.Join(home, "AppData", "Local", "Compos", filename)
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "Compos", filename)
	}
	return filepath.Join(home, ".local", "share", "compos", filename)
}

func removedTokenFlagError() error {
	return fmt.Errorf("--token has been removed; use the external identity integration")
}

func hasRemovedTokenFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--token" || strings.HasPrefix(arg, "--token=") {
			return true
		}
	}
	return false
}

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

func parseJSONObject(raw, name string) (map[string]any, error) {
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("%s must be JSON object: %w", name, err)
	}
	if value == nil {
		return nil, fmt.Errorf("%s must be JSON object", name)
	}
	return value, nil
}

func jsonHash(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func trimSlash(value string) string {
	return strings.TrimRight(value, "/")
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func newID(prefix string) string {
	if prefix == "cmd" {
		return newUUID()
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UTC().UnixNano(), hex.EncodeToString(b[:]))
}

func readHTTPBody(body io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, maxHTTPResponseBodyBytes))
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func truncateBodyForMessage(body []byte, limit int) string {
	trimmed := bytes.TrimSpace(body)
	if limit <= 0 || len(trimmed) <= limit {
		return string(trimmed)
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(trimmed[cut]) {
		cut--
	}
	return string(trimmed[:cut]) + "...(truncated)"
}

func shutdownHTTPServer(server *http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

func defaultJournal() (closeableCommandJournal, error) {
	path := defaultLedgerPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	return ledger.NewBboltJournal(path)
}
