// storage-import loads a verified v2 application export into a fresh Rhiza v0.19 store.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func main() {
	inputPath := flag.String("input", env("COMPOS_LOGICAL_IMPORT_PATH", ""), "logical export JSON file")
	verifyOnly := flag.Bool("verify-only", false, "verify an already imported target")
	rhizaData := flag.String("rhiza-data", env("COMPOS_RHIZA_DATA_DIR", ""), "fresh Rhiza data directory")
	clusterID := flag.String("cluster-id", env("COMPOS_RHIZA_CLUSTER_ID", ""), "fresh Rhiza cluster ID")
	nodeID := flag.String("node-id", env("COMPOS_RHIZA_NODE_ID", ""), "fresh Rhiza node ID")
	flag.Parse()
	if *inputPath == "" || *rhizaData == "" || !validRhizaIdentity(*clusterID) || !validRhizaIdentity(*nodeID) {
		log.Fatal("input, rhiza-data, cluster-id, and node-id are required")
	}
	info, err := os.Stat(*inputPath)
	if err != nil {
		log.Fatalf("stat input: %v", err)
	}
	if info.Size() <= 0 || info.Size() > 256<<20 {
		log.Fatal("input must be between 1 and 268435456 bytes")
	}
	data, err := os.ReadFile(*inputPath)
	if err != nil {
		log.Fatalf("read input: %v", err)
	}
	input, fileSHA, err := store.DecodeLogicalImport(data)
	if err != nil {
		log.Fatalf("validate input: %v", err)
	}
	insecure, err := strconv.ParseBool(env("COMPOS_RHIZA_OBJECT_INSECURE", "false"))
	if err != nil {
		log.Fatalf("parse COMPOS_RHIZA_OBJECT_INSECURE: %v", err)
	}
	provider, bucket, prefix := os.Getenv("COMPOS_RHIZA_OBJECT_PROVIDER"), os.Getenv("COMPOS_RHIZA_OBJECT_BUCKET"), os.Getenv("COMPOS_RHIZA_OBJECT_PREFIX")
	endpoint := strings.TrimPrefix(strings.TrimPrefix(os.Getenv("COMPOS_RHIZA_OBJECT_ENDPOINT"), "http://"), "https://")
	if provider == "" || bucket == "" || normalizePrefix(prefix) == "" {
		log.Fatal("COMPOS_RHIZA_OBJECT_PROVIDER, COMPOS_RHIZA_OBJECT_BUCKET, and COMPOS_RHIZA_OBJECT_PREFIX are required")
	}
	if invalidObjectPrefix(prefix) || invalidObjectPrefix(input.Source.ObjectPrefix) {
		log.Fatal("Rhiza object-store prefixes must be relative and must not contain traversal")
	}
	if input.Source.ClusterID == *clusterID {
		log.Fatal("target Rhiza cluster-id must differ from source")
	}
	if input.Source.NodeID == *nodeID {
		log.Fatal("target Rhiza node-id must differ from source")
	}
	if !*verifyOnly {
		if err := requireEmptyDataDir(*rhizaData); err != nil {
			log.Fatalf("fresh Rhiza data directory: %v", err)
		}
	}
	ctx := context.Background()
	target, err := store.OpenRhiza(ctx, rhiza.Config{
		ClusterID: *clusterID, NodeID: *nodeID, DataDir: *rhizaData, PeerAddr: env("COMPOS_RHIZA_PEER_ADDR", "127.0.0.1:0"),
		ObjStoreProvider: provider, ObjStoreBucket: bucket, ObjStorePrefix: normalizePrefix(prefix), ObjStoreEndpoint: endpoint,
		ObjStoreRegion: os.Getenv("COMPOS_RHIZA_OBJECT_REGION"), ObjStoreAccessKey: os.Getenv("COMPOS_RHIZA_OBJECT_ACCESS_KEY"),
		ObjStoreSecretKey: os.Getenv("COMPOS_RHIZA_OBJECT_SECRET_KEY"), ObjStoreSessionToken: os.Getenv("COMPOS_RHIZA_OBJECT_SESSION_TOKEN"),
		ObjStoreServiceAccount: os.Getenv("COMPOS_RHIZA_OBJECT_SERVICE_ACCOUNT"), ObjStoreInsecure: insecure,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		log.Fatalf("open fresh Rhiza target: %v", err)
	}
	defer target.Close()
	var summary store.LogicalImportSummary
	if *verifyOnly {
		summary, err = store.VerifyLogicalImport(ctx, target, input, fileSHA)
	} else {
		summary, err = store.ImportLogical(ctx, target, input, fileSHA)
	}
	if err != nil {
		log.Fatalf("logical import: %v", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		log.Fatalf("write summary: %v", err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func requireEmptyDataDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("%s is not empty", filepath.Clean(path))
	}
	return nil
}

func normalizePrefix(prefix string) string { return strings.Trim(strings.TrimSpace(prefix), "/") }

func invalidObjectPrefix(prefix string) bool {
	for _, part := range strings.Split(normalizePrefix(prefix), "/") {
		if part == "." || part == ".." {
			return true
		}
	}
	return false
}

func validRhizaIdentity(value string) bool {
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
