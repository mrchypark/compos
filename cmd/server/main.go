package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mrchypark/compos/internal/server"
	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func main() {
	bind := flag.String("bind", env("COMPOS_BIND", "127.0.0.1:3000"), "listen address")
	database := flag.String("database", env("COMPOS_SQLITE_PATH", "compos.sqlite"), "SQLite database path")
	rhizaData := flag.String("rhiza-data", env("COMPOS_RHIZA_DATA_DIR", ""), "embedded Rhiza data directory (enables SQL HA)")
	conductor := flag.String("conductor", env("COMPOS_CONDUCTOR_URL", ""), "optional Conductor execution service URL")
	searchURL := flag.String("meilisearch", env("COMPOS_MEILISEARCH_URL", ""), "optional Meilisearch URL")
	searchIndex := flag.String("search-index", env("COMPOS_MEILISEARCH_INDEX_UID", "compos-objects"), "Meilisearch index UID")
	backup := flag.String("backup", "", "write an online backup and exit")
	flag.Parse()
	ctx := context.Background()
	var db *store.Store
	var err error
	rhizaMemberCount := 0
	if *rhizaData != "" {
		clusterID, nodeID := env("COMPOS_RHIZA_CLUSTER_ID", "compos"), env("COMPOS_RHIZA_NODE_ID", "node-1")
		members, peerToken, err := rhizaMembers(os.Getenv("COMPOS_RHIZA_MEMBERS"), os.Getenv("COMPOS_RHIZA_PEER_TOKENS"), clusterID, nodeID, os.Getenv("COMPOS_RHIZA_PEER_TOKEN"))
		if err != nil {
			log.Fatalf("Rhiza peer credentials: %v", err)
		}
		rhizaMemberCount = len(members)
		insecure, err := strconv.ParseBool(env("COMPOS_RHIZA_OBJECT_INSECURE", "false"))
		if err != nil {
			log.Fatalf("parse COMPOS_RHIZA_OBJECT_INSECURE: %v", err)
		}
		provider, objectDir := os.Getenv("COMPOS_RHIZA_OBJECT_PROVIDER"), os.Getenv("COMPOS_RHIZA_OBJECT_DIR")
		objectEndpoint, objectBucket := os.Getenv("COMPOS_RHIZA_OBJECT_ENDPOINT"), os.Getenv("COMPOS_RHIZA_OBJECT_BUCKET")
		objectEndpoint = strings.TrimPrefix(strings.TrimPrefix(objectEndpoint, "http://"), "https://")
		durability := env("COMPOS_RHIZA_OBJECT_DURABILITY", "async")
		if provider != "" || objectDir != "" || objectEndpoint != "" || objectBucket != "" {
			durability = env("COMPOS_RHIZA_OBJECT_DURABILITY", "before-ack")
		}
		if len(members) > 1 && durability != string(rhiza.ObjectStoreDurabilityBeforeAck) {
			log.Fatal("multi-node Rhiza requires COMPOS_RHIZA_OBJECT_DURABILITY=before-ack")
		}
		db, err = store.OpenRhiza(ctx, rhiza.Config{
			ClusterID: clusterID, NodeID: nodeID,
			DataDir: *rhizaData, PeerAddr: env("COMPOS_RHIZA_PEER_ADDR", "127.0.0.1:0"), PeerToken: peerToken, Members: members, AdminToken: os.Getenv("COMPOS_RHIZA_ADMIN_TOKEN"),
			ObjStoreEndpoint: objectEndpoint, ObjStoreBucket: objectBucket,
			ObjStoreProvider: provider, ObjStoreDir: objectDir, ObjStorePrefix: os.Getenv("COMPOS_RHIZA_OBJECT_PREFIX"),
			ObjStoreRegion: os.Getenv("COMPOS_RHIZA_OBJECT_REGION"), ObjStoreAccessKey: os.Getenv("COMPOS_RHIZA_OBJECT_ACCESS_KEY"),
			ObjStoreSecretKey: os.Getenv("COMPOS_RHIZA_OBJECT_SECRET_KEY"), ObjStoreSessionToken: os.Getenv("COMPOS_RHIZA_OBJECT_SESSION_TOKEN"),
			ObjStoreInsecure: insecure, ObjStoreDurability: rhiza.ObjectStoreDurability(durability),
		})
	} else {
		db, err = store.Open(ctx, store.Options{Path: *database})
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	maintenanceDone := make(chan struct{})
	defer close(maintenanceDone)
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := db.Optimize(context.Background()); err != nil {
					log.Printf("sqlite planner optimization failed: %v", err)
				}
			case <-maintenanceDone:
				return
			}
		}
	}()
	if *backup != "" {
		if err := db.Backup(ctx, *backup); err != nil {
			log.Fatal(err)
		}
		return
	}
	handler := server.New(db)
	syncerWorkspaceID := os.Getenv("COMPOS_SEARCH_SYNCER_WORKSPACE_ID")
	if err := handler.ConfigureSearchSyncer(syncerWorkspaceID); err != nil {
		log.Fatal(err)
	}
	if err := handler.EnableSearch(*searchURL, *searchIndex, os.Getenv("COMPOS_MEILISEARCH_API_KEY")); err != nil {
		log.Fatal(err)
	}
	if *conductor != "" {
		if err := handler.EnableConductor(*conductor, os.Getenv("COMPOS_CONDUCTOR_TOKEN")); err != nil {
			log.Fatal(err)
		}
	}
	defer handler.Close()
	httpServer := &http.Server{Addr: *bind, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stopped
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}()
	log.Printf("compos-server topology=%s", topologyLabel(db.Kind(), rhizaMemberCount))
	log.Printf("compos-server listening on http://%s", *bind)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func topologyLabel(storage string, members int) string {
	if storage == "sqlite" {
		return "standalone/sqlite"
	}
	if members > 1 {
		return "ha/rhiza"
	}
	return "standalone/rhiza"
}

// rhizaMembers derives public membership identities from the private per-node
// token map without putting those tokens in COMPOS_RHIZA_MEMBERS.
func rhizaMembers(rawMembers, rawTokens, clusterID, nodeID, scalarToken string) ([]rhiza.Member, string, error) {
	var members []rhiza.Member
	if rawMembers != "" {
		if err := json.Unmarshal([]byte(rawMembers), &members); err != nil {
			return nil, "", fmt.Errorf("parse COMPOS_RHIZA_MEMBERS: %w", err)
		}
	}
	if rawTokens == "" {
		return members, scalarToken, nil
	}
	if scalarToken != "" {
		return nil, "", errors.New("COMPOS_RHIZA_PEER_TOKENS conflicts with COMPOS_RHIZA_PEER_TOKEN")
	}
	var tokens map[string]string
	if err := json.Unmarshal([]byte(rawTokens), &tokens); err != nil {
		return nil, "", fmt.Errorf("parse COMPOS_RHIZA_PEER_TOKENS: %w", err)
	}
	peerToken := tokens[nodeID]
	if peerToken == "" {
		return nil, "", fmt.Errorf("COMPOS_RHIZA_PEER_TOKENS has no entry for node %q", nodeID)
	}
	for index := range members {
		token := tokens[string(members[index].ID)]
		if token == "" {
			return nil, "", fmt.Errorf("COMPOS_RHIZA_PEER_TOKENS has no entry for member %q", members[index].ID)
		}
		derived := rhiza.PeerPublicKey(clusterID, string(members[index].ID), token)
		if members[index].PublicKey != (rhiza.PublicKey{}) && members[index].PublicKey != derived {
			return nil, "", fmt.Errorf("member %q public key does not match its peer token", members[index].ID)
		}
		members[index].PublicKey = derived
	}
	return members, peerToken, nil
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
