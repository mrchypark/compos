package store

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza"
)

func TestEmbeddedRhizaThreePeersKeepWritingAfterOneStops(t *testing.T) {
	endpoint := os.Getenv("COMPOS_RHIZA_CLUSTER_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set COMPOS_RHIZA_CLUSTER_TEST_ENDPOINT to an S3-compatible endpoint")
	}
	members := []rhiza.Member{
		{ID: "n1", URL: "http://127.0.0.1:19201", PeerURL: "quic://127.0.0.1:19101", PublicKey: rhiza.PeerPublicKey("compos-3peer", "n1", "test-voter-token-1")},
		{ID: "n2", URL: "http://127.0.0.1:19202", PeerURL: "quic://127.0.0.1:19102", PublicKey: rhiza.PeerPublicKey("compos-3peer", "n2", "test-voter-token-2")},
		{ID: "n3", URL: "http://127.0.0.1:19203", PeerURL: "quic://127.0.0.1:19103", PublicKey: rhiza.PeerPublicKey("compos-3peer", "n3", "test-voter-token-3")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := minio.New(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), &minio.Options{
		Creds: credentials.NewStaticV4("compos", "compos-secret", ""), Secure: strings.HasPrefix(endpoint, "https://"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MakeBucket(ctx, "compos-rhiza", minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		if exists, lookupErr := client.BucketExists(ctx, "compos-rhiza"); lookupErr != nil || !exists {
			t.Fatalf("create test bucket: %v", err)
		}
	}
	prefix := "compos-test-" + uuid.NewString()
	dataDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	config := func(index int) rhiza.Config {
		member := members[index]
		return rhiza.Config{
			ClusterID: "compos-3peer", NodeID: string(member.ID), DataDir: dataDirs[index],
			PeerAddr: member.PeerURL[len("quic://"):], AdminToken: "test-peer-token", Members: members,
			PeerToken:        []string{"test-voter-token-1", "test-voter-token-2", "test-voter-token-3"}[index],
			ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), ObjStoreBucket: "compos-rhiza", ObjStorePrefix: prefix,
			ObjStoreRegion: "us-east-1", ObjStoreInsecure: true, ObjStoreAccessKey: "compos", ObjStoreSecretKey: "compos-secret",
			ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
		}
	}
	type result struct {
		index int
		store *Store
		err   error
	}
	opened := make(chan result, len(members))
	for index := range members {
		go func(index int) {
			s, err := OpenRhiza(ctx, config(index))
			opened <- result{index: index, store: s, err: err}
		}(index)
	}
	stores := make([]*Store, len(members))
	for range members {
		result := <-opened
		if result.err != nil {
			t.Fatal(result.err)
		}
		stores[result.index] = result.store
	}
	defer stores[0].Close()
	defer stores[1].Close()
	defer func() { _ = stores[2].Close() }()
	for index, s := range stores {
		for !s.Ready() {
			select {
			case <-ctx.Done():
				t.Fatalf("peer %d did not become ready: %v", index, ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}

	insertWorkspace := func(s *Store, id string) error {
		return s.Exec(ctx, "cluster-"+id, `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES(?,?,?,1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, id, id, id)
	}
	if err := insertWorkspace(stores[0], "before-stop"); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	raced := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func(index int) {
			<-start
			suffix := []string{"a", "b"}[index]
			raced <- stores[index].Exec(ctx, "cluster-race-"+suffix, `INSERT INTO workspaces(id,key,name,version,created_at,updated_at) VALUES(?, 'same-key', ?, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, "race-"+suffix, "race")
		}(index)
	}
	close(start)
	succeeded := 0
	for range 2 {
		if err := <-raced; err == nil {
			succeeded++
		}
	}
	var count int
	if err := stores[0].ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces WHERE key='same-key'`).Scan(&count); err != nil || succeeded != 1 || count != 1 {
		t.Fatalf("concurrent unique-key writes succeeded=%d count=%d err=%v", succeeded, count, err)
	}
	if err := stores[2].Close(); err != nil {
		t.Fatal(err)
	}
	for {
		probe, err := net.ListenPacket("udp", "127.0.0.1:19103")
		if err == nil {
			_ = probe.Close()
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stopped peer did not release its QUIC port: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := insertWorkspace(stores[1], "after-stop"); err != nil {
		t.Fatalf("two-peer quorum write failed: %v", err)
	}
	if err := stores[1].ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces WHERE id IN ('before-stop','after-stop')`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("committed workspace count=%d err=%v", count, err)
	}
	lost := config(2)
	lost.DataDir = t.TempDir()
	if unexpected, err := OpenRhiza(ctx, lost); !errors.Is(err, rhiza.ErrVoterStateLost) {
		if unexpected != nil {
			_ = unexpected.Close()
		}
		t.Fatalf("lost voter state must fail closed: %v", err)
	}
	restarted, err := OpenRhiza(ctx, config(2))
	if err != nil {
		t.Fatalf("restart stopped peer: %v", err)
	}
	defer restarted.Close()
	for {
		err = restarted.ReadRow(ctx, ReadLocal, `SELECT COUNT(*) FROM workspaces WHERE id IN ('before-stop','after-stop') OR key='same-key'`).Scan(&count)
		if err == nil && count == 3 && restarted.Ready() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restarted peer did not catch up: count=%d err=%v", count, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err := insertWorkspace(restarted, "after-restart"); err != nil {
		t.Fatalf("restarted peer write failed: %v", err)
	}
	if err := stores[0].ReadRow(ctx, ReadLinearizable, `SELECT COUNT(*) FROM workspaces WHERE id='after-restart'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("restarted peer write did not replicate: count=%d err=%v", count, err)
	}
	t.Log("embedded Rhiza serialized competing writes, survived one stopped node, rejected lost voter state, restarted with retained state, and accepted a post-recovery write")
}
