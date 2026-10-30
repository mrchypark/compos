package server

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/compos/internal/store"
	"github.com/mrchypark/rhiza"
)

func TestBootstrapWorkspaceAndLoginEnrollment(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenRhiza(ctx, rhiza.Config{ClusterID: "bootstrap-test", NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(db)
	defer s.Close()
	id, err := s.BootstrapLoginWorkspace(ctx, "Compos")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.BootstrapLoginWorkspace(ctx, "Compos")
	if err != nil || second != id {
		t.Fatalf("bootstrap replay=%s err=%v", second, err)
	}
	identity := verifiedIdentity{principalID: "ppr_test", providerKey: "pocketbase:test", provider: "pocketbase", email: "user@example.invalid", name: "User", expiresAt: time.Now().Add(time.Hour).Unix()}
	for range 2 {
		result, failure := s.issueSession(ctx, identity, "", id)
		if failure != nil {
			t.Fatal(failure)
		}
		if result["workspace_id"] != id {
			t.Fatal("wrong login workspace")
		}
	}
	var count int
	if err := db.ReadRow(ctx, store.ReadLinearizable, `SELECT COUNT(*) FROM workspace_memberships WHERE workspace_id=? AND principal_id=? AND role='member'`, id, identity.principalID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("memberships=%d err=%v", count, err)
	}
}
