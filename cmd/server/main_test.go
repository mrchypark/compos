package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestTopologyLabel(t *testing.T) {
	for _, test := range []struct {
		storage string
		members int
		want    string
	}{
		{"sqlite", 0, "standalone/sqlite"},
		{"rhiza", 0, "standalone/rhiza"},
		{"rhiza", 1, "standalone/rhiza"},
		{"rhiza", 2, "ha/rhiza"},
	} {
		if got := topologyLabel(test.storage, test.members); got != test.want {
			t.Fatalf("%s/%d = %q, want %q", test.storage, test.members, got, test.want)
		}
	}
}

func TestRhizaMembersDerivePublicKeysFromPeerTokens(t *testing.T) {
	members, token, err := rhizaMembers(`[{"node_id":"n1","peer_url":"quic://n1"},{"node_id":"n2","peer_url":"quic://n2"}]`, `{"n1":"token-1","n2":"token-2"}`, "cluster", "n2", "")
	if err != nil || token != "token-2" {
		t.Fatalf("members=%+v token=%q err=%v", members, token, err)
	}
	if members[0].PublicKey != rhiza.PeerPublicKey("cluster", "n1", "token-1") || members[1].PublicKey != rhiza.PeerPublicKey("cluster", "n2", "token-2") {
		t.Fatalf("members did not receive their derived public keys: %+v", members)
	}
}

func TestRhizaMembersRejectInvalidPeerCredentials(t *testing.T) {
	const members = `[{"node_id":"n1","peer_url":"quic://n1"},{"node_id":"n2","peer_url":"quic://n2"}]`
	key := rhiza.PeerPublicKey("cluster", "n1", "token-1")
	for name, input := range map[string]struct {
		members, tokens, scalar string
	}{
		"missing local token":  {members, `{"n1":"token-1"}`, ""},
		"missing member token": {members, `{"n2":"token-2"}`, ""},
		"scalar conflicts":     {members, `{"n1":"token-1","n2":"token-2"}`, "token-2"},
		"mismatched key":       {`[{"node_id":"n1","peer_url":"quic://n1","public_key":"` + encodeKey(key) + `"},{"node_id":"n2","peer_url":"quic://n2"}]`, `{"n1":"wrong","n2":"token-2"}`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := rhizaMembers(input.members, input.tokens, "cluster", "n2", input.scalar); err == nil {
				t.Fatal("expected peer credential rejection")
			}
		})
	}
}

func TestRhizaMembersValidateExplicitPublicKey(t *testing.T) {
	key := rhiza.PeerPublicKey("cluster", "n1", "token-1")
	members := `[{"node_id":"n1","peer_url":"quic://n1","public_key":"` + encodeKey(key) + `"}]`
	if _, _, err := rhizaMembers(members, `{"n1":"token-1"}`, "cluster", "n1", ""); err != nil {
		t.Fatalf("matching public key rejected: %v", err)
	}
	if _, _, err := rhizaMembers(members, `{"n1":"wrong"}`, "cluster", "n1", ""); err == nil || !strings.Contains(err.Error(), "public key does not match its peer token") {
		t.Fatalf("expected public key mismatch, got %v", err)
	}
}

func encodeKey(key rhiza.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key[:])
}
