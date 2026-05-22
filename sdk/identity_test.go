package sdk

import "testing"

func TestIdentityHelpers(t *testing.T) {
	if got := ChatSourceID("weixin", "account-1", ChatTypeGroup, "group-1"); got != "weixin:account-1:group:group-1" {
		t.Fatalf("source id=%q", got)
	}
	if got := IMPersonParticipantID("weixin", ChatTypeGroup, "group-1", "user-1"); got != "im:weixin:group:group-1:user:user-1" {
		t.Fatalf("participant id=%q", got)
	}
	if got := BridgeParticipantID("weixin"); got != "bridge:weixin" {
		t.Fatalf("bridge id=%q", got)
	}
}
