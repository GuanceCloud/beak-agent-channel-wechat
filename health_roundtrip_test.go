package beakweixin

import (
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
	"testing"
	"time"
)

func TestSessionHealthSurvivesAccountStateRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	source := AccountState{AccountID: "bot", SessionStreamHealth: map[string]state.SessionStreamHealth{"session-1": {State: "reconnect_failed", LastError: "session unavailable", UpdatedAt: now}}}
	out := sdkAccountToState(sdk.ChannelAccount{UUID: "bot", State: stateToMap(source)})
	h := out.SessionStreamHealth["session-1"]
	if h.State != "reconnect_failed" || h.LastError != "session unavailable" || !h.UpdatedAt.Equal(now) {
		t.Fatalf("session health roundtrip: %+v", h)
	}
}
