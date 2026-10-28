package bridge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/beak"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
)

func TestSessionEventsMustNotOverwriteWeixinPollingHealth(t *testing.T) {
	for _, tc := range []struct {
		name      string
		streamErr error
	}{{"clean_eof", nil}, {"session_error", errors.New("session stream failed")}} {
		t.Run(tc.name, func(t *testing.T) {
			a := &state.AccountState{AccountID: "account-1", StreamConnectionState: sdk.RuntimeHealthStateConnected}
			a.EnsureMaps()
			r := testRunner(newMemoryStore(), a)
			r.options.StreamReconnect = time.Second
			r.beak = &fakeBeak{streamResults: []error{tc.streamErr}, streamBlock: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); r.streamLoop(ctx, "peer-1", "sess-1") }()
			waitForCondition(t, 100*time.Millisecond, func() bool { return r.beak.(*fakeBeak).streamCallCount() == 1 })
			time.Sleep(10 * time.Millisecond)
			r.mu.Lock()
			got := a.StreamConnectionState
			r.mu.Unlock()
			cancel()
			<-done
			if got != sdk.RuntimeHealthStateConnected {
				t.Fatalf("session stream must not overwrite polling status; got %q", got)
			}
		})
	}
}
func TestSessionHeartbeatDoesNotMaskPollingFailure(t *testing.T) {
	a := &state.AccountState{AccountID: "account-1", StreamConnectionState: sdk.RuntimeHealthStateReconnectFailed, StreamLastError: "poll failed"}
	a.EnsureMaps()
	r := testRunner(newMemoryStore(), a)
	if err := r.ProcessStreamEvent(context.Background(), "peer-1", beak.StreamEvent{EventType: "heartbeat"}); err != nil {
		t.Fatal(err)
	}
	if a.StreamConnectionState != sdk.RuntimeHealthStateReconnectFailed || a.StreamLastError != "poll failed" {
		t.Fatalf("session heartbeat masked polling failure: state=%q err=%q", a.StreamConnectionState, a.StreamLastError)
	}
}

func TestSessionHealthRetainsErrorAndIsBounded(t *testing.T) {
	a := &state.AccountState{AccountID: "account-1", StreamConnectionState: sdk.RuntimeHealthStateConnected}
	a.EnsureMaps()
	r := testRunner(newMemoryStore(), a)
	for i := 0; i < 257; i++ {
		if err := r.markSessionStream(context.Background(), fmt.Sprintf("session-%d", i), sdk.RuntimeHealthStateReconnectFailed, errors.New("connection failed")); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.SessionStreamHealth) != 256 {
		t.Fatalf("expected bounded session health, got %d", len(a.SessionStreamHealth))
	}
	if h := a.SessionStreamHealth["session-256"]; h.State != sdk.RuntimeHealthStateReconnectFailed || h.LastError != "connection failed" || h.UpdatedAt.IsZero() {
		t.Fatalf("missing error diagnostics: %+v", h)
	}
	if a.StreamConnectionState != sdk.RuntimeHealthStateConnected {
		t.Fatalf("session failures changed polling health: %q", a.StreamConnectionState)
	}
}
