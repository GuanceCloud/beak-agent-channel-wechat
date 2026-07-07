package state

import (
	"fmt"
	"sort"
	"time"
)

const (
	AccountStatusActive        = "active"
	AccountStatusLoginRequired = "login_required"
	maxTrackedStateKeys        = 4096
	trackedStateTTL            = 7 * 24 * time.Hour
)

type AccountState struct {
	AccountID                  string            `json:"account_id"`
	BotToken                   string            `json:"bot_token"`
	BaseURL                    string            `json:"base_url"`
	ILinkUserID                string            `json:"ilink_user_id,omitempty"`
	ILinkBotID                 string            `json:"ilink_bot_id,omitempty"`
	Status                     string            `json:"status,omitempty"`
	LastError                  string            `json:"last_error,omitempty"`
	ChannelLinkSession         string            `json:"channel_link_session,omitempty"`
	GetUpdatesBuf              string            `json:"get_updates_buf,omitempty"`
	ContextTokens              map[string]string `json:"context_tokens,omitempty"`
	TypingTickets              map[string]string `json:"typing_tickets,omitempty"`
	PeerSessions               map[string]string `json:"peer_sessions,omitempty"`
	InboundSeen                map[string]string `json:"inbound_seen,omitempty"`
	SentBeakMessages           map[string]string `json:"sent_beak_messages,omitempty"`
	StreamCursors              map[string]string `json:"stream_cursors,omitempty"`
	StreamConnectionState      string            `json:"stream_connection_state,omitempty"`
	StreamConnectedAt          time.Time         `json:"stream_connected_at,omitempty"`
	StreamDisconnectedAt       time.Time         `json:"stream_disconnected_at,omitempty"`
	StreamLastActivityAt       time.Time         `json:"stream_last_activity_at,omitempty"`
	StreamLastPingAt           time.Time         `json:"stream_last_ping_at,omitempty"`
	StreamLastPongAt           time.Time         `json:"stream_last_pong_at,omitempty"`
	StreamLastEventAt          time.Time         `json:"stream_last_event_at,omitempty"`
	StreamLastError            string            `json:"stream_last_error,omitempty"`
	StreamLastErrorAt          time.Time         `json:"stream_last_error_at,omitempty"`
	StreamReconnectRequestedAt time.Time         `json:"stream_reconnect_requested_at,omitempty"`
	StreamReconnectError       string            `json:"stream_reconnect_error,omitempty"`
	StreamReconnectErrorAt     time.Time         `json:"stream_reconnect_error_at,omitempty"`
	StreamSessionExpired       bool              `json:"stream_session_expired,omitempty"`
	StreamSessionExpiredAt     time.Time         `json:"stream_session_expired_at,omitempty"`
	StreamSessionExpiredReason string            `json:"stream_session_expired_reason,omitempty"`
	StreamSessionExpiredCode   int               `json:"stream_session_expired_code,omitempty"`
	StreamSessionExpiredMsg    string            `json:"stream_session_expired_msg,omitempty"`
	StreamSessionExpiredOp     string            `json:"stream_session_expired_operation,omitempty"`
	UpdatedAt                  time.Time         `json:"updated_at"`
}

func (a *AccountState) EnsureMaps() {
	if a == nil {
		return
	}
	if a.ContextTokens == nil {
		a.ContextTokens = make(map[string]string)
	}
	if a.TypingTickets == nil {
		a.TypingTickets = make(map[string]string)
	}
	if a.PeerSessions == nil {
		a.PeerSessions = make(map[string]string)
	}
	if a.InboundSeen == nil {
		a.InboundSeen = make(map[string]string)
	}
	if a.SentBeakMessages == nil {
		a.SentBeakMessages = make(map[string]string)
	}
	if a.StreamCursors == nil {
		a.StreamCursors = make(map[string]string)
	}
}

func (a *AccountState) LoginRequired() bool {
	return a != nil && a.Status == AccountStatusLoginRequired
}

func (a *AccountState) MarkLoginRequired(reason string) {
	a.MarkLoginRequiredWithDetails(reason, "", 0, "")
}

func (a *AccountState) MarkLoginRequiredWithDetails(reason, operation string, errCode int, errMsg string) {
	if a == nil {
		return
	}
	a.EnsureMaps()
	now := time.Now().UTC()
	a.Status = AccountStatusLoginRequired
	a.LastError = reason
	a.StreamConnectionState = "expired"
	a.StreamLastError = reason
	a.StreamLastErrorAt = now
	a.StreamSessionExpired = true
	a.StreamSessionExpiredAt = now
	a.StreamSessionExpiredReason = reason
	a.StreamSessionExpiredOp = operation
	a.StreamSessionExpiredCode = errCode
	a.StreamSessionExpiredMsg = errMsg
	a.BotToken = ""
	a.GetUpdatesBuf = ""
	a.ContextTokens = make(map[string]string)
	a.TypingTickets = make(map[string]string)
	a.UpdatedAt = now
}

func (a *AccountState) MarkActive() {
	if a == nil {
		return
	}
	a.EnsureMaps()
	a.Status = AccountStatusActive
	a.LastError = ""
	a.StreamConnectionState = ""
	a.StreamConnectedAt = time.Time{}
	a.StreamDisconnectedAt = time.Time{}
	a.StreamLastActivityAt = time.Time{}
	a.StreamLastPingAt = time.Time{}
	a.StreamLastPongAt = time.Time{}
	a.StreamLastEventAt = time.Time{}
	a.StreamSessionExpired = false
	a.StreamLastError = ""
	a.StreamLastErrorAt = time.Time{}
	a.StreamReconnectRequestedAt = time.Time{}
	a.StreamReconnectError = ""
	a.StreamReconnectErrorAt = time.Time{}
	a.StreamSessionExpiredAt = time.Time{}
	a.StreamSessionExpiredReason = ""
	a.StreamSessionExpiredOp = ""
	a.StreamSessionExpiredCode = 0
	a.StreamSessionExpiredMsg = ""
	a.GetUpdatesBuf = ""
	a.ContextTokens = make(map[string]string)
	a.TypingTickets = make(map[string]string)
	a.UpdatedAt = time.Now().UTC()
}

func TouchAccount(account *AccountState) error {
	if account == nil {
		return fmt.Errorf("account state is nil")
	}
	if account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	now := time.Now().UTC()
	pruneTimestampMap(account.InboundSeen, now)
	pruneTimestampMap(account.SentBeakMessages, now)
	account.UpdatedAt = now
	return nil
}

func pruneTimestampMap(values map[string]string, now time.Time) {
	for key, raw := range values {
		if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil && now.Sub(ts) > trackedStateTTL {
			delete(values, key)
		}
	}
	if len(values) <= maxTrackedStateKeys {
		return
	}
	type item struct {
		key string
		at  time.Time
	}
	items := make([]item, 0, len(values))
	for key, raw := range values {
		ts, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			ts = time.Time{}
		}
		items = append(items, item{key: key, at: ts})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].at.Before(items[j].at)
	})
	for len(values) > maxTrackedStateKeys && len(items) > 0 {
		delete(values, items[0].key)
		items = items[1:]
	}
}
