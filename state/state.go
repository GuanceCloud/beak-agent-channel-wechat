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
	AccountID          string            `json:"account_id"`
	BotToken           string            `json:"bot_token"`
	BaseURL            string            `json:"base_url"`
	ILinkUserID        string            `json:"ilink_user_id,omitempty"`
	ILinkBotID         string            `json:"ilink_bot_id,omitempty"`
	Status             string            `json:"status,omitempty"`
	LastError          string            `json:"last_error,omitempty"`
	ChannelLinkSession string            `json:"channel_link_session,omitempty"`
	GetUpdatesBuf      string            `json:"get_updates_buf,omitempty"`
	ContextTokens      map[string]string `json:"context_tokens,omitempty"`
	TypingTickets      map[string]string `json:"typing_tickets,omitempty"`
	PeerSessions       map[string]string `json:"peer_sessions,omitempty"`
	InboundSeen        map[string]string `json:"inbound_seen,omitempty"`
	SentBeakMessages   map[string]string `json:"sent_beak_messages,omitempty"`
	StreamCursors      map[string]string `json:"stream_cursors,omitempty"`
	UpdatedAt          time.Time         `json:"updated_at"`
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
	if a == nil {
		return
	}
	a.EnsureMaps()
	a.Status = AccountStatusLoginRequired
	a.LastError = reason
	a.BotToken = ""
	a.GetUpdatesBuf = ""
	a.ContextTokens = make(map[string]string)
	a.TypingTickets = make(map[string]string)
	a.UpdatedAt = time.Now().UTC()
}

func (a *AccountState) MarkActive() {
	if a == nil {
		return
	}
	a.EnsureMaps()
	a.Status = AccountStatusActive
	a.LastError = ""
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
