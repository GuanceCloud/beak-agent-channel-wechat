package state

import (
	"fmt"
	"time"
)

type AccountState struct {
	AccountID          string            `json:"account_id"`
	BotToken           string            `json:"bot_token"`
	BaseURL            string            `json:"base_url"`
	ILinkUserID        string            `json:"ilink_user_id,omitempty"`
	ChannelLinkSession string            `json:"channel_link_session,omitempty"`
	GetUpdatesBuf      string            `json:"get_updates_buf,omitempty"`
	ContextTokens      map[string]string `json:"context_tokens,omitempty"`
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

func TouchAccount(account *AccountState) error {
	if account == nil {
		return fmt.Errorf("account state is nil")
	}
	if account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	account.UpdatedAt = time.Now().UTC()
	return nil
}
