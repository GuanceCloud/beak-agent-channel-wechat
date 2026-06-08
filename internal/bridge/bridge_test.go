package bridge

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/beak"
	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
)

func TestProcessUpdateCreatesSessionAndPostsOnce(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1"}
	account.EnsureMaps()
	runner := testRunner(store, account)

	msg := weixin.WeixinMessage{
		MessageID:    101,
		FromUserID:   "peer-1",
		MessageType:  weixin.MessageTypeUser,
		MessageState: weixin.MessageStateFinish,
		ContextToken: "ctx-1",
		ItemList: []weixin.MessageItem{
			{Type: weixin.MessageItemTypeText, TextItem: &weixin.TextItem{Text: "hello"}},
		},
	}
	sessionUUID, processed, err := runner.ProcessUpdate(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !processed || sessionUUID != "sess-1" {
		t.Fatalf("processed=%v session=%q", processed, sessionUUID)
	}
	_, processed, err = runner.ProcessUpdate(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if processed {
		t.Fatal("duplicate update should be skipped")
	}
	fake := runner.beak.(*fakeBeak)
	if fake.ensureCalls != 1 {
		t.Fatalf("ensureCalls=%d", fake.ensureCalls)
	}
	if len(fake.createdMessages) != 1 {
		t.Fatalf("createdMessages=%d", len(fake.createdMessages))
	}
	if fake.createdMessages[0].SenderID != "im:weixin:direct:peer-1:user:peer-1" || fake.createdMessages[0].Content != "hello" {
		t.Fatalf("created message=%+v", fake.createdMessages[0])
	}
	if account.ContextTokens["peer-1"] != "ctx-1" {
		t.Fatalf("context token not stored")
	}
	wx := runner.wx.(*fakeWeixin)
	if len(wx.typing) != 1 || wx.typing[0].status != weixin.TypingStatusStart {
		t.Fatalf("typing=%+v", wx.typing)
	}
}

func TestProcessGroupUpdateUsesGroupChatIdentity(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1"}
	account.EnsureMaps()
	runner := testRunner(store, account)

	msg := weixin.WeixinMessage{
		MessageID:    102,
		FromUserID:   "user-1",
		ToUserID:     "bot-1",
		GroupID:      "group-1",
		MessageType:  weixin.MessageTypeUser,
		MessageState: weixin.MessageStateFinish,
		ContextToken: "ctx-group-1",
		MentionedMe:  true,
		MentionAll:   true,
		Mentions: []weixin.Mention{
			{UserID: "user-2", Name: "Bob"},
			{OpenID: "open-1", DisplayName: "Alice"},
		},
		ItemList: []weixin.MessageItem{
			{Type: weixin.MessageItemTypeText, TextItem: &weixin.TextItem{Text: "hello group"}},
		},
	}
	sessionUUID, processed, err := runner.ProcessUpdate(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !processed || sessionUUID != "sess-1" {
		t.Fatalf("processed=%v session=%q", processed, sessionUUID)
	}
	fake := runner.beak.(*fakeBeak)
	if fake.lastChatType != weixin.ChatTypeGroup || fake.lastChatID != "group-1" || fake.lastSenderID != "user-1" {
		t.Fatalf("chat identity=%s %s %s", fake.lastChatType, fake.lastChatID, fake.lastSenderID)
	}
	if account.ContextTokens["group:group-1"] != "ctx-group-1" {
		t.Fatalf("context token=%q", account.ContextTokens["group:group-1"])
	}
	if fake.createdMessages[0].SenderID != "im:weixin:group:group-1:user:user-1" {
		t.Fatalf("sender=%q", fake.createdMessages[0].SenderID)
	}
	if fake.createdMessages[0].Metadata["weixin_chat_type"] != weixin.ChatTypeGroup || fake.createdMessages[0].Metadata["weixin_chat_id"] != "group-1" {
		t.Fatalf("metadata=%+v", fake.createdMessages[0].Metadata)
	}
	if fake.createdMessages[0].Metadata["account_uuid"] != "account-1" {
		t.Fatalf("metadata=%+v", fake.createdMessages[0].Metadata)
	}
	inbound, ok := fake.createdMessages[0].Metadata["inbound_message"].(sdk.InboundMessage)
	if !ok {
		t.Fatalf("missing inbound message metadata: %+v", fake.createdMessages[0].Metadata)
	}
	if inbound.ChannelUUID != "channel-1" || inbound.AccountUUID != "account-1" || inbound.ChatType != sdk.ChatTypeGroup || inbound.ChatID != "group-1" || inbound.SenderID != "user-1" || inbound.Text != "hello group" {
		t.Fatalf("inbound=%+v", inbound)
	}
	if !inbound.MentionedMe || !inbound.MentionAll || len(inbound.Mentions) != 3 {
		t.Fatalf("inbound mentions=%+v mentioned_me=%v mention_all=%v", inbound.Mentions, inbound.MentionedMe, inbound.MentionAll)
	}
	if inbound.Raw["mention_all"] != true {
		t.Fatalf("raw=%+v", inbound.Raw)
	}
}

func TestBuildInboundMessageMentionAllDoesNotMentionBot(t *testing.T) {
	inbound := BuildInboundMessage("workspace-1", "channel-1", "account-1", weixin.WeixinMessage{
		MessageID:    103,
		FromUserID:   "user-1",
		ToUserID:     "bot-1",
		GroupID:      "group-1",
		MessageType:  weixin.MessageTypeUser,
		MessageState: weixin.MessageStateFinish,
		MentionAll:   true,
		ItemList: []weixin.MessageItem{
			{Type: weixin.MessageItemTypeText, TextItem: &weixin.TextItem{Text: "hello all"}},
		},
	}, "hello all")
	if !inbound.MentionAll || inbound.MentionedMe {
		t.Fatalf("inbound=%+v", inbound)
	}
}

func TestProcessGroupOnlyBotMentionWithEmptyTextIsDelivered(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1"}
	account.EnsureMaps()
	runner := testRunner(store, account)

	msg := weixin.WeixinMessage{
		MessageID:    104,
		FromUserID:   "user-1",
		ToUserID:     "bot-1",
		GroupID:      "group-1",
		MessageType:  weixin.MessageTypeUser,
		MessageState: weixin.MessageStateFinish,
		MentionedMe:  true,
		ItemList: []weixin.MessageItem{
			{Type: weixin.MessageItemTypeText, TextItem: &weixin.TextItem{Text: ""}},
		},
	}
	sessionUUID, processed, err := runner.ProcessUpdate(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !processed || sessionUUID != "sess-1" {
		t.Fatalf("processed=%v session=%q", processed, sessionUUID)
	}
	fake := runner.beak.(*fakeBeak)
	if len(fake.createdMessages) != 1 || strings.TrimSpace(fake.createdMessages[0].Content) != "" {
		t.Fatalf("createdMessages=%+v", fake.createdMessages)
	}
	inbound, ok := fake.createdMessages[0].Metadata["inbound_message"].(sdk.InboundMessage)
	if !ok || !inbound.MentionedMe || strings.TrimSpace(inbound.Text) != "" {
		t.Fatalf("inbound=%+v metadata=%+v", inbound, fake.createdMessages[0].Metadata)
	}
}

func TestProcessStreamEventSendsAgentMessageOnceAndStoresCursor(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1"}
	account.EnsureMaps()
	account.ContextTokens["peer-1"] = "ctx-1"
	account.PeerSessions["peer-1"] = "sess-1"
	runner := testRunner(store, account)

	payload, _ := json.Marshal(beak.MessageEventPayload{
		MessageUUID: "msg-1",
		SenderID:    "agent:agent-1",
		Content:     "reply",
	})
	event := beak.StreamEvent{
		EventUUID:   "evt-1",
		SessionUUID: "sess-1",
		EventType:   "message",
		Payload:     payload,
	}
	if err := runner.ProcessStreamEvent(context.Background(), "peer-1", event); err != nil {
		t.Fatal(err)
	}
	if err := runner.ProcessStreamEvent(context.Background(), "peer-1", event); err != nil {
		t.Fatal(err)
	}
	wx := runner.wx.(*fakeWeixin)
	if len(wx.sent) != 1 {
		t.Fatalf("sent=%d", len(wx.sent))
	}
	if wx.sent[0].to != "peer-1" || wx.sent[0].text != "reply" || wx.sent[0].contextToken != "ctx-1" {
		t.Fatalf("sent payload=%+v", wx.sent[0])
	}
	if len(wx.typing) != 1 || wx.typing[0].status != weixin.TypingStatusStop {
		t.Fatalf("typing=%+v", wx.typing)
	}
	if account.StreamCursors["sess-1"] != "evt-1" {
		t.Fatalf("cursor=%q", account.StreamCursors["sess-1"])
	}
}

func TestPollMarksSessionExpired(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1", BotToken: "token-1", GetUpdatesBuf: "buf-1"}
	account.EnsureMaps()
	account.ContextTokens["peer-1"] = "ctx-1"
	account.TypingTickets["peer-1"] = "ticket-1"
	runner := testRunner(store, account)
	runner.wx.(*fakeWeixin).updatesErr = weixin.ErrSessionExpired

	err := runner.Poll(context.Background())
	if err == nil {
		t.Fatal("expected session expired error")
	}
	if !account.LoginRequired() || account.BotToken != "" || account.GetUpdatesBuf != "" || len(account.ContextTokens) != 0 || len(account.TypingTickets) != 0 {
		t.Fatalf("account not marked expired: %+v", account)
	}
}

func TestProcessStreamEventMarksSessionExpiredOnSend(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1", BotToken: "token-1"}
	account.EnsureMaps()
	account.ContextTokens["peer-1"] = "ctx-1"
	account.PeerSessions["peer-1"] = "sess-1"
	runner := testRunner(store, account)
	runner.wx.(*fakeWeixin).sendErr = weixin.ErrSessionExpired

	payload, _ := json.Marshal(beak.MessageEventPayload{
		MessageUUID: "msg-1",
		SenderID:    "agent:agent-1",
		Content:     "reply",
	})
	err := runner.ProcessStreamEvent(context.Background(), "peer-1", beak.StreamEvent{
		EventUUID:   "evt-1",
		SessionUUID: "sess-1",
		EventType:   "message",
		Payload:     payload,
	})
	if err == nil {
		t.Fatal("expected session expired error")
	}
	if !account.LoginRequired() || account.BotToken != "" || len(account.ContextTokens) != 0 {
		t.Fatalf("account not marked expired: %+v", account)
	}
}

func TestProcessStreamEventSkipsMissingContextAndAdvancesCursor(t *testing.T) {
	store := newMemoryStore()
	account := &state.AccountState{AccountID: "account-1"}
	account.EnsureMaps()
	account.PeerSessions["peer-1"] = "sess-1"
	runner := testRunner(store, account)

	payload, _ := json.Marshal(beak.MessageEventPayload{
		MessageUUID: "msg-1",
		SenderID:    "agent:agent-1",
		Content:     "reply",
	})
	err := runner.ProcessStreamEvent(context.Background(), "peer-1", beak.StreamEvent{
		EventUUID:   "evt-1",
		SessionUUID: "sess-1",
		EventType:   "message",
		Payload:     payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	wx := runner.wx.(*fakeWeixin)
	if len(wx.sent) != 0 {
		t.Fatalf("sent=%d", len(wx.sent))
	}
	if account.StreamCursors["sess-1"] != "evt-1" {
		t.Fatalf("cursor=%q", account.StreamCursors["sess-1"])
	}
	if got := account.SentBeakMessages["msg-1"]; got == "" {
		t.Fatal("missing skipped marker")
	}
}

func TestBridgeRunKeepsPluginAliveWhenAccountFails(t *testing.T) {
	options := &Options{
		WorkspaceRef:        "workspace-1",
		AgentParticipantID:  "agent:agent-1",
		BridgeParticipantID: "bridge:weixin",
		PollInterval:        time.Millisecond,
		StreamReconnect:     time.Millisecond,
		Accounts: []AccountConfig{
			{AccountID: "expired-or-unconfigured-account"},
		},
	}
	runner := New(options, newMemoryStore(), &fakeBeak{}, nil, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := runner.Run(ctx)
	if err == nil || err != context.DeadlineExceeded {
		t.Fatalf("Run should stay alive until context deadline, got %v", err)
	}
}

func testRunner(store *memoryStore, account *state.AccountState) *AccountRunner {
	return &AccountRunner{
		options: &Options{
			WorkspaceRef:        "workspace-1",
			ChannelUUID:         "channel-1",
			AgentParticipantID:  "agent:agent-1",
			BridgeParticipantID: "bridge:weixin",
			PollInterval:        time.Millisecond,
			StreamReconnect:     time.Millisecond,
			Weixin: WeixinOptions{
				LongPollTimeout: time.Millisecond,
				RequestTimeout:  time.Millisecond,
			},
		},
		store:         store,
		beak:          &fakeBeak{},
		wx:            &fakeWeixin{},
		account:       account,
		logger:        log.New(io.Discard, "", 0),
		activeStreams: make(map[string]bool),
	}
}

type fakeBeak struct {
	ensureCalls     int
	lastChatType    string
	lastChatID      string
	lastSenderID    string
	createdMessages []beak.CreateMessageRequest
}

type memoryStore struct {
	mu       sync.Mutex
	accounts map[string]*state.AccountState
}

func newMemoryStore() *memoryStore {
	return &memoryStore{accounts: make(map[string]*state.AccountState)}
}

func (s *memoryStore) LoadAccount(ctx context.Context, accountID string) (*state.AccountState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if account, ok := s.accounts[accountID]; ok {
		return account, nil
	}
	account := &state.AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.accounts[accountID] = account
	return account, nil
}

func (s *memoryStore) SaveAccount(ctx context.Context, account *state.AccountState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := state.TouchAccount(account); err != nil {
		return err
	}
	s.accounts[account.AccountID] = account
	return nil
}

func (f *fakeBeak) EnsureWeixinChannel(context.Context, string) (string, error) {
	return "channel-1", nil
}

func (f *fakeBeak) EnsureChannelLinkSession(context.Context, string, string, string, string) (string, error) {
	return "channel-link-sess-1", nil
}

func (f *fakeBeak) EnsurePeerSession(context.Context, string, string, string, string, string) (string, error) {
	f.ensureCalls++
	return "sess-1", nil
}

func (f *fakeBeak) EnsureChatSession(_ context.Context, _, _, chatType, chatID, senderID, _, _ string) (string, error) {
	f.ensureCalls++
	f.lastChatType = chatType
	f.lastChatID = chatID
	f.lastSenderID = senderID
	return "sess-1", nil
}

func (f *fakeBeak) CreateMessage(_ context.Context, _ string, req beak.CreateMessageRequest) (*beak.CreateMessageResponse, error) {
	f.createdMessages = append(f.createdMessages, req)
	return &beak.CreateMessageResponse{MessageUUID: "beak-msg-1"}, nil
}

func (f *fakeBeak) StreamEvents(context.Context, string, beak.StreamRequest, func(beak.StreamEvent) error) error {
	return nil
}

type fakeWeixin struct {
	sent       []sentMessage
	typing     []typingMessage
	updatesErr error
	sendErr    error
}

type sentMessage struct {
	to           string
	text         string
	contextToken string
}

type typingMessage struct {
	to     string
	ticket string
	status int
}

func (f *fakeWeixin) GetUpdates(context.Context, string, time.Duration) (*weixin.GetUpdatesResponse, error) {
	if f.updatesErr != nil {
		return nil, f.updatesErr
	}
	return &weixin.GetUpdatesResponse{}, nil
}

func (f *fakeWeixin) SendText(_ context.Context, toUserID, text, contextToken string) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, sentMessage{to: toUserID, text: text, contextToken: contextToken})
	return nil
}

func (f *fakeWeixin) GetTypingTicket(context.Context, string, string) (string, error) {
	return "ticket-1", nil
}

func (f *fakeWeixin) SendTyping(_ context.Context, toUserID, ticket string, status int) error {
	f.typing = append(f.typing, typingMessage{to: toUserID, ticket: ticket, status: status})
	return nil
}

func (f *fakeWeixin) NotifyStart(context.Context) error {
	return nil
}

func (f *fakeWeixin) NotifyStop(context.Context) error {
	return nil
}
