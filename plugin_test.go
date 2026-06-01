package beakweixin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestPluginRegistersWeixinChannelWithNoSettings(t *testing.T) {
	api := &fakeAPI{}
	if err := Register(api); err != nil {
		t.Fatal(err)
	}
	if api.channel.Metadata().ID != ID {
		t.Fatalf("metadata=%+v", api.channel.Metadata())
	}
	schema := api.channel.SettingsSchema()
	if schema.Type != "object" || schema.AdditionalProperties || len(schema.Properties) != 0 {
		t.Fatalf("schema=%+v", schema)
	}
	capabilities := api.channel.Capabilities()
	if !capabilities.DirectChat || !capabilities.Text || capabilities.Media {
		t.Fatalf("capabilities=%+v", capabilities)
	}
}

func TestChannelSendTextUsesInjectedAccountStore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("Authorization=%q", got)
		}
		var body struct {
			BaseInfo struct {
				BotAgent string `json:"bot_agent"`
			} `json:"base_info"`
			Message struct {
				ToUserID     string `json:"to_user_id"`
				ContextToken string `json:"context_token"`
				ItemList     []struct {
					TextItem struct {
						Text string `json:"text"`
					} `json:"text_item"`
				} `json:"item_list"`
			} `json:"msg"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.BaseInfo.BotAgent != "Beak Agent Test" {
			t.Fatalf("bot_agent=%q", body.BaseInfo.BotAgent)
		}
		if body.Message.ToUserID != "peer-1" || body.Message.ContextToken != "ctx-1" {
			t.Fatalf("message=%+v", body.Message)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "hello" {
			t.Fatalf("items=%+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()
	targetURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Transport: rewriteTransport{target: targetURL, base: http.DefaultTransport}}

	store := newMemoryStore()
	account, err := store.SaveLogin(context.Background(), "account-1", "token-1", server.URL, "ilink-user-1")
	if err != nil {
		t.Fatal(err)
	}
	account.ContextTokens["peer-1"] = "ctx-1"
	if err := store.SaveAccount(context.Background(), account); err != nil {
		t.Fatal(err)
	}
	result, err := Channel{}.SendText(context.Background(), Runtime{
		State:      store,
		Weixin:     WeixinOptions{BotAgent: "Beak Agent Test"},
		Accounts:   []Account{{AccountID: "account-1"}},
		HTTPClient: httpClient,
	}, SendTextRequest{
		ToUserID: "peer-1",
		Text:     "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Channel != "weixin" || result.AccountID != "account-1" {
		t.Fatalf("result=%+v", result)
	}
}

func TestCloudLoginStartAndPollStoresAccount(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/get_bot_qrcode":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"qrcode":             "qr-1",
				"qrcode_img_content": "https://example.test/qr",
			})
		case "/ilink/bot/get_qrcode_status":
			if got := r.URL.Query().Get("qrcode"); got != "qr-1" {
				t.Fatalf("qrcode=%q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-1",
				"ilink_bot_id":  "account-1",
				"ilink_user_id": "ilink-user-1",
				"baseurl":       server.URL,
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	targetURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Transport: rewriteTransport{target: targetURL, base: http.DefaultTransport}}

	store := newMemoryStore()
	channel := Channel{}
	runtime := Runtime{State: store, HTTPClient: httpClient}
	challenge, err := channel.StartLogin(context.Background(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if challenge.QRCode.Code != "qr-1" || challenge.QRCode.URL != "https://example.test/qr" {
		t.Fatalf("challenge=%+v", challenge)
	}
	status, err := channel.PollLogin(context.Background(), runtime, challenge.QRCode.Code)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Confirmed || status.AccountID != "account-1" {
		t.Fatalf("status=%+v", status)
	}
	account, err := store.LoadAccount(context.Background(), "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if account.BotToken != "token-1" || account.BaseURL != server.URL || account.ILinkUserID != "ilink-user-1" {
		t.Fatalf("account=%+v", account)
	}
}

func TestChannelStartUsesInjectedRuntimeAndStore(t *testing.T) {
	var updatesServed int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/msg/notifystart", "/ilink/bot/msg/notifystop":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/getupdates":
			updatesServed++
			if updatesServed == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ret":             0,
					"get_updates_buf": "buf-1",
					"msgs": []map[string]any{
						{
							"message_id":    101,
							"from_user_id":  "peer-1",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-1",
							"item_list": []map[string]any{
								{
									"type":      1,
									"text_item": map[string]any{"text": "hello from weixin"},
								},
							},
						},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "get_updates_buf": "buf-1"})
		case "/ilink/bot/getconfig":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-1"})
		case "/ilink/bot/sendtyping":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	store := newMemoryStore()
	if _, err := store.SaveLogin(context.Background(), "account-1", "token-1", server.URL, "ilink-user-1"); err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntime{}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := Channel{}.Start(ctx, Runtime{
		Beak:            runtime,
		State:           store,
		Accounts:        []Account{{AccountID: "account-1"}},
		PollInterval:    time.Millisecond,
		StreamReconnect: time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error=%v", err)
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.channelLinkAccountID != "account-1" {
		t.Fatalf("channel link account=%q", runtime.channelLinkAccountID)
	}
	if runtime.peerAccountID != "account-1" || runtime.peerUserID != "peer-1" {
		t.Fatalf("peer session account=%q peer=%q", runtime.peerAccountID, runtime.peerUserID)
	}
	if len(runtime.userMessages) != 1 {
		t.Fatalf("messages=%d", len(runtime.userMessages))
	}
	msg := runtime.userMessages[0]
	if msg.SenderID != "im:weixin:direct:peer-1:user:peer-1" || msg.Content != "hello from weixin" {
		t.Fatalf("message=%+v", msg)
	}
	account, err := store.LoadAccount(context.Background(), "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if account.GetUpdatesBuf != "buf-1" || account.ContextTokens["peer-1"] != "ctx-1" {
		t.Fatalf("account state=%+v", account)
	}
}

type fakeAPI struct {
	channel Channel
}

func (f *fakeAPI) RegisterChannel(channel Channel) error {
	f.channel = channel
	return nil
}

type memoryStore struct {
	accounts map[string]*AccountState
}

func newMemoryStore() *memoryStore {
	return &memoryStore{accounts: make(map[string]*AccountState)}
}

func (s *memoryStore) LoadAccount(ctx context.Context, accountID string) (*AccountState, error) {
	if account, ok := s.accounts[accountID]; ok {
		return account, nil
	}
	account := &AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.accounts[accountID] = account
	return account, nil
}

func (s *memoryStore) SaveAccount(ctx context.Context, account *AccountState) error {
	if account == nil || account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	account.UpdatedAt = time.Now().UTC()
	s.accounts[account.AccountID] = account
	return nil
}

func (s *memoryStore) SaveLogin(ctx context.Context, accountID, botToken, baseURL, ilinkUserID string) (*AccountState, error) {
	account, err := s.LoadAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	account.BotToken = botToken
	account.BaseURL = baseURL
	account.ILinkUserID = ilinkUserID
	account.MarkActive()
	if err := s.SaveAccount(ctx, account); err != nil {
		return nil, err
	}
	return account, nil
}

type fakeRuntime struct {
	mu                   sync.Mutex
	channelLinkAccountID string
	peerAccountID        string
	peerUserID           string
	userMessages         []UserMessage
}

type rewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	if t.base == nil {
		t.base = http.DefaultTransport
	}
	return t.base.RoundTrip(clone)
}

func (f *fakeRuntime) CheckHealth(context.Context) error {
	return nil
}

func (f *fakeRuntime) EnsureWeixinChannel(context.Context) (string, error) {
	return "channel-1", nil
}

func (f *fakeRuntime) EnsureWeixinChannelLinkSession(_ context.Context, accountID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channelLinkAccountID = accountID
	return "channel-link-sess-1", nil
}

func (f *fakeRuntime) EnsureWeixinPeerSession(_ context.Context, accountID, peerUserID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.peerAccountID = accountID
	f.peerUserID = peerUserID
	return "peer-sess-1", nil
}

func (f *fakeRuntime) CreateWeixinUserMessage(_ context.Context, _ string, msg UserMessage) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.userMessages = append(f.userMessages, msg)
	return "message-1", nil
}

func (f *fakeRuntime) StreamWeixinSession(context.Context, string, StreamRequest, func(StreamEvent) error) error {
	return nil
}

func (f *fakeRuntime) AgentParticipantID() string {
	return "agent:agent-1"
}

func (f *fakeRuntime) BridgeParticipantID() string {
	return "bridge:weixin"
}
