package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/bridge"
)

func TestChannelLoginStoresAccount(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/get_bot_qrcode":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"qrcode":             "qr-1",
				"qrcode_img_content": "https://example.test/qr",
			})
		case "/ilink/bot/get_qrcode_status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-1",
				"ilink_bot_id":  "account-1",
				"ilink_user_id": "ilink-user-1",
				"baseurl":       server.URL,
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := New(Options{
		State: newMemoryStore(),
	}, withWeixinForTest(bridge.WeixinOptions{
		BaseURL:        server.URL,
		LoginTimeout:   time.Second,
		RequestTimeout: time.Second,
	}))
	if err != nil {
		t.Fatal(err)
	}
	var qrURL string
	result, err := client.Login(context.Background(), LoginOptions{
		PollInterval: time.Millisecond,
		OnQRCode: func(qr QRCode) {
			qrURL = qr.URL
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if qrURL != "https://example.test/qr" {
		t.Fatalf("qrURL=%q", qrURL)
	}
	if result.Account.AccountID != "ilink-user-1" || result.Account.BotToken != "token-1" || result.Account.ILinkBotID != "account-1" {
		t.Fatalf("account=%+v", result.Account)
	}
	if result.AccountID != "ilink-user-1" {
		t.Fatalf("login account id=%q", result.AccountID)
	}
	if len(client.Accounts()) != 0 {
		t.Fatalf("sdk must not mutate accounts, got %+v", client.Accounts())
	}
}

func TestChannelPollLoginUsesStableWeixinUserID(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/get_qrcode_status" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		switch r.URL.Query().Get("qrcode") {
		case "qr-first":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-first",
				"ilink_bot_id":  "bot-scan-first",
				"ilink_user_id": "ilink-user-stable",
				"baseurl":       server.URL,
			})
		case "qr-second":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-second",
				"ilink_bot_id":  "bot-scan-second",
				"ilink_user_id": "ilink-user-stable",
				"baseurl":       server.URL,
			})
		default:
			t.Fatalf("qrcode=%q", r.URL.Query().Get("qrcode"))
		}
	}))
	defer server.Close()

	store := newMemoryStore()
	client, err := New(Options{
		State: store,
	}, withWeixinForTest(bridge.WeixinOptions{
		BaseURL:        server.URL,
		RequestTimeout: time.Second,
	}))
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.PollLogin(context.Background(), "qr-first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.PollLogin(context.Background(), "qr-second")
	if err != nil {
		t.Fatal(err)
	}

	if first.AccountID != "ilink-user-stable" || second.AccountID != "ilink-user-stable" {
		t.Fatalf("account ids first=%q second=%q", first.AccountID, second.AccountID)
	}
	if len(store.accounts) != 1 {
		t.Fatalf("accounts=%+v", store.accounts)
	}
	account := store.accounts["ilink-user-stable"]
	if account == nil || account.BotToken != "token-second" || account.ILinkBotID != "bot-scan-second" {
		t.Fatalf("account=%+v", account)
	}
}

func TestChannelPollLoginTreatsClientTimeoutAsWait(t *testing.T) {
	client, err := New(Options{
		State: newMemoryStore(),
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: "Get", URL: r.URL.String(), Err: context.DeadlineExceeded}
		})},
	}, withWeixinForTest(bridge.WeixinOptions{
		BaseURL:         "https://ilinkai.weixin.qq.com",
		RequestTimeout:  time.Second,
		LongPollTimeout: time.Second,
	}))
	if err != nil {
		t.Fatal(err)
	}

	status, err := client.PollLogin(context.Background(), "qr-timeout")
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "wait" || status.Confirmed || status.Expired {
		t.Fatalf("status=%+v", status)
	}
}

func TestChannelPollLoginDoesNotHideParentCancellation(t *testing.T) {
	client, err := New(Options{
		State: newMemoryStore(),
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: "Get", URL: r.URL.String(), Err: context.DeadlineExceeded}
		})},
	}, withWeixinForTest(bridge.WeixinOptions{
		BaseURL:         "https://ilinkai.weixin.qq.com",
		RequestTimeout:  time.Second,
		LongPollTimeout: time.Second,
	}))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.PollLogin(ctx, "qr-cancelled"); err == nil {
		t.Fatal("expected parent context cancellation error")
	}
}

func withWeixinForTest(wxCfg bridge.WeixinOptions) Option {
	return func(client *Client) {
		client.weixin = wxCfg
	}
}

func TestChannelDoctorChecksBeakAndChannel(t *testing.T) {
	client, err := New(Options{
		Beak:  fakeRuntime{},
		State: newMemoryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := client.Doctor(context.Background(), DoctorOptions{EnsureChannel: true})
	if err != nil {
		t.Fatal(err)
	}
	if !report.RuntimeOK || report.ChannelUUID != "channel-1" {
		t.Fatalf("report=%+v", report)
	}
}

func TestChannelSendTextUsesStoredAccountAndContextToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("Authorization=%q", got)
		}
		var body struct {
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
		if body.Message.ToUserID != "peer-1" || body.Message.ContextToken != "ctx-1" {
			t.Fatalf("message=%+v", body.Message)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "hello" {
			t.Fatalf("items=%+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	store := newMemoryStore()
	account, err := store.SaveLogin(context.Background(), "account-1", "token-1", server.URL, "ilink-user-1", "ilink-bot-1")
	if err != nil {
		t.Fatal(err)
	}
	account.ContextTokens["peer-1"] = "ctx-1"
	if err := store.SaveAccount(context.Background(), account); err != nil {
		t.Fatal(err)
	}
	client, err := New(Options{
		State:    store,
		Accounts: []AccountConfig{{AccountID: "account-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.SendText(context.Background(), SendTextRequest{
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type fakeRuntime struct{}

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

func (s *memoryStore) SaveLogin(ctx context.Context, accountID, botToken, baseURL, ilinkUserID, ilinkBotID string) (*AccountState, error) {
	account, err := s.LoadAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	account.BotToken = botToken
	account.BaseURL = baseURL
	account.ILinkUserID = ilinkUserID
	account.ILinkBotID = ilinkBotID
	account.MarkActive()
	if err := s.SaveAccount(ctx, account); err != nil {
		return nil, err
	}
	return account, nil
}

func (fakeRuntime) CheckHealth(context.Context) error { return nil }

func (fakeRuntime) EnsureWeixinChannel(context.Context) (string, error) {
	return "channel-1", nil
}

func (fakeRuntime) EnsureWeixinChannelLinkSession(context.Context, string) (string, error) {
	return "channel-link-sess-1", nil
}

func (fakeRuntime) EnsureWeixinPeerSession(context.Context, string, string) (string, error) {
	return "sess-1", nil
}

func (fakeRuntime) CreateWeixinUserMessage(context.Context, string, UserMessage) (string, error) {
	return "msg-1", nil
}

func (fakeRuntime) StreamWeixinSession(context.Context, string, StreamRequest, func(StreamEvent) error) error {
	return nil
}

func (fakeRuntime) AgentParticipantID() string {
	return "agent:agent-1"
}

func (fakeRuntime) BridgeParticipantID() string {
	return "bridge:weixin"
}
