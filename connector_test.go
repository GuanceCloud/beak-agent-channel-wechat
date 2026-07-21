package beakweixin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

func TestWeixinConnectorMetadataAndSchema(t *testing.T) {
	var connector sdk.Connector = NewConnector()
	if _, ok := connector.(sdk.HostStreamConnector); ok {
		t.Fatal("NewConnector must not expose HostStreamConnector for SDK-owned Weixin runtime")
	}

	metadata := connector.Metadata()
	if metadata.ID != ID || metadata.Platform != Platform || metadata.Label != "Weixin" {
		t.Fatalf("metadata=%+v", metadata)
	}
	if !metadata.Capabilities.Text || !metadata.Capabilities.DirectChat || !metadata.Capabilities.GroupChat || metadata.Capabilities.Media {
		t.Fatalf("capabilities=%+v", metadata.Capabilities)
	}
	if !metadata.Capabilities.Stream || metadata.Capabilities.Webhook {
		t.Fatalf("stream/webhook capabilities=%+v", metadata.Capabilities)
	}
	if metadata.Capabilities.RuntimeOwnership != sdk.RuntimeOwnershipSDKOwned {
		t.Fatalf("runtime ownership=%q, want %q", metadata.Capabilities.RuntimeOwnership, sdk.RuntimeOwnershipSDKOwned)
	}
	if len(metadata.Capabilities.AckModes) != 1 || metadata.Capabilities.AckModes[0] != "typing" {
		t.Fatalf("ack modes=%+v", metadata.Capabilities.AckModes)
	}
	if len(metadata.Capabilities.LoginModes) != 1 || metadata.Capabilities.LoginModes[0] != sdk.LoginModeQRCode {
		t.Fatalf("login modes=%+v", metadata.Capabilities.LoginModes)
	}

	schema := connector.CredentialSchema(context.Background())
	if schema.Type != "object" || schema.AdditionalProperties {
		t.Fatalf("schema=%+v", schema)
	}
	if len(schema.LoginModes) != 1 || schema.LoginModes[0] != sdk.LoginModeQRCode {
		t.Fatalf("schema login modes=%+v", schema.LoginModes)
	}
}

func TestWeixinConnectorValidateCredentialDefaultsToValid(t *testing.T) {
	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{
			"bot_token":     "token-1",
			"ilink_bot_id":  "account-1",
			"ilink_user_id": "ilink-user-1",
			"base_url":      "https://ilinkai.weixin.qq.com",
		},
		State: map[string]any{"get_updates_buf": "buf-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.AccountKey != "ilink-user-1" {
		t.Fatalf("result=%+v", result)
	}
	if result.Credential["account_id"] != "ilink-user-1" || result.Credential["ilink_bot_id"] != "account-1" {
		t.Fatalf("credential=%+v", result.Credential)
	}
	if result.State["get_updates_buf"] != "buf-1" {
		t.Fatalf("state=%+v", result.State)
	}
	identity, ok := result.State["bot_identity"].(map[string]any)
	if !ok || identity["id"] != "account-1" || identity["id_type"] != "ilink_bot_id" {
		t.Fatalf("bot_identity=%+v state=%+v", result.State["bot_identity"], result.State)
	}
	if result.Metadata["validation"] != "default_pass" {
		t.Fatalf("metadata=%+v", result.Metadata)
	}
}

func TestWeixinConnectorRuntimeFromSDKPreservesNativeBotAgent(t *testing.T) {
	native, _ := Connector{}.runtimeFromSDK(sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1"},
		Native:        Runtime{Weixin: WeixinOptions{BotAgent: "Beak Agent Test"}},
	}, nil)
	if native.Weixin.BotAgent != "Beak Agent Test" {
		t.Fatalf("bot_agent=%q", native.Weixin.BotAgent)
	}
}

func TestWeixinConnectorQRCodeLoginThroughSDK(t *testing.T) {
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
	connector := NewConnector()
	runtime := sdk.Runtime{HTTPClient: httpClient}

	challenge, err := connector.StartLogin(context.Background(), sdk.LoginStartRequest{
		WorkspaceUUID: "workspace-1",
		ChannelUUID:   "channel-1",
		Runtime:       runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if challenge.Type != sdk.LoginModeQRCode || challenge.Code != "qr-1" || challenge.URL != "https://example.test/qr" {
		t.Fatalf("challenge=%+v", challenge)
	}

	status, err := connector.PollLogin(context.Background(), sdk.LoginPollRequest{
		WorkspaceUUID:  "workspace-1",
		ChannelUUID:    "channel-1",
		ChallengeState: challenge.State,
		Runtime:        runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Confirmed || status.Account.UUID != "ilink-user-1" {
		t.Fatalf("status=%+v", status)
	}
	if status.Credential["account_id"] != "ilink-user-1" || status.Credential["ilink_bot_id"] != "account-1" {
		t.Fatalf("credential=%+v", status.Credential)
	}
	if status.Credential["bot_token"] != "token-1" || status.Credential["base_url"] != server.URL {
		t.Fatalf("credential=%+v", status.Credential)
	}
	if status.State["context_tokens"] == nil || status.State["stream_cursors"] == nil {
		t.Fatalf("state=%+v", status.State)
	}
}

func TestWeixinConnectorPollLoginClearsExpiredStateForExistingAccount(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/get_qrcode_status" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("qrcode"); got != "qr-relogin" {
			t.Fatalf("qrcode=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":        "confirmed",
			"bot_token":     "token-new",
			"ilink_bot_id":  "bot-new",
			"ilink_user_id": "ilink-user-stable",
			"baseurl":       server.URL,
		})
	}))
	defer server.Close()

	targetURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Transport: rewriteTransport{target: targetURL, base: http.DefaultTransport}}
	connector := NewConnector()
	runtime := sdk.Runtime{
		HTTPClient: httpClient,
		Account: sdk.ChannelAccount{
			UUID:     "acct-existing",
			Platform: Platform,
			Credential: map[string]any{
				"account_id":    "ilink-user-stable",
				"bot_token":     "token-old",
				"base_url":      server.URL,
				"ilink_user_id": "ilink-user-stable",
				"ilink_bot_id":  "bot-old",
			},
			State: map[string]any{
				"status":     "login_required",
				"last_error": "getupdates session expired",
				sdk.RuntimeHealthKeyStreamConnectionState: sdk.RuntimeHealthStateExpired,
				sdk.RuntimeHealthKeyStreamLastError:       "getupdates session expired",
				sdk.RuntimeHealthKeyStreamSessionExpired:  true,
				"stream_session_expired_reason":           "getupdates session expired",
				"stream_session_expired_operation":        "getupdates",
				"stream_session_expired_code":             -14,
				"stream_session_expired_msg":              "session timeout",
			},
		},
	}

	status, err := connector.PollLogin(context.Background(), sdk.LoginPollRequest{
		WorkspaceUUID: "workspace-1",
		ChannelUUID:   "channel-1",
		ChallengeCode: "qr-relogin",
		Runtime:       runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Confirmed || status.Account.UUID != "acct-existing" {
		t.Fatalf("status=%+v", status)
	}
	if status.Credential["account_id"] != "ilink-user-stable" || status.Credential["bot_token"] != "token-new" || status.Credential["ilink_bot_id"] != "bot-new" {
		t.Fatalf("credential=%+v", status.Credential)
	}
	if status.State["status"] != "active" || status.State["last_error"] != "" {
		t.Fatalf("state=%+v", status.State)
	}
	if status.State[sdk.RuntimeHealthKeyStreamConnectionState] != "" || status.State[sdk.RuntimeHealthKeyStreamLastError] != "" || status.State[sdk.RuntimeHealthKeyStreamSessionExpired] != false {
		t.Fatalf("runtime health was not cleared: %+v", status.State)
	}
	if status.State["stream_session_expired_reason"] != "" || status.State["stream_session_expired_operation"] != "" || status.State["stream_session_expired_code"] != 0 || status.State["stream_session_expired_msg"] != "" {
		t.Fatalf("expired detail was not cleared: %+v", status.State)
	}
}

func TestWeixinConnectorScenarioQRCodeInboundAndFixedReply(t *testing.T) {
	const fixedReply = "Beak Agent 已收到你的消息"
	sentCh := make(chan scenarioSentMessage, 1)
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	var updatesServed int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/get_bot_qrcode":
			if got := r.URL.Query().Get("bot_type"); got != "3" {
				t.Fatalf("bot_type=%q", got)
			}
			if got := r.Header.Get("iLink-App-Id"); got != "bot" {
				t.Fatalf("iLink-App-Id=%q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"qrcode":             "qr-scenario-1",
				"qrcode_img_content": "https://example.test/qr-scenario-1",
			})
		case "/ilink/bot/get_qrcode_status":
			if got := r.URL.Query().Get("qrcode"); got != "qr-scenario-1" {
				t.Fatalf("qrcode=%q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-scenario-1",
				"ilink_bot_id":  "account-scenario-1",
				"ilink_user_id": "ilink-user-scenario-1",
				"baseurl":       server.URL,
			})
		case "/ilink/bot/msg/notifystart":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/msg/notifystop":
			stopOnce.Do(func() { close(stopCh) })
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/getupdates":
			updatesServed++
			if updatesServed == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ret":             0,
					"get_updates_buf": "buf-scenario-1",
					"msgs": []map[string]any{
						{
							"message_id":    1001,
							"from_user_id":  "user-scenario-1",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-scenario-1",
							"item_list": []map[string]any{
								{
									"type":      1,
									"text_item": map[string]any{"text": "你好 Beak"},
								},
							},
						},
					},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "get_updates_buf": "buf-scenario-1"})
		case "/ilink/bot/getconfig":
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				ContextToken string `json:"context_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "user-scenario-1" || body.ContextToken != "ctx-scenario-1" {
				t.Fatalf("getconfig body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-scenario-1"})
		case "/ilink/bot/sendtyping":
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				TypingTicket string `json:"typing_ticket"`
				Status       int    `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "user-scenario-1" || body.TypingTicket != "typing-ticket-scenario-1" || (body.Status != 1 && body.Status != 2) {
				t.Fatalf("sendtyping body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/sendmessage":
			if got := r.Header.Get("Authorization"); got != "Bearer token-scenario-1" {
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
			text := ""
			if len(body.Message.ItemList) > 0 {
				text = body.Message.ItemList[0].TextItem.Text
			}
			sentCh <- scenarioSentMessage{
				to:           body.Message.ToUserID,
				text:         text,
				contextToken: body.Message.ContextToken,
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
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
	connector := NewConnector()
	gateway := newScenarioSDKGateway(fixedReply)
	accountStore := newFakeSDKAccountStore()
	loginRuntime := sdk.Runtime{HTTPClient: httpClient}

	challenge, err := connector.StartLogin(context.Background(), sdk.LoginStartRequest{
		WorkspaceUUID: "workspace-scenario-1",
		ChannelUUID:   "channel-scenario-1",
		Runtime:       loginRuntime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if challenge.Type != sdk.LoginModeQRCode || challenge.Code != "qr-scenario-1" {
		t.Fatalf("challenge=%+v", challenge)
	}

	status, err := connector.PollLogin(context.Background(), sdk.LoginPollRequest{
		WorkspaceUUID:  "workspace-scenario-1",
		ChannelUUID:    "channel-scenario-1",
		ChallengeState: challenge.State,
		Runtime:        loginRuntime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Confirmed || status.Account.UUID != "ilink-user-scenario-1" {
		t.Fatalf("status=%+v", status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- connector.Start(ctx, sdk.Runtime{
			WorkspaceUUID: "workspace-scenario-1",
			Channel: sdk.Channel{
				UUID:          "channel-scenario-1",
				WorkspaceUUID: "workspace-scenario-1",
				Platform:      "weixin",
			},
			Account:         status.Account,
			Gateway:         gateway,
			AccountStore:    accountStore,
			HTTPClient:      httpClient,
			PollInterval:    time.Millisecond,
			StreamReconnect: time.Millisecond,
		})
	}()

	var sent scenarioSentMessage
	select {
	case sent = <-sentCh:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for fixed bot reply to be sent to weixin")
	}
	select {
	case err := <-gateway.streamDone:
		if err != nil {
			cancel()
			t.Fatalf("stream error=%v", err)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for fixed bot stream to finish")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Start error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connector to stop")
	}
	select {
	case <-stopCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for weixin notifystop")
	}

	if sent.to != "user-scenario-1" || sent.text != fixedReply || sent.contextToken != "ctx-scenario-1" {
		t.Fatalf("sent=%+v", sent)
	}
	gateway.mu.Lock()
	createdMessages := append([]sdk.CreateMessageRequest(nil), gateway.messages...)
	chatSessions := append([]sdk.EnsureChatSessionRequest(nil), gateway.chatSessions...)
	gateway.mu.Unlock()
	if len(createdMessages) != 1 {
		t.Fatalf("created messages=%+v", createdMessages)
	}
	created := createdMessages[0]
	if created.SessionUUID != "session-scenario-1" || created.Content != "你好 Beak" || created.SenderID != "im:weixin:direct:user-scenario-1:user:user-scenario-1" {
		t.Fatalf("created message=%+v", created)
	}
	if created.DedupeKey != "ilink-user-scenario-1:message:1001" {
		t.Fatalf("dedupe key=%q", created.DedupeKey)
	}
	if len(chatSessions) != 1 || chatSessions[0].AccountUUID != "ilink-user-scenario-1" || chatSessions[0].ChatType != sdk.ChatTypeDirect || chatSessions[0].ChatID != "user-scenario-1" {
		t.Fatalf("chat sessions=%+v", chatSessions)
	}

	state := accountStore.state("ilink-user-scenario-1")
	if state["get_updates_buf"] != "buf-scenario-1" {
		t.Fatalf("state=%+v", state)
	}
	contextTokens, ok := state["context_tokens"].(map[string]string)
	if !ok || contextTokens["user-scenario-1"] != "ctx-scenario-1" {
		t.Fatalf("context tokens=%+v", state["context_tokens"])
	}
	sentBeakMessages, ok := state["sent_beak_messages"].(map[string]string)
	if !ok || sentBeakMessages["agent-message-scenario-1"] == "" {
		t.Fatalf("sent beak messages=%+v", state["sent_beak_messages"])
	}

	status.Account.State = state
	mentionResult, err := connector.Send(context.Background(), sdk.Runtime{
		Account:    status.Account,
		HTTPClient: httpClient,
	}, sdk.OutboundMessage{
		AccountUUID: "ilink-user-scenario-1",
		ChatType:    sdk.ChatTypeDirect,
		ChatID:      "user-scenario-1",
		Text:        "mention reply",
		MessageUUID: "agent-message-mention",
		MentionAll:  true,
		Mentions: []sdk.MentionIdentity{
			{ID: "user-scenario-1", IDType: "user_id", DisplayName: "Alice"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mentionResult.AccountUUID != "ilink-user-scenario-1" || mentionResult.Platform != Platform {
		t.Fatalf("mention result=%+v", mentionResult)
	}
	select {
	case sent = <-sentCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for mention reply to be sent to weixin")
	}
	if sent.to != "user-scenario-1" || sent.text != "@all @Alice\nmention reply" || sent.contextToken != "ctx-scenario-1" {
		t.Fatalf("mention sent=%+v", sent)
	}
}

func TestWeixinConnectorStartProcessesInboundWithRuntimeAccount(t *testing.T) {
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
							"from_user_id":  "user-1",
							"to_user_id":    "bot-1",
							"group_id":      "group-1",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-group-1",
							"item_list": []map[string]any{
								{
									"type":      1,
									"text_item": map[string]any{"text": "hello group"},
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

	gateway := &fakeSDKGateway{}
	accountStore := newFakeSDKAccountStore()
	connector := NewConnector()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	err := connector.Start(ctx, sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: "weixin"},
		Account: sdk.ChannelAccount{
			UUID:          "account-1",
			WorkspaceUUID: "workspace-1",
			ChannelUUID:   "channel-1",
			Platform:      "weixin",
			Credential: map[string]any{
				"account_id":    "account-1",
				"bot_token":     "token-1",
				"base_url":      server.URL,
				"ilink_user_id": "ilink-user-1",
			},
			State: map[string]any{},
		},
		Gateway:         gateway,
		AccountStore:    accountStore,
		PollInterval:    time.Millisecond,
		StreamReconnect: time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error=%v", err)
	}

	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if gateway.channelLinkAccountUUID != "account-1" {
		t.Fatalf("channel link account=%q", gateway.channelLinkAccountUUID)
	}
	if len(gateway.chatSessions) != 1 {
		t.Fatalf("chat sessions=%+v", gateway.chatSessions)
	}
	chatReq := gateway.chatSessions[0]
	if chatReq.AccountUUID != "account-1" || chatReq.ChatType != sdk.ChatTypeGroup || chatReq.ChatID != "group-1" || chatReq.SenderID != "user-1" {
		t.Fatalf("chat session request=%+v", chatReq)
	}
	if len(gateway.messages) != 1 {
		t.Fatalf("messages=%+v", gateway.messages)
	}
	if gateway.messages[0].SenderID != "im:weixin:group:group-1:user:user-1" || gateway.messages[0].Content != "hello group" {
		t.Fatalf("message=%+v", gateway.messages[0])
	}
	inbound, ok := gateway.messages[0].Metadata["inbound_message"].(sdk.InboundMessage)
	if !ok {
		t.Fatalf("missing inbound metadata=%+v", gateway.messages[0].Metadata)
	}
	if inbound.WorkspaceUUID != "workspace-1" || inbound.ChannelUUID != "channel-1" || inbound.AccountUUID != "account-1" || inbound.ChatType != sdk.ChatTypeGroup || inbound.ChatID != "group-1" {
		t.Fatalf("inbound=%+v", inbound)
	}

	state := accountStore.state("account-1")
	if state["get_updates_buf"] != "buf-1" {
		t.Fatalf("saved state=%+v", state)
	}
	contextTokens, ok := state["context_tokens"].(map[string]string)
	if !ok || contextTokens["group:group-1"] != "ctx-group-1" {
		t.Fatalf("context tokens=%+v", state["context_tokens"])
	}
}

func TestWeixinConnectorScenarioPollingDedupesAndCachesChatContext(t *testing.T) {
	var updatesServed int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/msg/notifystart", "/ilink/bot/msg/notifystop":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/getupdates":
			var body struct {
				GetUpdatesBuf string `json:"get_updates_buf"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			updatesServed++
			switch updatesServed {
			case 1:
				if body.GetUpdatesBuf != "" {
					t.Fatalf("first getupdates buf=%q", body.GetUpdatesBuf)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ret":             0,
					"get_updates_buf": "buf-scenario-2-a",
					"msgs": []map[string]any{
						{
							"message_id":    201,
							"from_user_id":  "user-group-1",
							"to_user_id":    "bot-scenario-2",
							"group_id":      "group-scenario-2",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-group-2",
							"mention_all":   true,
							"mentions": []map[string]any{
								{"user_id": "user-group-1", "name": "Alice"},
							},
							"item_list": []map[string]any{
								{"type": 1, "text_item": map[string]any{"text": "group hello"}},
							},
						},
					},
				})
			case 2:
				if body.GetUpdatesBuf != "buf-scenario-2-a" {
					t.Fatalf("second getupdates buf=%q", body.GetUpdatesBuf)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ret":             0,
					"get_updates_buf": "buf-scenario-2-b",
					"msgs": []map[string]any{
						{
							"message_id":    201,
							"from_user_id":  "user-group-1",
							"to_user_id":    "bot-scenario-2",
							"group_id":      "group-scenario-2",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-duplicate-should-not-win",
							"item_list": []map[string]any{
								{"type": 1, "text_item": map[string]any{"text": "duplicate group"}},
							},
						},
						{
							"message_id":    202,
							"from_user_id":  "user-direct-2",
							"message_type":  1,
							"message_state": 2,
							"context_token": "ctx-direct-2",
							"item_list": []map[string]any{
								{"type": 1, "text_item": map[string]any{"text": "direct hello"}},
							},
						},
					},
				})
			default:
				if body.GetUpdatesBuf != "buf-scenario-2-b" {
					t.Fatalf("later getupdates buf=%q", body.GetUpdatesBuf)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "get_updates_buf": "buf-scenario-2-b"})
			}
		case "/ilink/bot/getconfig":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-scenario-2"})
		case "/ilink/bot/sendtyping":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	gateway := &fakeSDKGateway{}
	accountStore := newFakeSDKAccountStore()
	connector := NewConnector()
	ctx, cancel := context.WithTimeout(context.Background(), 160*time.Millisecond)
	defer cancel()
	err := connector.Start(ctx, sdk.Runtime{
		WorkspaceUUID: "workspace-2",
		Channel:       sdk.Channel{UUID: "channel-2", WorkspaceUUID: "workspace-2", Platform: Platform},
		Account: sdk.ChannelAccount{
			UUID:          "account-scenario-2",
			WorkspaceUUID: "workspace-2",
			ChannelUUID:   "channel-2",
			Platform:      Platform,
			Credential: map[string]any{
				"account_id":    "account-scenario-2",
				"bot_token":     "token-scenario-2",
				"base_url":      server.URL,
				"ilink_user_id": "ilink-user-scenario-2",
			},
			State: map[string]any{},
		},
		Gateway:         gateway,
		AccountStore:    accountStore,
		PollInterval:    time.Millisecond,
		StreamReconnect: time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error=%v", err)
	}

	gateway.mu.Lock()
	chatSessions := append([]sdk.EnsureChatSessionRequest(nil), gateway.chatSessions...)
	messages := append([]sdk.CreateMessageRequest(nil), gateway.messages...)
	gateway.mu.Unlock()
	if len(chatSessions) != 2 {
		t.Fatalf("chat sessions=%+v", chatSessions)
	}
	if chatSessions[0].ChatType != sdk.ChatTypeGroup || chatSessions[0].ChatID != "group-scenario-2" ||
		chatSessions[1].ChatType != sdk.ChatTypeDirect || chatSessions[1].ChatID != "user-direct-2" {
		t.Fatalf("chat sessions=%+v", chatSessions)
	}
	if len(messages) != 2 {
		t.Fatalf("messages=%+v", messages)
	}
	if messages[0].Content != "group hello" || messages[1].Content != "direct hello" {
		t.Fatalf("messages=%+v", messages)
	}
	if messages[0].DedupeKey != "account-scenario-2:message:201" || messages[1].DedupeKey != "account-scenario-2:message:202" {
		t.Fatalf("dedupe keys=%q %q", messages[0].DedupeKey, messages[1].DedupeKey)
	}
	inbound, ok := messages[0].Metadata["inbound_message"].(sdk.InboundMessage)
	if !ok || !inbound.MentionAll || inbound.MentionedMe || len(inbound.Mentions) != 2 {
		t.Fatalf("inbound=%+v metadata=%+v", inbound, messages[0].Metadata)
	}

	state := accountStore.state("account-scenario-2")
	if state["get_updates_buf"] != "buf-scenario-2-b" {
		t.Fatalf("state=%+v", state)
	}
	contextTokens, ok := state["context_tokens"].(map[string]string)
	if !ok || contextTokens["group:group-scenario-2"] != "ctx-group-2" || contextTokens["user-direct-2"] != "ctx-direct-2" {
		t.Fatalf("context tokens=%+v", state["context_tokens"])
	}
	inboundSeen, ok := state["inbound_seen"].(map[string]string)
	if !ok || len(inboundSeen) != 2 || inboundSeen["account-scenario-2:message:201"] == "" || inboundSeen["account-scenario-2:message:202"] == "" {
		t.Fatalf("inbound seen=%+v", state["inbound_seen"])
	}
}

func TestWeixinConnectorSendUsesRequestedAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-2" {
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
		if body.Message.ToUserID != "group-1" || body.Message.ContextToken != "ctx-account-2" {
			t.Fatalf("message=%+v", body.Message)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "@all @Alice\nreply" {
			t.Fatalf("items=%+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	connector := NewConnector()
	result, err := connector.Send(context.Background(), sdk.Runtime{
		Accounts: []sdk.ChannelAccount{
			sdkAccount("account-1", "token-1", server.URL, map[string]string{"group:group-1": "ctx-account-1"}),
			sdkAccount("account-2", "token-2", server.URL, map[string]string{"group:group-1": "ctx-account-2"}),
		},
	}, sdk.OutboundMessage{
		AccountUUID: "account-2",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Text:        "reply",
		MentionAll:  true,
		Mentions: []sdk.MentionIdentity{
			{ID: "user-1", IDType: "user_id", DisplayName: "Alice"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AccountUUID != "account-2" || result.Platform != Platform {
		t.Fatalf("result=%+v", result)
	}
}

func TestWeixinConnectorSendRejectsEmptyText(t *testing.T) {
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Account: sdkAccount("account-1", "token-1", "https://example.invalid", map[string]string{"group:group-1": "ctx-1"}),
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
	})
	if err == nil || result != nil || !strings.Contains(err.Error(), "text is required") {
		t.Fatalf("result=%+v error=%v, want empty text rejection", result, err)
	}
}

func TestWeixinConnectorSendMarkdownFallsBackToText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body struct {
			Message struct {
				ItemList []struct {
					TextItem struct {
						Text string `json:"text"`
					} `json:"text_item"`
				} `json:"item_list"`
			} `json:"msg"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "# 日志查询\n- 错误日志" {
			t.Fatalf("items=%+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	_, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Account: sdkAccount("account-1", "token-1", server.URL, map[string]string{"group:group-1": "ctx-1"}),
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Text:        "# 日志查询\n- 错误日志",
		Format:      "markdown",
		Title:       "日志查询",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWeixinConnectorAcknowledgeSendsTyping(t *testing.T) {
	var sawGetConfig bool
	var sawTyping bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/getconfig":
			sawGetConfig = true
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				ContextToken string `json:"context_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "group-1" || body.ContextToken != "ctx-group-1" {
				t.Fatalf("getconfig body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-1"})
		case "/ilink/bot/sendtyping":
			sawTyping = true
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				TypingTicket string `json:"typing_ticket"`
				Status       int    `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "group-1" || body.TypingTicket != "typing-ticket-1" || body.Status != 1 {
				t.Fatalf("sendtyping body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	result, err := NewConnector().Acknowledge(context.Background(), sdk.Runtime{
		Account: sdk.ChannelAccount{
			UUID:     "account-1",
			Platform: "weixin",
			Credential: map[string]any{
				"account_id": "account-1",
				"bot_token":  "token-1",
				"base_url":   server.URL,
			},
			State: map[string]any{
				"context_tokens": map[string]any{
					"group:group-1": "ctx-group-1",
				},
			},
		},
	}, sdk.OutboundAck{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Action:      "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawGetConfig || !sawTyping {
		t.Fatalf("sawGetConfig=%v sawTyping=%v", sawGetConfig, sawTyping)
	}
	if result.Status != "sent" || result.Mode != "typing" || result.Raw["context_key"] != "group:group-1" {
		t.Fatalf("result=%+v", result)
	}
}

func TestWeixinConnectorAcknowledgeRefreshesExpiredTypingTicket(t *testing.T) {
	var sendTypingCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/getconfig":
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				ContextToken string `json:"context_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "group-1" || body.ContextToken != "ctx-group-1" {
				t.Fatalf("getconfig body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-new"})
		case "/ilink/bot/sendtyping":
			sendTypingCalls++
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				TypingTicket string `json:"typing_ticket"`
				Status       int    `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if sendTypingCalls == 1 {
				if body.TypingTicket != "typing-ticket-old" {
					t.Fatalf("first sendtyping body=%+v", body)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"errcode": -14, "errmsg": "session expired"})
				return
			}
			if body.ILinkUserID != "group-1" || body.TypingTicket != "typing-ticket-new" || body.Status != 1 {
				t.Fatalf("second sendtyping body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	result, err := NewConnector().Acknowledge(context.Background(), sdk.Runtime{
		Account: sdk.ChannelAccount{
			UUID:     "account-1",
			Platform: "weixin",
			Credential: map[string]any{
				"account_id": "account-1",
				"bot_token":  "token-1",
				"base_url":   server.URL,
			},
			State: map[string]any{
				"context_tokens": map[string]any{
					"group:group-1": "ctx-group-1",
				},
				"typing_tickets": map[string]any{
					"group:group-1": "typing-ticket-old",
				},
			},
		},
	}, sdk.OutboundAck{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Action:      "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sendTypingCalls != 2 || result.Status != "sent" || result.Mode != "typing" {
		t.Fatalf("sendTypingCalls=%d result=%+v", sendTypingCalls, result)
	}
}

func TestWeixinConnectorAcknowledgeSkipsExpiredSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/getconfig" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"errcode": -14, "errmsg": "session expired"})
	}))
	defer server.Close()

	result, err := NewConnector().Acknowledge(context.Background(), sdk.Runtime{
		Account: sdk.ChannelAccount{
			UUID:     "account-1",
			Platform: "weixin",
			Credential: map[string]any{
				"account_id": "account-1",
				"bot_token":  "token-1",
				"base_url":   server.URL,
			},
			State: map[string]any{
				"context_tokens": map[string]any{
					"group:group-1": "ctx-group-1",
				},
			},
		},
	}, sdk.OutboundAck{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Action:      "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "skipped" || result.Mode != "typing" || result.Raw["reason"] != "session_expired" {
		t.Fatalf("result=%+v", result)
	}
}

func TestWeixinConnectorAcknowledgeSkipsWithoutContextToken(t *testing.T) {
	result, err := NewConnector().Acknowledge(context.Background(), sdk.Runtime{
		Account: sdk.ChannelAccount{
			UUID:     "account-1",
			Platform: "weixin",
			Credential: map[string]any{
				"account_id": "account-1",
				"bot_token":  "token-1",
			},
		},
	}, sdk.OutboundAck{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeDirect,
		ChatID:      "user-1",
		Action:      "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "skipped" || result.Mode != "typing" || result.Raw["reason"] != "missing_context_token" {
		t.Fatalf("result=%+v", result)
	}
}

func TestWeixinConnectorSendRawMentions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body struct {
			Message struct {
				ItemList []struct {
					TextItem struct {
						Text string `json:"text"`
					} `json:"text_item"`
				} `json:"item_list"`
			} `json:"msg"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "@all @Raw User\nreply" {
			t.Fatalf("items=%+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	_, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Account: sdkAccount("account-1", "token-1", server.URL, map[string]string{"group:group-1": "ctx-1"}),
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Text:        "reply",
		Raw: map[string]any{
			"mentionAll": true,
			"mentions": []any{
				map[string]any{"id": "raw-user", "display_name": "Raw User"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWeixinConnectorSendLoadsLatestAccountState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body struct {
			Message struct {
				ToUserID     string `json:"to_user_id"`
				ContextToken string `json:"context_token"`
			} `json:"msg"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Message.ToUserID != "group-1" || body.Message.ContextToken != "ctx-loaded" {
			t.Fatalf("message=%+v", body.Message)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	store := newFakeSDKAccountStore()
	if err := store.SaveChannelAccountState(context.Background(), "account-1", map[string]any{
		"context_tokens": map[string]string{"group:group-1": "ctx-loaded"},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Account:      sdkAccount("account-1", "token-1", server.URL, nil),
		AccountStore: store,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Text:        "reply",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AccountUUID != "account-1" || result.Platform != Platform {
		t.Fatalf("result=%+v", result)
	}
}

func TestWeixinConnectorSendResumesMultipartWithoutDuplicatingCompletedChunks(t *testing.T) {
	var mu sync.Mutex
	var requests []struct {
		Text     string
		ClientID string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Message struct {
				ClientID string `json:"client_id"`
				ItemList []struct {
					TextItem struct {
						Text string `json:"text"`
					} `json:"text_item"`
				} `json:"item_list"`
			} `json:"msg"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		call := len(requests) + 1
		requests = append(requests, struct {
			Text     string
			ClientID string
		}{Text: body.Message.ItemList[0].TextItem.Text, ClientID: body.Message.ClientID})
		mu.Unlock()
		if call == 2 {
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	store := newPatchSDKAccountStore(map[string]any{
		"context_tokens": map[string]string{"group:group-1": "ctx-1"},
	})
	runtime := sdk.Runtime{
		Account:      sdkAccount("account-1", "token-1", server.URL, nil),
		AccountStore: store,
	}
	req := sdk.OutboundMessage{
		AccountUUID: "account-1",
		MessageUUID: "message-resume-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "group-1",
		Text:        strings.Repeat("你", weixin.MaxTextRunes+10),
	}

	if _, err := NewConnector().Send(context.Background(), runtime, req); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("first Send() error = %v, want temporary platform failure", err)
	}
	result, err := NewConnector().Send(context.Background(), runtime, req)
	if err != nil {
		t.Fatalf("resumed Send() error = %v", err)
	}
	if result.Raw["resumed"] != true {
		t.Fatalf("resumed result = %#v", result)
	}
	mu.Lock()
	gotRequests := append([]struct {
		Text     string
		ClientID string
	}(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 3 {
		t.Fatalf("requests = %d, want 3", len(gotRequests))
	}
	if gotRequests[0].Text == gotRequests[2].Text {
		t.Fatal("completed first chunk was sent again during resume")
	}
	if gotRequests[1] != gotRequests[2] {
		t.Fatalf("failed/retried chunk differ: failed=%+v retried=%+v", gotRequests[1], gotRequests[2])
	}
	if gotRequests[0].ClientID == "" || gotRequests[0].ClientID == gotRequests[1].ClientID {
		t.Fatalf("client ids are not stable per chunk: %+v", gotRequests)
	}

	if _, err := NewConnector().Send(context.Background(), runtime, req); err != nil {
		t.Fatalf("completed replay error = %v", err)
	}
	mu.Lock()
	requestCount := len(requests)
	mu.Unlock()
	if requestCount != 3 {
		t.Fatalf("completed replay sent another request: %d", requestCount)
	}
	changed := req
	changed.Text += " changed"
	if _, err := NewConnector().Send(context.Background(), runtime, changed); err == nil || !strings.Contains(err.Error(), "different outbound payload") {
		t.Fatalf("changed payload error = %v, want message_uuid reuse rejection", err)
	}

	state, err := store.LoadChannelAccountState(context.Background(), "account-1")
	if err != nil {
		t.Fatal(err)
	}
	entry := mapValue(decodeOutboundChunkProgressEntries(state[outboundChunkProgressKey])[req.MessageUUID])
	if entry["completed"] != true || intValue(entry["next_index"]) != 2 {
		t.Fatalf("stored progress = %#v, want completed two chunks", entry)
	}
}

func TestWeixinConnectorSendMultipartRequiresRetryIdentityAndStore(t *testing.T) {
	text := strings.Repeat("你", weixin.MaxTextRunes+1)
	runtime := sdk.Runtime{Account: sdkAccount("account-1", "token-1", "https://example.invalid", map[string]string{"user-1": "ctx-1"})}
	request := sdk.OutboundMessage{AccountUUID: "account-1", ChatType: sdk.ChatTypeDirect, ChatID: "user-1", Text: text}
	if _, err := NewConnector().Send(context.Background(), runtime, request); err == nil || !strings.Contains(err.Error(), "message_uuid") {
		t.Fatalf("missing message_uuid error = %v", err)
	}
	request.MessageUUID = "message-1"
	if _, err := NewConnector().Send(context.Background(), runtime, request); err == nil || !strings.Contains(err.Error(), "AccountStore") {
		t.Fatalf("missing AccountStore error = %v", err)
	}
}

func TestWeixinConnectorSendProgressUsesBoundedPatchWithoutHealthOverwrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	oldEntries := make([]any, 0, maxTrackedOutboundProgress)
	for index := 0; index < maxTrackedOutboundProgress; index++ {
		oldEntries = append(oldEntries, map[string]any{
			"message_uuid": fmt.Sprintf("old-%03d", index),
			"fingerprint":  "old",
			"next_index":   1,
			"client_ids":   []any{"old-client"},
			"completed":    true,
			"updated_at":   time.Date(2026, 1, 1, 0, 0, index, 0, time.UTC).Format(time.RFC3339Nano),
		})
	}
	store := newPatchSDKAccountStore(map[string]any{
		"context_tokens":                      map[string]string{"user-1": "ctx-1"},
		sdk.RuntimeHealthKeyStreamLastEventAt: "2026-07-21T00:00:00Z",
		outboundChunkProgressKey:              oldEntries,
	})
	_, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Account:      sdkAccount("account-1", "token-1", server.URL, nil),
		AccountStore: store,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		MessageUUID: "message-new",
		ChatType:    sdk.ChatTypeDirect,
		ChatID:      "user-1",
		Text:        strings.Repeat("你", weixin.MaxTextRunes+1),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, patch := range store.savedPatches() {
		if _, ok := patch[sdk.RuntimeHealthKeyStreamLastEventAt]; ok {
			t.Fatalf("progress patch overwrote runtime health: %#v", patch)
		}
		if len(patch) != 2 || patch[outboundChunkProgressKey] == nil || patch["updated_at"] == nil {
			t.Fatalf("progress patch is not atomic and narrow: %#v", patch)
		}
	}
	state, err := store.LoadChannelAccountState(context.Background(), "account-1")
	if err != nil {
		t.Fatal(err)
	}
	if state[sdk.RuntimeHealthKeyStreamLastEventAt] != "2026-07-21T00:00:00Z" {
		t.Fatalf("runtime health was changed: %#v", state)
	}
	entries := decodeOutboundChunkProgressEntries(state[outboundChunkProgressKey])
	if len(entries) != maxTrackedOutboundProgress {
		t.Fatalf("progress entries = %d, want %d", len(entries), maxTrackedOutboundProgress)
	}
	if _, ok := entries["old-000"]; ok {
		t.Fatal("oldest progress entry was not pruned")
	}
	if _, ok := entries["message-new"]; !ok {
		t.Fatal("new progress entry was not stored")
	}
}

func TestWeixinConnectorSendRequiresAccountWhenAmbiguous(t *testing.T) {
	_, err := NewConnector().Send(context.Background(), sdk.Runtime{
		Accounts: []sdk.ChannelAccount{
			sdkAccount("account-1", "token-1", "https://example.invalid", nil),
			sdkAccount("account-2", "token-2", "https://example.invalid", nil),
		},
	}, sdk.OutboundMessage{
		ChatType: sdk.ChatTypeDirect,
		ChatID:   "user-1",
		Text:     "reply",
	})
	if err == nil {
		t.Fatal("expected ambiguous account error")
	}
}

func TestWeixinConnectorStopNotifiesPlatform(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/msg/notifystop" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("Authorization=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
	}))
	defer server.Close()

	if err := NewConnector().Stop(context.Background(), sdkAccount("account-1", "token-1", server.URL, nil)); err != nil {
		t.Fatal(err)
	}
}

func sdkAccount(accountUUID, token, baseURL string, contextTokens map[string]string) sdk.ChannelAccount {
	state := map[string]any{}
	if contextTokens != nil {
		state["context_tokens"] = contextTokens
	}
	return sdk.ChannelAccount{
		UUID:       accountUUID,
		Platform:   Platform,
		State:      state,
		Credential: map[string]any{"account_id": accountUUID, "bot_token": token, "base_url": baseURL, "ilink_user_id": "ilink-" + accountUUID},
	}
}

type fakeSDKGateway struct {
	mu                     sync.Mutex
	channelLinkAccountUUID string
	chatSessions           []sdk.EnsureChatSessionRequest
	messages               []sdk.CreateMessageRequest
}

func (f *fakeSDKGateway) EnsureChannel(context.Context, sdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}

func (f *fakeSDKGateway) EnsureChannelLinkSession(_ context.Context, req sdk.EnsureChannelLinkSessionRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channelLinkAccountUUID = req.AccountUUID
	return "channel-link-session-1", nil
}

func (f *fakeSDKGateway) EnsureChatSession(_ context.Context, req sdk.EnsureChatSessionRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chatSessions = append(f.chatSessions, req)
	return "session-1", nil
}

func (f *fakeSDKGateway) CreateMessage(_ context.Context, req sdk.CreateMessageRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, req)
	return "message-1", nil
}

func (f *fakeSDKGateway) StreamSession(context.Context, sdk.StreamSessionRequest, func(sdk.StreamEvent) error) error {
	return nil
}

func (f *fakeSDKGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (f *fakeSDKGateway) BridgeParticipantID(string) string {
	return "bridge:weixin"
}

type scenarioSDKGateway struct {
	mu           sync.Mutex
	fixedReply   string
	streamOnce   sync.Once
	streamDone   chan error
	chatSessions []sdk.EnsureChatSessionRequest
	messages     []sdk.CreateMessageRequest
}

type scenarioSentMessage struct {
	to           string
	text         string
	contextToken string
}

func newScenarioSDKGateway(fixedReply string) *scenarioSDKGateway {
	return &scenarioSDKGateway{fixedReply: fixedReply, streamDone: make(chan error, 1)}
}

func (g *scenarioSDKGateway) EnsureChannel(context.Context, sdk.EnsureChannelRequest) (string, error) {
	return "channel-scenario-1", nil
}

func (g *scenarioSDKGateway) EnsureChannelLinkSession(context.Context, sdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "channel-link-session-scenario-1", nil
}

func (g *scenarioSDKGateway) EnsureChatSession(_ context.Context, req sdk.EnsureChatSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.chatSessions = append(g.chatSessions, req)
	return "session-scenario-1", nil
}

func (g *scenarioSDKGateway) CreateMessage(_ context.Context, req sdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messages = append(g.messages, req)
	return "message-scenario-1", nil
}

func (g *scenarioSDKGateway) StreamSession(_ context.Context, req sdk.StreamSessionRequest, handle func(sdk.StreamEvent) error) error {
	var err error
	g.streamOnce.Do(func() {
		err = handle(sdk.StreamEvent{
			EventUUID:   "event-scenario-1",
			SessionUUID: req.SessionUUID,
			EventType:   "message",
			MessageUUID: "agent-message-scenario-1",
			SenderID:    g.AgentParticipantID(),
			Content:     g.fixedReply,
		})
		g.streamDone <- err
	})
	return err
}

func (g *scenarioSDKGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *scenarioSDKGateway) BridgeParticipantID(string) string {
	return "bridge:weixin"
}

type fakeSDKAccountStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

type patchSDKAccountStore struct {
	mu      sync.Mutex
	state   map[string]any
	patches []map[string]any
}

func newPatchSDKAccountStore(state map[string]any) *patchSDKAccountStore {
	return &patchSDKAccountStore{state: cloneMap(state)}
}

func (s *patchSDKAccountStore) SaveChannelAccountState(_ context.Context, _ string, patch map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyPatch := cloneMap(patch)
	s.patches = append(s.patches, copyPatch)
	for key, value := range copyPatch {
		s.state[key] = value
	}
	return nil
}

func (s *patchSDKAccountStore) LoadChannelAccountState(_ context.Context, _ string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMap(s.state), nil
}

func (s *patchSDKAccountStore) savedPatches() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, 0, len(s.patches))
	for _, patch := range s.patches {
		out = append(out, cloneMap(patch))
	}
	return out
}

func newFakeSDKAccountStore() *fakeSDKAccountStore {
	return &fakeSDKAccountStore{states: make(map[string]map[string]any)}
}

func (s *fakeSDKAccountStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make(map[string]any, len(state))
	for key, value := range state {
		copied[key] = value
	}
	s.states[accountUUID] = copied
	return nil
}

func (s *fakeSDKAccountStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	return s.state(accountUUID), nil
}

func (s *fakeSDKAccountStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID]
}
