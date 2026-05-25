package weixin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetUpdatesBuildsIlinkRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/getupdates" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if got := r.Header.Get("AuthorizationType"); got != "ilink_bot_token" {
			t.Fatalf("AuthorizationType=%q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("Authorization=%q", got)
		}
		if got := r.Header.Get("iLink-App-Id"); got != DefaultAppID {
			t.Fatalf("iLink-App-Id=%q", got)
		}
		if got := r.Header.Get("X-WECHAT-UIN"); got == "" {
			t.Fatal("missing X-WECHAT-UIN")
		}
		var body GetUpdatesRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.GetUpdatesBuf != "buf-1" {
			t.Fatalf("get_updates_buf=%q", body.GetUpdatesBuf)
		}
		if body.BaseInfo.ChannelVersion == "" || body.BaseInfo.BotAgent != DefaultBotAgent {
			t.Fatalf("missing base_info: %+v", body.BaseInfo)
		}
		_ = json.NewEncoder(w).Encode(GetUpdatesResponse{Ret: 0, GetUpdatesBuf: "buf-2"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token-1")
	resp, err := client.GetUpdates(context.Background(), "buf-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetUpdatesBuf != "buf-2" {
		t.Fatalf("GetUpdatesBuf=%q", resp.GetUpdatesBuf)
	}
}

func TestSendTextBuildsMessageRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Message.ToUserID != "peer-1" {
			t.Fatalf("to_user_id=%q", body.Message.ToUserID)
		}
		if body.Message.ContextToken != "ctx-1" {
			t.Fatalf("context_token=%q", body.Message.ContextToken)
		}
		if body.Message.MessageType != MessageTypeBot || body.Message.MessageState != MessageStateFinish {
			t.Fatalf("unexpected message type/state: %+v", body.Message)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem.Text != "hello" {
			t.Fatalf("unexpected item_list: %+v", body.Message.ItemList)
		}
		_ = json.NewEncoder(w).Encode(SendMessageResponse{Ret: 0})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token-1")
	if err := client.SendText(context.Background(), "peer-1", "hello", "ctx-1"); err != nil {
		t.Fatal(err)
	}
}

func TestSendTextRequiresContextToken(t *testing.T) {
	client := NewClient("https://example.invalid", "token-1")
	err := client.SendText(context.Background(), "peer-1", "hello", "")
	if err == nil || !strings.Contains(err.Error(), "context_token") {
		t.Fatalf("err=%v", err)
	}
}

func TestSendTextSplitsLongMessage(t *testing.T) {
	var texts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var body SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Message.ContextToken != "ctx-1" {
			t.Fatalf("context_token=%q", body.Message.ContextToken)
		}
		if len(body.Message.ItemList) != 1 || body.Message.ItemList[0].TextItem == nil {
			t.Fatalf("unexpected item_list: %+v", body.Message.ItemList)
		}
		texts = append(texts, body.Message.ItemList[0].TextItem.Text)
		_ = json.NewEncoder(w).Encode(SendMessageResponse{Ret: 0})
	}))
	defer server.Close()

	client := NewClient(server.URL, "token-1")
	longText := strings.Repeat("你", MaxTextRunes) + "\n\n" + strings.Repeat("好", 10)
	if err := client.SendText(context.Background(), "peer-1", longText, "ctx-1"); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 2 {
		t.Fatalf("chunks=%d texts=%+v", len(texts), texts)
	}
	if runeCount(texts[0]) > MaxTextRunes || runeCount(texts[1]) > MaxTextRunes {
		t.Fatalf("chunk sizes=%d,%d", runeCount(texts[0]), runeCount(texts[1]))
	}
	if strings.Join(texts, "") != strings.Repeat("你", MaxTextRunes)+strings.Repeat("好", 10) {
		t.Fatalf("unexpected split texts=%+v", texts)
	}
}

func TestTypingEndpointsBuildRequests(t *testing.T) {
	var gotConfig bool
	var gotTyping bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/getconfig":
			gotConfig = true
			var body GetConfigRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "peer-1" || body.ContextToken != "ctx-1" {
				t.Fatalf("getconfig body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(GetConfigResponse{Ret: 0, TypingTicket: "ticket-1"})
		case "/ilink/bot/sendtyping":
			gotTyping = true
			var body SendTypingRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ILinkUserID != "peer-1" || body.TypingTicket != "ticket-1" || body.Status != TypingStatusStart {
				t.Fatalf("sendtyping body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(SendTypingResponse{Ret: 0})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "token-1")
	ticket, err := client.GetTypingTicket(context.Background(), "peer-1", "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	if ticket != "ticket-1" {
		t.Fatalf("ticket=%q", ticket)
	}
	if err := client.SendTyping(context.Background(), "peer-1", ticket, TypingStatusStart); err != nil {
		t.Fatal(err)
	}
	if !gotConfig || !gotTyping {
		t.Fatalf("gotConfig=%v gotTyping=%v", gotConfig, gotTyping)
	}
}

func TestQRCodeLoginEndpoints(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/get_bot_qrcode":
			if got := r.URL.Query().Get("bot_type"); got != "3" {
				t.Fatalf("bot_type=%q", got)
			}
			_ = json.NewEncoder(w).Encode(QRCodeResponse{QRCode: "qr-1", QRCodeImgContent: "https://example.test/qr"})
		case "/ilink/bot/get_qrcode_status":
			if got := r.URL.Query().Get("qrcode"); got != "qr-1" {
				t.Fatalf("qrcode=%q", got)
			}
			if r.Header.Get("iLink-App-ClientVersion") == "" {
				t.Fatal("missing iLink-App-ClientVersion")
			}
			if got := r.Header.Get("iLink-App-Id"); got != DefaultAppID {
				t.Fatalf("iLink-App-Id=%q", got)
			}
			_ = json.NewEncoder(w).Encode(QRCodeStatusResponse{Status: "confirmed", BotToken: "token-1", ILinkBotID: "account-1", BaseURL: server.URL})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "")
	qr, err := client.GetQRCode(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.GetQRCodeStatus(context.Background(), qr.QRCode)
	if err != nil {
		t.Fatal(err)
	}
	if status.BotToken != "token-1" || status.ILinkBotID != "account-1" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestWeixinMessageChatIdentity(t *testing.T) {
	direct := WeixinMessage{FromUserID: "user-1"}
	directChat := direct.ChatIdentity()
	if directChat.ChatType != ChatTypeDirect || directChat.ChatID != "user-1" || directChat.SenderID != "user-1" || directChat.StateKey() != "user-1" {
		t.Fatalf("direct chat=%+v key=%q", directChat, directChat.StateKey())
	}

	group := WeixinMessage{FromUserID: "user-1", ToUserID: "bot-1", GroupID: "group-1"}
	groupChat := group.ChatIdentity()
	if groupChat.ChatType != ChatTypeGroup || groupChat.ChatID != "group-1" || groupChat.SenderID != "user-1" || groupChat.StateKey() != "group:group-1" {
		t.Fatalf("group chat=%+v key=%q", groupChat, groupChat.StateKey())
	}

	restored := ChatIdentityFromStateKey("group:group-1")
	if restored.ChatType != ChatTypeGroup || restored.ChatID != "group-1" || restored.ReplyToUserID != "group-1" {
		t.Fatalf("restored=%+v", restored)
	}
}
