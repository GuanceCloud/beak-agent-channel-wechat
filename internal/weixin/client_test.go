package weixin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		if body.BaseInfo.ChannelVersion == "" || body.BaseInfo.BotAgent == "" {
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
