package weixin

import "strings"

type BaseInfo struct {
	ChannelVersion string `json:"channel_version,omitempty"`
	BotAgent       string `json:"bot_agent,omitempty"`
}

type QRCodeResponse struct {
	Ret              int    `json:"ret,omitempty"`
	ErrCode          int    `json:"errcode,omitempty"`
	ErrMsg           string `json:"errmsg,omitempty"`
	QRCode           string `json:"qrcode"`
	QRCodeImgContent string `json:"qrcode_img_content"`
}

type QRCodeStatusResponse struct {
	Ret         int    `json:"ret,omitempty"`
	ErrCode     int    `json:"errcode,omitempty"`
	ErrMsg      string `json:"errmsg,omitempty"`
	Status      string `json:"status"`
	BotToken    string `json:"bot_token"`
	ILinkBotID  string `json:"ilink_bot_id"`
	ILinkUserID string `json:"ilink_user_id"`
	BaseURL     string `json:"baseurl,omitempty"`
	BaseURLAlt  string `json:"base_url,omitempty"`
}

func (r QRCodeStatusResponse) EffectiveBaseURL(defaultBaseURL string) string {
	if r.BaseURL != "" {
		return r.BaseURL
	}
	if r.BaseURLAlt != "" {
		return r.BaseURLAlt
	}
	return defaultBaseURL
}

const (
	MessageTypeUser = 1
	MessageTypeBot  = 2

	MessageStateNew        = 0
	MessageStateGenerating = 1
	MessageStateFinish     = 2

	MessageItemTypeText = 1

	TypingStatusStart = 1
	TypingStatusStop  = 2

	ChatTypeDirect = "direct"
	ChatTypeGroup  = "group"
)

type TextItem struct {
	Text string `json:"text,omitempty"`
}

type MessageItem struct {
	Type     int       `json:"type,omitempty"`
	TextItem *TextItem `json:"text_item,omitempty"`
}

type WeixinMessage struct {
	Seq          int64         `json:"seq,omitempty"`
	MessageID    int64         `json:"message_id,omitempty"`
	FromUserID   string        `json:"from_user_id,omitempty"`
	ToUserID     string        `json:"to_user_id,omitempty"`
	ClientID     string        `json:"client_id,omitempty"`
	CreateTimeMS int64         `json:"create_time_ms,omitempty"`
	SessionID    string        `json:"session_id,omitempty"`
	GroupID      string        `json:"group_id,omitempty"`
	MessageType  int           `json:"message_type,omitempty"`
	MessageState int           `json:"message_state,omitempty"`
	ItemList     []MessageItem `json:"item_list,omitempty"`
	ContextToken string        `json:"context_token,omitempty"`
}

type ChatIdentity struct {
	ChatType      string
	ChatID        string
	SenderID      string
	ReplyToUserID string
}

func (m WeixinMessage) ChatIdentity() ChatIdentity {
	senderID := strings.TrimSpace(m.FromUserID)
	if groupID := strings.TrimSpace(m.GroupID); groupID != "" {
		replyTo := strings.TrimSpace(m.ToUserID)
		if replyTo == "" {
			replyTo = groupID
		}
		return ChatIdentity{
			ChatType:      ChatTypeGroup,
			ChatID:        groupID,
			SenderID:      senderID,
			ReplyToUserID: replyTo,
		}
	}
	chatID := senderID
	if chatID == "" {
		chatID = strings.TrimSpace(m.SessionID)
	}
	return ChatIdentity{
		ChatType:      ChatTypeDirect,
		ChatID:        chatID,
		SenderID:      senderID,
		ReplyToUserID: chatID,
	}
}

func ChatIdentityFromStateKey(key string) ChatIdentity {
	if strings.HasPrefix(key, ChatTypeGroup+":") {
		chatID := strings.TrimPrefix(key, ChatTypeGroup+":")
		return ChatIdentity{ChatType: ChatTypeGroup, ChatID: chatID, ReplyToUserID: chatID}
	}
	return ChatIdentity{ChatType: ChatTypeDirect, ChatID: key, SenderID: key, ReplyToUserID: key}
}

func (c ChatIdentity) StateKey() string {
	switch c.ChatType {
	case ChatTypeGroup:
		return ChatTypeGroup + ":" + c.ChatID
	default:
		return c.ChatID
	}
}

func (m WeixinMessage) Text() string {
	for _, item := range m.ItemList {
		if item.Type == MessageItemTypeText && item.TextItem != nil {
			return item.TextItem.Text
		}
	}
	return ""
}

func (m WeixinMessage) DedupeKey(accountID string) string {
	if m.MessageID != 0 {
		return accountID + ":message:" + itoa64(m.MessageID)
	}
	if m.Seq != 0 {
		return accountID + ":seq:" + itoa64(m.Seq)
	}
	if m.ClientID != "" {
		return accountID + ":client:" + m.ClientID
	}
	return accountID + ":peer:" + m.FromUserID + ":time:" + itoa64(m.CreateTimeMS)
}

type GetUpdatesRequest struct {
	GetUpdatesBuf string   `json:"get_updates_buf"`
	BaseInfo      BaseInfo `json:"base_info"`
}

type GetUpdatesResponse struct {
	Ret                  int             `json:"ret,omitempty"`
	ErrCode              int             `json:"errcode,omitempty"`
	ErrMsg               string          `json:"errmsg,omitempty"`
	Messages             []WeixinMessage `json:"msgs,omitempty"`
	GetUpdatesBuf        string          `json:"get_updates_buf,omitempty"`
	LongPollingTimeoutMS int             `json:"longpolling_timeout_ms,omitempty"`
}

type SendMessageRequest struct {
	Message  WeixinMessage `json:"msg"`
	BaseInfo BaseInfo      `json:"base_info"`
}

type SendMessageResponse struct {
	Ret     int    `json:"ret,omitempty"`
	ErrCode int    `json:"errcode,omitempty"`
	ErrMsg  string `json:"errmsg,omitempty"`
}

type GetConfigRequest struct {
	ILinkUserID  string   `json:"ilink_user_id"`
	ContextToken string   `json:"context_token,omitempty"`
	BaseInfo     BaseInfo `json:"base_info"`
}

type GetConfigResponse struct {
	Ret          int    `json:"ret,omitempty"`
	ErrCode      int    `json:"errcode,omitempty"`
	ErrMsg       string `json:"errmsg,omitempty"`
	TypingTicket string `json:"typing_ticket,omitempty"`
}

type SendTypingRequest struct {
	ILinkUserID  string   `json:"ilink_user_id"`
	TypingTicket string   `json:"typing_ticket"`
	Status       int      `json:"status"`
	BaseInfo     BaseInfo `json:"base_info"`
}

type SendTypingResponse struct {
	Ret     int    `json:"ret,omitempty"`
	ErrCode int    `json:"errcode,omitempty"`
	ErrMsg  string `json:"errmsg,omitempty"`
}

type NotifyResponse struct {
	Ret     int    `json:"ret,omitempty"`
	ErrCode int    `json:"errcode,omitempty"`
	ErrMsg  string `json:"errmsg,omitempty"`
}
