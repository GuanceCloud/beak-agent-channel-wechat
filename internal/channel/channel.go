package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/beak"
	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/bridge"
	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
)

type AccountState = state.AccountState

type Options struct {
	Beak            BeakRuntime
	State           StateStore
	WorkspaceRef    string
	ChannelUUID     string
	Accounts        []AccountConfig
	PollInterval    time.Duration
	StreamReconnect time.Duration
	HTTPClient      *http.Client
}

type AccountConfig struct {
	AccountID string
}

type StateStore interface {
	LoadAccount(ctx context.Context, accountID string) (*AccountState, error)
	SaveAccount(ctx context.Context, account *AccountState) error
	SaveLogin(ctx context.Context, accountID, botToken, baseURL, ilinkUserID string) (*AccountState, error)
}

type BeakRuntime interface {
	CheckHealth(ctx context.Context) error
	EnsureWeixinChannel(ctx context.Context) (string, error)
	EnsureWeixinChannelLinkSession(ctx context.Context, accountID string) (string, error)
	EnsureWeixinPeerSession(ctx context.Context, accountID, peerUserID string) (string, error)
	CreateWeixinUserMessage(ctx context.Context, sessionUUID string, msg UserMessage) (string, error)
	StreamWeixinSession(ctx context.Context, sessionUUID string, req StreamRequest, handle func(StreamEvent) error) error
	AgentParticipantID() string
	BridgeParticipantID() string
}

type BeakChatRuntime interface {
	EnsureWeixinChatSession(ctx context.Context, accountID, chatType, chatID, senderID string) (string, error)
}

type UserMessage struct {
	AccountID  string
	PeerUserID string
	SenderID   string
	Content    string
	Metadata   map[string]any
}

type SendTextRequest struct {
	AccountID    string
	ToUserID     string
	Text         string
	ContextToken string
}

type SendTextResult struct {
	Channel   string
	AccountID string
	MessageID string
}

type StreamRequest struct {
	SubscriberID  string
	LastEventUUID string
}

type StreamEvent struct {
	EventUUID     string
	WorkspaceUUID string
	SessionUUID   string
	EventType     string
	MessageUUID   string
	SenderID      string
	Content       string
	Payload       json.RawMessage
}

type Client struct {
	options Options
	store   StateStore
	beak    BeakRuntime
	logger  *log.Logger
	weixin  bridge.WeixinOptions
	http    *http.Client
}

type Option func(*Client)

type QRCode struct {
	AccountHint string
	Code        string
	URL         string
}

type LoginChallenge struct {
	QRCode QRCode
}

type LoginStatus struct {
	Status    string
	Confirmed bool
	Expired   bool
	Account   *AccountState
	AccountID string
}

type LoginOptions struct {
	OnQRCode     func(QRCode)
	PollInterval time.Duration
}

type DoctorOptions struct {
	EnsureChannel bool
}

type DoctorReport struct {
	RuntimeOK   bool
	ChannelUUID string
	Accounts    []AccountReport
}

type AccountReport struct {
	AccountID string
	HasToken  bool
	BaseURL   string
	Peers     int
}

type LoginResult struct {
	Account   *AccountState
	AccountID string
}

func WithLogger(logger *log.Logger) Option {
	return func(client *Client) {
		if logger != nil {
			client.logger = logger
		}
	}
}

func New(options Options, opts ...Option) (*Client, error) {
	if options.State == nil {
		return nil, fmt.Errorf("state store is required")
	}
	client := &Client{
		options: normalizeOptions(options),
		store:   options.State,
		beak:    options.Beak,
		logger:  log.Default(),
		http:    options.HTTPClient,
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

func (c *Client) AddAccount(accountID string) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return
	}
	for _, account := range c.options.Accounts {
		if account.AccountID == accountID {
			return
		}
	}
	c.options.Accounts = append(c.options.Accounts, AccountConfig{AccountID: accountID})
}

func (c *Client) Accounts() []AccountConfig {
	out := make([]AccountConfig, len(c.options.Accounts))
	copy(out, c.options.Accounts)
	return out
}

func (c *Client) Login(ctx context.Context, opts LoginOptions) (*LoginResult, error) {
	challenge, err := c.StartLogin(ctx)
	if err != nil {
		return nil, err
	}
	if opts.OnQRCode != nil {
		opts.OnQRCode(challenge.QRCode)
	}

	wxCfg := c.weixinRuntimeConfig()
	loginCtx, cancel := context.WithTimeout(ctx, wxCfg.LoginTimeout)
	defer cancel()
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-loginCtx.Done():
			return nil, loginCtx.Err()
		case <-ticker.C:
			status, err := c.PollLogin(loginCtx, challenge.QRCode.Code)
			if err != nil {
				return nil, err
			}
			switch {
			case status.Confirmed:
				return &LoginResult{Account: status.Account, AccountID: status.AccountID}, nil
			case status.Expired:
				return nil, fmt.Errorf("weixin QR code expired")
			case isScannedStatus(status.Status):
				c.logger.Print("weixin QR scanned; waiting for phone confirmation")
			default:
				c.logger.Printf("waiting for weixin QR confirmation: status=%s", status.Status)
			}
		}
	}
}

func (c *Client) StartLogin(ctx context.Context) (*LoginChallenge, error) {
	wxCfg := c.weixinRuntimeConfig()
	wxClient := c.newWeixinClient(wxCfg, "")
	qr, err := wxClient.GetQRCode(ctx)
	if err != nil {
		return nil, err
	}
	return &LoginChallenge{
		QRCode: QRCode{
			Code: qr.QRCode,
			URL:  qr.QRCodeImgContent,
		},
	}, nil
}

func (c *Client) PollLogin(ctx context.Context, qrcode string) (*LoginStatus, error) {
	qrcode = strings.TrimSpace(qrcode)
	if qrcode == "" {
		return nil, fmt.Errorf("qrcode is required")
	}
	wxCfg := c.weixinRuntimeConfig()
	wxClient := c.newWeixinClient(wxCfg, "")
	statusTimeout := wxCfg.LongPollTimeout + 2*time.Second
	if statusTimeout <= 2*time.Second {
		statusTimeout = wxCfg.RequestTimeout
	}
	statusCtx, statusCancel := context.WithTimeout(ctx, statusTimeout)
	status, err := wxClient.GetQRCodeStatus(statusCtx, qrcode)
	statusCancel()
	if err != nil {
		if statusCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return &LoginStatus{Status: "wait"}, nil
		}
		return nil, err
	}
	out := &LoginStatus{Status: status.Status}
	switch strings.ToLower(status.Status) {
	case "confirmed":
		if status.BotToken == "" || status.ILinkBotID == "" {
			return nil, fmt.Errorf("confirmed login response missing bot_token or ilink_bot_id")
		}
		account, err := c.store.SaveLogin(ctx, status.ILinkBotID, status.BotToken, status.EffectiveBaseURL(wxCfg.BaseURL), status.ILinkUserID)
		if err != nil {
			return nil, err
		}
		out.Confirmed = true
		out.Account = account
		out.AccountID = account.AccountID
	case "expired":
		out.Expired = true
	}
	return out, nil
}

func (c *Client) Doctor(ctx context.Context, opts DoctorOptions) (*DoctorReport, error) {
	if c.beak == nil {
		return nil, fmt.Errorf("beak runtime is required")
	}
	report := &DoctorReport{}
	if err := c.beak.CheckHealth(ctx); err != nil {
		return report, fmt.Errorf("beak health: %w", err)
	}
	report.RuntimeOK = true
	if opts.EnsureChannel {
		channelUUID, err := c.beak.EnsureWeixinChannel(ctx)
		if err != nil {
			return report, fmt.Errorf("beak weixin channel: %w", err)
		}
		report.ChannelUUID = channelUUID
	}
	wxCfg := c.weixinRuntimeConfig()
	for _, accountCfg := range c.options.Accounts {
		account, err := c.store.LoadAccount(ctx, accountCfg.AccountID)
		if err != nil {
			return report, err
		}
		report.Accounts = append(report.Accounts, AccountReport{
			AccountID: account.AccountID,
			HasToken:  account.BotToken != "",
			BaseURL:   valueOrDefault(account.BaseURL, wxCfg.BaseURL),
			Peers:     len(account.PeerSessions),
		})
	}
	return report, nil
}

func isScannedStatus(status string) bool {
	switch strings.ToLower(status) {
	case "scaned", "scanned":
		return true
	default:
		return false
	}
}

func (c *Client) Run(ctx context.Context) error {
	if c.beak == nil {
		return fmt.Errorf("beak runtime is required")
	}
	runtimeOptions := c.runtimeOptions()
	if err := runtimeOptions.ValidateForRun(); err != nil {
		return err
	}
	runner := bridge.New(runtimeOptions, c.store, runtimeBridgeAdapter{runtime: c.beak}, nil, c.logger)
	return runner.Run(ctx)
}

func (c *Client) SendText(ctx context.Context, req SendTextRequest) (*SendTextResult, error) {
	accountID, account, err := c.resolveOutboundAccount(ctx, req.AccountID, req.ToUserID)
	if err != nil {
		return nil, err
	}
	if account.BotToken == "" {
		return nil, fmt.Errorf("weixin account %s is not logged in", accountID)
	}
	contextToken := req.ContextToken
	if contextToken == "" {
		contextToken = account.ContextTokens[req.ToUserID]
	}
	wxCfg := c.weixinRuntimeConfig()
	if account.BaseURL != "" {
		wxCfg.BaseURL = account.BaseURL
	}
	wxClient := c.newWeixinClient(wxCfg, account.BotToken)
	if err := wxClient.SendText(ctx, req.ToUserID, req.Text, contextToken); err != nil {
		return nil, err
	}
	return &SendTextResult{Channel: "weixin", AccountID: accountID}, nil
}

func Run(ctx context.Context, options Options, opts ...Option) error {
	client, err := New(options, opts...)
	if err != nil {
		return err
	}
	return client.Run(ctx)
}

func (c *Client) resolveOutboundAccount(ctx context.Context, accountID, toUserID string) (string, *AccountState, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID != "" {
		account, err := c.store.LoadAccount(ctx, accountID)
		if err != nil {
			return "", nil, err
		}
		return accountID, account, nil
	}
	if len(c.options.Accounts) == 1 {
		accountID = c.options.Accounts[0].AccountID
		account, err := c.store.LoadAccount(ctx, accountID)
		if err != nil {
			return "", nil, err
		}
		return accountID, account, nil
	}
	var matched []string
	for _, candidate := range c.options.Accounts {
		account, err := c.store.LoadAccount(ctx, candidate.AccountID)
		if err != nil {
			return "", nil, err
		}
		if account.ContextTokens[toUserID] != "" {
			matched = append(matched, candidate.AccountID)
		}
	}
	switch len(matched) {
	case 1:
		account, err := c.store.LoadAccount(ctx, matched[0])
		if err != nil {
			return "", nil, err
		}
		return matched[0], account, nil
	case 0:
		return "", nil, fmt.Errorf("cannot resolve weixin account for to=%s", toUserID)
	default:
		return "", nil, fmt.Errorf("ambiguous weixin account for to=%s: %s", toUserID, strings.Join(matched, ","))
	}
}

func (c *Client) newWeixinClient(wxCfg bridge.WeixinOptions, token string) *weixin.Client {
	client := wxCfg.NewClient(wxCfg.BaseURL, token)
	if c.http != nil {
		client.HTTPClient = c.http
	}
	return client
}

func (c *Client) runtimeOptions() *bridge.Options {
	wxCfg := c.weixinRuntimeConfig()
	return &bridge.Options{
		WorkspaceRef:        valueOrDefault(c.options.WorkspaceRef, "beak-runtime"),
		ChannelUUID:         c.options.ChannelUUID,
		AgentParticipantID:  c.beak.AgentParticipantID(),
		BridgeParticipantID: c.beak.BridgeParticipantID(),
		PollInterval:        c.options.PollInterval,
		StreamReconnect:     c.options.StreamReconnect,
		Weixin:              wxCfg,
		HTTPClient:          c.options.HTTPClient,
		Accounts:            toBridgeAccounts(c.options.Accounts),
	}
}

func (c *Client) weixinRuntimeConfig() bridge.WeixinOptions {
	wxCfg := c.weixin
	wxCfg.ApplyDefaults()
	return wxCfg
}

func normalizeOptions(options Options) Options {
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	if options.StreamReconnect <= 0 {
		options.StreamReconnect = 30 * time.Second
	}
	return options
}

func toBridgeAccounts(accounts []AccountConfig) []bridge.AccountConfig {
	out := make([]bridge.AccountConfig, 0, len(accounts))
	for _, account := range accounts {
		out = append(out, bridge.AccountConfig{AccountID: account.AccountID})
	}
	return out
}

type runtimeBridgeAdapter struct {
	runtime BeakRuntime
}

func (a runtimeBridgeAdapter) EnsureWeixinChannel(ctx context.Context, _ string) (string, error) {
	return a.runtime.EnsureWeixinChannel(ctx)
}

func (a runtimeBridgeAdapter) EnsureChannelLinkSession(ctx context.Context, _ string, accountID string, _ string, _ string) (string, error) {
	return a.runtime.EnsureWeixinChannelLinkSession(ctx, accountID)
}

func (a runtimeBridgeAdapter) EnsurePeerSession(ctx context.Context, _ string, accountID, peerUserID, _ string, _ string) (string, error) {
	return a.runtime.EnsureWeixinPeerSession(ctx, accountID, peerUserID)
}

func (a runtimeBridgeAdapter) EnsureChatSession(ctx context.Context, _ string, accountID, chatType, chatID, senderID, _ string, _ string) (string, error) {
	if runtime, ok := a.runtime.(BeakChatRuntime); ok {
		return runtime.EnsureWeixinChatSession(ctx, accountID, chatType, chatID, senderID)
	}
	peerID := chatID
	if chatType == weixin.ChatTypeGroup {
		peerID = weixin.ChatTypeGroup + ":" + chatID
	}
	return a.runtime.EnsureWeixinPeerSession(ctx, accountID, peerID)
}

func (a runtimeBridgeAdapter) CreateMessage(ctx context.Context, sessionUUID string, req beak.CreateMessageRequest) (*beak.CreateMessageResponse, error) {
	msg := UserMessage{
		AccountID:  metadataString(req.Metadata, "weixin_account_id"),
		PeerUserID: metadataString(req.Metadata, "weixin_peer_id"),
		SenderID:   req.SenderID,
		Content:    req.Content,
		Metadata:   req.Metadata,
	}
	if msg.PeerUserID == "" {
		msg.PeerUserID = strings.TrimPrefix(req.SenderID, "user:weixin:")
	}
	messageUUID, err := a.runtime.CreateWeixinUserMessage(ctx, sessionUUID, msg)
	if err != nil {
		return nil, err
	}
	return &beak.CreateMessageResponse{MessageUUID: messageUUID}, nil
}

func (a runtimeBridgeAdapter) StreamEvents(ctx context.Context, sessionUUID string, req beak.StreamRequest, handle func(beak.StreamEvent) error) error {
	return a.runtime.StreamWeixinSession(ctx, sessionUUID, StreamRequest{
		SubscriberID:  req.SubscriberID,
		LastEventUUID: req.LastEventUUID,
	}, func(event StreamEvent) error {
		if handle == nil {
			return nil
		}
		return handle(beak.StreamEvent{
			EventUUID:     event.EventUUID,
			WorkspaceUUID: event.WorkspaceUUID,
			SessionUUID:   event.SessionUUID,
			EventType:     event.EventType,
			MessageUUID:   event.MessageUUID,
			SenderID:      event.SenderID,
			Content:       event.Content,
			Payload:       event.Payload,
		})
	})
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	value, _ := metadata[key].(string)
	return value
}

func valueOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
