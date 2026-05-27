package beakweixin

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/channel"
	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
)

const (
	ID       = "beak-agent-weixin"
	Platform = "weixin"
)

type API interface {
	RegisterChannel(Channel) error
}

type Plugin struct{}

type Channel struct{}

type Metadata struct {
	ID          string
	Platform    string
	Label       string
	Description string
}

type Capabilities struct {
	DirectChat     bool
	Text           bool
	Media          bool
	BlockStreaming bool
}

type SettingsSchema struct {
	Type                 string         `json:"type"`
	AdditionalProperties bool           `json:"additionalProperties"`
	Properties           map[string]any `json:"properties"`
}

type Runtime struct {
	Beak            BeakRuntime
	State           StateStore
	WorkspaceUUID   string
	ChannelUUID     string
	Accounts        []Account
	PollInterval    time.Duration
	StreamReconnect time.Duration
	HTTPClient      *http.Client
	Logger          *log.Logger
}

type Account struct {
	AccountID string
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

type StateStore interface {
	LoadAccount(accountID string) (*AccountState, error)
	SaveAccount(account *AccountState) error
	SaveLogin(accountID, botToken, baseURL, ilinkUserID string) (*AccountState, error)
}

type AccountState = state.AccountState

type UserMessage struct {
	AccountID  string
	PeerUserID string
	SenderID   string
	Content    string
	Metadata   map[string]any
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

type LoginResult struct {
	Account   *AccountState
	AccountID string
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

func New() Plugin {
	return Plugin{}
}

func Register(api API) error {
	return New().Register(api)
}

func (Plugin) Register(api API) error {
	return api.RegisterChannel(Channel{})
}

func (Plugin) Channel() Channel {
	return Channel{}
}

func (Channel) Metadata() Metadata {
	return Metadata{
		ID:          ID,
		Platform:    Platform,
		Label:       "Weixin",
		Description: "Weixin connector for Beak channel gateway sessions",
	}
}

func (Channel) Capabilities() Capabilities {
	return Capabilities{
		DirectChat:     true,
		Text:           true,
		Media:          false,
		BlockStreaming: true,
	}
}

func (Channel) SettingsSchema() SettingsSchema {
	return SettingsSchema{
		Type:                 "object",
		AdditionalProperties: false,
		Properties:           map[string]any{},
	}
}

func (ch Channel) Login(ctx context.Context, runtime Runtime, opts LoginOptions) (*LoginResult, error) {
	client, err := ch.client(runtime)
	if err != nil {
		return nil, err
	}
	result, err := client.Login(ctx, channel.LoginOptions{
		PollInterval: opts.PollInterval,
		OnQRCode: func(qr channel.QRCode) {
			if opts.OnQRCode != nil {
				opts.OnQRCode(fromChannelQRCode(qr))
			}
		},
	})
	if err != nil {
		return nil, err
	}
	return fromChannelLoginResult(result), nil
}

func (ch Channel) StartLogin(ctx context.Context, runtime Runtime) (*LoginChallenge, error) {
	client, err := ch.client(runtime)
	if err != nil {
		return nil, err
	}
	challenge, err := client.StartLogin(ctx)
	if err != nil {
		return nil, err
	}
	return &LoginChallenge{QRCode: fromChannelQRCode(challenge.QRCode)}, nil
}

func (ch Channel) PollLogin(ctx context.Context, runtime Runtime, qrcode string) (*LoginStatus, error) {
	client, err := ch.client(runtime)
	if err != nil {
		return nil, err
	}
	status, err := client.PollLogin(ctx, qrcode)
	if err != nil {
		return nil, err
	}
	return fromChannelLoginStatus(status), nil
}

func (ch Channel) Start(ctx context.Context, runtime Runtime) error {
	client, err := ch.client(runtime)
	if err != nil {
		return err
	}
	return client.Run(ctx)
}

func (ch Channel) Doctor(ctx context.Context, runtime Runtime) (*DoctorReport, error) {
	client, err := ch.client(runtime)
	if err != nil {
		return nil, err
	}
	report, err := client.Doctor(ctx, channel.DoctorOptions{EnsureChannel: true})
	if err != nil {
		return nil, err
	}
	return fromChannelDoctorReport(report), nil
}

func (ch Channel) SendText(ctx context.Context, runtime Runtime, req SendTextRequest) (*SendTextResult, error) {
	client, err := ch.client(runtime)
	if err != nil {
		return nil, err
	}
	result, err := client.SendText(ctx, channel.SendTextRequest{
		AccountID:    req.AccountID,
		ToUserID:     req.ToUserID,
		Text:         req.Text,
		ContextToken: req.ContextToken,
	})
	if err != nil {
		return nil, err
	}
	return fromChannelSendTextResult(result), nil
}

func (Channel) Stop(context.Context, Runtime) error {
	return nil
}

func (Channel) client(runtime Runtime) (*channel.Client, error) {
	var beak channel.BeakRuntime
	if runtime.Beak != nil {
		beak = beakRuntimeAdapter{runtime: runtime.Beak}
	}
	options := channel.Options{
		Beak:            beak,
		State:           runtime.State,
		WorkspaceRef:    runtime.WorkspaceUUID,
		ChannelUUID:     runtime.ChannelUUID,
		Accounts:        make([]channel.AccountConfig, 0, len(runtime.Accounts)),
		PollInterval:    runtime.PollInterval,
		StreamReconnect: runtime.StreamReconnect,
		HTTPClient:      runtime.HTTPClient,
	}
	for _, account := range runtime.Accounts {
		options.Accounts = append(options.Accounts, channel.AccountConfig{AccountID: account.AccountID})
	}
	var opts []channel.Option
	if runtime.Logger != nil {
		opts = append(opts, channel.WithLogger(runtime.Logger))
	}
	return channel.New(options, opts...)
}

type beakRuntimeAdapter struct {
	runtime BeakRuntime
}

func (a beakRuntimeAdapter) CheckHealth(ctx context.Context) error {
	return a.runtime.CheckHealth(ctx)
}

func (a beakRuntimeAdapter) EnsureWeixinChannel(ctx context.Context) (string, error) {
	return a.runtime.EnsureWeixinChannel(ctx)
}

func (a beakRuntimeAdapter) EnsureWeixinChannelLinkSession(ctx context.Context, accountID string) (string, error) {
	return a.runtime.EnsureWeixinChannelLinkSession(ctx, accountID)
}

func (a beakRuntimeAdapter) EnsureWeixinPeerSession(ctx context.Context, accountID, peerUserID string) (string, error) {
	return a.runtime.EnsureWeixinPeerSession(ctx, accountID, peerUserID)
}

func (a beakRuntimeAdapter) EnsureWeixinChatSession(ctx context.Context, accountID, chatType, chatID, senderID string) (string, error) {
	if runtime, ok := a.runtime.(interface {
		EnsureWeixinChatSession(context.Context, string, string, string, string) (string, error)
	}); ok {
		return runtime.EnsureWeixinChatSession(ctx, accountID, chatType, chatID, senderID)
	}
	peerID := chatID
	if chatType == weixin.ChatTypeGroup {
		peerID = weixin.ChatTypeGroup + ":" + chatID
	}
	return a.runtime.EnsureWeixinPeerSession(ctx, accountID, peerID)
}

func (a beakRuntimeAdapter) CreateWeixinUserMessage(ctx context.Context, sessionUUID string, msg channel.UserMessage) (string, error) {
	return a.runtime.CreateWeixinUserMessage(ctx, sessionUUID, UserMessage{
		AccountID:  msg.AccountID,
		PeerUserID: msg.PeerUserID,
		SenderID:   msg.SenderID,
		Content:    msg.Content,
		Metadata:   msg.Metadata,
	})
}

func (a beakRuntimeAdapter) StreamWeixinSession(ctx context.Context, sessionUUID string, req channel.StreamRequest, handle func(channel.StreamEvent) error) error {
	return a.runtime.StreamWeixinSession(ctx, sessionUUID, StreamRequest{
		SubscriberID:  req.SubscriberID,
		LastEventUUID: req.LastEventUUID,
	}, func(event StreamEvent) error {
		if handle == nil {
			return nil
		}
		return handle(channel.StreamEvent{
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

func (a beakRuntimeAdapter) AgentParticipantID() string {
	return a.runtime.AgentParticipantID()
}

func (a beakRuntimeAdapter) BridgeParticipantID() string {
	return a.runtime.BridgeParticipantID()
}

func fromChannelQRCode(qr channel.QRCode) QRCode {
	return QRCode{
		AccountHint: qr.AccountHint,
		Code:        qr.Code,
		URL:         qr.URL,
	}
}

func fromChannelLoginResult(result *channel.LoginResult) *LoginResult {
	if result == nil {
		return nil
	}
	return &LoginResult{Account: result.Account, AccountID: result.AccountID}
}

func fromChannelLoginStatus(status *channel.LoginStatus) *LoginStatus {
	if status == nil {
		return nil
	}
	return &LoginStatus{
		Status:    status.Status,
		Confirmed: status.Confirmed,
		Expired:   status.Expired,
		Account:   status.Account,
		AccountID: status.AccountID,
	}
}

func fromChannelDoctorReport(report *channel.DoctorReport) *DoctorReport {
	if report == nil {
		return nil
	}
	out := &DoctorReport{
		RuntimeOK:   report.RuntimeOK,
		ChannelUUID: report.ChannelUUID,
		Accounts:    make([]AccountReport, 0, len(report.Accounts)),
	}
	for _, account := range report.Accounts {
		out.Accounts = append(out.Accounts, AccountReport{
			AccountID: account.AccountID,
			HasToken:  account.HasToken,
			BaseURL:   account.BaseURL,
			Peers:     account.Peers,
		})
	}
	return out
}

func fromChannelSendTextResult(result *channel.SendTextResult) *SendTextResult {
	if result == nil {
		return nil
	}
	return &SendTextResult{
		Channel:   result.Channel,
		AccountID: result.AccountID,
		MessageID: result.MessageID,
	}
}
