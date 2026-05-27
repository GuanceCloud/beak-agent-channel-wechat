package beakweixin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

type Connector struct {
	channel Channel
}

func NewConnector() Connector {
	return Connector{channel: Channel{}}
}

func (c Connector) Metadata() sdk.ConnectorMetadata {
	meta := c.channel.Metadata()
	caps := c.channel.Capabilities()
	return sdk.ConnectorMetadata{
		ID:          meta.ID,
		Platform:    meta.Platform,
		Label:       meta.Label,
		Description: meta.Description,
		Capabilities: sdk.Capabilities{
			LoginModes:     []string{sdk.LoginModeQRCode},
			Text:           caps.Text,
			Media:          caps.Media,
			GroupChat:      true,
			DirectChat:     caps.DirectChat,
			BlockStreaming: caps.BlockStreaming,
		},
	}
}

func (Connector) CredentialSchema(context.Context) sdk.CredentialSchema {
	return sdk.CredentialSchema{
		Type:                 "object",
		LoginModes:           []string{sdk.LoginModeQRCode},
		Properties:           map[string]sdk.CredentialField{},
		AdditionalProperties: false,
	}
}

func (c Connector) StartLogin(ctx context.Context, req sdk.LoginStartRequest) (*sdk.LoginChallenge, error) {
	runtime, _ := c.runtimeFromSDK(req.Runtime, nil)
	challenge, err := c.channel.StartLogin(ctx, runtime)
	if err != nil {
		return nil, err
	}
	return &sdk.LoginChallenge{
		Type: sdk.LoginModeQRCode,
		Code: challenge.QRCode.Code,
		URL:  challenge.QRCode.URL,
		State: map[string]any{
			"qrcode":     challenge.QRCode.Code,
			"qrcode_url": challenge.QRCode.URL,
		},
	}, nil
}

func (c Connector) PollLogin(ctx context.Context, req sdk.LoginPollRequest) (*sdk.LoginStatus, error) {
	qrcode := strings.TrimSpace(req.ChallengeCode)
	if qrcode == "" {
		qrcode, _ = req.ChallengeState["qrcode"].(string)
	}
	runtime, store := c.runtimeFromSDK(req.Runtime, nil)
	status, err := c.channel.PollLogin(ctx, runtime, qrcode)
	if err != nil {
		return nil, err
	}
	out := &sdk.LoginStatus{
		Status:    status.Status,
		Confirmed: status.Confirmed,
		Expired:   status.Expired,
	}
	if status.Account != nil {
		account := store.accountToSDK(*status.Account)
		account.WorkspaceUUID = req.WorkspaceUUID
		account.ChannelUUID = req.ChannelUUID
		out.Account = account
		out.Credential = account.Credential
		out.State = account.State
	}
	return out, nil
}

func (c Connector) Start(ctx context.Context, runtime sdk.Runtime) error {
	native, store := c.runtimeFromSDK(runtime, nil)
	if native.Beak == nil {
		if runtime.Gateway == nil {
			return fmt.Errorf("weixin connector requires sdk.Runtime.Gateway or sdk.Runtime.Native beakweixin.Runtime")
		}
		native.Beak = gatewayRuntimeAdapter{workspaceUUID: runtime.WorkspaceUUID, platform: Platform, gateway: runtime.Gateway}
	}
	if len(native.Accounts) == 0 {
		for _, account := range runtimeAccountCandidates(runtime) {
			store.seed(account)
			if accountID := store.accountID(account); accountID != "" {
				native.Accounts = appendUniqueNativeAccount(native.Accounts, accountID)
			}
		}
	}
	return c.channel.Start(ctx, native)
}

func (c Connector) Send(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	native, store := c.runtimeFromSDK(runtime, &account)
	accountID := store.accountID(account)
	if accountID == "" {
		return nil, fmt.Errorf("weixin outbound account is required")
	}
	toUserID := req.ChatID
	if toUserID == "" {
		toUserID = req.AccountUUID
	}
	contextKey := outboundStateKey(req)
	if contextKey == "" {
		contextKey = toUserID
	}
	result, err := c.channel.SendText(ctx, native, SendTextRequest{
		AccountID:    accountID,
		ToUserID:     toUserID,
		Text:         req.Text,
		ContextToken: sdkAccountToState(account).ContextTokens[contextKey],
	})
	if err != nil {
		return nil, err
	}
	return &sdk.SendResult{
		Platform:    Platform,
		AccountUUID: result.AccountID,
		MessageID:   result.MessageID,
	}, nil
}

func (Connector) Stop(ctx context.Context, account sdk.ChannelAccount) error {
	state := sdkAccountToState(account)
	if strings.TrimSpace(state.BotToken) == "" {
		return nil
	}
	client := weixin.NewClient(state.BaseURL, state.BotToken)
	return client.NotifyStop(ctx)
}

func (c Connector) runtimeFromSDK(runtime sdk.Runtime, account *sdk.ChannelAccount) (Runtime, *connectorStateStore) {
	store := newConnectorStateStore(runtime.AccountStore)
	if account != nil {
		store.seed(*account)
	}
	for _, item := range runtimeAccountCandidates(runtime) {
		store.seed(item)
	}
	if native, ok := runtime.Native.(Runtime); ok {
		if native.WorkspaceUUID == "" {
			native.WorkspaceUUID = runtime.WorkspaceUUID
		}
		if native.ChannelUUID == "" {
			native.ChannelUUID = runtime.Channel.UUID
		}
		if native.State == nil {
			native.State = store
		}
		if native.HTTPClient == nil {
			native.HTTPClient = runtime.HTTPClient
		}
		if native.Logger == nil {
			native.Logger = runtime.Logger
		}
		return native, store
	}
	if native, ok := runtime.Native.(*Runtime); ok && native != nil {
		out := *native
		if out.WorkspaceUUID == "" {
			out.WorkspaceUUID = runtime.WorkspaceUUID
		}
		if out.ChannelUUID == "" {
			out.ChannelUUID = runtime.Channel.UUID
		}
		if out.State == nil {
			out.State = store
		}
		if out.HTTPClient == nil {
			out.HTTPClient = runtime.HTTPClient
		}
		if out.Logger == nil {
			out.Logger = runtime.Logger
		}
		return out, store
	}
	out := Runtime{
		WorkspaceUUID:   runtime.WorkspaceUUID,
		ChannelUUID:     runtime.Channel.UUID,
		State:           store,
		HTTPClient:      runtime.HTTPClient,
		Logger:          runtime.Logger,
		PollInterval:    runtime.PollInterval,
		StreamReconnect: runtime.StreamReconnect,
	}
	for _, item := range runtimeAccountCandidates(runtime) {
		if accountID := store.accountID(item); accountID != "" {
			out.Accounts = appendUniqueNativeAccount(out.Accounts, accountID)
		}
	}
	if account != nil && store.accountID(*account) != "" && len(out.Accounts) == 0 {
		out.Accounts = append(out.Accounts, Account{AccountID: store.accountID(*account)})
	}
	return out, store
}

func runtimeAccountCandidates(runtime sdk.Runtime) []sdk.ChannelAccount {
	seen := make(map[string]bool)
	var out []sdk.ChannelAccount
	add := func(account sdk.ChannelAccount) {
		key := accountKey(account)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		out = append(out, account)
	}
	add(runtime.Account)
	for _, account := range runtime.Accounts {
		add(account)
	}
	return out
}

func selectRuntimeAccount(runtime sdk.Runtime, accountUUID string) (sdk.ChannelAccount, error) {
	accountUUID = strings.TrimSpace(accountUUID)
	candidates := runtimeAccountCandidates(runtime)
	if accountUUID != "" {
		for _, account := range candidates {
			if accountMatches(account, accountUUID) {
				return account, nil
			}
		}
		return sdk.ChannelAccount{}, fmt.Errorf("weixin account %s not found in runtime", accountUUID)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) == 0 {
		return sdk.ChannelAccount{}, fmt.Errorf("weixin outbound account is required")
	}
	return sdk.ChannelAccount{}, fmt.Errorf("weixin outbound account is ambiguous; account_uuid is required")
}

func accountMatches(account sdk.ChannelAccount, accountID string) bool {
	if strings.TrimSpace(account.UUID) == accountID {
		return true
	}
	if strings.TrimSpace(stringValue(account.Credential["account_id"])) == accountID {
		return true
	}
	if strings.TrimSpace(stringValue(account.Credential["ilink_bot_id"])) == accountID {
		return true
	}
	return false
}

func accountKey(account sdk.ChannelAccount) string {
	return firstString(account.UUID, account.Credential["account_id"], account.Credential["ilink_bot_id"])
}

func appendUniqueNativeAccount(accounts []Account, accountID string) []Account {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return accounts
	}
	for _, account := range accounts {
		if account.AccountID == accountID {
			return accounts
		}
	}
	return append(accounts, Account{AccountID: accountID})
}

type connectorStateStore struct {
	mu           sync.Mutex
	accounts     map[string]*AccountState
	sdkAccounts  map[string]sdk.ChannelAccount
	accountStore sdk.AccountStore
}

func newConnectorStateStore(accountStore sdk.AccountStore) *connectorStateStore {
	return &connectorStateStore{
		accounts:     make(map[string]*AccountState),
		sdkAccounts:  make(map[string]sdk.ChannelAccount),
		accountStore: accountStore,
	}
}

func (s *connectorStateStore) seed(account sdk.ChannelAccount) {
	accountID := s.accountID(account)
	if accountID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := sdkAccountToState(account)
	s.accounts[accountID] = &state
	s.sdkAccounts[accountID] = account
}

func (s *connectorStateStore) LoadAccount(accountID string) (*AccountState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if account, ok := s.accounts[accountID]; ok {
		return account, nil
	}
	account := &AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.accounts[accountID] = account
	return account, nil
}

func (s *connectorStateStore) SaveAccount(account *AccountState) error {
	if account == nil || account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	account.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	s.accounts[account.AccountID] = account
	existing := s.sdkAccounts[account.AccountID]
	sdkAccount := accountStateToSDK(*account, existing)
	s.sdkAccounts[account.AccountID] = sdkAccount
	accountStore := s.accountStore
	s.mu.Unlock()
	if accountStore != nil && sdkAccount.UUID != "" {
		return accountStore.SaveChannelAccountState(context.Background(), sdkAccount.UUID, sdkAccount.State)
	}
	return nil
}

func (s *connectorStateStore) SaveLogin(accountID, botToken, baseURL, ilinkUserID string) (*AccountState, error) {
	account, err := s.LoadAccount(accountID)
	if err != nil {
		return nil, err
	}
	account.BotToken = botToken
	account.BaseURL = baseURL
	account.ILinkUserID = ilinkUserID
	account.MarkActive()
	if err := s.SaveAccount(account); err != nil {
		return nil, err
	}
	return account, nil
}

func (s *connectorStateStore) accountID(account sdk.ChannelAccount) string {
	if account.UUID != "" {
		return account.UUID
	}
	if value, _ := account.Credential["ilink_bot_id"].(string); value != "" {
		return value
	}
	if value, _ := account.Credential["account_id"].(string); value != "" {
		return value
	}
	return ""
}

func (s *connectorStateStore) accountToSDK(account AccountState) sdk.ChannelAccount {
	s.mu.Lock()
	existing := s.sdkAccounts[account.AccountID]
	s.mu.Unlock()
	return accountStateToSDK(account, existing)
}

func accountStateToSDK(account AccountState, existing sdk.ChannelAccount) sdk.ChannelAccount {
	if existing.UUID == "" {
		existing.UUID = account.AccountID
	}
	existing.Platform = Platform
	existing.Credential = map[string]any{
		"account_id":    account.AccountID,
		"bot_token":     account.BotToken,
		"base_url":      account.BaseURL,
		"ilink_user_id": account.ILinkUserID,
		"ilink_bot_id":  account.AccountID,
	}
	existing.State = stateToMap(account)
	return existing
}

type gatewayRuntimeAdapter struct {
	workspaceUUID string
	platform      string
	gateway       sdk.Gateway
}

func (a gatewayRuntimeAdapter) CheckHealth(context.Context) error {
	return nil
}

func (a gatewayRuntimeAdapter) EnsureWeixinChannel(ctx context.Context) (string, error) {
	return a.gateway.EnsureChannel(ctx, sdk.EnsureChannelRequest{
		WorkspaceUUID: a.workspaceUUID,
		Platform:      a.platform,
		Name:          "Weixin",
		Config: map[string]any{
			"bridge": ID,
		},
	})
}

func (a gatewayRuntimeAdapter) EnsureWeixinChannelLinkSession(ctx context.Context, accountID string) (string, error) {
	return a.gateway.EnsureChannelLinkSession(ctx, sdk.EnsureChannelLinkSessionRequest{
		WorkspaceUUID:       a.workspaceUUID,
		Platform:            a.platform,
		AccountUUID:         accountID,
		AgentParticipantID:  a.AgentParticipantID(),
		BridgeParticipantID: a.BridgeParticipantID(),
	})
}

func (a gatewayRuntimeAdapter) EnsureWeixinPeerSession(ctx context.Context, accountID, peerUserID string) (string, error) {
	chat := weixinLegacyChatIdentity(peerUserID)
	return a.EnsureWeixinChatSession(ctx, accountID, chat.ChatType, chat.ChatID, chat.SenderID)
}

func weixinLegacyChatIdentity(peerID string) weixin.ChatIdentity {
	peerID = strings.TrimSpace(peerID)
	if strings.HasPrefix(peerID, weixin.ChatTypeGroup+":") {
		chatID := strings.TrimPrefix(peerID, weixin.ChatTypeGroup+":")
		return weixin.ChatIdentity{ChatType: weixin.ChatTypeGroup, ChatID: chatID, SenderID: chatID, ReplyToUserID: chatID}
	}
	return weixin.ChatIdentity{ChatType: weixin.ChatTypeDirect, ChatID: peerID, SenderID: peerID, ReplyToUserID: peerID}
}

func outboundStateKey(req sdk.OutboundMessage) string {
	if req.ChatType == sdk.ChatTypeGroup {
		return weixin.ChatTypeGroup + ":" + req.ChatID
	}
	return req.ChatID
}

func (a gatewayRuntimeAdapter) EnsureWeixinChatSession(ctx context.Context, accountID, chatType, chatID, senderID string) (string, error) {
	return a.gateway.EnsureChatSession(ctx, sdk.EnsureChatSessionRequest{
		WorkspaceUUID:       a.workspaceUUID,
		Platform:            a.platform,
		AccountUUID:         accountID,
		ChatType:            chatType,
		ChatID:              chatID,
		SenderID:            senderID,
		AgentParticipantID:  a.AgentParticipantID(),
		BridgeParticipantID: a.BridgeParticipantID(),
		Metadata: map[string]any{
			"source":       "weixin",
			"account_uuid": accountID,
		},
	})
}

func (a gatewayRuntimeAdapter) CreateWeixinUserMessage(ctx context.Context, sessionUUID string, msg UserMessage) (string, error) {
	return a.gateway.CreateMessage(ctx, sdk.CreateMessageRequest{
		WorkspaceUUID: a.workspaceUUID,
		SessionUUID:   sessionUUID,
		SenderID:      msg.SenderID,
		Content:       msg.Content,
		Metadata:      msg.Metadata,
	})
}

func (a gatewayRuntimeAdapter) StreamWeixinSession(ctx context.Context, sessionUUID string, req StreamRequest, handle func(StreamEvent) error) error {
	return a.gateway.StreamSession(ctx, sdk.StreamSessionRequest{
		WorkspaceUUID: a.workspaceUUID,
		SessionUUID:   sessionUUID,
		SubscriberID:  req.SubscriberID,
		LastEventUUID: req.LastEventUUID,
	}, func(event sdk.StreamEvent) error {
		if handle == nil {
			return nil
		}
		return handle(StreamEvent{
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

func (a gatewayRuntimeAdapter) AgentParticipantID() string {
	return a.gateway.AgentParticipantID()
}

func (a gatewayRuntimeAdapter) BridgeParticipantID() string {
	return a.gateway.BridgeParticipantID(a.platform)
}

func sdkAccountToState(account sdk.ChannelAccount) AccountState {
	state := AccountState{
		AccountID:   firstString(account.UUID, account.Credential["account_id"], account.Credential["ilink_bot_id"]),
		BotToken:    stringValue(account.Credential["bot_token"]),
		BaseURL:     stringValue(account.Credential["base_url"]),
		ILinkUserID: stringValue(account.Credential["ilink_user_id"]),
	}
	state.Status = stringValue(account.State["status"])
	state.LastError = stringValue(account.State["last_error"])
	state.ChannelLinkSession = stringValue(account.State["channel_link_session"])
	state.GetUpdatesBuf = stringValue(account.State["get_updates_buf"])
	state.ContextTokens = stringMap(account.State["context_tokens"])
	state.TypingTickets = stringMap(account.State["typing_tickets"])
	state.PeerSessions = stringMap(account.State["peer_sessions"])
	state.InboundSeen = stringMap(account.State["inbound_seen"])
	state.SentBeakMessages = stringMap(account.State["sent_beak_messages"])
	state.StreamCursors = stringMap(account.State["stream_cursors"])
	state.EnsureMaps()
	return state
}

func stateToMap(account AccountState) map[string]any {
	account.EnsureMaps()
	return map[string]any{
		"channel_link_session": account.ChannelLinkSession,
		"status":               account.Status,
		"last_error":           account.LastError,
		"get_updates_buf":      account.GetUpdatesBuf,
		"context_tokens":       account.ContextTokens,
		"typing_tickets":       account.TypingTickets,
		"peer_sessions":        account.PeerSessions,
		"inbound_seen":         account.InboundSeen,
		"sent_beak_messages":   account.SentBeakMessages,
		"stream_cursors":       account.StreamCursors,
		"updated_at":           account.UpdatedAt,
	}
}

func stringMap(value any) map[string]string {
	out := make(map[string]string)
	switch typed := value.(type) {
	case map[string]string:
		for key, item := range typed {
			out[key] = item
		}
	case map[string]any:
		for key, item := range typed {
			if stringItem, ok := item.(string); ok {
				out[key] = stringItem
			}
		}
	case json.RawMessage:
		_ = json.Unmarshal(typed, &out)
	}
	return out
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func firstString(values ...any) string {
	for _, value := range values {
		if stringValue := strings.TrimSpace(stringValue(value)); stringValue != "" {
			return stringValue
		}
	}
	return ""
}

var _ sdk.Connector = Connector{}
