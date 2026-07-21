package beakweixin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

const (
	maxTrackedOutboundProgress = 256
	outboundChunkProgressKey   = "outbound_chunk_progress"
)

var outboundSendLocks [64]sync.Mutex

type Connector struct {
	channel Channel
}

func NewConnector() sdk.Connector {
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
			LoginModes:       []string{sdk.LoginModeQRCode},
			Text:             caps.Text,
			Media:            caps.Media,
			GroupChat:        true,
			DirectChat:       caps.DirectChat,
			Stream:           true,
			Webhook:          false,
			BlockStreaming:   caps.BlockStreaming,
			AckModes:         []string{"typing"},
			RuntimeOwnership: sdk.RuntimeOwnershipSDKOwned,
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

func (Connector) ValidateCredential(_ context.Context, req sdk.CredentialValidationRequest) (*sdk.CredentialValidationResult, error) {
	credential := cloneMap(req.Credential)
	state := cloneMap(req.State)
	accountKey := firstString(credential["ilink_user_id"], credential["account_id"], credential["ilink_bot_id"])
	if accountKey != "" {
		credential["account_id"] = accountKey
	}
	if ilinkBotID := firstString(credential["ilink_bot_id"], state["ilink_bot_id"], standardBotIdentityValue(state, "ilink_bot_id")); ilinkBotID != "" {
		credential["ilink_bot_id"] = ilinkBotID
		state["ilink_bot_id"] = ilinkBotID
		identity := weixinBotIdentityState(AccountState{ILinkBotID: ilinkBotID})
		if len(identity) > 0 {
			state["bot_identity"] = identity
		}
	}
	return &sdk.CredentialValidationResult{
		Valid:       true,
		AccountKey:  accountKey,
		DisplayName: firstString(credential["display_name"], credential["nickname"], credential["ilink_user_id"], accountKey, "Weixin"),
		Credential:  credential,
		State:       state,
		Metadata: map[string]any{
			"platform":   Platform,
			"validation": "default_pass",
		},
	}, nil
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
	accountState, err := store.LoadAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	text := weixinOutboundMentionText(req)
	chunks := weixin.SplitText(text, weixin.MaxTextRunes)
	if len(chunks) == 0 {
		return nil, fmt.Errorf("weixin outbound text is required")
	}
	if len(chunks) > 1 && strings.TrimSpace(req.MessageUUID) == "" {
		return nil, fmt.Errorf("weixin multipart outbound requires message_uuid for retry-safe delivery")
	}
	if len(chunks) > 1 && runtime.AccountStore == nil {
		return nil, fmt.Errorf("weixin multipart outbound requires sdk.Runtime.AccountStore for retry-safe delivery")
	}

	progressEntries := map[string]any{}
	progress := outboundChunkProgress{}
	resumed := false
	if len(chunks) > 1 {
		lock := outboundSendLock(accountID)
		lock.Lock()
		defer lock.Unlock()

		progressEntries, progress, err = loadOutboundChunkProgress(ctx, runtime, account, req, contextKey, toUserID, chunks)
		if err != nil {
			return nil, err
		}
		if progress.Completed {
			return weixinSendResult(accountID, progress.ClientIDs, len(chunks), true), nil
		}
		resumed = progress.NextIndex > 0
	}

	clientIDs := append([]string(nil), progress.ClientIDs...)
	for index := progress.NextIndex; index < len(chunks); index++ {
		clientID := ""
		if progress.Key != "" {
			clientID = weixinOutboundClientID(req.MessageUUID, progress.Fingerprint, index)
		}
		result, err := c.channel.SendText(ctx, native, SendTextRequest{
			AccountID:    accountID,
			ToUserID:     toUserID,
			Text:         chunks[index],
			ContextToken: accountState.ContextTokens[contextKey],
			ClientID:     clientID,
		})
		if err != nil {
			return nil, err
		}
		if clientID == "" {
			clientID = result.MessageID
		}
		if clientID != "" {
			clientIDs = append(clientIDs, clientID)
		}
		if progress.Key != "" {
			progress.NextIndex = index + 1
			progress.ClientIDs = append([]string(nil), clientIDs...)
			progress.Completed = progress.NextIndex == len(chunks)
			progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			progressEntries[progress.Key] = encodeOutboundChunkProgress(progress)
			pruneOutboundChunkProgress(progressEntries, maxTrackedOutboundProgress)
			patch := map[string]any{
				outboundChunkProgressKey: encodeOutboundChunkProgressEntries(progressEntries),
				"updated_at":             progress.UpdatedAt,
			}
			if err := runtime.AccountStore.SaveChannelAccountState(ctx, accountID, patch); err != nil {
				return nil, fmt.Errorf("weixin persist send progress after chunk %d/%d: %w", index+1, len(chunks), err)
			}
		}
	}
	return weixinSendResult(accountID, clientIDs, len(chunks), resumed), nil
}

type outboundChunkProgress struct {
	Key         string
	Fingerprint string
	NextIndex   int
	ClientIDs   []string
	Completed   bool
	UpdatedAt   string
}

func loadOutboundChunkProgress(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req sdk.OutboundMessage, contextKey, toUserID string, chunks []string) (map[string]any, outboundChunkProgress, error) {
	progress := outboundChunkProgress{
		Key:         strings.TrimSpace(req.MessageUUID),
		Fingerprint: weixinOutboundFingerprint(accountKey(account), req, contextKey, toUserID, chunks),
	}
	state := cloneMap(account.State)
	stored, err := runtime.AccountStore.LoadChannelAccountState(ctx, accountKey(account))
	if err != nil {
		return nil, outboundChunkProgress{}, err
	}
	for key, value := range stored {
		state[key] = value
	}
	entries := decodeOutboundChunkProgressEntries(state[outboundChunkProgressKey])
	storedValue, exists := entries[progress.Key]
	if !exists {
		return entries, progress, nil
	}
	storedProgress := decodeOutboundChunkProgress(progress.Key, storedValue)
	if storedProgress.Fingerprint != progress.Fingerprint {
		return nil, outboundChunkProgress{}, fmt.Errorf("weixin message_uuid %s was already used for a different outbound payload", progress.Key)
	}
	if storedProgress.NextIndex < 0 || storedProgress.NextIndex > len(chunks) || len(storedProgress.ClientIDs) != storedProgress.NextIndex {
		return nil, outboundChunkProgress{}, fmt.Errorf("weixin outbound progress for message_uuid %s is invalid", progress.Key)
	}
	storedProgress.Key = progress.Key
	storedProgress.Completed = storedProgress.NextIndex == len(chunks)
	return entries, storedProgress, nil
}

func weixinOutboundFingerprint(accountUUID string, req sdk.OutboundMessage, contextKey, toUserID string, chunks []string) string {
	value, _ := json.Marshal(struct {
		AccountUUID string   `json:"account_uuid"`
		ChatType    string   `json:"chat_type"`
		ChatID      string   `json:"chat_id"`
		ContextKey  string   `json:"context_key"`
		ToUserID    string   `json:"to_user_id"`
		Chunks      []string `json:"chunks"`
	}{
		AccountUUID: strings.TrimSpace(accountUUID),
		ChatType:    strings.TrimSpace(req.ChatType),
		ChatID:      strings.TrimSpace(req.ChatID),
		ContextKey:  strings.TrimSpace(contextKey),
		ToUserID:    strings.TrimSpace(toUserID),
		Chunks:      chunks,
	})
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func weixinOutboundClientID(messageUUID, fingerprint string, index int) string {
	messageUUID = strings.TrimSpace(messageUUID)
	if messageUUID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(messageUUID + "\x00" + fingerprint + "\x00" + strconv.Itoa(index)))
	return "beak-" + hex.EncodeToString(sum[:16])
}

func weixinSendResult(accountUUID string, clientIDs []string, chunkCount int, resumed bool) *sdk.SendResult {
	result := &sdk.SendResult{
		Platform:    Platform,
		AccountUUID: accountUUID,
		Raw: map[string]any{
			"chunk_count": chunkCount,
			"client_ids":  append([]string(nil), clientIDs...),
		},
	}
	if resumed {
		result.Raw["resumed"] = true
	}
	return result
}

func encodeOutboundChunkProgress(progress outboundChunkProgress) map[string]any {
	clientIDs := make([]any, 0, len(progress.ClientIDs))
	for _, clientID := range progress.ClientIDs {
		clientIDs = append(clientIDs, clientID)
	}
	return map[string]any{
		"fingerprint": progress.Fingerprint,
		"next_index":  progress.NextIndex,
		"client_ids":  clientIDs,
		"completed":   progress.Completed,
		"updated_at":  progress.UpdatedAt,
	}
}

func decodeOutboundChunkProgress(key string, value any) outboundChunkProgress {
	raw := mapValue(value)
	progress := outboundChunkProgress{
		Key:         key,
		Fingerprint: strings.TrimSpace(stringValue(raw["fingerprint"])),
		NextIndex:   intValue(raw["next_index"]),
		Completed:   boolValue(raw["completed"]),
		UpdatedAt:   strings.TrimSpace(stringValue(raw["updated_at"])),
	}
	switch values := raw["client_ids"].(type) {
	case []any:
		for _, value := range values {
			if clientID := strings.TrimSpace(stringValue(value)); clientID != "" {
				progress.ClientIDs = append(progress.ClientIDs, clientID)
			}
		}
	case []string:
		for _, value := range values {
			if clientID := strings.TrimSpace(value); clientID != "" {
				progress.ClientIDs = append(progress.ClientIDs, clientID)
			}
		}
	}
	return progress
}

func decodeOutboundChunkProgressEntries(value any) map[string]any {
	entries := map[string]any{}
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			entries[key] = item
		}
	case []any:
		for _, item := range typed {
			raw := mapValue(item)
			key := strings.TrimSpace(stringValue(raw["message_uuid"]))
			if key == "" {
				continue
			}
			delete(raw, "message_uuid")
			entries[key] = raw
		}
	case []map[string]any:
		for _, item := range typed {
			raw := cloneMap(item)
			key := strings.TrimSpace(stringValue(raw["message_uuid"]))
			if key == "" {
				continue
			}
			delete(raw, "message_uuid")
			entries[key] = raw
		}
	}
	return entries
}

func encodeOutboundChunkProgressEntries(entries map[string]any) []any {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded := make([]any, 0, len(keys))
	for _, key := range keys {
		entry := mapValue(entries[key])
		entry["message_uuid"] = key
		encoded = append(encoded, entry)
	}
	return encoded
}

func pruneOutboundChunkProgress(entries map[string]any, limit int) {
	if limit <= 0 || len(entries) <= limit {
		return
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return stringValue(mapValue(entries[keys[i]])["updated_at"]) < stringValue(mapValue(entries[keys[j]])["updated_at"])
	})
	for len(entries) > limit {
		delete(entries, keys[0])
		keys = keys[1:]
	}
}

func outboundSendLock(accountUUID string) *sync.Mutex {
	sum := sha256.Sum256([]byte(strings.TrimSpace(accountUUID)))
	return &outboundSendLocks[int(sum[0])%len(outboundSendLocks)]
}

func (c Connector) Acknowledge(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundAck) (*sdk.AckResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	native, store := c.runtimeFromSDK(runtime, &account)
	accountID := store.accountID(account)
	if accountID == "" {
		return nil, fmt.Errorf("weixin ack account is required")
	}
	result := &sdk.AckResult{
		Platform:    Platform,
		AccountUUID: accountID,
		Mode:        "typing",
		Status:      "skipped",
	}
	if !weixinAckWantsTyping(req) {
		result.Status = "unsupported"
		return result, nil
	}
	status, ok := weixinAckTypingStatus(req)
	if !ok {
		return result, nil
	}
	accountState, err := store.LoadAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	contextKey := ackStateKey(req)
	if contextKey == "" {
		result.Raw = map[string]any{"reason": "missing_chat_id"}
		return result, nil
	}
	contextToken := accountState.ContextTokens[contextKey]
	if strings.TrimSpace(contextToken) == "" {
		result.Raw = map[string]any{"reason": "missing_context_token", "context_key": contextKey}
		return result, nil
	}
	chat := weixin.ChatIdentityFromStateKey(contextKey)
	if strings.TrimSpace(chat.ReplyToUserID) == "" {
		result.Raw = map[string]any{"reason": "missing_reply_to_user_id", "context_key": contextKey}
		return result, nil
	}
	client := weixin.NewClient(accountState.BaseURL, accountState.BotToken)
	client.HTTPClient = native.HTTPClient
	if err := sendWeixinAckTyping(ctx, client, store, accountState, chat.ReplyToUserID, contextKey, contextToken, status); err != nil {
		if errors.Is(err, weixin.ErrSessionExpired) {
			result.Raw = map[string]any{"reason": "session_expired", "context_key": contextKey}
			return result, nil
		}
		return nil, err
	}
	result.Status = "sent"
	result.Raw = map[string]any{
		"context_key": contextKey,
		"status":      status,
	}
	return result, nil
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
	if strings.TrimSpace(stringValue(account.Credential["ilink_user_id"])) == accountID {
		return true
	}
	if strings.TrimSpace(stringValue(account.Credential["ilink_bot_id"])) == accountID {
		return true
	}
	return false
}

func accountKey(account sdk.ChannelAccount) string {
	return firstString(account.UUID, account.Credential["account_id"], account.Credential["ilink_user_id"], account.Credential["ilink_bot_id"])
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
	state := sdkAccountToState(account)
	aliases := accountAliases(account, state.AccountID)
	if len(aliases) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, alias := range aliases {
		s.accounts[alias] = &state
		s.sdkAccounts[alias] = account
	}
}

func (s *connectorStateStore) LoadAccount(ctx context.Context, accountID string) (*AccountState, error) {
	s.mu.Lock()
	if account, ok := s.accounts[accountID]; ok {
		sdkAccount := s.sdkAccounts[accountID]
		accountStore := s.accountStore
		s.mu.Unlock()
		if refreshed, ok, err := loadAccountState(ctx, accountStore, sdkAccount); err != nil {
			return nil, err
		} else if ok {
			s.mu.Lock()
			s.accounts[accountID] = refreshed
			sdkAccount.State = stateToMap(*refreshed)
			s.sdkAccounts[accountID] = sdkAccount
			s.mu.Unlock()
			return refreshed, nil
		}
		return account, nil
	}
	accountStore := s.accountStore
	s.mu.Unlock()
	if refreshed, ok, err := loadAccountState(ctx, accountStore, sdk.ChannelAccount{UUID: accountID}); err != nil {
		return nil, err
	} else if ok {
		s.mu.Lock()
		s.accounts[accountID] = refreshed
		s.sdkAccounts[accountID] = accountStateToSDK(*refreshed, sdk.ChannelAccount{UUID: accountID})
		s.mu.Unlock()
		return refreshed, nil
	}
	account := &AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.mu.Lock()
	s.accounts[accountID] = account
	s.mu.Unlock()
	return account, nil
}

func loadAccountState(ctx context.Context, accountStore sdk.AccountStore, account sdk.ChannelAccount) (*AccountState, bool, error) {
	if accountStore == nil || strings.TrimSpace(account.UUID) == "" {
		return nil, false, nil
	}
	stateMap, err := accountStore.LoadChannelAccountState(ctx, account.UUID)
	if err != nil {
		return nil, false, err
	}
	if len(stateMap) == 0 {
		return nil, false, nil
	}
	account.State = stateMap
	refreshed := sdkAccountToState(account)
	return &refreshed, true, nil
}

func (s *connectorStateStore) SaveAccount(ctx context.Context, account *AccountState) error {
	if account == nil || account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	account.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	existing := s.sdkAccounts[account.AccountID]
	sdkAccount := accountStateToSDK(*account, existing)
	for _, alias := range accountAliases(sdkAccount, account.AccountID) {
		s.accounts[alias] = account
		s.sdkAccounts[alias] = sdkAccount
	}
	accountStore := s.accountStore
	s.mu.Unlock()
	if accountStore != nil && sdkAccount.UUID != "" {
		return accountStore.SaveChannelAccountState(ctx, sdkAccount.UUID, sdkAccount.State)
	}
	return nil
}

func (s *connectorStateStore) SaveLogin(ctx context.Context, accountID, botToken, baseURL, ilinkUserID, ilinkBotID string) (*AccountState, error) {
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

func (s *connectorStateStore) accountID(account sdk.ChannelAccount) string {
	if account.UUID != "" {
		return account.UUID
	}
	if value, _ := account.Credential["account_id"].(string); value != "" {
		return value
	}
	if value, _ := account.Credential["ilink_user_id"].(string); value != "" {
		return value
	}
	if value, _ := account.Credential["ilink_bot_id"].(string); value != "" {
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
	ilinkBotID := strings.TrimSpace(account.ILinkBotID)
	if ilinkBotID == "" && strings.TrimSpace(account.ILinkUserID) == "" {
		ilinkBotID = account.AccountID
	}
	existing.Credential = map[string]any{
		"account_id":    account.AccountID,
		"bot_token":     account.BotToken,
		"base_url":      account.BaseURL,
		"ilink_user_id": account.ILinkUserID,
		"ilink_bot_id":  ilinkBotID,
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

func ackStateKey(req sdk.OutboundAck) string {
	if req.ChatType == sdk.ChatTypeGroup {
		return weixin.ChatTypeGroup + ":" + req.ChatID
	}
	return req.ChatID
}

func weixinAckWantsTyping(req sdk.OutboundAck) bool {
	mode := strings.ToLower(strings.TrimSpace(firstString(req.Mode, req.Raw["mode"])))
	return mode == "" || mode == "auto" || mode == "typing"
}

func weixinAckTypingStatus(req sdk.OutboundAck) (int, bool) {
	action := strings.ToLower(strings.TrimSpace(firstString(req.Action, req.Raw["action"])))
	switch action {
	case "", "start", "processing":
		return weixin.TypingStatusStart, true
	case "stop", "finish", "finished", "done":
		return weixin.TypingStatusStop, true
	default:
		return 0, false
	}
}

func sendWeixinAckTyping(ctx context.Context, client *weixin.Client, store *connectorStateStore, account *AccountState, toUserID, contextKey, contextToken string, status int) error {
	typingTicket := account.TypingTickets[contextKey]
	if strings.TrimSpace(typingTicket) == "" {
		ticket, err := client.GetTypingTicket(ctx, toUserID, contextToken)
		if err != nil {
			return err
		}
		typingTicket = ticket
		account.TypingTickets[contextKey] = ticket
		if err := store.SaveAccount(ctx, account); err != nil {
			return err
		}
	}
	err := client.SendTyping(ctx, toUserID, typingTicket, status)
	if err == nil {
		return nil
	}
	if errors.Is(err, weixin.ErrSessionExpired) {
		delete(account.TypingTickets, contextKey)
	}
	ticket, refreshErr := client.GetTypingTicket(ctx, toUserID, contextToken)
	if refreshErr != nil {
		return refreshErr
	}
	account.TypingTickets[contextKey] = ticket
	if saveErr := store.SaveAccount(ctx, account); saveErr != nil {
		return saveErr
	}
	return client.SendTyping(ctx, toUserID, ticket, status)
}

func weixinOutboundMentionText(req sdk.OutboundMessage) string {
	text := strings.TrimSpace(req.Text)
	var tags []string
	if req.MentionAll || boolValue(req.Raw["mention_all"]) || boolValue(req.Raw["mentionAll"]) ||
		boolValue(req.Raw["at_all"]) || boolValue(req.Raw["atAll"]) || boolValue(req.Raw["isAtAll"]) {
		tags = append(tags, "@all")
	}
	for _, id := range stringSlice(firstValue(req.Raw["mention_ids"], req.Raw["mentionIds"], req.Raw["at_user_ids"], req.Raw["atUserIds"])) {
		tags = append(tags, "@"+id)
	}
	mentions := append([]sdk.MentionIdentity{}, req.Mentions...)
	mentions = append(mentions, rawMentionIdentities(req.Raw["mentions"])...)
	for _, mention := range mentions {
		id := strings.TrimSpace(mention.ID)
		if id == "" {
			continue
		}
		if strings.EqualFold(id, "all") || strings.EqualFold(strings.TrimSpace(mention.IDType), "all") {
			tags = append(tags, "@all")
			continue
		}
		label := strings.TrimSpace(mention.DisplayName)
		if label == "" {
			label = id
		}
		if !strings.HasPrefix(label, "@") {
			label = "@" + label
		}
		tags = append(tags, label)
	}
	tags = uniqueStringList(tags)
	if len(tags) == 0 {
		return text
	}
	prefix := strings.Join(tags, " ")
	if text == "" {
		return prefix
	}
	return prefix + "\n" + text
}

func rawMentionIdentities(value any) []sdk.MentionIdentity {
	switch typed := value.(type) {
	case []sdk.MentionIdentity:
		return typed
	case []any:
		out := make([]sdk.MentionIdentity, 0, len(typed))
		for _, item := range typed {
			out = append(out, mentionIdentityFromAny(item))
		}
		return out
	case []map[string]any:
		out := make([]sdk.MentionIdentity, 0, len(typed))
		for _, item := range typed {
			out = append(out, mentionIdentityFromAny(item))
		}
		return out
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		var parsed []map[string]any
		if err := json.Unmarshal([]byte(typed), &parsed); err == nil {
			out := make([]sdk.MentionIdentity, 0, len(parsed))
			for _, item := range parsed {
				out = append(out, mentionIdentityFromAny(item))
			}
			return out
		}
	case json.RawMessage:
		var parsed []map[string]any
		if err := json.Unmarshal(typed, &parsed); err == nil {
			out := make([]sdk.MentionIdentity, 0, len(parsed))
			for _, item := range parsed {
				out = append(out, mentionIdentityFromAny(item))
			}
			return out
		}
	}
	return nil
}

func mentionIdentityFromAny(value any) sdk.MentionIdentity {
	mention, ok := value.(sdk.MentionIdentity)
	if ok {
		return mention
	}
	item, ok := value.(map[string]any)
	if !ok {
		return sdk.MentionIdentity{}
	}
	return sdk.MentionIdentity{
		ID:          firstString(item["id"], item["ID"], item["user_id"], item["userId"]),
		IDType:      firstString(item["id_type"], item["idType"], item["IDType"], item["type"]),
		DisplayName: firstString(item["display_name"], item["displayName"], item["name"]),
	}
}

func (a gatewayRuntimeAdapter) EnsureWeixinChatSession(ctx context.Context, accountID, chatType, chatID, senderID string) (string, error) {
	identity := weixinSDKChatIdentity(weixin.ChatIdentity{ChatType: chatType, ChatID: chatID, SenderID: senderID})
	return a.gateway.EnsureChatSession(ctx, sdk.EnsureChatSessionRequest{
		WorkspaceUUID:       a.workspaceUUID,
		Platform:            a.platform,
		AccountUUID:         accountID,
		ChatType:            chatType,
		ChatID:              chatID,
		ChatIdentity:        identity,
		SenderID:            senderID,
		AgentParticipantID:  a.AgentParticipantID(),
		BridgeParticipantID: a.BridgeParticipantID(),
		Metadata: map[string]any{
			"source":        "weixin",
			"account_uuid":  accountID,
			"chat_identity": identity,
		},
	})
}

func weixinSDKChatIdentity(chat weixin.ChatIdentity) sdk.ChatIdentity {
	return sdk.ChatIdentity{
		ID:     strings.TrimSpace(chat.ChatID),
		IDType: "chat_id",
		Type:   strings.TrimSpace(chat.ChatType),
	}
}

func (a gatewayRuntimeAdapter) CreateWeixinUserMessage(ctx context.Context, sessionUUID string, msg UserMessage) (string, error) {
	return a.gateway.CreateMessage(ctx, sdk.CreateMessageRequest{
		WorkspaceUUID: a.workspaceUUID,
		SessionUUID:   sessionUUID,
		SenderID:      msg.SenderID,
		Content:       msg.Content,
		DedupeKey:     msg.DedupeKey,
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
		AccountID:   firstString(account.Credential["account_id"], account.Credential["ilink_user_id"], account.Credential["ilink_bot_id"], account.UUID),
		BotToken:    stringValue(account.Credential["bot_token"]),
		BaseURL:     stringValue(account.Credential["base_url"]),
		ILinkUserID: firstString(account.Credential["ilink_user_id"], account.State["ilink_user_id"]),
		ILinkBotID:  firstString(account.Credential["ilink_bot_id"], account.State["ilink_bot_id"], standardBotIdentityValue(account.State, "ilink_bot_id")),
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
	state.StreamConnectionState = stringValue(account.State[sdk.RuntimeHealthKeyStreamConnectionState])
	state.StreamConnectedAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamConnectedAt])
	state.StreamDisconnectedAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamDisconnectedAt])
	state.StreamLastActivityAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamLastActivityAt])
	state.StreamLastPingAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamLastPingAt])
	state.StreamLastPongAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamLastPongAt])
	state.StreamLastEventAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamLastEventAt])
	state.StreamLastError = stringValue(account.State[sdk.RuntimeHealthKeyStreamLastError])
	state.StreamLastErrorAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamLastErrorAt])
	state.StreamReconnectRequestedAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamReconnectRequestedAt])
	state.StreamReconnectError = stringValue(account.State[sdk.RuntimeHealthKeyStreamReconnectError])
	state.StreamReconnectErrorAt = timeValue(account.State[sdk.RuntimeHealthKeyStreamReconnectErrorAt])
	state.StreamSessionExpired = boolValue(account.State[sdk.RuntimeHealthKeyStreamSessionExpired])
	state.StreamSessionExpiredAt = timeValue(account.State["stream_session_expired_at"])
	state.StreamSessionExpiredReason = stringValue(account.State["stream_session_expired_reason"])
	state.StreamSessionExpiredOp = stringValue(account.State["stream_session_expired_operation"])
	state.StreamSessionExpiredCode = intValue(account.State["stream_session_expired_code"])
	state.StreamSessionExpiredMsg = stringValue(account.State["stream_session_expired_msg"])
	state.LastInboundSkipReason = stringValue(account.State["last_inbound_skip_reason"])
	state.LastInboundSkipAt = timeValue(account.State["last_inbound_skip_at"])
	state.LastInboundSkipMessageID = stringValue(account.State["last_inbound_skip_message_id"])
	state.LastInboundError = stringValue(account.State["last_inbound_error"])
	state.LastInboundErrorAt = timeValue(account.State["last_inbound_error_at"])
	state.LastInboundErrorMessageID = stringValue(account.State["last_inbound_error_message_id"])
	state.EnsureMaps()
	return state
}

func accountAliases(account sdk.ChannelAccount, accountID string) []string {
	return uniqueStringList([]string{
		accountID,
		account.UUID,
		stringValue(account.Credential["account_id"]),
		stringValue(account.Credential["ilink_user_id"]),
		stringValue(account.Credential["ilink_bot_id"]),
	})
}

func stateToMap(account AccountState) map[string]any {
	account.EnsureMaps()
	out := map[string]any{
		"channel_link_session": account.ChannelLinkSession,
		"ilink_user_id":        account.ILinkUserID,
		"ilink_bot_id":         account.ILinkBotID,
		"status":               account.Status,
		"last_error":           account.LastError,
		"get_updates_buf":      account.GetUpdatesBuf,
		"context_tokens":       account.ContextTokens,
		"typing_tickets":       account.TypingTickets,
		"peer_sessions":        account.PeerSessions,
		"inbound_seen":         account.InboundSeen,
		"sent_beak_messages":   account.SentBeakMessages,
		"stream_cursors":       account.StreamCursors,
		sdk.RuntimeHealthKeyStreamConnectionState:      account.StreamConnectionState,
		sdk.RuntimeHealthKeyStreamConnectedAt:          account.StreamConnectedAt,
		sdk.RuntimeHealthKeyStreamDisconnectedAt:       account.StreamDisconnectedAt,
		sdk.RuntimeHealthKeyStreamLastActivityAt:       account.StreamLastActivityAt,
		sdk.RuntimeHealthKeyStreamLastPingAt:           account.StreamLastPingAt,
		sdk.RuntimeHealthKeyStreamLastPongAt:           account.StreamLastPongAt,
		sdk.RuntimeHealthKeyStreamLastEventAt:          account.StreamLastEventAt,
		sdk.RuntimeHealthKeyStreamLastError:            account.StreamLastError,
		sdk.RuntimeHealthKeyStreamLastErrorAt:          account.StreamLastErrorAt,
		sdk.RuntimeHealthKeyStreamReconnectRequestedAt: account.StreamReconnectRequestedAt,
		sdk.RuntimeHealthKeyStreamReconnectError:       account.StreamReconnectError,
		sdk.RuntimeHealthKeyStreamReconnectErrorAt:     account.StreamReconnectErrorAt,
		sdk.RuntimeHealthKeyStreamSessionExpired:       account.StreamSessionExpired,
		"stream_session_expired_at":                    account.StreamSessionExpiredAt,
		"stream_session_expired_reason":                account.StreamSessionExpiredReason,
		"stream_session_expired_operation":             account.StreamSessionExpiredOp,
		"stream_session_expired_code":                  account.StreamSessionExpiredCode,
		"stream_session_expired_msg":                   account.StreamSessionExpiredMsg,
		"last_inbound_skip_reason":                     account.LastInboundSkipReason,
		"last_inbound_skip_at":                         account.LastInboundSkipAt,
		"last_inbound_skip_message_id":                 account.LastInboundSkipMessageID,
		"last_inbound_error":                           account.LastInboundError,
		"last_inbound_error_at":                        account.LastInboundErrorAt,
		"last_inbound_error_message_id":                account.LastInboundErrorMessageID,
		"updated_at":                                   account.UpdatedAt,
	}
	if identity := weixinBotIdentityState(account); len(identity) > 0 {
		out["bot_identity"] = identity
		out["bot_identities"] = []map[string]any{identity}
	}
	return out
}

func weixinBotIdentityState(account AccountState) map[string]any {
	ilinkBotID := strings.TrimSpace(account.ILinkBotID)
	if ilinkBotID == "" {
		return nil
	}
	return map[string]any{
		"id":      ilinkBotID,
		"id_type": "ilink_bot_id",
	}
}

func standardBotIdentityValue(state map[string]any, idTypes ...string) string {
	wanted := make(map[string]struct{}, len(idTypes))
	for _, idType := range idTypes {
		idType = strings.TrimSpace(idType)
		if idType != "" {
			wanted[idType] = struct{}{}
		}
	}
	for _, identity := range standardBotIdentityMaps(state) {
		idType := strings.TrimSpace(stringValue(identity["id_type"]))
		if len(wanted) > 0 {
			if _, ok := wanted[idType]; !ok {
				continue
			}
		}
		if id := strings.TrimSpace(stringValue(identity["id"])); id != "" {
			return id
		}
	}
	return ""
}

func standardBotIdentityMaps(state map[string]any) []map[string]any {
	if len(state) == 0 {
		return nil
	}
	var out []map[string]any
	out = append(out, botIdentityMapsFromAny(state["bot_identities"])...)
	out = append(out, botIdentityMapsFromAny(state["bot_identity"])...)
	return out
}

func botIdentityMapsFromAny(value any) []map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return []map[string]any{typed}
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, botIdentityMapsFromAny(item)...)
		}
		return out
	case json.RawMessage:
		var list []map[string]any
		if err := json.Unmarshal(typed, &list); err == nil {
			return list
		}
		var item map[string]any
		if err := json.Unmarshal(typed, &item); err == nil {
			return []map[string]any{item}
		}
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return botIdentityMapsFromAny(json.RawMessage(typed))
	}
	return nil
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

func stringSlice(value any) []string {
	var values []any
	switch typed := value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		values = typed
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		var parsed []any
		if err := json.Unmarshal([]byte(typed), &parsed); err == nil {
			values = parsed
			break
		}
		return []string{strings.TrimSpace(typed)}
	case json.RawMessage:
		var parsed []any
		if err := json.Unmarshal(typed, &parsed); err == nil {
			values = parsed
		}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if item := strings.TrimSpace(stringValue(value)); item != "" {
			out = append(out, item)
		}
	}
	return uniqueStringList(out)
}

func uniqueStringList(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func timeValue(value any) time.Time {
	switch typed := value.(type) {
	case time.Time:
		return typed
	case string:
		if strings.TrimSpace(typed) == "" {
			return time.Time{}
		}
		parsed, _ := time.Parse(time.RFC3339Nano, typed)
		return parsed
	case json.RawMessage:
		var text string
		if err := json.Unmarshal(typed, &text); err == nil {
			return timeValue(text)
		}
	}
	return time.Time{}
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

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case uint8:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	case json.RawMessage:
		var number json.Number
		if err := json.Unmarshal(typed, &number); err == nil {
			return intValue(number)
		}
		var text string
		if err := json.Unmarshal(typed, &text); err == nil {
			return intValue(text)
		}
	}
	return 0
}

func firstString(values ...any) string {
	for _, value := range values {
		if stringValue := strings.TrimSpace(stringValue(value)); stringValue != "" {
			return stringValue
		}
	}
	return ""
}

func firstValue(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func cloneMap(value map[string]any) map[string]any {
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func mapValue(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	default:
		return map[string]any{}
	}
}

var _ sdk.Connector = Connector{}
