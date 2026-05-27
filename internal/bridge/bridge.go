package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/beak"
	"github.com/GuanceCloud/beak-agent-channel-wechat/internal/weixin"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	"github.com/GuanceCloud/beak-agent-channel-wechat/state"
)

type WeixinClient interface {
	GetUpdates(ctx context.Context, getUpdatesBuf string, timeout time.Duration) (*weixin.GetUpdatesResponse, error)
	SendText(ctx context.Context, toUserID, text, contextToken string) error
	GetTypingTicket(ctx context.Context, ilinkUserID, contextToken string) (string, error)
	SendTyping(ctx context.Context, ilinkUserID, typingTicket string, status int) error
	NotifyStart(ctx context.Context) error
	NotifyStop(ctx context.Context) error
}

type BeakClient interface {
	EnsureWeixinChannel(ctx context.Context, workspaceUUID string) (string, error)
	EnsureChannelLinkSession(ctx context.Context, workspaceUUID, accountID, agentParticipantID, bridgeParticipantID string) (string, error)
	EnsurePeerSession(ctx context.Context, workspaceUUID, accountID, peerUserID, agentParticipantID, bridgeParticipantID string) (string, error)
	EnsureChatSession(ctx context.Context, workspaceUUID, accountID, chatType, chatID, senderID, agentParticipantID, bridgeParticipantID string) (string, error)
	CreateMessage(ctx context.Context, sessionUUID string, req beak.CreateMessageRequest) (*beak.CreateMessageResponse, error)
	StreamEvents(ctx context.Context, sessionUUID string, req beak.StreamRequest, handle func(beak.StreamEvent) error) error
}

type StateStore interface {
	LoadAccount(accountID string) (*state.AccountState, error)
	SaveAccount(account *state.AccountState) error
}

type WeixinFactory func(account state.AccountState, accountCfg AccountConfig) WeixinClient

type Bridge struct {
	options       *Options
	store         StateStore
	beak          BeakClient
	weixinFactory WeixinFactory
	logger        *log.Logger
}

func New(options *Options, store StateStore, beakClient BeakClient, factory WeixinFactory, logger *log.Logger) *Bridge {
	if options == nil {
		options = &Options{}
	}
	options.ApplyDefaults()
	if factory == nil {
		factory = func(account state.AccountState, _ AccountConfig) WeixinClient {
			client := options.Weixin.NewClient(account.BaseURL, account.BotToken)
			if options.HTTPClient != nil {
				client.HTTPClient = options.HTTPClient
			}
			return client
		}
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Bridge{
		options:       options,
		store:         store,
		beak:          beakClient,
		weixinFactory: factory,
		logger:        logger,
	}
}

func (b *Bridge) Run(ctx context.Context) error {
	if err := b.options.ValidateForRun(); err != nil {
		return err
	}
	if _, err := b.beak.EnsureWeixinChannel(ctx, b.options.WorkspaceRef); err != nil {
		return err
	}

	for _, accountCfg := range b.options.Accounts {
		accountCfg := accountCfg
		go func() {
			if err := b.runAccount(ctx, accountCfg); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				b.logger.Printf("weixin account stopped account=%s error=%v", accountCfg.AccountID, err)
			}
		}()
	}
	<-ctx.Done()
	return ctx.Err()
}

func (b *Bridge) runAccount(ctx context.Context, accountCfg AccountConfig) error {
	account, err := b.store.LoadAccount(accountCfg.AccountID)
	if err != nil {
		return err
	}
	if account.LoginRequired() {
		return fmt.Errorf("weixin account %s requires login: %s", account.AccountID, account.LastError)
	}
	if account.BotToken == "" {
		return fmt.Errorf("weixin account %s is not logged in; call channel.Login first", account.AccountID)
	}
	if account.BaseURL == "" {
		account.BaseURL = b.options.Weixin.BaseURL
	}
	if err := b.store.SaveAccount(account); err != nil {
		return err
	}
	channelLinkSession, err := b.beak.EnsureChannelLinkSession(ctx, b.options.WorkspaceRef, account.AccountID, b.options.AgentParticipantID, b.options.BridgeParticipantID)
	if err != nil {
		return err
	}
	if account.ChannelLinkSession != channelLinkSession {
		account.ChannelLinkSession = channelLinkSession
		if err := b.store.SaveAccount(account); err != nil {
			return err
		}
	}

	runner := &AccountRunner{
		options:       b.options,
		store:         b.store,
		beak:          b.beak,
		wx:            b.weixinFactory(*account, accountCfg),
		account:       account,
		logger:        b.logger,
		activeStreams: make(map[string]bool),
	}
	if err := runner.EnsureKnownSessions(ctx); err != nil {
		return err
	}
	if err := runner.wx.NotifyStart(ctx); err != nil {
		runner.logger.Printf("notifyStart account=%s error=%v", account.AccountID, err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), b.options.Weixin.RequestTimeout)
		defer cancel()
		if err := runner.wx.NotifyStop(stopCtx); err != nil {
			runner.logger.Printf("notifyStop account=%s error=%v", account.AccountID, err)
		}
	}()
	runner.StartKnownStreams(ctx)
	return runner.Poll(ctx)
}

type AccountRunner struct {
	options *Options
	store   StateStore
	beak    BeakClient
	wx      WeixinClient
	account *state.AccountState
	logger  *log.Logger

	mu            sync.Mutex
	activeStreams map[string]bool
}

func (r *AccountRunner) Poll(ctx context.Context) error {
	longPollTimeout := r.options.Weixin.LongPollTimeout
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		r.mu.Lock()
		buf := r.account.GetUpdatesBuf
		r.mu.Unlock()

		resp, err := r.wx.GetUpdates(ctx, buf, longPollTimeout+2*time.Second)
		if err != nil {
			if errors.Is(err, weixin.ErrSessionExpired) {
				_ = r.markSessionExpired("getupdates session expired")
				return fmt.Errorf("weixin account %s session expired; run login again: %w", r.account.AccountID, err)
			}
			r.logger.Printf("getUpdates account=%s error=%v", r.account.AccountID, err)
			if !sleepOrDone(ctx, r.options.PollInterval) {
				return ctx.Err()
			}
			continue
		}

		r.mu.Lock()
		if resp.GetUpdatesBuf != "" {
			r.account.GetUpdatesBuf = resp.GetUpdatesBuf
			_ = r.store.SaveAccount(r.account)
		}
		r.mu.Unlock()

		if resp.LongPollingTimeoutMS > 0 {
			longPollTimeout = time.Duration(resp.LongPollingTimeoutMS) * time.Millisecond
		}
		for _, msg := range resp.Messages {
			sessionUUID, processed, err := r.ProcessUpdate(ctx, msg)
			if err != nil {
				r.logger.Printf("process update account=%s peer=%s error=%v", r.account.AccountID, msg.FromUserID, err)
				continue
			}
			if processed {
				r.StartStream(ctx, msg.ChatIdentity().StateKey(), sessionUUID)
			}
		}
		if !sleepOrDone(ctx, r.options.PollInterval) {
			return ctx.Err()
		}
	}
}

func (r *AccountRunner) ProcessUpdate(ctx context.Context, msg weixin.WeixinMessage) (string, bool, error) {
	text := strings.TrimSpace(msg.Text())
	chat := msg.ChatIdentity()
	if chat.ChatID == "" || chat.SenderID == "" || text == "" {
		return "", false, nil
	}
	if msg.MessageType != 0 && msg.MessageType != weixin.MessageTypeUser {
		return "", false, nil
	}
	if msg.MessageState != 0 && msg.MessageState != weixin.MessageStateFinish {
		return "", false, nil
	}
	inbound := BuildInboundMessage(r.options.WorkspaceRef, r.options.ChannelUUID, r.account.AccountID, msg, text)

	key := inbound.DedupeKey
	chatKey := chat.StateKey()
	r.mu.Lock()
	if _, ok := r.account.InboundSeen[key]; ok {
		r.mu.Unlock()
		return r.account.PeerSessions[chatKey], false, nil
	}
	if msg.ContextToken != "" {
		r.account.ContextTokens[chatKey] = msg.ContextToken
	}
	sessionUUID := r.account.PeerSessions[chatKey]
	r.mu.Unlock()

	if sessionUUID == "" {
		var err error
		sessionUUID, err = r.beak.EnsureChatSession(ctx, r.options.WorkspaceRef, r.account.AccountID, chat.ChatType, chat.ChatID, chat.SenderID, r.options.AgentParticipantID, r.options.BridgeParticipantID)
		if err != nil {
			return "", false, err
		}
	}

	_, err := r.beak.CreateMessage(ctx, sessionUUID, beak.CreateMessageRequest{
		WorkspaceUUID: r.options.WorkspaceRef,
		SenderID:      beak.IMUserParticipantID(beak.PlatformWeixin, chat.ChatType, chat.ChatID, chat.SenderID),
		Content:       text,
		Metadata: map[string]any{
			"source":             "weixin",
			"platform":           "weixin",
			"account_uuid":       r.account.AccountID,
			"weixin_account_id":  r.account.AccountID,
			"weixin_chat_type":   chat.ChatType,
			"weixin_chat_id":     chat.ChatID,
			"weixin_sender_id":   chat.SenderID,
			"weixin_peer_id":     chat.ChatID,
			"weixin_message_id":  msg.MessageID,
			"weixin_sequence_id": msg.Seq,
			"weixin_session_id":  msg.SessionID,
			"weixin_group_id":    msg.GroupID,
			"inbound_message":    inbound,
		},
	})
	if err != nil {
		return "", false, err
	}

	r.mu.Lock()
	r.account.PeerSessions[chatKey] = sessionUUID
	r.account.InboundSeen[key] = time.Now().UTC().Format(time.RFC3339Nano)
	if msg.ContextToken != "" {
		r.account.ContextTokens[chatKey] = msg.ContextToken
	}
	err = r.store.SaveAccount(r.account)
	r.mu.Unlock()
	if err != nil {
		return "", false, err
	}
	if err := r.sendTyping(ctx, chatKey, weixin.TypingStatusStart); err != nil {
		if errors.Is(err, weixin.ErrSessionExpired) {
			_ = r.markSessionExpired("typing session expired")
			return "", false, err
		}
		r.logger.Printf("send typing start account=%s peer=%s error=%v", r.account.AccountID, chatKey, err)
	}
	return sessionUUID, true, nil
}

func BuildInboundMessage(workspaceRef, channelUUID, accountID string, msg weixin.WeixinMessage, text string) sdk.InboundMessage {
	chat := msg.ChatIdentity()
	messageID := ""
	if msg.MessageID != 0 {
		messageID = fmt.Sprint(msg.MessageID)
	}
	return sdk.InboundMessage{
		WorkspaceUUID: workspaceRef,
		Platform:      beak.PlatformWeixin,
		AccountUUID:   accountID,
		ChannelUUID:   channelUUID,
		ChatType:      chat.ChatType,
		ChatID:        chat.ChatID,
		SenderID:      chat.SenderID,
		MessageID:     messageID,
		Text:          text,
		DedupeKey:     msg.DedupeKey(accountID),
		Raw: map[string]any{
			"seq":             msg.Seq,
			"message_id":      msg.MessageID,
			"from_user_id":    msg.FromUserID,
			"to_user_id":      msg.ToUserID,
			"client_id":       msg.ClientID,
			"create_time_ms":  msg.CreateTimeMS,
			"session_id":      msg.SessionID,
			"group_id":        msg.GroupID,
			"message_type":    msg.MessageType,
			"message_state":   msg.MessageState,
			"context_token":   msg.ContextToken,
			"item_list_count": len(msg.ItemList),
		},
	}
}

func (r *AccountRunner) EnsureKnownSessions(ctx context.Context) error {
	r.mu.Lock()
	peers := make([]string, 0, len(r.account.PeerSessions))
	for peerID := range r.account.PeerSessions {
		peers = append(peers, peerID)
	}
	r.mu.Unlock()

	changed := false
	for _, peerID := range peers {
		chat := weixin.ChatIdentityFromStateKey(peerID)
		sessionUUID, err := r.beak.EnsureChatSession(ctx, r.options.WorkspaceRef, r.account.AccountID, chat.ChatType, chat.ChatID, chat.SenderID, r.options.AgentParticipantID, r.options.BridgeParticipantID)
		if err != nil {
			return err
		}
		r.mu.Lock()
		if r.account.PeerSessions[peerID] != sessionUUID {
			r.account.PeerSessions[peerID] = sessionUUID
			changed = true
		}
		r.mu.Unlock()
	}
	if changed {
		r.mu.Lock()
		err := r.store.SaveAccount(r.account)
		r.mu.Unlock()
		return err
	}
	return nil
}

func (r *AccountRunner) StartKnownStreams(ctx context.Context) {
	r.mu.Lock()
	pairs := make(map[string]string, len(r.account.PeerSessions))
	for peerID, sessionUUID := range r.account.PeerSessions {
		pairs[peerID] = sessionUUID
	}
	r.mu.Unlock()
	for peerID, sessionUUID := range pairs {
		r.StartStream(ctx, peerID, sessionUUID)
	}
}

func (r *AccountRunner) StartStream(ctx context.Context, peerID, sessionUUID string) {
	if peerID == "" || sessionUUID == "" {
		return
	}
	r.mu.Lock()
	if r.activeStreams[sessionUUID] {
		r.mu.Unlock()
		return
	}
	r.activeStreams[sessionUUID] = true
	r.mu.Unlock()

	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.activeStreams, sessionUUID)
			r.mu.Unlock()
		}()
		r.streamLoop(ctx, peerID, sessionUUID)
	}()
}

func (r *AccountRunner) streamLoop(ctx context.Context, peerID, sessionUUID string) {
	reconnect := r.options.StreamReconnect
	if reconnect <= 0 {
		reconnect = 30 * time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r.mu.Lock()
		lastEventUUID := r.account.StreamCursors[sessionUUID]
		r.mu.Unlock()

		streamCtx, cancel := context.WithTimeout(ctx, reconnect)
		err := r.beak.StreamEvents(streamCtx, sessionUUID, beak.StreamRequest{
			WorkspaceUUID: r.options.WorkspaceRef,
			SubscriberID:  r.options.BridgeParticipantID,
			LastEventUUID: lastEventUUID,
		}, func(event beak.StreamEvent) error {
			return r.ProcessStreamEvent(ctx, peerID, event)
		})
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil && streamCtx.Err() == nil {
			r.logger.Printf("beak stream account=%s session=%s error=%v", r.account.AccountID, sessionUUID, err)
		}
		if !sleepOrDone(ctx, reconnect) {
			return
		}
	}
}

func (r *AccountRunner) ProcessStreamEvent(ctx context.Context, peerID string, event beak.StreamEvent) error {
	if event.EventType == "heartbeat" || event.EventType == "" {
		return nil
	}
	if event.EventType == "error" {
		return fmt.Errorf("beak stream error: %s", string(event.Payload))
	}

	payload, err := event.MessagePayload()
	if event.EventType == "message" && err != nil {
		return err
	}

	r.mu.Lock()
	sessionUUID := event.SessionUUID
	if sessionUUID == "" {
		sessionUUID = r.account.PeerSessions[peerID]
	}
	if event.EventType != "message" {
		if event.EventUUID != "" && sessionUUID != "" {
			r.account.StreamCursors[sessionUUID] = event.EventUUID
			err := r.store.SaveAccount(r.account)
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
		return nil
	}
	if payload.SenderID != r.options.AgentParticipantID {
		if event.EventUUID != "" && sessionUUID != "" {
			r.account.StreamCursors[sessionUUID] = event.EventUUID
			err := r.store.SaveAccount(r.account)
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
		return nil
	}
	messageUUID := payload.MessageUUID
	if messageUUID == "" {
		messageUUID = event.MessageUUID
	}
	if messageUUID == "" {
		messageUUID = event.EventUUID
	}
	if messageUUID != "" {
		if _, ok := r.account.SentBeakMessages[messageUUID]; ok {
			if event.EventUUID != "" && sessionUUID != "" {
				r.account.StreamCursors[sessionUUID] = event.EventUUID
				err := r.store.SaveAccount(r.account)
				r.mu.Unlock()
				return err
			}
			r.mu.Unlock()
			return nil
		}
	}
	contextToken := r.account.ContextTokens[peerID]
	r.mu.Unlock()

	content := strings.TrimSpace(payload.Content)
	if content == "" {
		return r.advanceCursor(sessionUUID, event.EventUUID)
	}
	if contextToken == "" {
		r.logger.Printf("skip beak message account=%s peer=%s message=%s: missing context_token", r.account.AccountID, peerID, messageUUID)
		r.mu.Lock()
		if messageUUID != "" {
			r.account.SentBeakMessages[messageUUID] = "skipped:missing_context_token:" + time.Now().UTC().Format(time.RFC3339Nano)
		}
		if event.EventUUID != "" && sessionUUID != "" {
			r.account.StreamCursors[sessionUUID] = event.EventUUID
		}
		err := r.store.SaveAccount(r.account)
		r.mu.Unlock()
		return err
	}
	replyToUserID := weixin.ChatIdentityFromStateKey(peerID).ReplyToUserID
	if err := r.wx.SendText(ctx, replyToUserID, content, contextToken); err != nil {
		if errors.Is(err, weixin.ErrSessionExpired) {
			_ = r.markSessionExpired("sendmessage session expired")
		}
		return err
	}
	if err := r.sendTyping(ctx, peerID, weixin.TypingStatusStop); err != nil {
		if errors.Is(err, weixin.ErrSessionExpired) {
			_ = r.markSessionExpired("typing session expired")
			return err
		}
		r.logger.Printf("send typing stop account=%s peer=%s error=%v", r.account.AccountID, peerID, err)
	}

	r.mu.Lock()
	if messageUUID != "" {
		r.account.SentBeakMessages[messageUUID] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if event.EventUUID != "" && sessionUUID != "" {
		r.account.StreamCursors[sessionUUID] = event.EventUUID
	}
	err = r.store.SaveAccount(r.account)
	r.mu.Unlock()
	return err
}

func (r *AccountRunner) advanceCursor(sessionUUID, eventUUID string) error {
	if sessionUUID == "" || eventUUID == "" {
		return nil
	}
	r.mu.Lock()
	r.account.StreamCursors[sessionUUID] = eventUUID
	err := r.store.SaveAccount(r.account)
	r.mu.Unlock()
	return err
}

func (r *AccountRunner) sendTyping(ctx context.Context, peerID string, status int) error {
	if r.options.Weixin.DisableTyping {
		return nil
	}
	chat := weixin.ChatIdentityFromStateKey(peerID)
	if chat.ReplyToUserID == "" {
		return nil
	}

	r.mu.Lock()
	contextToken := r.account.ContextTokens[peerID]
	typingTicket := r.account.TypingTickets[peerID]
	r.mu.Unlock()
	if contextToken == "" {
		return nil
	}

	if typingTicket == "" {
		ticket, err := r.wx.GetTypingTicket(ctx, chat.ReplyToUserID, contextToken)
		if err != nil {
			return err
		}
		typingTicket = ticket
		r.mu.Lock()
		r.account.TypingTickets[peerID] = typingTicket
		err = r.store.SaveAccount(r.account)
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}

	err := r.wx.SendTyping(ctx, chat.ReplyToUserID, typingTicket, status)
	if err == nil || errors.Is(err, weixin.ErrSessionExpired) {
		return err
	}

	ticket, refreshErr := r.wx.GetTypingTicket(ctx, chat.ReplyToUserID, contextToken)
	if refreshErr != nil {
		return err
	}
	r.mu.Lock()
	r.account.TypingTickets[peerID] = ticket
	saveErr := r.store.SaveAccount(r.account)
	r.mu.Unlock()
	if saveErr != nil {
		return saveErr
	}
	return r.wx.SendTyping(ctx, chat.ReplyToUserID, ticket, status)
}

func (r *AccountRunner) markSessionExpired(reason string) error {
	r.mu.Lock()
	r.account.MarkLoginRequired(reason)
	err := r.store.SaveAccount(r.account)
	r.mu.Unlock()
	return err
}

func sleepOrDone(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		duration = time.Second
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
