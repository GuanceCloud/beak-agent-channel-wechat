package bridge

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"beak-agent-weixin/internal/weixin"
)

const (
	defaultWorkspaceRef          = "beak-runtime"
	defaultBridgeParticipantID   = "bridge:weixin"
	defaultPollInterval          = time.Second
	defaultStreamReconnect       = 30 * time.Second
	defaultWeixinLoginTimeout    = 5 * time.Minute
	defaultWeixinLongPollTimeout = 35 * time.Second
	defaultWeixinRequestTimeout  = 15 * time.Second
)

type Options struct {
	WorkspaceRef        string
	ChannelUUID         string
	AgentParticipantID  string
	BridgeParticipantID string
	PollInterval        time.Duration
	StreamReconnect     time.Duration
	Weixin              WeixinOptions
	HTTPClient          *http.Client
	Accounts            []AccountConfig
}

type AccountConfig struct {
	AccountID string
}

type WeixinOptions struct {
	BaseURL          string
	BotType          int
	RouteTag         string
	ChannelVersion   string
	BotAgent         string
	AppID            string
	AppClientVersion string
	LoginTimeout     time.Duration
	LongPollTimeout  time.Duration
	RequestTimeout   time.Duration
}

func (c *Options) ApplyDefaults() {
	if c.WorkspaceRef == "" {
		c.WorkspaceRef = defaultWorkspaceRef
	}
	if c.BridgeParticipantID == "" {
		c.BridgeParticipantID = defaultBridgeParticipantID
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.StreamReconnect <= 0 {
		c.StreamReconnect = defaultStreamReconnect
	}
	c.Weixin.ApplyDefaults()
}

func (c *Options) ValidateForRun() error {
	c.ApplyDefaults()
	if c.AgentParticipantID == "" {
		return fmt.Errorf("agent participant id is required")
	}
	if len(c.Accounts) == 0 {
		return fmt.Errorf("at least one weixin account is required")
	}
	for i, account := range c.Accounts {
		if account.AccountID == "" {
			return fmt.Errorf("accounts[%d] account id is required", i)
		}
	}
	return nil
}

func (w *WeixinOptions) ApplyDefaults() {
	if w.BaseURL == "" {
		w.BaseURL = weixin.DefaultBaseURL
	}
	if w.BotType == 0 {
		w.BotType = 3
	}
	if w.ChannelVersion == "" {
		w.ChannelVersion = weixin.DefaultClientVersion
	}
	if w.BotAgent == "" {
		w.BotAgent = weixin.DefaultBotAgent
	}
	if w.AppID == "" {
		w.AppID = "bot"
	}
	if w.AppClientVersion == "" {
		w.AppClientVersion = EncodeAppClientVersion(w.ChannelVersion)
	}
	if w.LoginTimeout <= 0 {
		w.LoginTimeout = defaultWeixinLoginTimeout
	}
	if w.LongPollTimeout <= 0 {
		w.LongPollTimeout = defaultWeixinLongPollTimeout
	}
	if w.RequestTimeout <= 0 {
		w.RequestTimeout = defaultWeixinRequestTimeout
	}
}

func (w WeixinOptions) NewClient(baseURL, token string) *weixin.Client {
	w.ApplyDefaults()
	if baseURL == "" {
		baseURL = w.BaseURL
	}
	client := weixin.NewClient(baseURL, token)
	client.BotType = w.BotType
	client.RouteTag = w.RouteTag
	client.ChannelVersion = w.ChannelVersion
	client.BotAgent = w.BotAgent
	client.AppID = w.AppID
	client.AppClientVersion = w.AppClientVersion
	client.RequestTimeout = w.RequestTimeout
	return client
}

func EncodeAppClientVersion(version string) string {
	if version == "" {
		version = weixin.DefaultClientVersion
	}
	parts := strings.Split(version, ".")
	read := func(i int) int {
		if i >= len(parts) {
			return 0
		}
		value, err := strconv.Atoi(parts[i])
		if err != nil {
			return 0
		}
		return value & 0xff
	}
	return strconv.Itoa((read(0) << 16) | (read(1) << 8) | read(2))
}
