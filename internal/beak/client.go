package beak

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	PlatformWeixin              = "weixin"
	SourceTypeWeixinChannelLink = "weixin_channel_link"
	SourceTypeWeixinPeer        = "weixin_peer"
	SourceTypeIMChat            = "im_chat"
)

type Client struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

func NewClient(baseURL, token string) *Client {
	return &Client{
		BaseURL:    baseURL,
		Token:      token,
		HTTPClient: http.DefaultClient,
	}
}

func (c *Client) CheckHealth(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url("/health", nil), nil)
	if err != nil {
		return err
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beak health failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) EnsureWeixinChannel(ctx context.Context, workspaceUUID string) (string, error) {
	channels, err := c.ListChannels(ctx, workspaceUUID)
	if err != nil {
		return "", err
	}
	for _, channel := range channels {
		if channel.Platform == PlatformWeixin && channel.Status != "disabled" {
			return channel.ChannelUUID, nil
		}
	}
	resp, err := c.CreateChannel(ctx, CreateChannelRequest{
		WorkspaceUUID: workspaceUUID,
		Platform:      PlatformWeixin,
		Name:          "Weixin",
		Config: map[string]any{
			"bridge": "beak-agent-weixin",
		},
	})
	if err != nil {
		return "", err
	}
	return resp.ChannelUUID, nil
}

func (c *Client) EnsureChannelLinkSession(ctx context.Context, workspaceUUID, accountID, agentParticipantID, bridgeParticipantID string) (string, error) {
	sourceID := accountID
	sessions, err := c.ListSessions(ctx, workspaceUUID, map[string]string{
		"platform":    PlatformWeixin,
		"source_type": SourceTypeWeixinChannelLink,
		"source_id":   sourceID,
	})
	if err != nil {
		return "", err
	}
	participants := uniqueNonEmpty([]string{
		agentParticipantID,
		bridgeParticipantID,
	})
	if len(sessions) > 0 {
		session := sessions[0]
		missing := missingParticipants(session.Participants, participants)
		if len(missing) > 0 {
			if _, err := c.AddParticipants(ctx, workspaceUUID, session.SessionUUID, missing, "weixin channel link participant sync"); err != nil {
				return "", err
			}
		}
		return session.SessionUUID, nil
	}
	resp, err := c.CreateSession(ctx, CreateSessionRequest{
		WorkspaceUUID:  workspaceUUID,
		Name:           "Weixin Channel Link " + accountID,
		Platform:       PlatformWeixin,
		SessionType:    "channel_link",
		SourceType:     SourceTypeWeixinChannelLink,
		SourceID:       sourceID,
		ParticipantIDs: participants,
		Metadata: map[string]any{
			"weixin_account_id": accountID,
			"bridge":            "beak-agent-weixin",
		},
	})
	if err != nil {
		return "", err
	}
	return resp.SessionUUID, nil
}

func (c *Client) ListChannels(ctx context.Context, workspaceUUID string) ([]Channel, error) {
	var resp ListChannelsResponse
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/channels", map[string]string{"workspace_uuid": workspaceUUID}, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Channels, nil
}

func (c *Client) CreateChannel(ctx context.Context, req CreateChannelRequest) (*CreateChannelResponse, error) {
	var resp CreateChannelResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/channels", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) EnsurePeerSession(ctx context.Context, workspaceUUID, accountID, peerUserID, agentParticipantID, bridgeParticipantID string) (string, error) {
	sourceID := accountID + ":" + peerUserID
	sessions, err := c.ListSessions(ctx, workspaceUUID, map[string]string{
		"platform":    PlatformWeixin,
		"source_type": SourceTypeWeixinPeer,
		"source_id":   sourceID,
	})
	if err != nil {
		return "", err
	}
	participants := uniqueNonEmpty([]string{
		UserParticipantID(peerUserID),
		agentParticipantID,
		bridgeParticipantID,
	})
	if len(sessions) > 0 {
		session := sessions[0]
		missing := missingParticipants(session.Participants, participants)
		if len(missing) > 0 {
			if _, err := c.AddParticipants(ctx, workspaceUUID, session.SessionUUID, missing, "weixin bridge session participant sync"); err != nil {
				return "", err
			}
		}
		return session.SessionUUID, nil
	}
	resp, err := c.CreateSession(ctx, CreateSessionRequest{
		WorkspaceUUID:  workspaceUUID,
		Name:           "Weixin " + peerUserID,
		Platform:       PlatformWeixin,
		SessionType:    "manual",
		SourceType:     SourceTypeWeixinPeer,
		SourceID:       sourceID,
		ParticipantIDs: participants,
		Metadata: map[string]any{
			"weixin_account_id": accountID,
			"weixin_peer_id":    peerUserID,
			"bridge":            "beak-agent-weixin",
		},
	})
	if err != nil {
		return "", err
	}
	return resp.SessionUUID, nil
}

func (c *Client) EnsureChatSession(ctx context.Context, workspaceUUID, accountID, chatType, chatID, senderID, agentParticipantID, bridgeParticipantID string) (string, error) {
	accountID = strings.TrimSpace(accountID)
	chatType = strings.TrimSpace(chatType)
	chatID = strings.TrimSpace(chatID)
	if accountID == "" {
		return "", fmt.Errorf("account_id is required for weixin chat session")
	}
	if chatType == "" {
		return "", fmt.Errorf("chat_type is required for weixin chat session")
	}
	if chatID == "" {
		return "", fmt.Errorf("chat_id is required for weixin chat session")
	}
	sourceID := ChatSourceID(PlatformWeixin, accountID, chatType, chatID)
	sessions, err := c.ListSessions(ctx, workspaceUUID, map[string]string{
		"platform":    PlatformWeixin,
		"source_type": SourceTypeIMChat,
		"source_id":   sourceID,
	})
	if err != nil {
		return "", err
	}
	participants := uniqueNonEmpty([]string{
		IMUserParticipantID(PlatformWeixin, chatType, chatID, senderID),
		agentParticipantID,
		bridgeParticipantID,
	})
	if len(sessions) > 0 {
		session := sessions[0]
		missing := missingParticipants(session.Participants, participants)
		if len(missing) > 0 {
			if _, err := c.AddParticipants(ctx, workspaceUUID, session.SessionUUID, missing, "weixin im chat participant sync"); err != nil {
				return "", err
			}
		}
		return session.SessionUUID, nil
	}
	resp, err := c.CreateSession(ctx, CreateSessionRequest{
		WorkspaceUUID:  workspaceUUID,
		Name:           "Weixin " + chatType + " " + chatID,
		Platform:       PlatformWeixin,
		SessionType:    "manual",
		SourceType:     SourceTypeIMChat,
		SourceID:       sourceID,
		ParticipantIDs: participants,
		Metadata: map[string]any{
			"account_uuid":      accountID,
			"weixin_account_id": accountID,
			"weixin_chat_type":  chatType,
			"weixin_chat_id":    chatID,
			"weixin_source_id":  sourceID,
			"bridge":            "beak-agent-weixin",
		},
	})
	if err != nil {
		return "", err
	}
	return resp.SessionUUID, nil
}

func (c *Client) ListSessions(ctx context.Context, workspaceUUID string, filters map[string]string) ([]Session, error) {
	query := map[string]string{"workspace_uuid": workspaceUUID}
	for key, value := range filters {
		if value != "" {
			query[key] = value
		}
	}
	var resp ListSessionsResponse
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/sessions", query, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Sessions, nil
}

func (c *Client) CreateSession(ctx context.Context, req CreateSessionRequest) (*CreateSessionResponse, error) {
	var resp CreateSessionResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/sessions", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) AddParticipants(ctx context.Context, workspaceUUID, sessionUUID string, participantIDs []string, reason string) (*AddParticipantsResponse, error) {
	var resp AddParticipantsResponse
	req := AddParticipantsRequest{WorkspaceUUID: workspaceUUID, ParticipantIDs: participantIDs, Reason: reason}
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/sessions/"+url.PathEscape(sessionUUID)+"/participants", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) CreateMessage(ctx context.Context, sessionUUID string, req CreateMessageRequest) (*CreateMessageResponse, error) {
	var resp CreateMessageResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/sessions/"+url.PathEscape(sessionUUID)+"/messages", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) StreamEvents(ctx context.Context, sessionUUID string, req StreamRequest, handle func(StreamEvent) error) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("encode stream request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url("/api/v1/sessions/"+url.PathEscape(sessionUUID)+"/stream", nil), bytes.NewReader(data))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/x-ndjson")
	c.applyAuth(httpReq)

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("stream events failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		event, err := DecodeStreamLine(line)
		if err != nil {
			return err
		}
		if handle != nil {
			if err := handle(*event); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, query map[string]string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url(path, query), reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	c.applyAuth(req)

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, path, resp.StatusCode, string(data))
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) url(path string, query map[string]string) string {
	base := strings.TrimRight(c.BaseURL, "/")
	values := url.Values{}
	for key, value := range query {
		if value != "" {
			values.Set(key, value)
		}
	}
	out := base + "/" + strings.TrimLeft(path, "/")
	if encoded := values.Encode(); encoded != "" {
		out += "?" + encoded
	}
	return out
}

func (c *Client) applyAuth(req *http.Request) {
	if c.Token == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
}

func UserParticipantID(peerUserID string) string {
	return "user:weixin:" + peerUserID
}

func ChatSourceID(platform, accountID, chatType, chatID string) string {
	return strings.TrimSpace(platform) + ":" + strings.TrimSpace(accountID) + ":" + strings.TrimSpace(chatType) + ":" + strings.TrimSpace(chatID)
}

func IMUserParticipantID(platform, chatType, chatID, senderID string) string {
	return "im:" + strings.TrimSpace(platform) + ":" + strings.TrimSpace(chatType) + ":" + strings.TrimSpace(chatID) + ":user:" + strings.TrimSpace(senderID)
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func missingParticipants(current []SessionParticipant, expected []string) []string {
	have := make(map[string]bool)
	for _, participant := range current {
		if participant.Status == "" || participant.Status == "active" {
			have[participant.ParticipantID] = true
		}
	}
	var missing []string
	for _, id := range expected {
		if !have[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

func ShortTimeout() time.Duration {
	return 10 * time.Second
}
