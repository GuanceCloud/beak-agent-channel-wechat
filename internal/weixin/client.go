package weixin

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL        = "https://ilinkai.weixin.qq.com"
	DefaultClientVersion  = "0.1.0"
	DefaultBotAgent       = "Beak Agent"
	DefaultAppID          = "bot"
	MaxTextRunes          = 2000
	defaultClientVersion  = DefaultClientVersion
	defaultAppID          = DefaultAppID
	defaultBotAgent       = DefaultBotAgent
	defaultAPITimeout     = 15 * time.Second
	defaultLongPoll       = 35 * time.Second
	sessionExpiredErrCode = -14
)

type Client struct {
	BaseURL          string
	Token            string
	BotType          int
	ChannelVersion   string
	BotAgent         string
	RouteTag         string
	AppID            string
	AppClientVersion string
	RequestTimeout   time.Duration
	HTTPClient       *http.Client
}

func NewClient(baseURL, token string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		BaseURL:          baseURL,
		Token:            token,
		BotType:          3,
		ChannelVersion:   defaultClientVersion,
		BotAgent:         defaultBotAgent,
		AppID:            defaultAppID,
		AppClientVersion: strconv.Itoa(buildClientVersion(defaultClientVersion)),
		RequestTimeout:   defaultAPITimeout,
		HTTPClient:       http.DefaultClient,
	}
}

func (c *Client) GetQRCode(ctx context.Context) (*QRCodeResponse, error) {
	var resp QRCodeResponse
	botType := c.BotType
	if botType == 0 {
		botType = 3
	}
	if err := c.doGET(ctx, "ilink/bot/get_bot_qrcode?bot_type="+strconv.Itoa(botType), &resp); err != nil {
		return nil, err
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return nil, fmt.Errorf("get qrcode failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	if resp.QRCode == "" || resp.QRCodeImgContent == "" {
		return nil, fmt.Errorf("get qrcode failed: missing qrcode fields")
	}
	return &resp, nil
}

func (c *Client) GetQRCodeStatus(ctx context.Context, qrcode string) (*QRCodeStatusResponse, error) {
	values := url.Values{"qrcode": []string{qrcode}}
	var resp QRCodeStatusResponse
	if err := c.doGET(ctx, "ilink/bot/get_qrcode_status?"+values.Encode(), &resp); err != nil {
		return nil, err
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return nil, fmt.Errorf("get qrcode status failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	return &resp, nil
}

func (c *Client) GetUpdates(ctx context.Context, getUpdatesBuf string, timeout time.Duration) (*GetUpdatesResponse, error) {
	if timeout <= 0 {
		timeout = defaultLongPoll
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body := GetUpdatesRequest{
		GetUpdatesBuf: getUpdatesBuf,
		BaseInfo:      c.baseInfo(),
	}
	var resp GetUpdatesResponse
	err := c.doPOSTWithClient(reqCtx, "ilink/bot/getupdates", body, &resp, c.longPollHTTPClient(timeout))
	if err != nil {
		if reqCtx.Err() != nil || isTimeoutError(err) {
			return &GetUpdatesResponse{Ret: 0, Messages: nil, GetUpdatesBuf: getUpdatesBuf}, nil
		}
		return nil, err
	}
	if resp.ErrCode == sessionExpiredErrCode {
		return nil, NewSessionExpiredError("getupdates", resp.ErrCode, resp.ErrMsg)
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return nil, fmt.Errorf("get updates failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	if resp.GetUpdatesBuf == "" {
		resp.GetUpdatesBuf = getUpdatesBuf
	}
	return &resp, nil
}

func (c *Client) SendText(ctx context.Context, toUserID, text, contextToken string) error {
	if strings.TrimSpace(toUserID) == "" {
		return fmt.Errorf("to_user_id is required")
	}
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("text is required")
	}
	if strings.TrimSpace(contextToken) == "" {
		return fmt.Errorf("context_token is required")
	}
	chunks := SplitText(text, MaxTextRunes)
	if len(chunks) == 0 {
		return fmt.Errorf("text is required")
	}
	for _, chunk := range chunks {
		if err := c.sendTextChunk(ctx, toUserID, chunk, contextToken); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) sendTextChunk(ctx context.Context, toUserID, text, contextToken string) error {
	body := SendMessageRequest{
		Message: WeixinMessage{
			ToUserID:     toUserID,
			ClientID:     newClientID(),
			MessageType:  MessageTypeBot,
			MessageState: MessageStateFinish,
			ContextToken: contextToken,
			ItemList: []MessageItem{
				{
					Type:     MessageItemTypeText,
					TextItem: &TextItem{Text: text},
				},
			},
		},
		BaseInfo: c.baseInfo(),
	}
	var resp SendMessageResponse
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultAPITimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.doPOST(reqCtx, "ilink/bot/sendmessage", body, &resp); err != nil {
		return err
	}
	if resp.ErrCode == sessionExpiredErrCode {
		return NewSessionExpiredError("sendmessage", resp.ErrCode, resp.ErrMsg)
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return fmt.Errorf("send message failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	return nil
}

func (c *Client) GetTypingTicket(ctx context.Context, ilinkUserID, contextToken string) (string, error) {
	if strings.TrimSpace(ilinkUserID) == "" {
		return "", fmt.Errorf("ilink_user_id is required")
	}
	if strings.TrimSpace(contextToken) == "" {
		return "", fmt.Errorf("context_token is required")
	}
	body := GetConfigRequest{
		ILinkUserID:  ilinkUserID,
		ContextToken: contextToken,
		BaseInfo:     c.baseInfo(),
	}
	var resp GetConfigResponse
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultAPITimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.doPOST(reqCtx, "ilink/bot/getconfig", body, &resp); err != nil {
		return "", err
	}
	if resp.ErrCode == sessionExpiredErrCode {
		return "", NewSessionExpiredError("getconfig", resp.ErrCode, resp.ErrMsg)
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return "", fmt.Errorf("get config failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	if strings.TrimSpace(resp.TypingTicket) == "" {
		return "", fmt.Errorf("get config failed: missing typing_ticket")
	}
	return resp.TypingTicket, nil
}

func (c *Client) SendTyping(ctx context.Context, ilinkUserID, typingTicket string, status int) error {
	if strings.TrimSpace(ilinkUserID) == "" {
		return fmt.Errorf("ilink_user_id is required")
	}
	if strings.TrimSpace(typingTicket) == "" {
		return fmt.Errorf("typing_ticket is required")
	}
	if status != TypingStatusStart && status != TypingStatusStop {
		return fmt.Errorf("unsupported typing status: %d", status)
	}
	body := SendTypingRequest{
		ILinkUserID:  ilinkUserID,
		TypingTicket: typingTicket,
		Status:       status,
		BaseInfo:     c.baseInfo(),
	}
	var resp SendTypingResponse
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultAPITimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.doPOST(reqCtx, "ilink/bot/sendtyping", body, &resp); err != nil {
		return err
	}
	if resp.ErrCode == sessionExpiredErrCode {
		return NewSessionExpiredError("sendtyping", resp.ErrCode, resp.ErrMsg)
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return fmt.Errorf("send typing failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	return nil
}

func (c *Client) NotifyStart(ctx context.Context) error {
	return c.notify(ctx, "ilink/bot/msg/notifystart")
}

func (c *Client) NotifyStop(ctx context.Context) error {
	return c.notify(ctx, "ilink/bot/msg/notifystop")
}

func (c *Client) notify(ctx context.Context, endpoint string) error {
	var resp NotifyResponse
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultAPITimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.doPOST(reqCtx, endpoint, map[string]any{"base_info": c.baseInfo()}, &resp); err != nil {
		return err
	}
	if resp.ErrCode == sessionExpiredErrCode {
		return NewSessionExpiredError(endpoint, resp.ErrCode, resp.ErrMsg)
	}
	if resp.Ret != 0 || resp.ErrCode != 0 {
		return fmt.Errorf("%s failed: ret=%d errcode=%d errmsg=%s", endpoint, resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	return nil
}

func (c *Client) doGET(ctx context.Context, endpoint string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint(endpoint), nil)
	if err != nil {
		return err
	}
	for key, value := range c.commonHeaders(false) {
		req.Header.Set(key, value)
	}
	return c.do(req, out)
}

func (c *Client) doPOST(ctx context.Context, endpoint string, body any, out any) error {
	return c.doPOSTWithClient(ctx, endpoint, body, out, c.HTTPClient)
}

func (c *Client) doPOSTWithClient(ctx context.Context, endpoint string, body any, out any, client *http.Client) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(endpoint), bytes.NewReader(data))
	if err != nil {
		return err
	}
	for key, value := range c.commonHeaders(true) {
		req.Header.Set(key, value)
	}
	return c.doWithClient(req, out, client)
}

func (c *Client) do(req *http.Request, out any) error {
	return c.doWithClient(req, out, c.HTTPClient)
}

func (c *Client) doWithClient(req *http.Request, out any, client *http.Client) error {
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
		return fmt.Errorf("%s %s failed: status=%d body=%s", req.Method, req.URL.String(), resp.StatusCode, string(data))
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) longPollHTTPClient(timeout time.Duration) *http.Client {
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	if timeout <= 0 {
		timeout = defaultLongPoll
	}
	minClientTimeout := timeout + time.Second
	if client.Timeout > 0 && client.Timeout <= minClientTimeout {
		clone := *client
		clone.Timeout = minClientTimeout
		return &clone
	}
	return client
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (c *Client) endpoint(endpoint string) string {
	base := strings.TrimRight(c.BaseURL, "/") + "/"
	return base + strings.TrimLeft(endpoint, "/")
}

func (c *Client) commonHeaders(auth bool) map[string]string {
	headers := map[string]string{
		"iLink-App-Id":            valueOrDefault(c.AppID, defaultAppID),
		"iLink-App-ClientVersion": valueOrDefault(c.AppClientVersion, strconv.Itoa(buildClientVersion(defaultClientVersion))),
	}
	if c.RouteTag != "" {
		headers["SKRouteTag"] = c.RouteTag
	}
	if auth {
		headers["Content-Type"] = "application/json"
		headers["AuthorizationType"] = "ilink_bot_token"
		headers["X-WECHAT-UIN"] = randomWechatUIN()
		if strings.TrimSpace(c.Token) != "" {
			headers["Authorization"] = "Bearer " + strings.TrimSpace(c.Token)
		}
	}
	return headers
}

func (c *Client) baseInfo() BaseInfo {
	return BaseInfo{
		ChannelVersion: valueOrDefault(c.ChannelVersion, defaultClientVersion),
		BotAgent:       valueOrDefault(c.BotAgent, defaultBotAgent),
	}
}

func buildClientVersion(version string) int {
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
	return (read(0) << 16) | (read(1) << 8) | read(2)
}

func randomWechatUIN() string {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
	}
	value := binary.BigEndian.Uint32(buf[:])
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(uint64(value), 10)))
}

func newClientID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "beak-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "beak-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + base64.RawURLEncoding.EncodeToString(buf[:])
}

func SplitText(text string, maxRunes int) []string {
	if maxRunes <= 0 {
		maxRunes = MaxTextRunes
	}
	remaining := strings.TrimSpace(text)
	var chunks []string
	for runeCount(remaining) > maxRunes {
		prefix := firstRunes(remaining, maxRunes)
		cut := bestTextCut(prefix)
		if cut <= 0 {
			cut = len(prefix)
		}
		chunk := strings.TrimSpace(remaining[:cut])
		if chunk != "" {
			chunks = append(chunks, chunk)
		}
		remaining = strings.TrimSpace(remaining[cut:])
	}
	if remaining != "" {
		chunks = append(chunks, remaining)
	}
	return chunks
}

func bestTextCut(prefix string) int {
	if idx := strings.LastIndex(prefix, "\n\n"); idx > 0 {
		return idx + len("\n\n")
	}
	if idx := strings.LastIndex(prefix, "\n"); idx > 0 {
		return idx + len("\n")
	}
	if idx := strings.LastIndex(prefix, " "); idx > 0 {
		return idx + len(" ")
	}
	return -1
}

func firstRunes(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	count := 0
	for idx := range text {
		if count == maxRunes {
			return text[:idx]
		}
		count++
	}
	return text
}

func runeCount(text string) int {
	count := 0
	for range text {
		count++
	}
	return count
}

func itoa64(value int64) string {
	return strconv.FormatInt(value, 10)
}

func valueOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
