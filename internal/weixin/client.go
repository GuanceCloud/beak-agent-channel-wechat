package weixin

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
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
	defaultCDNBaseURL     = "https://novac2c.cdn.weixin.qq.com/c2c"
	maxMediaBytes         = 64 << 20
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
	CDNBaseURL       string
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
		CDNBaseURL:       defaultCDNBaseURL,
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
	return c.SendTextWithClientID(ctx, toUserID, text, contextToken, "")
}

// SendTextWithClientID sends text with a caller-supplied idempotency key. A
// non-empty clientID is only valid for a single platform-sized chunk.
func (c *Client) SendTextWithClientID(ctx context.Context, toUserID, text, contextToken, clientID string) error {
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
	if strings.TrimSpace(clientID) != "" && len(chunks) != 1 {
		return fmt.Errorf("client_id requires a single text chunk")
	}
	for _, chunk := range chunks {
		if err := c.sendTextChunk(ctx, toUserID, chunk, contextToken, clientID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) SendMedia(ctx context.Context, req SendMediaRequest) error {
	if strings.TrimSpace(req.ToUserID) == "" {
		return fmt.Errorf("to_user_id is required")
	}
	if strings.TrimSpace(req.ContextToken) == "" {
		return fmt.Errorf("context_token is required")
	}
	path, cleanup, err := c.resolveMediaPath(ctx, req.Path, req.URL)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	uploaded, err := c.uploadMedia(ctx, req.ToUserID, req.Kind, path)
	if err != nil {
		return err
	}
	item := uploaded.messageItem(req.Kind, req.FileName)
	if strings.TrimSpace(req.ClientID) == "" {
		req.ClientID = newClientID()
	}
	body := SendMessageRequest{Message: WeixinMessage{ToUserID: req.ToUserID, ClientID: req.ClientID, MessageType: MessageTypeBot, MessageState: MessageStateFinish, ContextToken: req.ContextToken, ItemList: []MessageItem{item}}, BaseInfo: c.baseInfo()}
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
		return fmt.Errorf("send media failed: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.ErrCode, resp.ErrMsg)
	}
	return nil
}

// DownloadMedia downloads and, when needed, decrypts an iLink CDN resource to
// a temporary file. The caller owns the returned cleanup function and must
// keep the file alive until the host has imported it.
func (c *Client) DownloadMedia(ctx context.Context, media CDNMedia) (string, func(), error) {
	endpoint := strings.TrimSpace(media.FullURL)
	if endpoint == "" && strings.TrimSpace(media.EncryptQueryParam) != "" {
		base := strings.TrimRight(c.CDNBaseURL, "/")
		if base == "" {
			base = defaultCDNBaseURL
		}
		values := url.Values{}
		values.Set("encrypted_query_param", media.EncryptQueryParam)
		endpoint = base + "/download?" + values.Encode()
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", nil, fmt.Errorf("weixin media URL must be absolute HTTP or HTTPS")
	}
	return c.downloadMediaURL(ctx, parsed.String(), &media)
}

type uploadedMedia struct {
	FileKey                string
	AESKey                 []byte
	DownloadEncryptedParam string
	PlaintextSize          int64
	CiphertextSize         int64
}

func (m uploadedMedia) messageItem(kind, fileName string) MessageItem {
	media := &CDNMedia{
		EncryptQueryParam: m.DownloadEncryptedParam,
		// iLink clients decode this as base64(hex(aes_key)). Keeping the
		// encoding here avoids leaking platform-specific details to Beak.
		AESKey:      base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(m.AESKey))),
		EncryptType: 1,
	}
	switch strings.TrimSpace(kind) {
	case "image", "sticker":
		return MessageItem{Type: MessageItemTypeImage, ImageItem: &ImageItem{Media: media, MidSize: m.CiphertextSize}}
	case "audio":
		return MessageItem{Type: MessageItemTypeVoice, VoiceItem: &VoiceItem{Media: media}}
	case "video":
		return MessageItem{Type: MessageItemTypeVideo, VideoItem: &VideoItem{Media: media, VideoSize: m.CiphertextSize}}
	default:
		return MessageItem{Type: MessageItemTypeFile, FileItem: &FileItem{Media: media, FileName: fileName, Len: strconv.FormatInt(m.PlaintextSize, 10)}}
	}
}

type getUploadURLRequest struct {
	FileKey     string   `json:"filekey"`
	MediaType   int      `json:"media_type"`
	ToUserID    string   `json:"to_user_id"`
	RawSize     int64    `json:"rawsize"`
	RawFileMD5  string   `json:"rawfilemd5"`
	FileSize    int64    `json:"filesize"`
	NoNeedThumb bool     `json:"no_need_thumb"`
	AESKey      string   `json:"aeskey"`
	BaseInfo    BaseInfo `json:"base_info"`
}

type getUploadURLResponse struct {
	Ret           int    `json:"ret,omitempty"`
	ErrCode       int    `json:"errcode,omitempty"`
	ErrMsg        string `json:"errmsg,omitempty"`
	UploadParam   string `json:"upload_param,omitempty"`
	UploadFullURL string `json:"upload_full_url,omitempty"`
}

func (c *Client) uploadMedia(ctx context.Context, toUserID, kind, path string) (uploadedMedia, error) {
	file, err := os.Open(path)
	if err != nil {
		return uploadedMedia{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return uploadedMedia{}, err
	}
	if !info.Mode().IsRegular() {
		return uploadedMedia{}, fmt.Errorf("media path is not a regular file")
	}
	if info.Size() > maxMediaBytes {
		return uploadedMedia{}, fmt.Errorf("weixin media exceeds %d bytes", maxMediaBytes)
	}
	plaintext, err := io.ReadAll(io.LimitReader(file, maxMediaBytes+1))
	if err != nil {
		return uploadedMedia{}, err
	}
	if int64(len(plaintext)) > maxMediaBytes {
		return uploadedMedia{}, fmt.Errorf("weixin media exceeds %d bytes", maxMediaBytes)
	}
	key := make([]byte, aes.BlockSize)
	if _, err := rand.Read(key); err != nil {
		return uploadedMedia{}, err
	}
	ciphertext, err := encryptAESECB(plaintext, key)
	if err != nil {
		return uploadedMedia{}, err
	}
	fileKeyBytes := make([]byte, 16)
	if _, err := rand.Read(fileKeyBytes); err != nil {
		return uploadedMedia{}, err
	}
	fileKey := hex.EncodeToString(fileKeyBytes)
	mediaType := uploadMediaType(kind)
	request := getUploadURLRequest{
		FileKey:     fileKey,
		MediaType:   mediaType,
		ToUserID:    toUserID,
		RawSize:     int64(len(plaintext)),
		RawFileMD5:  fmt.Sprintf("%x", md5.Sum(plaintext)),
		FileSize:    int64(len(ciphertext)),
		NoNeedThumb: true,
		AESKey:      hex.EncodeToString(key),
		BaseInfo:    c.baseInfo(),
	}
	var response getUploadURLResponse
	if err := c.doPOST(ctx, "ilink/bot/getuploadurl", request, &response); err != nil {
		return uploadedMedia{}, err
	}
	if response.Ret != 0 || response.ErrCode != 0 {
		return uploadedMedia{}, fmt.Errorf("get upload URL failed: ret=%d errcode=%d errmsg=%s", response.Ret, response.ErrCode, response.ErrMsg)
	}
	if response.UploadParam == "" && response.UploadFullURL == "" {
		return uploadedMedia{}, fmt.Errorf("get upload URL returned no upload parameter")
	}
	downloadParam, err := c.uploadCDN(ctx, fileKey, response, ciphertext)
	if err != nil {
		return uploadedMedia{}, err
	}
	return uploadedMedia{FileKey: fileKey, AESKey: key, DownloadEncryptedParam: downloadParam, PlaintextSize: int64(len(plaintext)), CiphertextSize: int64(len(ciphertext))}, nil
}

func uploadMediaType(kind string) int {
	switch strings.TrimSpace(kind) {
	case "image", "sticker":
		return 1
	case "video":
		return 2
	case "audio":
		return 4
	default:
		return 3
	}
}

func (c *Client) uploadCDN(ctx context.Context, fileKey string, response getUploadURLResponse, ciphertext []byte) (string, error) {
	endpoint := strings.TrimSpace(response.UploadFullURL)
	if endpoint == "" {
		base := strings.TrimRight(c.CDNBaseURL, "/")
		if base == "" {
			base = defaultCDNBaseURL
		}
		values := url.Values{}
		values.Set("encrypted_query_param", response.UploadParam)
		values.Set("filekey", fileKey)
		endpoint = base + "/upload?" + values.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(ciphertext))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("weixin CDN upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	param := strings.TrimSpace(resp.Header.Get("x-encrypted-param"))
	if param == "" {
		return "", fmt.Errorf("weixin CDN upload returned no encrypted parameter")
	}
	return param, nil
}

func encryptAESECB(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := make([]byte, len(plaintext)+padding)
	copy(padded, plaintext)
	for index := len(plaintext); index < len(padded); index++ {
		padded[index] = byte(padding)
	}
	ciphertext := make([]byte, len(padded))
	for offset := 0; offset < len(padded); offset += aes.BlockSize {
		block.Encrypt(ciphertext[offset:offset+aes.BlockSize], padded[offset:offset+aes.BlockSize])
	}
	return ciphertext, nil
}

func decryptAESECB(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("invalid AES-ECB ciphertext size")
	}
	plaintext := make([]byte, len(ciphertext))
	for offset := 0; offset < len(ciphertext); offset += aes.BlockSize {
		block.Decrypt(plaintext[offset:offset+aes.BlockSize], ciphertext[offset:offset+aes.BlockSize])
	}
	padding := int(plaintext[len(plaintext)-1])
	if padding == 0 || padding > aes.BlockSize || padding > len(plaintext) {
		return nil, fmt.Errorf("invalid AES-ECB padding")
	}
	for _, value := range plaintext[len(plaintext)-padding:] {
		if int(value) != padding {
			return nil, fmt.Errorf("invalid AES-ECB padding")
		}
	}
	return plaintext[:len(plaintext)-padding], nil
}

func (c *Client) resolveMediaPath(ctx context.Context, path, rawURL string) (string, func(), error) {
	if strings.TrimSpace(path) != "" {
		info, err := os.Stat(path)
		if err != nil {
			return "", nil, err
		}
		if !info.Mode().IsRegular() {
			return "", nil, fmt.Errorf("media path is not a regular file")
		}
		return path, nil, nil
	}
	if strings.TrimSpace(rawURL) == "" {
		return "", nil, fmt.Errorf("media path is required")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", nil, fmt.Errorf("media URL must be absolute HTTP or HTTPS")
	}
	return c.downloadMediaURL(ctx, parsed.String(), nil)
}

func (c *Client) downloadMediaURL(ctx context.Context, rawURL string, media *CDNMedia) (string, func(), error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", nil, err
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil, fmt.Errorf("weixin media download failed: status=%d", resp.StatusCode)
	}
	ciphertext, err := io.ReadAll(io.LimitReader(resp.Body, maxMediaBytes+aes.BlockSize+1))
	if err != nil {
		return "", nil, err
	}
	data := ciphertext
	if media != nil && strings.TrimSpace(media.AESKey) != "" {
		key, err := decodeAESKey(media.AESKey)
		if err != nil {
			return "", nil, err
		}
		data, err = decryptAESECB(ciphertext, key)
		if err != nil {
			return "", nil, err
		}
	}
	if int64(len(data)) > maxMediaBytes {
		return "", nil, fmt.Errorf("weixin media exceeds %d bytes", maxMediaBytes)
	}
	tmp, err := os.CreateTemp("", "beak-weixin-media-*")
	if err != nil {
		return "", nil, err
	}
	path := tmp.Name()
	cleanup := func() { _ = os.Remove(path) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", nil, err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func decodeAESKey(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err == nil {
		if len(decoded) == aes.BlockSize {
			return decoded, nil
		}
		if len(decoded) == aes.BlockSize*2 {
			if key, hexErr := hex.DecodeString(string(decoded)); hexErr == nil && len(key) == aes.BlockSize {
				return key, nil
			}
		}
	}
	if key, hexErr := hex.DecodeString(strings.TrimSpace(value)); hexErr == nil && len(key) == aes.BlockSize {
		return key, nil
	}
	return nil, fmt.Errorf("invalid Weixin AES key")
}

func (c *Client) sendTextChunk(ctx context.Context, toUserID, text, contextToken, clientID string) error {
	if strings.TrimSpace(clientID) == "" {
		clientID = newClientID()
	}
	body := SendMessageRequest{
		Message: WeixinMessage{
			ToUserID:     toUserID,
			ClientID:     clientID,
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
