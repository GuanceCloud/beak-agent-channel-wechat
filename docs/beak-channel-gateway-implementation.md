# Beak Channel Gateway Implementation Guide

## 目标

Beak Channel Gateway 的目标是让 Beak 可以通过统一接入规范连接任意 IM bot，例如微信 bot、飞书机器人、DingTalk bot。

产品侧目标如下：

- 用户在 Beak 客户端完成 channel 接入，可以是扫码登录，也可以是填写 API key、webhook token、secret 等信息。
- Beak 将这些 bot account credential 保存到 Beak 数据库。
- Bot account 启用后由对应 connector 负责接收 IM 平台消息。
- 用户第一次从某个 bot account 连接中的某个群或某个单聊发送消息时，Beak 创建或复用一个 session。
- Session 必须按连接维度隔离：同一个 bot account 连接中的同一个群只对应一个 Beak session，同一个 bot account 连接中的同一个单聊只对应一个 Beak session。
- 如果同一个群里接入多个 bot account，每个 bot account 都要有自己的 Beak session。
- 最终任何 IM bot 都可以通过统一 Channel Gateway 接入 Beak，而不是为每个平台在 Beak host 里写一套专用逻辑。

本指南只描述 Beak host 后续应如何实现 Channel Gateway。本轮不要求修改 Beak host 代码、不新增 DB migration、不实现飞书或 DingTalk 实际 API。

## 两套规范

Channel Gateway 分成两层规范：Connector Plugin SDK 和 Beak Channel Gateway。

### Connector Plugin SDK

Connector Plugin SDK 是平台 connector 要实现的 Go 接口。每个平台只负责自己平台的登录、消息接收、消息发送和平台状态同步。

建议接口形态：

```go
type Connector interface {
	Metadata() ConnectorMetadata
	CredentialSchema(ctx context.Context) CredentialSchema
	StartLogin(ctx context.Context, req LoginStartRequest) (*LoginChallenge, error)
	PollLogin(ctx context.Context, req LoginPollRequest) (*LoginStatus, error)
	Start(ctx context.Context, rt Runtime) error
	Send(ctx context.Context, rt Runtime, req OutboundMessage) (*SendResult, error)
	Stop(ctx context.Context, account ChannelAccount) error
}
```

Connector 输出给 Beak 的入站消息必须被标准化成 `InboundMessage`：

```go
type InboundMessage struct {
	WorkspaceUUID string
	Platform      string
	AccountUUID   string // Beak channel_accounts.uuid, also the session connection dimension
	ChannelUUID   string
	ChatType      string // "group" or "direct"
	ChatID        string
	SenderID      string
	MessageID     string
	Text          string
	DedupeKey     string
	Raw           map[string]any
}
```

Beak 输出给 connector 的出站消息使用 `OutboundMessage`：

```go
type OutboundMessage struct {
	WorkspaceUUID string
	Platform      string
	AccountUUID   string
	ChannelUUID   string
	SessionUUID   string
	MessageUUID   string
	ChatType      string
	ChatID        string
	Text          string
}
```

Connector 不直接创建 Beak session、不直接写 Beak message 表、不创建 task。Connector 只把 IM 平台事件标准化后交给 Gateway Runtime。

### Beak Channel Gateway

Beak Channel Gateway 是 Beak host 内部的运行时层，负责把通用 connector 接入 Beak 现有 channel、session、message 和 stream 能力。

Gateway 的职责：

- 管理 channel、channel account、login challenge、credential 和 connector runtime。
- 接收客户端提交的扫码登录请求或 API key/token credential。
- 加密保存 credential JSON。
- 启动和停止 connector。
- 接收 connector 标准化后的 `InboundMessage`。
- 根据统一 session key 创建或复用 Beak session。
- 将入站 IM 消息写入 Beak message。
- 订阅 Beak agent stream，把 agent 输出转换成 `OutboundMessage` 发回 connector。

Gateway 创建或复用 chat session 时必须带上连接账号维度：

```go
type EnsureChatSessionRequest struct {
	WorkspaceUUID       string
	Platform            string
	AccountUUID         string
	ChatType            string
	ChatID              string
	SenderID            string
	AgentParticipantID  string
	BridgeParticipantID string
	Metadata            map[string]any
}
```

## Beak 侧职责

Beak host 需要成为所有 connector 的统一宿主，而不是让每个 connector 自己保存配置、自己创建 session、自己维护本地状态。

### 凭证采集

客户端根据 connector 的 `CredentialSchema` 渲染接入表单或扫码登录入口：

- 微信 bot：展示二维码，用户扫码确认。
- 飞书机器人：填写 app id、app secret、verification token、encrypt key 等。
- DingTalk bot：填写 access token、secret、robot code 等。

客户端提交后，Beak host 负责把 credential 保存到数据库。Connector 不读取本地配置文件，不要求用户在服务器上写 config。

### 凭证保存

Beak host 保存 credential 时必须区分公开配置和敏感字段。

推荐做法：

- `config_json` 保存非敏感配置，例如 display name、默认启用状态、connector feature flags。
- `credential_encrypted` 保存加密后的敏感 credential JSON。
- `state_json` 保存 connector 运行状态，例如 cursor、dedupe、context token。

默认推荐 AES-256-GCM 加密 credential JSON。生产环境必须通过环境变量或 secret 管理系统提供 32-byte key，例如：

```text
CHANNEL_CREDENTIAL_KEY=<base64-encoded-32-byte-key>
```

如果生产环境缺少 key，Beak host 应拒绝启动。测试环境可以使用固定 test key。

### Connector 生命周期

Beak host 按 channel account 启动 connector：

1. 读取 channel account。
2. 解密 credential。
3. 读取 state。
4. 创建 connector runtime。
5. 调用 connector `Start(ctx, runtime)`。
6. connector 收到 IM 消息后调用 Gateway runtime 的入站处理能力，例如 `EnsureChatSession` 和 `CreateMessage`。
7. Beak host 停止账号时调用 connector `Stop(ctx, account)`。

Connector 进程形态由 Beak host 决定。v1 可以先在 Beak server 进程内运行 goroutine；后续可以迁移为独立 worker 或 gateway service。

## Beak 当前实现审计与改造方案

本节基于当前 Beak host 代码结构给出落地方案。文件路径均为 Beak host 仓库内的相对路径。

### 当前实现现状

当前 Beak host 已具备以下基础能力：

- `internal/model/models.go` 已有 `Session`、`Message`、`Channel`、`SessionParticipant`、`SessionEvent` 模型。
- `internal/handler/session.go` 已有 session create/list/get、participants、message stream 等基础 API。
- `internal/handler/message.go` 已有向 session 写入 message 的 HTTP API。
- `internal/handler/channel.go` 已有 channel create/list API。
- `internal/store/memory.go` 已有内存版 session、message、channel、event 存储。
- `internal/handler/router.go` 已注册基础 `/api/v1/channels`、`/api/v1/sessions`、`/api/v1/sessions/{session_uuid}/messages`、`/api/v1/sessions/{session_uuid}/stream` 路由。

这些能力可以作为 Channel Gateway 的底座，但还不是完整 Gateway。

当前缺口：

- 缺 `channel_accounts` 概念，无法表达一个 channel 下多个 bot account 连接。
- 缺 credential 加密保存层，敏感 token 不能放在 `channels.config`。
- 缺 `channel_login_challenges`，无法承载云端扫码登录过程态。
- 缺 delivery state，无法持久化 connector cursor、dedupe、context token、outbound sent state。
- 缺 connector registry 和 connector runtime manager，Beak host 还不能按 account 启停 connector。
- 缺 Gateway runtime adapter，无法把 connector 的 `EnsureChatSession`、`CreateMessage`、`StreamSession` 映射到 Beak 内部能力。
- 缺原子 `EnsureChatSession`，connector 只能通过 list + create 组合，存在并发下重复创建 session 的风险。
- message 写入缺 sender participant 校验，当前仅校验 session 存在，不足以满足 session 隔离规则。
- stream 当前是“历史事件 + heartbeat”，SDK 可通过 reconnect 工作，但 Gateway 后续最好支持实时 fanout。

### 建议新增模块

建议新增独立 Gateway 包，不把逻辑塞进现有 `ChannelHandler`：

```text
internal/channelgateway/
  service.go
  registry.go
  runtime.go
  credential.go
  state_store.go
  docs.go
```

职责划分：

- `registry.go`：维护 `platform -> sdk.Connector`，例如 `weixin -> beakweixin.NewConnector()`。
- `service.go`：实现 login start/poll、account create/list/start/stop、connector runtime 生命周期。
- `runtime.go`：实现 Connector SDK 的 `sdk.Gateway`，把 connector 调用映射到 Beak session/message/stream。
- `credential.go`：处理 credential JSON 加密/解密。
- `state_store.go`：实现 Connector SDK 的 `sdk.AccountStore`，保存 `channel_accounts.state_json`。

### 建议新增模型

在 `internal/model/models.go` 增加：

```go
type ChannelAccount struct {
	ID                  string                 `json:"id,omitempty"`
	UUID                string                 `json:"account_uuid"`
	WorkspaceUUID       string                 `json:"workspace_uuid"`
	ChannelUUID         string                 `json:"channel_uuid"`
	Platform            string                 `json:"platform"`
	DisplayName         string                 `json:"display_name,omitempty"`
	Config              map[string]interface{} `json:"config,omitempty"`
	CredentialEncrypted string                 `json:"-"`
	State               map[string]interface{} `json:"state,omitempty"`
	Status              string                 `json:"status"`
	LastError           string                 `json:"last_error,omitempty"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`
}

type ChannelLoginChallenge struct {
	ID               string                 `json:"id,omitempty"`
	UUID             string                 `json:"challenge_uuid"`
	WorkspaceUUID    string                 `json:"workspace_uuid"`
	ChannelUUID      string                 `json:"channel_uuid"`
	Platform         string                 `json:"platform"`
	ChallengePayload map[string]interface{} `json:"challenge_payload,omitempty"`
	Status           string                 `json:"status"`
	ExpiresAt        time.Time              `json:"expires_at"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
}
```

v1 可以先把 delivery state 放在 `ChannelAccount.State`，暂不单独建 `ChannelDeliveryState` 模型。后续吞吐量变大后再拆表。

### 建议新增 store 接口

在 handler/service 层不要直接依赖具体内存 store。建议增加 Gateway 所需的 store 接口：

```go
type ChannelAccountStore interface {
	CreateChannelAccount(account *model.ChannelAccount) error
	GetChannelAccount(workspaceUUID, accountUUID string) (*model.ChannelAccount, error)
	GetChannelAccounts(workspaceUUID, channelUUID string) ([]*model.ChannelAccount, error)
	UpdateChannelAccount(account *model.ChannelAccount) error
	UpdateChannelAccountState(workspaceUUID, accountUUID string, state map[string]interface{}) error
}

type ChannelLoginChallengeStore interface {
	CreateChannelLoginChallenge(challenge *model.ChannelLoginChallenge) error
	GetChannelLoginChallenge(workspaceUUID, challengeUUID string) (*model.ChannelLoginChallenge, error)
	UpdateChannelLoginChallenge(challenge *model.ChannelLoginChallenge) error
}

type GatewaySessionStore interface {
	EnsureSessionBySource(req EnsureSessionBySourceRequest) (*model.Session, error)
	CreateMessage(message *model.Message) error
	GetSessionEvents(workspaceUUID, sessionUUID, lastEventUUID string) ([]*model.SessionEvent, error)
}
```

`EnsureSessionBySource` 必须是原子操作。内存实现可在同一把 lock 下完成 lookup/create；数据库实现需要唯一约束配合 upsert。

### 建议新增 Gateway API handler

建议新增：

```text
internal/handler/channel_gateway.go
```

新增路由：

```text
POST /api/v1/channel-connectors/{platform}/login/start
POST /api/v1/channel-connectors/{platform}/login/poll
POST /api/v1/channels/{channel_uuid}/accounts
GET  /api/v1/channels/{channel_uuid}/accounts
POST /api/v1/channel-accounts/{account_uuid}/start
POST /api/v1/channel-accounts/{account_uuid}/stop
```

`internal/handler/router.go` 需要增加 channel gateway 路由解析。不要复用 `/api/v1/channels` 的现有 handler 分支承载全部逻辑，否则 channel 平台级配置和 bot account 运行时职责会混在一起。

### 原子 EnsureChatSession 落地

Gateway runtime 的 `EnsureChatSession` 应映射到内部方法：

```go
func (s *Service) EnsureIMChatSession(ctx context.Context, req sdk.EnsureChatSessionRequest) (string, error)
```

内部构造：

```text
source_type=im_chat
source_id=<platform>:<account_uuid>:<chat_type>:<chat_id>
session_type=manual
platform=<platform>
```

创建 session 时至少包含：

```text
im:<platform>:<chat_type>:<chat_id>:user:<sender_id>
agent:<agent_uuid>
bridge:<platform>
```

如果 session 已存在但缺 sender participant，应调用当前已有的 participant 添加能力补齐，而不是新建 session。

数据库落地时建议增加唯一索引：

```sql
CREATE UNIQUE INDEX uniq_sessions_source
ON sessions (workspace_uuid, platform, source_type, source_id)
WHERE source_type IS NOT NULL AND source_id IS NOT NULL;
```

### Message 写入校验

`internal/handler/message.go` 和 store 层需要补 sender 校验：

- `sender_id` 必须是当前 session 的 active participant。
- `reply_to_uuid` 如果存在，必须属于同 workspace、同 session。
- Gateway 写入 connector inbound message 前会补 participant，但 Beak 侧仍要强校验，避免任意 sender 写入任意 session。

内存 store 可以在 `CreateMessage` 内通过 `sessionParticipants[session_uuid][sender_id]` 校验。数据库实现应使用 transaction 或外键/查询校验。

### Credential 加密

credential 保存规则：

- 客户端提交或扫码确认后，明文 credential 只在请求处理过程和 connector runtime 内短暂存在。
- 入库只保存 `credential_encrypted`。
- `channels.config` 不保存 token、secret、bot_token、app_secret、encrypt_key 等敏感字段。

建议默认 AES-256-GCM：

```text
CHANNEL_CREDENTIAL_KEY=<base64-encoded-32-byte-key>
```

如果生产环境未配置 key，Beak host 应拒绝启动 Gateway service。测试环境可以使用固定 test key。

### Connector runtime 生命周期

运行时 manager 应维护：

```text
account_uuid -> cancel function / runtime state
```

启动流程：

1. 读取 `channel_accounts`。
2. 解密 credential。
3. 组装 `sdk.ChannelAccount`。
4. 构造 `sdk.Runtime`，注入 `sdk.Gateway` 和 `sdk.AccountStore`。
5. 调用 `connector.Start(ctx, runtime)`，由 manager 维护 goroutine 生命周期。

停止流程：

1. 找到 account runtime。
2. cancel context。
3. 调用 `connector.Stop(ctx, account)`。
4. 更新 `channel_accounts.status`。
5. 保存最后错误或停止原因。

### Agent stream 到 outbound

Gateway 需要负责把 agent stream event 转成 `sdk.OutboundMessage`：

- 只处理 `event_type=message`。
- 只处理 sender 是当前 session 绑定 agent participant 的消息。
- 根据 session metadata/source_id 找到 `platform + account_uuid + chat_type + chat_id`。
- 使用 `account_uuid` 找到 channel account 和 connector。
- 调用 `connector.Send(ctx, runtime, outbound)`。
- 保存 outbound sent state，避免重发。

v1 可以沿用现有 stream API 和 connector reconnect 逻辑。后续如果要降低延迟，应在 Beak host 中给 `SessionEvent` 增加实时 fanout。

### 推荐落地顺序

1. 新增 model 和 memory store：`ChannelAccount`、`ChannelLoginChallenge`，并实现 state 保存。
2. 新增 credential 加密 helper，明确生产环境 key 校验。
3. 新增 `internal/channelgateway` service、registry、runtime adapter、account state adapter。
4. 新增 Gateway API handler 和 router。
5. 新增原子 `EnsureSessionBySource`，并让 Gateway runtime 使用它。
6. 补 message sender participant 校验。
7. 注册 Weixin connector，完成扫码登录、account start/stop、入站文本、出站文本闭环。
8. 补数据库 SQL/migration 文档，与当前 Go model 对齐。
9. 后续优化 stream 实时 fanout。

## Session 归一规则

Beak Gateway 必须统一 session 复用规则，不能由各 connector 自己决定。

Session key 固定为：

```text
workspace_uuid + platform + account_uuid + chat_type + chat_id
```

`account_uuid` 是 Beak `channel_accounts.uuid`，表示一次已保存的 bot account 连接。session key 必须包含该连接维度，不能只按群或单聊复用。

原因：同一个 IM 群里可能同时接入多个 bot account。每个 bot account 代表一个独立连接、独立 credential、独立 delivery state、独立 context token，因此必须创建或复用各自的 Beak session。

### chat_type

`chat_type` 只有两个 v1 必须支持的值：

- `group`：群聊。
- `direct`：单聊。

当 `chat_type=group` 时，同一个 `account_uuid + group chat_id` 对应一个 Beak session。

当 `chat_type=direct` 时，同一个 `account_uuid + direct chat_id` 对应一个 Beak session。

### Beak session 字段

推荐创建或查询 session 时使用以下字段：

```text
platform=<platform>
session_type=manual
source_type=im_chat
source_id=<platform>:<account_uuid>:<chat_type>:<chat_id>
```

示例：

```text
platform=weixin
session_type=manual
source_type=im_chat
source_id=weixin:account_a:group:group_123
```

```text
platform=weixin
session_type=manual
source_type=im_chat
source_id=weixin:account_a:direct:user_456
```

同一个群里有多个 bot account 时，必须是多个 session：

```text
source_id=weixin:account_a:group:group_123
source_id=weixin:account_b:group:group_123
```

### participants

Beak 仍使用统一 `participant_id` 编码。

推荐 participant id：

```text
im:<platform>:<chat_type>:<chat_id>:user:<sender_id>
agent:<agent_uuid>
bridge:<platform>
```

示例：

```text
im:weixin:group:group_123:user:user_456
agent:agent-demo
bridge:weixin
```

创建 session 时至少包含：

- 首条消息发送者 participant。
- 当前会话要路由到的 agent participant。
- 当前 platform 的 bridge participant。

如果 session 已存在，但缺少新的 sender participant，Beak host 应补齐 participant，而不是新建 session。

## 数据模型建议

### channels

`channels` 表表示 workspace 下的平台级 channel 配置。现有 Beak `channels` 表可复用。

建议用途：

- `workspace_uuid`：所属 workspace。
- `platform`：例如 `weixin`、`feishu`、`dingtalk`。
- `name`：用户可读名称。
- `config`：非敏感 channel 配置。
- `status`：channel 是否启用。

### channel_accounts

`channel_accounts` 表表示某个 channel 下的一个 bot account。一个 channel 可以绑定多个 bot account。

建议字段：

```text
uuid
workspace_uuid
channel_uuid
platform
display_name
credential_encrypted
state_json
status
last_error
created_at
updated_at
```

`credential_encrypted` 保存加密后的 credential JSON。`state_json` 保存 connector 运行态，例如：

- 微信 `get_updates_buf`
- 微信 peer/chat 的 `context_token`
- 入站消息 dedupe key
- Beak stream cursor
- 已发送 outbound message id

### channel_login_challenges

`channel_login_challenges` 表保存扫码登录过程态。

建议字段：

```text
uuid
workspace_uuid
channel_uuid
platform
challenge_payload
status
expires_at
created_at
updated_at
```

微信扫码登录时，`challenge_payload` 可以保存：

```json
{
  "qrcode": "qr-code-id",
  "qrcode_url": "https://..."
}
```

客户端轮询登录状态时，只需要带 Beak 生成的 `challenge_uuid`，不要让客户端直接持有平台敏感 token。

### channel_delivery_state

`channel_delivery_state` 保存 cursor、dedupe、outbound sent state。v1 可以先合并在 `channel_accounts.state_json`，后续吞吐量变大后再拆独立表。

如果拆表，建议按以下维度存储：

```text
workspace_uuid
account_uuid
platform
scope_type
scope_id
state_key
state_value
updated_at
```

示例：

```text
scope_type=chat
scope_id=weixin:account_a:group:group_123
state_key=context_token
```

```text
scope_type=session
scope_id=<session_uuid>
state_key=last_event_uuid
```

## Gateway API 建议

### Start login

```http
POST /api/v1/channel-connectors/{platform}/login/start
```

用途：开始扫码登录或平台授权流程。

请求体建议：

```json
{
  "workspace_uuid": "workspace-demo",
  "channel_uuid": "channel-demo"
}
```

响应体建议：

```json
{
  "challenge_uuid": "challenge-demo",
  "type": "qr_code",
  "qr_code_url": "https://...",
  "expires_at": "2026-05-22T12:00:00Z"
}
```

### Poll login

```http
POST /api/v1/channel-connectors/{platform}/login/poll
```

用途：轮询扫码或授权状态。确认后 Beak 创建或更新 `channel_accounts`。

请求体建议：

```json
{
  "workspace_uuid": "workspace-demo",
  "challenge_uuid": "challenge-demo"
}
```

响应体建议：

```json
{
  "status": "confirmed",
  "account_uuid": "account-demo"
}
```

状态建议：

- `pending`
- `scanned`
- `confirmed`
- `expired`
- `failed`

### Create channel account

```http
POST /api/v1/channels/{channel_uuid}/accounts
```

用途：保存用户填写的 API key/token 类型 credential，或手动新增 bot account。

请求体建议：

```json
{
  "workspace_uuid": "workspace-demo",
  "display_name": "Ops Feishu Bot",
  "credential": {
    "app_id": "...",
    "app_secret": "..."
  }
}
```

响应体建议：

```json
{
  "account_uuid": "account-demo",
  "status": "inactive"
}
```

### List channel accounts

```http
GET /api/v1/channels/{channel_uuid}/accounts?workspace_uuid=workspace-demo
```

用途：展示某个 channel 下已绑定的 bot accounts。响应不返回明文 credential。

### Start account

```http
POST /api/v1/channel-accounts/{account_uuid}/start
```

用途：启动某个 bot account 的 connector runtime。

### Stop account

```http
POST /api/v1/channel-accounts/{account_uuid}/stop
```

用途：停止某个 bot account 的 connector runtime。

## 消息流

### 入站消息

入站消息流程：

1. 用户扫码或填写 API key/token。
2. Beak 创建 channel account，并加密保存 credential。
3. Beak 启动对应 connector。
4. Connector 收到 IM 平台消息。
5. Connector 将平台消息转换成标准 `InboundMessage`。
6. Gateway 使用 `workspace_uuid + platform + account_uuid + chat_type + chat_id` 查找 Beak session。
7. 如果 session 存在，复用该 session。
8. 如果 session 不存在，创建新 session。
9. Gateway 补齐 sender、agent、bridge participants。
10. Gateway 将消息写入 Beak message。
11. Beak 现有 agent/routing 逻辑继续处理该 session。

### 出站消息

出站消息流程：

1. Gateway 订阅或轮询 Beak session stream。
2. Gateway 只处理 agent participant 发出的 message event。
3. Gateway 根据 session metadata 找到 `platform + account_uuid + chat_type + chat_id`。
4. Gateway 使用该 session 绑定的 channel account。
5. Gateway 构造 `OutboundMessage`。
6. Connector `Send` 将文本发回对应 IM chat。
7. Gateway 记录 outbound sent state，避免重复发送。

### Dedupe

Gateway 和 connector 都需要配合 dedupe。

入站 dedupe key 推荐：

```text
<platform>:<account_uuid>:<chat_type>:<chat_id>:<message_id>
```

如果平台没有稳定 `message_id`，connector 必须从 raw payload 中构造尽可能稳定的 key，并在 metadata 中标明来源。

出站 dedupe key 推荐：

```text
<platform>:<account_uuid>:<session_uuid>:<message_uuid>
```

## 微信参考实现对齐

微信 connector 是 v1 的完整 reference connector。

微信 connector 使用 Tencent iLink Weixin APIs：

- `ilink/bot/get_bot_qrcode`
- `ilink/bot/get_qrcode_status`
- `ilink/bot/getupdates`
- `ilink/bot/sendmessage`
- `ilink/bot/msg/notifystart`
- `ilink/bot/msg/notifystop`

微信 credential 建议包含：

```json
{
  "bot_token": "...",
  "base_url": "https://ilinkai.weixin.qq.com",
  "ilink_user_id": "..."
}
```

微信 state 建议包含：

```json
{
  "get_updates_buf": "...",
  "context_tokens": {
    "user_123": "...",
    "group:group_456": "..."
  },
  "inbound_seen": {},
  "stream_cursors": {},
  "sent_beak_messages": {}
}
```

其中 `context_tokens` 的 key 是微信 connector 内部的 delivery state key。单聊使用 `<user_id>`，群聊使用 `group:<group_id>`；Beak session 的 `source_id` 使用统一的 `<platform>:<account_uuid>:<chat_type>:<chat_id>`。

微信 v1 只做 text-only：

- 支持扫码登录。
- 支持文本入站。
- 支持文本出站。
- 不支持 media、voice、typing status。
- 不要求微信 connector 维护本地配置文件。
- 不把微信 connector 做成 CLI。

微信消息标准化要求：

- 能识别单聊时，输出 `chat_type=direct`。
- 能识别群聊时，输出 `chat_type=group`。
- `chat_id` 必须是平台内稳定 ID。
- `sender_id` 必须是消息真实发送者 ID。
- `raw` 保留平台原始 payload，便于后续补齐字段。

如果某个 iLink payload 暂时无法可靠识别群聊字段，connector 应返回明确 unsupported/error，并保留 raw payload 用于后续协议对齐，而不是错误地把群聊当单聊。

## 不做的事

本阶段不做以下事项：

- 不修改 Beak host 仓库代码。
- 不新增 Beak DB migration。
- 不实现 Beak host 的 Gateway API。
- 不接入飞书或 DingTalk 实际 API。
- 不实现 media、voice、typing status。
- 不把微信 connector 做成 CLI。
- 不让 SDK 维护本地配置文件或本地状态目录。

## 验收标准

后续实现 Beak Channel Gateway 时，至少需要满足以下验收标准：

- Beak 客户端可以发起扫码或提交 API key/token credential。
- Beak host 能加密保存 bot account credential 到数据库。
- Beak host 不向客户端返回明文 credential。
- Connector 启动后能将 IM 消息转换为标准 `InboundMessage`。
- Beak host 按 `workspace_uuid + platform + account_uuid + chat_type + chat_id` 创建或复用 session。
- 同一个 bot account 连接中的同一个群只创建一个 session。
- 同一个 bot account 连接中的同一个单聊只创建一个 session。
- 同 workspace、同平台、同群/单聊但不同 bot account 时，必须创建不同 session。
- Beak host 写入 message 后，agent 可以按现有 session/message/stream 机制处理。
- Beak host 能将 agent stream message 转换为 `OutboundMessage` 并交给 connector 发回 IM。
- Connector 不创建 task。
- SDK 不读取配置文件，不维护本地状态目录。
