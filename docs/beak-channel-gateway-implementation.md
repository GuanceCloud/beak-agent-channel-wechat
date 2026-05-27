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

本指南描述 Beak host 应如何落地 Channel Gateway，并作为当前 Beak 改造和后续 connector 接入的工程对齐文档。

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

这些能力可以作为 Channel Gateway 的底座。

当前 Beak 改造分支已落地的最小 Gateway 能力：

- 已新增 `internal/channelgateway` 包，包含 connector registry、credential codec、login/account/service、inbound session/message adapter。
- 已新增 `ChannelAccount` 和 `ChannelLoginChallenge` 模型。
- 已新增 memory/http store 对 `channel_accounts`、`channel_login_challenges` 的读写能力，并在 SQLite/MySQL 初始化 schema 中加入对应表。
- 已支持 `GET /api/v1/channel-connectors`、`GET /api/v1/channel-connectors/{platform}`、扫码 start/poll、account create/list/start/stop API。
- 已支持 `POST /api/v1/channel-webhooks/{platform}/{account_uuid}`，用于把 webhook 型平台的原始请求分发给对应 connector SDK。
- 已将 account credential 落库改成 `credential_encrypted`，通过 AES-256-GCM codec 加密保存。
- 已将 Gateway session 归一为 `session_type=conversation + source_id=<platform>:<account_uuid>:<chat_type>:<chat_id>`，不新增 IM 专用 `source_type`。
- 已将普通任务型 session 默认改为 `session_type=task`。
- 已将 connector inbound message 写入后接入 Beak realtime bus：Gateway 会发布 `session_event` 和 `session_command`，在线 agent 可以沿现有会话命令链路收到消息。
- 已实现 account state 级 inbound dedupe：`HandleInboundMessage` 会按 `platform + account_uuid + chat_type + chat_id + message_id` 写入 `channel_accounts.state.inbound_dedupe`，重复消息直接返回原始 session/message，不重复触发 agent。
- 已实现 Gateway outbound 分发：运行中的 account 会订阅对应 conversation session 的 realtime event，`chat:agent` 消息会按 session 绑定的 agent participant 校验后转换成 `OutboundMessage`，调用 connector `Send`，并把发送结果写入 `channel_accounts.state.outbound_sent` 避免重复投递。
- 已实现 store-level `EnsureSessionBySourceScoped`：MemoryStore 在同一把 lock 下查找/创建，HTTPStore 在进程内串行化查找/创建；Gateway `EnsureChatSession` 会优先使用该能力，避免并发首消息在同一 Beak 实例内创建重复 session。
- 已给初始化 schema 补 `idx_sessions_gateway_source(site_code, workspace_uuid, session_type, platform, source_id, status)` 查询索引。
- 已将 `channel_accounts.config` 作为非敏感账号运行配置保存；`CreateAccount` 和扫码 `login/start` 传入的 `agent_uuid`/`agent_participant_id` 会写入 account config，Gateway 创建 conversation session 时会自动补 `agent:<agent_uuid>` participant，并用该 agent 过滤 outbound。
- 已实现 account runtime manager：account start 会先持久化 running 状态，再用可取消 context 在后台启动 connector `Start`，避免微信 long-poll 阻塞 HTTP 请求；重复 start 不会创建多个 runtime，stop 会 cancel runtime、调用 connector `Stop` 并停止 outbound 订阅；`Start` 返回错误会把 account 标记为 failed 并写入 `state.last_runtime_error`。
- 已实现 Beak 启动恢复：Router 初始化 Channel Gateway 时会查询 `runtime_status=running` 的 active channel account，并按 Beak lifecycle context 自动恢复 connector runtime；服务关闭时 lifecycle context cancel 会停止恢复出的 runtime。
- 已补强 store-level message 写入校验：`sender_id` 必须是当前 session 的 active participant，`reply_to_uuid` 必须属于同 workspace、同 session。
- 已补强 HTTPStore `last_event_uuid` 持久事件续读：同一时间戳内按自增 `id` 继续 replay，避免多个事件 `created_at` 相同导致 Gateway stream catch-up 漏发。
- 已新增 Beak 侧 SDK adapter/registration：`cmd/server/internal/app` 初始化 `ChannelGatewayRegistry`，并通过 `internal/channelgateway/sdkconnectors` 注册 GitHub module `github.com/GuanceCloud/beak-agent-channel-wechat`、`github.com/GuanceCloud/beak-agent-channel-dingtalk`、`github.com/GuanceCloud/beak-agent-channel-lark`。

仍需继续补齐的缺口：

- delivery state 目前先放在 `channel_accounts.state`，后续高吞吐场景可拆 `channel_delivery_state`。
- 生产多实例部署仍建议补数据库唯一约束或 upsert/lock 语义，保证跨进程并发首消息也不会创建重复 session。
- connector runtime 的后台托管、启动恢复和三方 SDK 注册已落地，后续仍需补异常重启策略、指标和多实例 lease。
- HTTP stream 已支持基于 realtime bus 的实时 `SessionEvent` fanout，并支持用 `last_event_uuid` 从持久化 `SessionEvent` 里做 catch-up replay。
- inbound message 已能使用 account config 中的默认 agent 生成 `session_message_type=directed` 和 `target_agent_uuids`，并触发现有会话命令链路；后续仍需补更完整的 routing rule 策略，例如按群、关键词或平台事件类型选择不同 agent。

### 当前 Beak 代码改造清单

继续补齐 Beak host Gateway 时，建议按当前代码边界拆成以下改造项。

#### model 层

目标文件：

- `internal/model/models.go`

需要新增：

- `ChannelAccount`：表示一个已保存的 bot account 连接。
- `ChannelLoginChallenge`：表示扫码登录或授权登录过程态。
- 可选 `ChannelRuntimeStatus`：如果不希望 runtime 状态和 account 主表混合，可以独立表达内存态或 Redis 态。

需要调整：

- `Session.Metadata` 建议写入 `platform`、`account_uuid`、`chat_type`、`chat_id`、`channel_uuid`，便于 outbound 从 session 找回 connector 目标。
- `Message.Metadata` 建议写入 `platform_message_id`、`dedupe_key`、`raw` 的摘要，不建议保存超大平台 payload。

#### store 层

目标文件：

- `internal/store/memory.go`
- 后续 DB store 实现文件

需要新增：

- `channelAccounts map[string]*model.ChannelAccount`
- `channelLoginChallenges map[string]*model.ChannelLoginChallenge`
- `CreateChannelAccount`
- `GetChannelAccount`
- `GetChannelAccounts`
- `UpdateChannelAccount`
- `UpdateChannelAccountState`
- `CreateChannelLoginChallenge`
- `GetChannelLoginChallenge`
- `UpdateChannelLoginChallenge`
- `EnsureSessionBySource`

需要调整：

- `CreateMessage` 必须校验 `sender_id` 是当前 session 的 active participant。
- `CreateMessage` 写入 `SessionEvent` 后要触发实时事件通知；v1 可以先不做，但要保留接口边界。
- `GetSessionEvents` 保持 `last_event_uuid` 续传语义，供 connector outbound worker 使用。

#### handler/router 层

目标文件：

- `internal/handler/router.go`
- 新增 `internal/handler/channel_gateway.go`

需要新增路由：

- `GET /api/v1/channel-connectors`
- `GET /api/v1/channel-connectors/{platform}`
- `POST /api/v1/channel-connectors/{platform}/login/start`
- `POST /api/v1/channel-connectors/{platform}/login/poll`
- `POST /api/v1/channels/{channel_uuid}/accounts`
- `GET /api/v1/channels/{channel_uuid}/accounts`
- `POST /api/v1/channel-accounts/{account_uuid}/start`
- `POST /api/v1/channel-accounts/{account_uuid}/stop`
- `POST /api/v1/channel-webhooks/{platform}/{account_uuid}`

不要把这些能力塞进现有 `ChannelHandler`。`ChannelHandler` 只保留平台级 channel 配置；`ChannelGatewayHandler` 负责 connector、account、login、runtime、webhook。

#### server 层

目标文件：

- `cmd/server/internal/app/app.go`
- `internal/handler/router.go`

需要调整：

- 初始化 `channelgateway.Registry`。
- 注册 `weixin`、`dingtalk`、`lark` connector factory。
- 初始化 `channelgateway.Service` 和 `RuntimeManager`。
- 将 Gateway store、credential codec、logger、HTTP client 注入 service。
- 服务启动后可按数据库中 running active accounts 自动恢复 connector runtime。当前分支已在 Router 初始化时恢复 `runtime_status=running` 的账号。
- 服务关闭时统一 cancel runtime，并保存最后状态。

当前 Beak server 已通过 `cmd/server/internal/app` 统一注入 HTTP store、realtime bus 和 lifecycle context，并通过 `internal/channelgateway/sdkconnectors.RegisterAll` 把已发布的三个 connector SDK 注册进 Beak 构建产物。

#### agent/message 链路

目标文件：

- `internal/handler/message.go`
- `internal/store/memory.go`
- 后续新增 application service，例如 `internal/session/service.go` 或 `internal/channelgateway/message_flow.go`

需要补齐：

- connector inbound message 写入后，要触发现有 agent/routing 处理链路。
- 当前 Gateway 已发布 `session_command` 唤醒在线 agent，并会根据 account/session 默认 agent 写入 directed metadata；后续需要补更完整的 routing rule 策略。
- 建议抽一个共享的 message application service，让 HTTP 写消息和 connector inbound 写消息走同一套校验、事件、agent trigger。
- agent 回复写回同一个 session 后，Gateway outbound worker 再投递到 IM 平台。

#### stream/event 层

目标文件：

- `internal/handler/session.go`
- `internal/store/memory.go`
- 可新增 `internal/session/eventhub.go`

当前状态：

- Stream handler 已基于 realtime bus 订阅 session event，并输出 NDJSON realtime chunks。
- `CreateMessage` 写入 event 后会 publish session event，Gateway outbound 也已复用该实时链路。

当前状态：

- `last_event_uuid` 已作为断线续传边界使用；Stream 建连时如果传入该 cursor，会读取持久化 `SessionEvent` 并 replay cursor 之后的事件。
- HTTPStore 按 `(created_at, id)` 稳定顺序 replay cursor 之后的事件；如果 cursor 不存在，保持原语义返回该 session 的全部持久事件。
- 无 `last_event_uuid` 的首次 stream 仍保持现有行为：只补 pending approval 相关历史，不扩大为全量消息历史，避免影响现有客户端。

后续仍需补齐：

- 多实例场景下需要保证 stream 节点能访问完整持久事件，并结合 bus/lease 避免跨实例漏发。
- 可按客户端能力进一步区分 full history replay 和 cursor-only catch-up。

#### docs/schema 层

目标文件：

- `docs/database-design.md`
- `docs/database.mysql.sql`
- `docs/database.sql`
- `docs/apis.md`

需要调整：

- `database-design.md` 增加 `channel_accounts`、`channel_login_challenges`、`channel_delivery_state`。
- `database.mysql.sql` 同步新增表和唯一索引。
- `database.sql` 当前仍是旧 `agent_id/user_id` session 模型，必须重写或标记 deprecated，避免后续照旧 schema 实现。
- `apis.md` 增加 Gateway API、登录方式、webhook、账号生命周期、session key 规则。

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

### Gateway 如何识别不同 channel 配置

Gateway 不能根据 `channels.config` 中的随意字段判断平台，也不应该在 handler 中写 `if platform == "weixin"` 这类分支。

推荐规则：

1. `channels.platform` 是平台级入口，例如 `weixin`、`dingtalk`、`lark`。
2. `channel_accounts.platform` 必须等于所属 `channels.platform`。
3. API path 中的 `{platform}` 必须能在 `ConnectorRegistry` 中找到 connector。
4. 创建 account 或 login challenge 时，必须校验 path platform、channel platform、connector metadata platform 三者一致。
5. webhook 入口使用 `platform + account_uuid` 定位 connector 和 account。

示例 registry：

```go
type ConnectorFactory func() sdk.Connector

type Registry struct {
	connectors map[string]ConnectorFactory
}

func (r *Registry) Register(platform string, factory ConnectorFactory) {
	r.connectors[platform] = factory
}

func (r *Registry) Connector(platform string) (sdk.Connector, bool) {
	factory, ok := r.connectors[platform]
	if !ok {
		return nil, false
	}
	connector := factory()
	if connector.Metadata().Platform != platform {
		return nil, false
	}
	return connector, true
}
```

注册示例：

```go
registry.Register("weixin", func() sdk.Connector {
	return beakweixin.NewConnector()
})

registry.Register("dingtalk", func() sdk.Connector {
	return beakdingtalk.NewConnector()
})

registry.Register("lark", func() sdk.Connector {
	return beaklark.NewConnector()
})
```

API 调用规则：

- `GET /api/v1/channel-connectors` 从 registry 返回所有 connector metadata，客户端据此渲染接入入口。
- `GET /api/v1/channel-connectors/{platform}` 返回指定平台的 metadata 和 credential schema。
- 微信 metadata 暴露 `login_modes=["qr_code"]`，客户端展示扫码入口。
- 飞书和 DingTalk metadata 暴露 `login_modes=["credential"]`，客户端展示 credential 表单。
- 后续新增 IM bot 时，只新增 SDK 包和 registry 注册，不改 session、message、runtime 主逻辑。

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

`EnsureSessionBySource` 必须是原子操作。当前改造分支已新增 store-level `EnsureSessionBySourceScoped`：内存实现已在同一把 lock 下完成 lookup/create，HTTPStore 已在同一进程内串行化 lookup/create。生产多实例部署仍建议继续补数据库唯一约束或 upsert/lock 语义。

### 建议新增 Gateway API handler

建议新增：

```text
internal/handler/channel_gateway.go
```

新增路由：

```text
GET  /api/v1/channel-connectors
GET  /api/v1/channel-connectors/{platform}
POST /api/v1/channel-connectors/{platform}/login/start
POST /api/v1/channel-connectors/{platform}/login/poll
POST /api/v1/channels/{channel_uuid}/accounts
GET  /api/v1/channels/{channel_uuid}/accounts
POST /api/v1/channel-accounts/{account_uuid}/start
POST /api/v1/channel-accounts/{account_uuid}/stop
POST /api/v1/channel-webhooks/{platform}/{account_uuid}
```

`internal/handler/router.go` 需要增加 channel gateway 路由解析。不要复用 `/api/v1/channels` 的现有 handler 分支承载全部逻辑，否则 channel 平台级配置和 bot account 运行时职责会混在一起。

### 登录方式分流

Gateway 通过 connector metadata 决定登录方式，不在 Beak host 中写死某个平台的 UI 或流程。

QR 登录流程：

1. 客户端读取 connector metadata，发现 `login_modes` 包含 `qr_code`。
2. 客户端调用 `POST /api/v1/channel-connectors/{platform}/login/start`。
3. Gateway 创建 `channel_login_challenges`。
4. Gateway 调用 connector `StartLogin`。
5. 客户端展示返回的二维码。
6. 客户端轮询 `login/poll`。
7. connector 返回 confirmed 后，Gateway 创建或更新 `channel_accounts`，加密保存 credential。

Credential 登录流程：

1. 客户端读取 connector metadata，发现 `login_modes` 包含 `credential`。
2. 客户端根据 `CredentialSchema` 渲染表单。
3. 客户端调用 `POST /api/v1/channels/{channel_uuid}/accounts`。
4. Gateway 校验 schema、加密 credential、创建 `channel_accounts`。
5. 用户点击启用后调用 `POST /api/v1/channel-accounts/{account_uuid}/start`。

当前规划：

- 微信使用 QR 登录。
- DingTalk 使用 credential 登录。
- Lark/飞书使用 credential 登录。

### 原子 EnsureChatSession 落地

Gateway runtime 的 `EnsureChatSession` 应映射到内部方法：

```go
func (s *Service) EnsureIMChatSession(ctx context.Context, req sdk.EnsureChatSessionRequest) (string, error)
```

内部构造：

```text
source_id=<platform>:<account_uuid>:<chat_type>:<chat_id>
session_type=conversation
platform=<platform>
```

不要为 Channel Gateway 新增 IM 专用 `source_type` 语义；`source_type` 保留 Beak 现有来源对象语义。Gateway 的复用键由 `session_type=conversation + platform + source_id` 表达。

创建 session 时至少包含：

```text
im:<platform>:<chat_type>:<chat_id>:user:<sender_id>
agent:<agent_uuid>
bridge:<platform>
```

如果 session 已存在但缺 sender participant，应调用当前已有的 participant 添加能力补齐，而不是新建 session。

数据库落地时至少需要查询索引；当前改造分支已在初始化 schema 中加入：

```sql
CREATE INDEX idx_sessions_gateway_source
ON sessions (site_code, workspace_uuid, session_type, platform, source_id, status);
```

生产多实例部署建议进一步增加唯一约束或等价 upsert/lock 方案。若数据库支持 partial unique index，可使用：

```sql
CREATE UNIQUE INDEX uniq_gateway_conversation_source
ON sessions (site_code, workspace_uuid, platform, source_id)
WHERE session_type = 'conversation' AND source_id IS NOT NULL;
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

- 只处理 `chat:agent` 文本消息。
- 只处理 sender 是当前 session 绑定 agent participant 的消息。
- 根据 session metadata/source_id 找到 `platform + account_uuid + chat_type + chat_id`。
- 使用 `account_uuid` 找到 channel account 和 connector。
- 调用 `connector.Send(ctx, runtime, outbound)`。
- 保存 outbound sent state，避免重发。

当前 Beak 改造分支已经先在 `internal/channelgateway.Service` 中实现这条链路：运行中的 account 会订阅该 account 下的 conversation session realtime event，收到 `chat:agent` 后执行 agent 校验、credential 解密、`connector.Send` 和 `outbound_sent` 状态写入。

HTTP stream 已有 realtime bus fanout 和 `last_event_uuid` 持久 catch-up；Gateway 内部 outbound 直接复用 bus 订阅，不依赖 HTTP stream reconnect。

### Inbound 到 agent 处理

Gateway 写入 IM inbound message 后，不能停在 message/event 层。后续 Beak host 必须补齐 agent 触发点。

建议目标行为：

1. Connector 收到 IM 消息并标准化为 `sdk.InboundMessage`。
2. Gateway 执行 inbound dedupe。
3. Gateway 调用 `EnsureIMChatSession`。
4. Gateway 写入 user message。
5. Gateway 根据 account/session 默认 agent 写入 directed metadata；后续可扩展 routing rule 选择不同 agent。
6. Beak agent 处理该 session 的最新用户消息。
7. Agent 回复写入同一个 session，sender 为 `agent:<agent_uuid>`。
8. Outbound worker 看到 agent message 后调用 connector `Send`。

如果 Beak 现有 agent 机制暂时只能通过 WebSocket agent connection 工作，Gateway 需要把 message 以现有 agent protocol 投递给在线 agent；如果没有在线 agent，应记录失败状态，不应吞掉 inbound message。

### Webhook 平台处理

Webhook 型平台不需要长轮询，但 Gateway 仍然按 account 维度分发。

推荐入口：

```text
POST /api/v1/channel-webhooks/{platform}/{account_uuid}
```

当前 Beak Gateway 分支已提供该入口，并通过 optional `WebhookConnector` capability 将原始 webhook request 分发给平台 SDK。

处理流程：

1. 用 `account_uuid` 读取 `channel_accounts`。
2. 校验 account 的 `platform` 与 path `{platform}` 一致。
3. 解密 credential。
4. 从 registry 创建对应 connector。
5. 如果 connector 实现 webhook 扩展接口，则调用 `HandleWebhook`。
6. connector 校验平台签名、解密 payload、标准化 inbound message。
7. Gateway 进入统一 inbound 流程。

微信 iLink v1 当前以 polling 为主，不要求使用 webhook。Lark 和 DingTalk 更适合 webhook 入口。

### 推荐落地顺序

1. 补 `ChannelAccount`、`ChannelLoginChallenge` model。
2. 补 memory store 原型能力，包含 account、challenge、state、原子 `EnsureSessionBySource`。
3. 补 credential 加密 helper，并明确生产环境 key 校验。
4. 新增 `internal/channelgateway` registry、service、runtime adapter、account state adapter。
5. 新增 Gateway API handler 和 router，先完成 metadata/account/login/start/stop。
6. 补 message sender participant 校验和 reply 校验。已落地：store 写入层会拒绝非 participant sender 和跨 session reply。
7. 补 inbound 到 agent 的 application service，不让 connector 直接绕过 Beak 消息链路。
8. 补 Gateway outbound dispatcher：订阅 conversation session event、过滤 agent 消息、调用 connector `Send`、写入 outbound sent state。
9. 注册 Weixin connector，完成扫码、account start、getupdates、文本 inbound、固定回复或 agent 回复 outbound。
10. 注册 Lark 和 DingTalk connector，完成 credential account、webhook inbound、文本 outbound。
11. 补数据库 SQL/migration 文档，与当前 Go model 对齐。
12. 补失败重试、指标和多实例 lease。

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
session_type=conversation
source_id=<platform>:<account_uuid>:<chat_type>:<chat_id>
```

`source_type` 不参与 Gateway 归一规则；除非命中 Beak 已有来源对象语义，否则保持为空。

示例：

```text
platform=weixin
session_type=conversation
source_id=weixin:account_a:group:group_123
```

```text
platform=weixin
session_type=conversation
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
config
credential_encrypted
state_json
status
last_error
created_at
updated_at
```

`config` 保存非敏感运行配置，例如 `agent_uuid`、`agent_participant_id`、入站过滤策略等。敏感 token/API key 必须进入 credential 并加密保存。

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

### List connectors

```http
GET /api/v1/channel-connectors
```

用途：返回 Beak host 当前已注册的所有 connector metadata，供客户端渲染接入入口。

响应体建议：

```json
{
  "connectors": [
    {
      "platform": "weixin",
      "label": "WeChat",
      "capabilities": {
        "login_modes": ["qr_code"],
        "text": true,
        "group_chat": true,
        "direct_chat": true
      }
    },
    {
      "platform": "lark",
      "label": "Lark",
      "capabilities": {
        "login_modes": ["credential"],
        "text": true,
        "group_chat": true,
        "direct_chat": true
      }
    }
  ]
}
```

### Get connector

```http
GET /api/v1/channel-connectors/{platform}
```

用途：返回单个平台的 connector metadata 和 credential schema。

客户端必须以该接口返回的 `login_modes` 决定展示扫码入口还是 credential 表单。

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

### Channel webhook

```http
POST /api/v1/channel-webhooks/{platform}/{account_uuid}
```

用途：接收 webhook 型 IM 平台事件，例如 Lark/飞书和 DingTalk。

处理要求：

- 按 `account_uuid` 查找 channel account。
- 校验 account platform 与 path platform 一致。
- 解密 credential 后交给对应 connector 校验签名、解密和解析。
- connector 输出标准 `InboundMessage` 后进入统一 inbound message 流程。
- 响应格式遵循平台 webhook 要求，例如 Lark URL verification 需要返回平台指定 challenge。

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

如果平台没有稳定 `message_id`，connector 必须从 raw payload 中构造尽可能稳定的 `dedupe_key`，并在 metadata 中标明来源。若 connector 既没有传 `dedupe_key` 也没有传 `message_id`，Gateway 不应使用空 key 去重，避免误跳过同一个 chat 后续消息。

出站 dedupe key 推荐：

```text
outbound:<platform>:<account_uuid>:<session_uuid>:<message_uuid>
```

## 微信参考实现对齐

微信 connector 是 v1 的完整 reference connector。

微信 connector 使用 Tencent iLink Weixin APIs：

- `ilink/bot/get_bot_qrcode`
- `ilink/bot/get_qrcode_status`
- `ilink/bot/getupdates`
- `ilink/bot/getconfig`
- `ilink/bot/sendtyping`
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
  "typing_tickets": {
    "user_123": "..."
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
- 支持 typing status。
- 不支持 media、voice。
- 不要求微信 connector 维护本地配置文件。
- 不把微信 connector 做成 CLI。

微信消息标准化要求：

- 能识别单聊时，输出 `chat_type=direct`。
- 能识别群聊时，输出 `chat_type=group`。
- `chat_id` 必须是平台内稳定 ID。
- `sender_id` 必须是消息真实发送者 ID。
- `raw` 保留平台原始 payload，便于后续补齐字段。

如果某个 iLink payload 暂时无法可靠识别群聊字段，connector 应返回明确 unsupported/error，并保留 raw payload 用于后续协议对齐，而不是错误地把群聊当单聊。

## 测试清单

后续实现 Beak host Gateway 时，建议至少覆盖以下测试。

### Registry 和 metadata

- 注册 `weixin`、`dingtalk`、`lark` 后，`GET /api/v1/channel-connectors` 返回三类 connector。
- `GET /api/v1/channel-connectors/weixin` 返回 `login_modes=["qr_code"]`。
- `GET /api/v1/channel-connectors/lark` 和 DingTalk 返回 `login_modes=["credential"]`。
- path platform、channel platform、connector metadata platform 不一致时拒绝创建 account 或 challenge。

### Credential 和 account

- credential 创建后只保存密文，list/get account 不返回明文 credential。
- 缺少生产环境 credential key 时，Gateway service 拒绝启动。
- `Start account` 会解密 credential，并构造 `sdk.Runtime`。
- `Stop account` 会 cancel runtime，更新 account status，并保存最后错误。

### QR 登录

- 微信 `login/start` 创建 `channel_login_challenges` 并返回二维码。
- 微信 `login/poll` pending/scanned/confirmed/expired/failed 状态能正确落库。
- confirmed 后创建或更新 `channel_accounts`，并加密保存 `bot_token`。

### Session 复用

- 同一个 `workspace_uuid + platform + account_uuid + group + chat_id` 重复入站只创建一个 session。
- 同一个 `workspace_uuid + platform + account_uuid + direct + chat_id` 重复入站只创建一个 session。
- 同一个群内两个不同 `account_uuid` 创建两个不同 session。
- 并发两条首消息进入同一个 chat 时，`EnsureSessionBySource` 不创建重复 session。
- session 已存在但 sender participant 不存在时，只补 participant，不创建新 session。

### Message 和 stream

- sender 不是 session active participant 时，写 message 被拒绝。
- `reply_to_uuid` 不属于同 workspace/session 时，写 message 被拒绝。
- 写入 message 后生成 `SessionEvent`，`last_event_uuid` 能正确续传。
- event hub 开启后，新 message 能实时推送给 stream subscriber。

### Inbound/outbound 闭环

- connector inbound text 经过 Gateway 后写入 Beak message，并触发 agent/routing。
- agent message event 只在 sender 等于绑定 agent participant 时触发 outbound。
- outbound sent state 能阻止同一 `message_uuid` 重复发回 IM。
- webhook 型平台按 `platform + account_uuid` 分发，并完成签名校验。

## 不做的事

本阶段仍不做以下事项：

- 不接入飞书或 DingTalk 实际 API。
- 不实现 media、voice。
- 不把微信 connector 做成 CLI。
- 不让 SDK 维护本地配置文件或本地状态目录。
- 不为 Channel Gateway 新增 IM 专用 `source_type` 语义。

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
