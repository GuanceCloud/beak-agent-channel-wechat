# Beak Agent Weixin Connector SDK

[English](README.md)

这是一个 Go SDK 包，用于把 Beak Channel Gateway 接入微信 bot account，并通过 Tencent iLink Weixin APIs 完成扫码登录、消息接收和消息发送。

本仓库提供的是可被 Beak host `import` 的库代码，不是命令行工具。SDK 不读取用户编写的运行时配置文件，不维护本地状态目录，不拥有数据库持久化，也不要求用户登录服务器修改文件。Beak host 负责客户端 UI、credential 持久化、account state 持久化、session 创建、message 写入、agent 出站消息订阅和 connector runtime 打包。SDK 只负责微信 connector 逻辑：二维码登录、iLink update 轮询、文本发送、typing 状态、消息去重、`context_token` 处理，以及把微信消息标准化为 Beak Gateway 能理解的消息。

## 范围

v1 支持：

- 通过 `beakweixin.NewConnector()` 暴露通用 `sdk.Connector` 实现。
- 通过 Tencent iLink 完成微信 bot account 二维码登录。
- 由 Beak host 保存 credential 和 connector state。
- `ilink/bot/getupdates` 中的微信文本消息入站到 Beak session。
- Beak agent 文本或 markdown 格式输出通过 `connector.Send` / `ilink/bot/sendmessage` 回发到微信；markdown 使用同一组通用字段，并在 SDK 内退化为 text。
- 通过 `getconfig` 和 `sendtyping` 发送微信 typing 状态。
- 单聊和显式群聊标准化。
- 标准 `bot_identity` state 用于统一 SDK 暴露；账号身份仍优先使用稳定的 `ilink_user_id`。
- 一个已连接 bot account 中的一个群聊对应一个 Beak session。
- 一个已连接 bot account 中的一个单聊对应一个 Beak session。
- 如果同一个微信群里接入多个微信 bot account，每个 bot account 都创建或复用自己的 Beak session。
- bot account 连接保存在 `channel_accounts`，启动连接不会创建 task，也不会额外创建 link session。

v1 不支持：

- media、voice。
- CDN/AES 媒体上传下载。
- 把微信 connector 做成 CLI。
- 让 SDK 维护本地配置文件或本地状态目录。

## OpenClaw 参考实现对齐

上游 [`Tencent/openclaw-weixin`](https://github.com/Tencent/openclaw-weixin) 在 `gateway.startAccount` 中启动 `monitorWeixinProvider`。该 monitor 长轮询 `ilink/bot/getupdates`，保存 `get_updates_buf`，提取每条消息的 `context_token`，并通过 `ilink/bot/sendmessage` 发送文本。

微信参考实现没有 Beak-facing 入站 webhook。旧日志里出现的 "webhook" 只是 polling monitor 的日志文案，不代表 HTTP callback 契约。

Go SDK 保留了旧 bridge/runtime 兼容代码和 stream cursor 字段。新的 Beak Channel Gateway 接入应把平台入站理解为 Weixin polling，把平台出站理解为 host dispatch 后调用 `connector.Send`。

## 包结构

- `sdk`：通用 Beak Connector Plugin SDK 接口和消息类型。
- 根包：微信 connector 实现，以及兼容旧接入方式的 channel adapter。
- `internal/weixin`：Tencent iLink Weixin HTTP client 和协议模型。
- `internal/bridge`：微信 update 到 Beak session/message/stream 的 bridge。
- `internal/beak`：面向测试和参考代码的 REST 风格 Beak runtime adapter。
- `docs/beak-channel-gateway-implementation.md`：Beak host 实现 Channel Gateway 的工程说明。

## 公开入口

```go
import (
	beakweixin "github.com/GuanceCloud/beak-agent-channel-wechat"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

func WeixinConnector() sdk.Connector {
	return beakweixin.NewConnector()
}
```

本包提供两个 Go 入口：

- `beakweixin.NewConnector()`：返回通用 `sdk.Connector` 实现。
- `beakweixin.New().Channel()`：返回旧版 channel adapter，用于兼容已有 Beak channel 接入。

新的 Beak Channel Gateway 应使用 `NewConnector()`。

## Connector 元数据

`connector.Metadata()` 返回微信 connector 的能力描述：

- `id=beak-agent-weixin`
- `platform=weixin`
- label 为 `Weixin`
- 登录模式为 `qr_code`
- 支持文本单聊
- 当 iLink 入站 payload 有明确 `group_id` 时支持文本群聊
- 不支持媒体
- 不支持 block streaming

`connector.CredentialSchema(ctx)` 在 v1 中没有需要用户手动填写的微信字段。微信 account credential 来自二维码登录成功后的返回值，并由 Beak host 加密保存。

## Host 边界

Connector 不直接调用 Beak 数据库。Beak host 在启动 connector 时注入 `sdk.Runtime`：

```go
runtime := sdk.Runtime{
	WorkspaceUUID: "workspace-demo",
	Channel: sdk.Channel{
		UUID:          "channel-demo",
		WorkspaceUUID: "workspace-demo",
		Platform:      "weixin",
	},
	Accounts:     accounts,
	Gateway:      gateway,
	AccountStore: accountStore,
}
```

如果 Beak host 需要调整 iLink 元数据，可以通过 `sdk.Runtime.Native` 传入 connector 专属运行时选项：

```go
runtime.Native = beakweixin.Runtime{
	Weixin: beakweixin.WeixinOptions{
		BotAgent: "Beak Agent",
	},
}
```

`BotAgent` 会作为 iLink `base_info.bot_agent` 发送给上游，用于可观测性标识。它不会改变微信扫码确认页的标题；Tencent iLink 公开的二维码登录 API 没有暴露标题字段。

通用出站字段 `Format` / `Title` 会被接受，便于 Beak host 统一建模。Beak host 应该像飞书和钉钉一样原样传入这些字段；当前微信 iLink 文本发送路径没有暴露 markdown renderer，所以 `Format="markdown"` 会在 SDK 内退化为普通 text 发送。

`sdk.Gateway` 是 Beak host 需要实现的运行时接口：

```go
type Gateway interface {
	EnsureChannel(ctx context.Context, req EnsureChannelRequest) (string, error)
	EnsureChannelLinkSession(ctx context.Context, req EnsureChannelLinkSessionRequest) (string, error)
	EnsureChatSession(ctx context.Context, req EnsureChatSessionRequest) (string, error)
	CreateMessage(ctx context.Context, req CreateMessageRequest) (string, error)
	StreamSession(ctx context.Context, req StreamSessionRequest, handle func(StreamEvent) error) error
	AgentParticipantID() string
	BridgeParticipantID(platform string) string
}
```

`sdk.AccountStore` 是 Beak host 提供给 connector 的 account state 持久化接口：

```go
type AccountStore interface {
	LoadChannelAccountState(ctx context.Context, accountUUID string) (map[string]any, error)
	SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error
}
```

Beak host 负责从数据库加载 `sdk.ChannelAccount`，在进程内解密 credential JSON 后传给 connector，并实现 `AccountStore`，让 connector 能读取最新 state 并保存 cursor、dedupe、token、context token 等运行态。

## 云端二维码登录

Beak 是云端托管产品时，扫码登录必须是后端驱动的浏览器流程，而不是在服务器命令行打印二维码。

推荐流程：

1. 浏览器调用 Beak：`POST /api/v1/channel-connectors/weixin/login/start`。
2. Beak host 调用 `connector.StartLogin(ctx, req)`。
3. Beak host 把 challenge state 保存到数据库或缓存。
4. 浏览器展示 Beak 返回的二维码 URL。
5. 浏览器或后端 worker 轮询 Beak：`POST /api/v1/channel-connectors/weixin/login/poll`。
6. Beak host 调用 `connector.PollLogin(ctx, req)`。
7. 用户确认后，Beak 创建或更新 `channel_accounts`，并加密保存返回的 credential JSON。

开始登录：

```go
connector := beakweixin.NewConnector()

challenge, err := connector.StartLogin(ctx, sdk.LoginStartRequest{
	WorkspaceUUID: workspaceUUID,
	ChannelUUID:   channelUUID,
	Runtime:        runtime,
})
if err != nil {
	return err
}

// 把 challenge.URL 返回给浏览器展示。不要从服务器进程打印二维码。
```

轮询登录状态：

```go
status, err := connector.PollLogin(ctx, sdk.LoginPollRequest{
	WorkspaceUUID:  workspaceUUID,
	ChannelUUID:    channelUUID,
	ChallengeCode:  challenge.Code,
	ChallengeState: challenge.State,
	Runtime:        runtime,
})
if err != nil {
	return err
}
if status.Confirmed {
	// status.Credential 和 status.State 由 Beak host 保存。
	// credential 中的敏感字段必须先加密再入库。
}
```

微信登录成功后会返回 credential 字段：

- `account_id`：SDK 归一化后的稳定账号 key。微信优先使用 `ilink_user_id`，只有缺失时才兼容回退到 `ilink_bot_id`。
- `bot_token`
- `base_url`
- `ilink_user_id`：微信 iLink 用户身份，作为账号稳定身份来源。
- `ilink_bot_id`：微信 iLink bot 标识，可能随扫码登录变化，不要作为账号去重或绑定主键。

## 启动账号连接

当 Beak host 已经保存 channel account 后，用已加载的 accounts 启动 connector：

```go
err := connector.Start(ctx, sdk.Runtime{
	WorkspaceUUID: workspaceUUID,
	Channel:       channel,
	Accounts:      accounts,
	Gateway:       gateway,
	AccountStore:  accountStore,
})
```

每个 `sdk.ChannelAccount` 应包含：

- `UUID`：Beak `channel_accounts.uuid`。
- `WorkspaceUUID`：所属 workspace。
- `ChannelUUID`：所属 channel。
- `Platform`：`weixin`。
- `Credential`：仅在当前进程内使用的已解密 credential JSON。
- `State`：从数据库加载的 connector state JSON。

Connector 会为每个 account 启动微信 update polling，并把入站消息标准化后写入 Beak host 注入的 Gateway runtime。SDK 的 `InboundMessage` 契约包含 `chat_identity`、`chat_display_name`、`mentions`、`mention_all` 和 `mentioned_me`；当前 iLink 文本路径会提供稳定的 `chat_identity.id/type`，display 字段只有在后续微信 update payload 暴露可靠 chat 名称时才填。如果 payload 明确标记当前 bot 被提及，即使正文为空也会进入 Beak，用于 follow-up；正文为空且没有 bot mention 时可以被忽略。

## Credential 和 State

Credential 是敏感数据，应由 Beak host 加密保存：

```json
{
  "account_id": "ilink-user-id",
  "bot_token": "...",
  "base_url": "https://ilinkai.weixin.qq.com",
  "ilink_user_id": "ilink-user-id",
  "ilink_bot_id": "ilink-bot-id"
}
```

State 不是 credential。Beak host 可以先把它保存在 channel account 上，后续再拆到独立 delivery-state 表：

```json
{
  "get_updates_buf": "...",
  "context_tokens": {
    "group:group_123": "...",
    "user_456": "..."
  },
  "inbound_seen": {},
  "peer_sessions": {},
  "stream_cursors": {},
  "sent_beak_messages": {},
  "bot_identity": {
    "id": "ilink-bot-id",
    "id_type": "ilink_bot_id"
  },
  "bot_identities": [
    {
      "id": "ilink-bot-id",
      "id_type": "ilink_bot_id"
    }
  ]
}
```

Connector 通过 `sdk.AccountStore` 更新 state。SDK 不写本地文件。

`ValidateCredential(ctx, req)` 对微信默认返回 `Valid=true`，因为有效 token 来自已成功的二维码登录。它会优先用 `ilink_user_id` 归一化 `account_id`，并把 `ilink_bot_id` 只作为 bot identity metadata 保存。不要用 `ilink_bot_id` 做 account 去重或 Agent 绑定身份，因为它可能在重复扫码后变化。

## Session 规则

Gateway session identity 必须包含已连接 bot account 和 IM 平台 chat identity。必须包含 account 维度，因为同一个 IM 群可能同时存在多个 bot account，而每个 bot 连接都应该有自己的 Beak session。

标准 session key：

```text
workspace_uuid + platform + account_uuid + chat_type + chat_id
```

`account_uuid` 是 Beak `channel_accounts.uuid`。对于还没有 Beak uuid 的兼容旧 adapter，可使用该连接保存的稳定 account id。

`peer_sessions` 是 chat 维度缓存，不要把 update seq、message id 或未来可能出现的 thread/topic id 拼进这个 key。

推荐 Beak session 字段：

```text
platform=weixin
session_type=conversation
source_id=weixin:<account_uuid>:<chat_type>:<chat_id>
```

`source_type` 不参与 Gateway 归一规则。除非这个 session 命中 Beak 已有来源对象语义，否则保持为空，不为 IM Gateway 新增专用值。

单聊：

```text
chat_type=direct
chat_id=<from_user_id>
source_id=weixin:<account_uuid>:direct:<from_user_id>
```

群聊：

```text
chat_type=group
chat_id=<group_id>
source_id=weixin:<account_uuid>:group:<group_id>
```

只有当 iLink payload 明确包含 `group_id` 时才进入群聊模式。如果 payload 没有可靠群 ID，connector 不会自行伪造群 ID。

同一个微信群里有两个微信 bot account 时，source id 必须不同：

```text
source_id=weixin:account_a:group:group_123
source_id=weixin:account_b:group:group_123
```

推荐 participant id：

```text
im:weixin:<chat_type>:<chat_id>:user:<sender_id>
agent:<agent_uuid>
bridge:weixin
```

账号连接本身由 `channel_accounts` 表示，不再额外创建 Beak session。启动 connector account 不能创建 Beak task。

## 消息流

微信文本入站：

1. Connector long-poll `ilink/bot/getupdates`。
2. Connector 通过 host state 保存返回的 `get_updates_buf`。
3. Connector 跳过非文本、不完整或重复 update。
4. Connector 从 `group_id` 或 `from_user_id` 标准化 chat identity。
5. Connector 缓存最新 chat `context_token`。
6. Connector 在 Beak agent 处理期间按需发送微信 typing 状态。
7. Gateway 确保存在 `weixin:<account_uuid>:<chat_type>:<chat_id>` 对应的 Beak session。
8. Gateway 写入 Beak message，sender 为 `im:weixin:<chat_type>:<chat_id>:user:<sender_id>`。

Beak agent 文本出站：

1. Beak host 订阅该 session 的 agent 消息。
2. Beak host 用 host-owned outbound state 跳过非 agent、heartbeat 和已投递消息。
3. Beak host 调用 `connector.Send(ctx, runtime, outbound)`。
4. Connector 调用 `ilink/bot/sendmessage`，并带上 chat id 和缓存的 `context_token`。
5. Connector 发送前会把超长文本切成兼容微信的多条消息。
6. 如果启用了 typing，Connector 在成功投递后发送 typing stop。

旧 bridge adapter 仍可直接消费 Beak stream events，并在该模式下维护 `stream_cursors` / `sent_beak_messages`。新的 host 接入应把出站归属放在 Beak host，然后调用 `connector.Send`。

## 微信协议

Connector 内部使用 Tencent iLink Weixin endpoints：

- `ilink/bot/get_bot_qrcode`
- `ilink/bot/get_qrcode_status`
- `ilink/bot/getupdates`
- `ilink/bot/getconfig`
- `ilink/bot/sendtyping`
- `ilink/bot/sendmessage`
- `ilink/bot/msg/notifystart`
- `ilink/bot/msg/notifystop`

iLink headers、bot type、app id、client version、route tag、long-poll timeout 和 request timeout 都是 SDK 内部协议默认值，不是用户需要填写的 Beak channel 配置。当前 SDK 只公开 `BotAgent` 这个微信运行时选项。

## 示例

最小 host-side import skeleton 见 [examples/basic/main.go](examples/basic/main.go)。

更完整的 Beak host 侧 Channel Gateway 设计见 [docs/beak-channel-gateway-implementation.md](docs/beak-channel-gateway-implementation.md)。

## 验证

```sh
go test ./...
go build ./...
```
