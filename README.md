# Beak Agent Weixin Connector SDK

[中文文档](README.zh-CN.md)

Go SDK package for connecting Beak Channel Gateway to Weixin bot accounts through Tencent iLink Weixin APIs.

This repository is importable library code. It is not a CLI, does not read user-authored runtime files, does not own persistence, and does not require users to edit server files. The Beak host owns UI, credential persistence, account state persistence, session creation, message writes, outbound agent-message subscription, and runtime packaging. This SDK owns the Weixin connector logic: QR login, iLink update polling, text send, typing status, message dedupe, `context_token` handling, and Weixin-to-Beak message normalization.

## Scope

- Generic `sdk.Connector` implementation exposed by `beakweixin.NewConnector()`.
- Tencent iLink QR login for Weixin bot accounts.
- Host-backed credential and state persistence.
- Text-only inbound Weixin messages from `ilink/bot/getupdates` to Beak sessions.
- Text or markdown-formatted Beak agent output back to Weixin through `connector.Send` / `ilink/bot/sendmessage`; markdown uses the same common fields and falls back to text inside the SDK.
- Weixin typing status through `getconfig` and `sendtyping`.
- Direct chat and explicit group chat normalization.
- Standard `bot_identity` state for unified SDK exposure, while account identity remains the stable `ilink_user_id` when available.
- One connected bot account plus one group chat maps to one Beak session; one connected bot account plus one direct chat maps to one Beak session.
- If multiple Weixin bot accounts are in the same group, each bot account creates or reuses its own Beak session for that group.
- The bot account connection is stored as `channel_accounts`; starting it does not create a task or an extra link session.

Out of v1 scope for this Weixin SDK: media, voice, and CDN/AES media upload/download.

## OpenClaw Reference Alignment

The upstream [`Tencent/openclaw-weixin`](https://github.com/Tencent/openclaw-weixin) implementation starts `monitorWeixinProvider` from `gateway.startAccount`. That monitor long-polls `ilink/bot/getupdates`, persists `get_updates_buf`, extracts each message's `context_token`, and sends text through `ilink/bot/sendmessage`.

There is no Beak-facing inbound webhook in the Weixin reference flow. Any "webhook" wording in old logs is just a log label around the polling monitor, not an HTTP callback contract.

The Go SDK keeps older bridge/runtime helpers for compatibility, including stream cursor fields. New Beak Channel Gateway integrations should treat platform inbound as Weixin polling and platform outbound as host dispatch through `connector.Send`.

## Package Layout

- `sdk`: generic Beak Connector Plugin SDK interfaces and message types.
- package root: Weixin connector implementation and legacy channel adapter.
- `internal/weixin`: Tencent iLink Weixin HTTP client and protocol models.
- `internal/bridge`: Weixin update to Beak session/message/stream bridge.
- `internal/beak`: REST-oriented Beak runtime adapter used by tests and reference code.
- `docs/beak-channel-gateway-implementation.md`: Beak host implementation guide for Channel Gateway.

## Public Entrypoints

```go
import (
	beakweixin "github.com/GuanceCloud/beak-agent-channel-wechat"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

func WeixinConnector() sdk.Connector {
	return beakweixin.NewConnector()
}
```

The package exposes two Go entrypoints:

- `beakweixin.NewConnector()` returns the generic `sdk.Connector` implementation.
- `beakweixin.New().Channel()` returns the older channel adapter for compatibility with existing Beak channel integrations.

New Beak Channel Gateway work should use `NewConnector()`.

## Connector Metadata

The Weixin connector metadata is returned from `connector.Metadata()`:

- `id=beak-agent-weixin`
- `platform=weixin`
- label `Weixin`
- login mode `qr_code`
- text direct chat enabled
- text group chat enabled when incoming iLink payloads carry an explicit `group_id`
- media disabled
- block streaming disabled

The credential schema returned from `connector.CredentialSchema(ctx)` has no user-entered Weixin fields for v1. Weixin account credential is produced by QR login and must be stored by Beak host after successful login.

## Host Boundary

The connector does not call Beak database code directly. Beak host injects a `sdk.Runtime`:

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

Connector-specific iLink metadata can be passed through `sdk.Runtime.Native`:

```go
runtime.Native = beakweixin.Runtime{
	Weixin: beakweixin.WeixinOptions{
		BotAgent: "Beak Agent",
	},
}
```

`BotAgent` is sent as iLink `base_info.bot_agent` for upstream observability. It does not change the Weixin QR scan confirmation title; Tencent iLink's public QR login API does not expose a title field.

The common outbound `Format` / `Title` fields are accepted for host-side consistency. Beak host should pass them exactly as it does for Lark and DingTalk; Weixin currently falls back to plain text for `Format="markdown"` inside the SDK because the iLink text path does not expose a markdown renderer.

`sdk.Gateway` is the host runtime contract:

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

`sdk.AccountStore` is the host persistence hook for connector runtime state:

```go
type AccountStore interface {
	LoadChannelAccountState(ctx context.Context, accountUUID string) (map[string]any, error)
	SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error
}
```

Beak host is responsible for loading `sdk.ChannelAccount` records from its database, decrypting credential JSON before passing accounts into the runtime, and implementing `AccountStore` so connectors can read the latest state and save updated cursor, dedupe, token, and context-token maps.

## Cloud QR Login

For hosted Beak, QR login is a backend-driven browser flow:

1. Browser calls Beak: `POST /api/v1/channel-connectors/weixin/login/start`.
2. Beak host calls `connector.StartLogin(ctx, req)`.
3. Beak stores the returned challenge state in its database or cache.
4. Browser renders the QR code URL returned by Beak.
5. Browser or backend worker polls Beak: `POST /api/v1/channel-connectors/weixin/login/poll`.
6. Beak host calls `connector.PollLogin(ctx, req)`.
7. After confirmation, Beak creates or updates `channel_accounts` and encrypts returned credential JSON.

Start login:

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

// Return challenge.URL to the browser. Do not print it from a server process.
```

Poll login:

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
	// status.Credential and status.State are saved by Beak host.
	// Sensitive credential fields must be encrypted before persistence.
}
```

Confirmed Weixin login returns credential fields:

- `account_id`: SDK-normalized stable account key. Weixin prefers `ilink_user_id` and only falls back to `ilink_bot_id` when the user id is absent.
- `bot_token`
- `base_url`
- `ilink_user_id`: Weixin iLink user identity and the stable account identity source.
- `ilink_bot_id`: Weixin iLink bot identity. It can change across QR logins, so do not use it as the account dedupe or binding key.

## Starting Accounts

After Beak host has a saved channel account, it starts the connector with loaded accounts:

```go
err := connector.Start(ctx, sdk.Runtime{
	WorkspaceUUID: workspaceUUID,
	Channel:       channel,
	Accounts:      accounts,
	Gateway:       gateway,
	AccountStore:  accountStore,
})
```

Each `sdk.ChannelAccount` should include:

- `UUID`: Beak channel account uuid.
- `WorkspaceUUID`: owning workspace.
- `ChannelUUID`: owning channel.
- `Platform`: `weixin`.
- `Credential`: decrypted credential JSON for this process only.
- `State`: persisted connector state JSON.

The connector starts Weixin update polling for each account and sends standardized inbound messages into the injected Gateway runtime. The SDK `InboundMessage` contract includes `mentions`, `mention_all`, and `mentioned_me`; the current iLink text path leaves them empty unless future Weixin update payloads expose mention metadata. If a payload explicitly marks the current bot as mentioned, an empty text message is still delivered to Beak for follow-up handling; empty text without a bot mention can be ignored.

## Credential And State

Credential is secret and should be encrypted by Beak host:

```json
{
  "account_id": "ilink-user-id",
  "bot_token": "...",
  "base_url": "https://ilinkai.weixin.qq.com",
  "ilink_user_id": "ilink-user-id",
  "ilink_bot_id": "ilink-bot-id"
}
```

State is not credential. Beak host stores it with the channel account, or later in a separate delivery-state table:

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

The connector updates state through `sdk.AccountStore`. It does not write local files.

`ValidateCredential(ctx, req)` defaults to `Valid=true` for Weixin because successful QR login has already produced the token. It normalizes `account_id` from `ilink_user_id` when present and preserves `ilink_bot_id` only as bot identity metadata. Do not use `ilink_bot_id` as account dedupe or Agent binding identity because it can change across scans.

## Session Rules

Gateway session identity is the connected bot account plus platform chat identity. The account dimension is required because the same IM group can contain multiple bot accounts, and each bot connection must have its own Beak session.

Canonical session key:

```text
workspace_uuid + platform + account_uuid + chat_type + chat_id
```

`account_uuid` is the Beak `channel_accounts.uuid` for the connected bot account. For legacy adapters that do not have a Beak uuid yet, use the stable account id stored with that connection.

`peer_sessions` is chat-scoped. Do not include update sequence, message id, or future thread/topic ids in this cache key.

Recommended Beak session fields:

```text
platform=weixin
session_type=conversation
source_id=weixin:<account_uuid>:<chat_type>:<chat_id>
```

`source_type` is not part of the Gateway identity rule. Leave it empty unless the session is tied to an existing Beak source object with established semantics.

Direct chat:

```text
chat_type=direct
chat_id=<from_user_id>
source_id=weixin:<account_uuid>:direct:<from_user_id>
```

Group chat:

```text
chat_type=group
chat_id=<group_id>
source_id=weixin:<account_uuid>:group:<group_id>
```

Group mode is used only when the iLink payload explicitly includes `group_id`. If the payload does not include a reliable group id, the connector will not invent one.

When two Weixin bot accounts are present in the same group, the source ids are intentionally different:

```text
source_id=weixin:account_a:group:group_123
source_id=weixin:account_b:group:group_123
```

Recommended participant ids:

```text
im:weixin:<chat_type>:<chat_id>:user:<sender_id>
agent:<agent_uuid>
bridge:weixin
```

The account connection itself is represented by `channel_accounts`, not by an additional Beak session. Starting a connector account must not create a Beak task.

## Message Flow

Inbound Weixin text:

1. Connector long-polls `ilink/bot/getupdates`.
2. Connector persists returned `get_updates_buf` through host state.
3. Connector skips non-text, incomplete, or duplicate updates.
4. Connector normalizes chat identity from `group_id` or `from_user_id`.
5. Connector caches the latest chat `context_token`.
6. Connector optionally sends Weixin typing status while the Beak agent is processing.
7. Gateway ensures one Beak session for `weixin:<account_uuid>:<chat_type>:<chat_id>`.
8. Gateway writes Beak message as sender `im:weixin:<chat_type>:<chat_id>:user:<sender_id>`.

Outbound Beak agent text:

1. Beak host subscribes to agent messages for the session.
2. Beak host ignores non-agent, heartbeat, and already-sent messages with host-owned outbound state.
3. Beak host calls `connector.Send(ctx, runtime, outbound)`.
4. Connector calls `ilink/bot/sendmessage` with chat id and cached `context_token`.
5. Connector splits long text into compatible chunks before sending.
6. Connector sends typing stop after successful delivery when typing was enabled.

The legacy bridge adapter can still consume Beak stream events directly and stores `stream_cursors` / `sent_beak_messages` for that mode. New host integrations should keep outbound ownership in Beak host and use `connector.Send`.

## Weixin Protocol

The connector uses Tencent iLink Weixin endpoints internally:

- `ilink/bot/get_bot_qrcode`
- `ilink/bot/get_qrcode_status`
- `ilink/bot/getupdates`
- `ilink/bot/getconfig`
- `ilink/bot/sendtyping`
- `ilink/bot/sendmessage`
- `ilink/bot/msg/notifystart`
- `ilink/bot/msg/notifystop`

iLink headers, bot type, app id, client version, route tag, long-poll timeout, and request timeout are internal protocol defaults. They are not user-facing Beak channel settings. `BotAgent` is the only public Weixin runtime option exposed by this SDK today.

## Example

See [examples/basic/main.go](examples/basic/main.go) for a minimal host-side import skeleton using the generic Connector SDK.

## Verification

```sh
go test ./...
go build ./...
```
