package basic

import (
	"context"

	beakweixin "github.com/GuanceCloud/beak-agent-channel-wechat"
	"github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
)

func Connector() sdk.Connector {
	return beakweixin.NewConnector()
}

func StartLogin(ctx context.Context, runtime sdk.Runtime, workspaceUUID, channelUUID string) (*sdk.LoginChallenge, error) {
	return Connector().StartLogin(ctx, sdk.LoginStartRequest{
		WorkspaceUUID: workspaceUUID,
		ChannelUUID:   channelUUID,
		Runtime:       runtime,
	})
}

func PollLogin(ctx context.Context, runtime sdk.Runtime, workspaceUUID, channelUUID string, challenge sdk.LoginChallenge) (*sdk.LoginStatus, error) {
	return Connector().PollLogin(ctx, sdk.LoginPollRequest{
		WorkspaceUUID:  workspaceUUID,
		ChannelUUID:    channelUUID,
		ChallengeCode:  challenge.Code,
		ChallengeState: challenge.State,
		Runtime:        runtime,
	})
}

func Start(ctx context.Context, runtime sdk.Runtime) error {
	return Connector().Start(ctx, runtime)
}

func Send(ctx context.Context, runtime sdk.Runtime, outbound sdk.OutboundMessage) (*sdk.SendResult, error) {
	return Connector().Send(ctx, runtime, outbound)
}
