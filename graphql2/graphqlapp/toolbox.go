package graphqlapp

import (
	"context"

	"github.com/target/goalert/gadb"
	"github.com/target/goalert/validation"

	"github.com/target/goalert/graphql2"
	"github.com/target/goalert/permission"
)

func (q *Query) DebugMessageStatus(ctx context.Context, input graphql2.DebugMessageStatusInput) (*graphql2.DebugMessageStatusInfo, error) {
	err := permission.LimitCheckAny(ctx, permission.Admin)
	if err != nil {
		return nil, err
	}

	id, err := gadb.ParseProviderMessageID(input.ProviderMessageID)
	if err != nil {
		return nil, validation.NewFieldError("ProviderMessageID", err.Error())
	}

	status, err := q.NotificationManager.MessageStatus(ctx, id)
	if err != nil {
		return nil, err
	}

	return &graphql2.DebugMessageStatusInfo{
		State: notificationStateFromSendResult(*status, status.SrcValue),
	}, nil
}
