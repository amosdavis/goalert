package engine

import (
	"context"

	"fmt"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/engine/message"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/util/log"
)

// messageContext is the context in which a message is sent, and logged.
func (p *Engine) messageContext(ctx context.Context, msg *message.Message) context.Context {
	ctx = log.WithField(ctx, "CallbackID", msg.ID)

	if msg.DestID.IsUserCM() {
		ctx = permission.UserSourceContext(ctx, msg.UserID, permission.RoleUser, &permission.SourceInfo{
			Type: permission.SourceTypeContactMethod,
			ID:   msg.DestID.String(),
		})
	} else {
		ctx = permission.SystemContext(ctx, "SendMessage")
		ctx = permission.SourceContext(ctx, &permission.SourceInfo{
			Type: permission.SourceTypeNotificationChannel,
			ID:   msg.DestID.String(),
		})
	}
	return ctx
}

// buildMessage looks up what a message says. If there is nothing to send, there
// is a result instead. isFirstAlertMessage is whether its destination has not been
// notified of its alert before.
func (p *Engine) buildMessage(ctx context.Context, msg *message.Message) (notifMsg notification.Message, isFirstAlertMessage bool, res *notification.SendResult, err error) {
	switch msg.Type {
	case notification.MessageTypeAlertBundle:
		name, count, err := p.a.ServiceInfo(ctx, msg.ServiceID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup service info")
		}
		if count == 0 {
			// already acked/closed, don't send bundled notification
			return nil, false, &notification.SendResult{
				ID: msg.ID,
				Status: notification.Status{
					Details: "alerts acked/closed before message sent",
					State:   notification.StateFailedPerm,
				},
			}, nil
		}
		notifMsg = notification.AlertBundle{
			Base:        msg.Base(),
			ServiceID:   msg.ServiceID,
			ServiceName: name,
			Count:       count,
		}
	case notification.MessageTypeAlert:
		name, _, err := p.a.ServiceInfo(ctx, msg.ServiceID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup service info")
		}
		a, err := p.a.FindOne(ctx, msg.AlertID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup alert")
		}
		stat, err := p.cfg.NotificationStore.OriginalMessageStatus(ctx, msg.AlertID, msg.DestID)
		if err != nil {
			return nil, false, nil, fmt.Errorf("lookup original message: %w", err)
		}
		if stat != nil && stat.ID == msg.ID {
			// set to nil if it's the current message
			stat = nil
		}
		meta, err := p.a.Metadata(ctx, nil, msg.AlertID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup alert metadata")
		}
		notifMsg = notification.Alert{
			Base:        msg.Base(),
			AlertID:     msg.AlertID,
			Summary:     a.Summary,
			Details:     a.Details,
			ServiceID:   a.ServiceID,
			ServiceName: name,
			Meta:        meta,

			OriginalStatus: stat,
		}
		isFirstAlertMessage = stat == nil
	case notification.MessageTypeAlertStatus:
		e, err := p.cfg.AlertLogStore.FindOne(ctx, msg.AlertLogID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup alert log entry")
		}
		a, err := p.cfg.AlertStore.FindOne(ctx, msg.AlertID)
		if err != nil {
			return nil, false, nil, fmt.Errorf("lookup original alert: %w", err)
		}
		stat, err := p.cfg.NotificationStore.OriginalMessageStatus(ctx, msg.AlertID, msg.DestID)
		if err != nil {
			return nil, false, nil, fmt.Errorf("lookup original message: %w", err)
		}
		if stat == nil {
			return nil, false, nil, fmt.Errorf("could not find original notification for alert %d to %s", msg.AlertID, msg.Dest.String())
		}
		name, _, err := p.a.ServiceInfo(ctx, a.ServiceID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup service info")
		}
		meta, err := p.a.Metadata(ctx, nil, msg.AlertID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup alert metadata")
		}

		var status notification.AlertState
		switch e.Type() {
		case alertlog.TypeAcknowledged:
			status = notification.AlertStateAcknowledged
		case alertlog.TypeEscalated:
			status = notification.AlertStateUnacknowledged
		case alertlog.TypeClosed:
			status = notification.AlertStateClosed
		}

		notifMsg = notification.AlertStatus{
			Base:           msg.Base(),
			AlertID:        e.AlertID(),
			ServiceID:      a.ServiceID,
			LogEntry:       e.String(ctx),
			Summary:        a.Summary,
			Details:        a.Details,
			NewAlertState:  status,
			OriginalStatus: *stat,
			ServiceName:    name,
			Meta:           meta,
		}
	case notification.MessageTypeTest:
		notifMsg = notification.Test{
			Base: msg.Base(),
		}
	case notification.MessageTypeVerification:
		code, err := p.cfg.NotificationStore.Code(ctx, msg.VerifyID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup verification code")
		}
		notifMsg = notification.Verification{
			Base: msg.Base(),
			Code: fmt.Sprintf("%06d", code),
		}
	case notification.MessageTypeScheduleOnCallUsers:
		users, err := p.cfg.OnCallStore.OnCallUsersBySchedule(ctx, msg.ScheduleID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup on call users by schedule")
		}
		sched, err := p.cfg.ScheduleStore.FindOne(ctx, msg.ScheduleID)
		if err != nil {
			return nil, false, nil, errors.Wrap(err, "lookup schedule by id")
		}

		var onCallUsers []notification.User
		for _, u := range users {
			onCallUsers = append(onCallUsers, notification.User{
				Name: u.Name,
				ID:   u.ID,
				URL:  p.cfg.ConfigSource.Config().CallbackURL("/users/" + u.ID),
			})
		}

		notifMsg = notification.ScheduleOnCallUsers{
			Base:         msg.Base(),
			ScheduleName: sched.Name,
			ScheduleURL:  p.cfg.ConfigSource.Config().CallbackURL("/schedules/" + msg.ScheduleID),
			ScheduleID:   msg.ScheduleID,
			Users:        onCallUsers,
		}

	default:
		log.Log(ctx, errors.New("SEND NOT IMPLEMENTED FOR MESSAGE TYPE "+string(msg.Type)))
		return nil, false, &notification.SendResult{ID: msg.ID, Status: notification.Status{State: notification.StateFailedPerm}}, nil
	}
	return notifMsg, isFirstAlertMessage, nil, nil
}
