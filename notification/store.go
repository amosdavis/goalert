package notification

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/search"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"

	"github.com/pkg/errors"
)

const minTimeBetweenTests = time.Minute

// Store provides notification operations on the Delta backend.
type Store struct {
	delta *deltaStore
}

// OriginalMessageStatus will return the status of the first alert notification sent to `dest` for the given `alertID`.
func (s *Store) OriginalMessageStatus(ctx context.Context, alertID int, dstID DestID) (*SendResult, error) {
	err := permission.LimitCheckAny(ctx, permission.System)
	if err != nil {
		return nil, err
	}

	return s.delta.originalMessageStatus(ctx, alertID, dstID)
}

func outgoingMessageToSendResult(msg gadb.OutgoingMessage, cm, ch gadb.NullDestV1) (*SendResult, error) {
	res := SendResult{
		ID:                msg.ID.String(),
		ProviderMessageID: msg.ProviderMsgID,
	}

	switch {
	case cm.Valid:
		res.DestType = cm.DestV1.Type
	case ch.Valid:
		res.DestType = ch.DestV1.Type
	}

	state, err := messageStateFromStatus(msg.LastStatus, msg.NextRetryAt.Valid)
	if err != nil {
		return nil, err
	}

	res.Status = Status{
		State:    state,
		Details:  msg.StatusDetails,
		Sequence: int(msg.ProviderSeq),
		SrcValue: msg.SrcValue.String,
	}
	if msg.LastStatusAt.Valid {
		res.Age = msg.LastStatusAt.Time.Sub(msg.CreatedAt)
	}

	return &res, nil
}

func (s *Store) cmUserID(ctx context.Context, id string) (string, error) {
	err := permission.LimitCheckAny(ctx, permission.User, permission.Admin)
	if err != nil {
		return "", err
	}

	err = validate.UUID("ContactMethodID", id)
	if err != nil {
		return "", err
	}

	userID, err := s.delta.methodOwner(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", validation.NewFieldError("ContactMethodID", "does not exist")
	}
	if err != nil {
		return "", err
	}

	// only admin or same-user can verify
	err = permission.LimitCheckAny(ctx, permission.Admin, permission.MatchUser(userID))
	if err != nil {
		return "", err
	}

	return userID, nil
}

func (s *Store) Code(ctx context.Context, id string) (int, error) {
	err := permission.LimitCheckAny(ctx, permission.System)
	if err != nil {
		return 0, err
	}
	err = validate.UUID("VerificationCodeID", id)
	if err != nil {
		return 0, err
	}

	return s.delta.code(ctx, id)
}

func (s *Store) SendContactMethodTest(ctx context.Context, id string) error {
	cmUserID, err := s.cmUserID(ctx, id)
	if err != nil {
		return err
	}

	// due to potential regulations around consent with phone calls and SMS, we
	// only allow users to send test messages to their own contact methods
	err = permission.LimitCheckAny(ctx, permission.MatchUser(cmUserID))
	if err != nil {
		return err
	}

	return s.delta.sendTest(ctx, id)
}

func (s *Store) SendContactMethodVerification(ctx context.Context, cmID string) error {
	_, err := s.cmUserID(ctx, cmID)
	if err != nil {
		return err
	}

	return s.delta.sendVerification(ctx, cmID)
}

func (s *Store) VerifyContactMethod(ctx context.Context, cmID string, code int) error {
	_, err := s.cmUserID(ctx, cmID)
	if err != nil {
		return err
	}

	ok, err := s.delta.verify(ctx, cmID, code)
	if err != nil {
		return err
	}
	if !ok {
		return validation.NewFieldError("code", "invalid code")
	}

	// NOTE: maintain a record of consent/dissent
	logCtx := log.WithFields(ctx, log.Fields{
		"contactMethodID": cmID,
	})

	log.Logf(logCtx, "Contact method ENABLED/VERIFIED.")

	return nil
}

func messageStateFromStatus(lastStatus gadb.EnumOutgoingMessagesStatus, hasNextRetry bool) (State, error) {
	switch lastStatus {
	case gadb.EnumOutgoingMessagesStatusQueuedRemotely, gadb.EnumOutgoingMessagesStatusSending:
		return StateSending, nil
	case gadb.EnumOutgoingMessagesStatusPending:
		return StatePending, nil
	case gadb.EnumOutgoingMessagesStatusRead:
		return StateRead, nil
	case gadb.EnumOutgoingMessagesStatusSent:
		return StateSent, nil
	case gadb.EnumOutgoingMessagesStatusDelivered:
		return StateDelivered, nil
	case gadb.EnumOutgoingMessagesStatusFailed, gadb.EnumOutgoingMessagesStatusBundled: // bundled message was not sent (replaced) and should never be re-sent
		// temporary if retry
		if hasNextRetry {
			return StateFailedTemp, nil
		} else {
			return StateFailedPerm, nil
		}
	default:
		return -1, fmt.Errorf("unknown last_status %s", lastStatus)
	}
}

func (s *Store) FindManyMessageStatuses(ctx context.Context, strIDs []string) ([]SendResult, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}
	if len(strIDs) == 0 {
		return nil, nil
	}

	ids, err := validate.ParseManyUUID("IDs", strIDs, search.MaxResults)
	if err != nil {
		return nil, err
	}

	return s.delta.findManyMessageStatuses(ctx, ids)
}

// LastMessageStatus will return the MessageStatus and creation time of the most recent message of the requested type
// for the provided contact method ID, if one was created from the provided from time.
func (s *Store) LastMessageStatus(ctx context.Context, typ gadb.EnumOutgoingMessagesType, cmIDStr string, from time.Time) (*SendResult, time.Time, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, time.Time{}, err
	}

	cmID, err := validate.ParseUUID("Contact Method ID", cmIDStr)
	if err != nil {
		return nil, time.Time{}, err
	}

	return s.delta.lastMessageStatus(ctx, typ, cmID, from)
}
