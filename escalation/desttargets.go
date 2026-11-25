package escalation

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// actionTarget is what an action notifies; exactly one is set.
type actionTarget struct {
	userID, scheduleID, rotationID, channelID uuid.NullUUID
}

// parseTarget returns what dest notifies. channelID returns the id of the
// notification channel with that destination.
func parseTarget(dest gadb.DestV1, channelID func() (uuid.UUID, error)) (t actionTarget, err error) {
	var id uuid.UUID
	target := &t.channelID
	switch dest.Type {
	case user.DestTypeUser:
		target = &t.userID
		id, err = validate.ParseUUID("ID", dest.Arg(user.FieldUserID))
	case schedule.DestTypeSchedule:
		target = &t.scheduleID
		id, err = validate.ParseUUID("ID", dest.Arg(schedule.FieldScheduleID))
	case rotation.DestTypeRotation:
		target = &t.rotationID
		id, err = validate.ParseUUID("ID", dest.Arg(rotation.FieldRotationID))
	default:
		id, err = channelID()
	}
	if err != nil {
		return actionTarget{}, err
	}
	*target = uuid.NullUUID{UUID: id, Valid: true}
	return t, nil
}

func (s *Store) AddStepActionTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, dest gadb.DestV1) error {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return err
	}

	typeInfo, err := s.reg.TypeInfo(ctx, dest.Type)
	if err != nil {
		return err
	}
	if !typeInfo.IsEPTarget() {
		return validation.NewGenericError("not a valid escalation policy destination")
	}

	if s.delta != nil {
		return s.delta.addAction(ctx, tx, stepID, dest)
	}

	stx := sqltxn.Unwrap(tx)
	tgt, err := parseTarget(dest, func() (uuid.UUID, error) { return s.ncStore.MapDestToID(ctx, stx, dest) })
	if err != nil {
		return err
	}

	return gadb.New(stx).EPStepActionsAddAction(ctx, gadb.EPStepActionsAddActionParams{
		EscalationPolicyStepID: stepID,
		UserID:                 tgt.userID,
		ScheduleID:             tgt.scheduleID,
		RotationID:             tgt.rotationID,
		ChannelID:              tgt.channelID,
	})
}

func (s *Store) DeleteStepActionTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, dest gadb.DestV1) error {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return err
	}

	if s.delta != nil {
		return s.delta.removeAction(ctx, tx, stepID, dest)
	}

	stx := sqltxn.Unwrap(tx)
	tgt, err := parseTarget(dest, func() (uuid.UUID, error) { return s.ncStore.LookupDestID(ctx, stx, dest) })
	if err != nil {
		return err
	}

	return gadb.New(stx).EPStepActionsDeleteAction(ctx, gadb.EPStepActionsDeleteActionParams{
		EscalationPolicyStepID: stepID,
		UserID:                 tgt.userID,
		ScheduleID:             tgt.scheduleID,
		RotationID:             tgt.rotationID,
		ChannelID:              tgt.channelID,
	})
}

func (s *Store) FindAllStepActionsTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID) ([]gadb.DestV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	if s.delta != nil {
		return s.delta.findAllActions(ctx, tx, stepID)
	}

	dbtx := sqltxn.DBTX(s.db, tx)
	actions, err := gadb.New(dbtx).EPStepActionsByStepId(ctx, stepID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	result := make([]gadb.DestV1, 0, len(actions))
	for _, a := range actions {
		switch {
		case a.UserID.Valid:
			result = append(result, user.DestFromID(a.UserID.UUID.String()))
		case a.ScheduleID.Valid:
			result = append(result, schedule.DestFromID(a.ScheduleID.UUID.String()))
		case a.RotationID.Valid:
			result = append(result, rotation.DestFromID(a.RotationID.UUID.String()))
		case a.Dest.Valid:
			result = append(result, a.Dest.DestV1)
		}
	}

	return result, nil
}
