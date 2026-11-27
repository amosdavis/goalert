package escalation

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/txn"
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

	return s.delta.addAction(ctx, tx, stepID, dest)
}

func (s *Store) DeleteStepActionTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID, dest gadb.DestV1) error {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return err
	}

	return s.delta.removeAction(ctx, tx, stepID, dest)
}

func (s *Store) FindAllStepActionsTx(ctx context.Context, tx txn.Tx, stepID uuid.UUID) ([]gadb.DestV1, error) {
	err := permission.LimitCheckAny(ctx, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllActions(ctx, tx, stepID)
}
