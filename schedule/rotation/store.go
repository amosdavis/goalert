package rotation

import (
	"context"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/validation/validate"
)

// ErrNoState is returned when there is no state information available for a rotation.
var ErrNoState = errors.New("no state available")

// Store provides rotation operations on the Delta backend.
type Store struct {
	delta *deltaRotations
}

func (s *Store) State(ctx context.Context, id string) (*State, error) {
	return s.StateTx(ctx, nil, id)
}

func (s *Store) StateTx(ctx context.Context, tx txn.Tx, id string) (*State, error) {
	err := validate.UUID("RotationID", id)
	if err != nil {
		return nil, err
	}
	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	return s.delta.state(ctx, tx, id)
}

func (s *Store) CreateRotationTx(ctx context.Context, tx txn.Tx, r *Rotation) (*Rotation, error) {
	n, err := r.Normalize()
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	n.ID = uuid.New().String()
	if err := s.delta.create(ctx, tx, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (s *Store) UpdateRotationTx(ctx context.Context, tx txn.Tx, r *Rotation) error {
	err := validate.UUID("RotationID", r.ID)
	if err != nil {
		return err
	}
	n, err := r.Normalize()
	if err != nil {
		return err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return err
	}

	return s.delta.update(ctx, tx, n)
}

func (s *Store) FindMany(ctx context.Context, ids []string) ([]Rotation, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}
	err = validate.ManyUUID("RotationID", ids, 200)
	if err != nil {
		return nil, err
	}

	return s.delta.findMany(ctx, ids)
}

func (s *Store) FindRotation(ctx context.Context, id string) (*Rotation, error) {
	err := validate.UUID("RotationID", id)
	if err != nil {
		return nil, err
	}
	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.find(ctx, nil, id, false)
}

func (s *Store) FindParticipantCount(ctx context.Context, id string) (int, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return -1, err
	}

	err = validate.UUID("RotationID", id)
	if err != nil {
		return -1, err
	}

	return s.delta.participantCount(ctx, id)
}

func (s *Store) FindRotationForUpdateTx(ctx context.Context, tx txn.Tx, rotationID string) (*Rotation, error) {
	err := permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	err = validate.UUID("RotationID", rotationID)
	if err != nil {
		return nil, err
	}

	return s.delta.find(ctx, tx, rotationID, true)
}

func (s *Store) DeleteManyTx(ctx context.Context, tx txn.Tx, ids []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	err = validate.ManyUUID("RotationID", ids, 50)
	if err != nil {
		return err
	}

	return s.delta.deleteMany(ctx, tx, ids)
}

func (s *Store) FindAllParticipantsTx(ctx context.Context, tx txn.Tx, rotationID string) ([]Participant, error) {
	err := validate.UUID("RotationID", rotationID)
	if err != nil {
		return nil, err
	}
	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllParticipants(ctx, tx, rotationID)
}

func (s *Store) FindAllParticipants(ctx context.Context, rotationID string) ([]Participant, error) {
	return s.FindAllParticipantsTx(ctx, nil, rotationID)
}

func (s *Store) SetActiveIndexTx(ctx context.Context, tx txn.Tx, rotID string, position int) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.Many(
		validate.UUID("RotationID", rotID),
	)
	if err != nil {
		return err
	}

	return s.delta.setActiveIndex(ctx, tx, rotID, position)
}

func (s *Store) FindParticipant(ctx context.Context, id string) (*Participant, error) {
	err := validate.UUID("RotationParticipantID", id)
	if err != nil {
		return nil, err
	}

	err = permission.LimitCheckAny(ctx, permission.All)
	if err != nil {
		return nil, err
	}

	return s.delta.findParticipant(ctx, id)
}

func (s *Store) AddRotationUsersTx(ctx context.Context, tx txn.Tx, rotationID string, userIDs []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.ManyUUID("UserIDs", userIDs, 50)
	if err != nil {
		return err
	}

	return s.delta.addUsers(ctx, tx, rotationID, userIDs)
}

func (s *Store) DeleteRotationParticipantsTx(ctx context.Context, tx txn.Tx, partIDs []string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.ManyUUID("ParticipantIDs", partIDs, 50)
	if err != nil {
		return err
	}

	return s.delta.deleteParticipants(ctx, tx, partIDs)
}

func (s *Store) UpdateParticipantUserIDTx(ctx context.Context, tx txn.Tx, partID, userID string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}

	err = validate.Many(
		validate.UUID("ParticipantID", partID),
		validate.UUID("UserID", userID),
	)
	if err != nil {
		return err
	}

	return s.delta.updateParticipantUserID(ctx, tx, partID, userID)
}

func (s *Store) DeleteStateTx(ctx context.Context, tx txn.Tx, rotationID string) error {
	err := permission.LimitCheckAny(ctx, permission.Admin, permission.User)
	if err != nil {
		return err
	}
	err = validate.UUID("RotationID", rotationID)
	if err != nil {
		return err
	}

	return s.delta.deleteState(ctx, tx, rotationID)
}
