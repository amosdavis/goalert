package label

import (
	"context"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
)

// Store allows the lookup and management of Labels on the Delta backend.
type Store struct {
	delta *deltaLabels
}

func (l *Label) tgtEP() uuid.NullUUID {
	if l.Target.TargetType() != assignment.TargetTypeEscalationPolicy {
		return uuid.NullUUID{}
	}

	return uuid.NullUUID{UUID: uuid.MustParse(l.Target.TargetID()), Valid: true}
}

func (l *Label) tgtSvc() uuid.NullUUID {
	if l.Target.TargetType() != assignment.TargetTypeService {
		return uuid.NullUUID{}
	}

	return uuid.NullUUID{UUID: uuid.MustParse(l.Target.TargetID()), Valid: true}
}

func (l *Label) tgtSched() uuid.NullUUID {
	if l.Target.TargetType() != assignment.TargetTypeSchedule {
		return uuid.NullUUID{}
	}

	return uuid.NullUUID{UUID: uuid.MustParse(l.Target.TargetID()), Valid: true}
}

func (l *Label) tgtRot() uuid.NullUUID {
	if l.Target.TargetType() != assignment.TargetTypeRotation {
		return uuid.NullUUID{}
	}

	return uuid.NullUUID{UUID: uuid.MustParse(l.Target.TargetID()), Valid: true}
}

// SetTx will set a label for the service. It can be used to set the key-value pair for the label,
// delete a label or update the value given the label's key.
func (s *Store) SetTx(ctx context.Context, tx txn.Tx, label *Label) error {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return err
	}

	n, err := label.Normalize()
	if err != nil {
		return err
	}

	if n.Value == "" {
		return s.delta.deleteKey(ctx, tx, label)
	}
	return s.delta.set(ctx, tx, label)
}

// FindAllByTarget finds all labels for a particular target. It returns all key-value pairs.
func (s *Store) FindAllByTarget(ctx context.Context, tx txn.Tx, t assignment.Target) ([]Label, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.findAllByTarget(ctx, tx, t)
}

func (s *Store) UniqueKeysTx(ctx context.Context, tx txn.Tx) ([]string, error) {
	err := permission.LimitCheckAny(ctx, permission.System, permission.User)
	if err != nil {
		return nil, err
	}

	return s.delta.uniqueKeys(ctx, tx)
}
