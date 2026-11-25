package rotationmanager

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/log"
)

// DeltaDB manages rotations on the Delta backend. It looks at every rotation in
// every cycle, so each is as its participants and the time say, whatever
// happened to it meanwhile (F52).
type DeltaDB struct{ t *rotation.DeltaTables }

// NewDeltaDB returns a rotation manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables) *DeltaDB { return &DeltaDB{t: t.Rotation} }

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.RotationManager" }

// UpdateAll starts the rotations that have participants, ends those that have
// none, and advances those whose shift is over. A rotation that can't be
// updated is logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	var rows []rotation.RotationRow
	db.t.Rotations.Ascend(tx, func(r rotation.RotationRow) bool {
		rows = append(rows, r)
		return true
	})
	err := enginetables.Each(tx, rows, func(row rotation.RotationRow) error {
		return errors.Wrapf(db.update(ctx, tx, row), "update rotation %s", row.ID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func (db *DeltaDB) update(ctx context.Context, tx *filestore.Tx, row rotation.RotationRow) error {
	state, started := db.t.State.Get(tx, row.ID)
	if row.ParticipantCount == 0 && !started {
		return nil
	}
	parts := db.t.OrderedParticipants(tx, row.ID)
	if len(parts) == 0 {
		// no participants, but we have a position, so clear it
		_, err := db.t.State.Delete(tx, row.ID)
		return err
	}
	if !started {
		// no state, but we have participants, so start at the beginning
		id, err := tx.NextID("rotation_state_id_seq")
		if err != nil {
			return err
		}
		return db.t.State.Insert(tx, rotation.StateRow{
			ID: id, RotationID: row.ID, ParticipantID: parts[0].ID, ShiftStart: tx.Now(), Version: 2,
		}, "rotation_state_pkey")
	}

	loc, err := util.LoadLocation(row.TimeZone)
	if err != nil {
		return fmt.Errorf("load location: %w", err)
	}
	adv, err := calcAdvance(ctx, tx.Now(), &rotation.Rotation{
		ID: row.ID, Type: rotation.Type(row.Type), Start: row.Start.In(loc), ShiftLength: int(row.ShiftLength),
	}, rotState{
		ShiftStart: state.ShiftStart.In(loc), Position: int(state.Position), Version: int(state.Version),
	}, len(parts))
	if err != nil {
		return fmt.Errorf("calc advance: %w", err)
	}
	if adv == nil {
		return nil // no advancement needed
	}
	state.Position, state.ParticipantID = int32(adv.newPosition), parts[adv.newPosition].ID
	state.ShiftStart, state.Version = tx.Now(), 2
	return db.t.State.Put(tx, state)
}
