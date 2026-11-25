package favorite

import (
	"context"

	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
)

// favRow is a user_favorites row on the Delta backend; exactly one target
// is set.
type favRow struct {
	ID                 int64   `parquet:"id"`
	UserID             string  `parquet:"user_id"`
	TgtServiceID       *string `parquet:"tgt_service_id,optional"`
	TgtScheduleID      *string `parquet:"tgt_schedule_id,optional"`
	TgtRotationID      *string `parquet:"tgt_rotation_id,optional"`
	TgtEscalationPolID *string `parquet:"tgt_escalation_policy_id,optional"`
	TgtUserID          *string `parquet:"tgt_user_id,optional"`
}

func (r favRow) target() (assignment.Target, bool) {
	switch {
	case r.TgtServiceID != nil:
		return assignment.ServiceTarget(*r.TgtServiceID), true
	case r.TgtScheduleID != nil:
		return assignment.ScheduleTarget(*r.TgtScheduleID), true
	case r.TgtRotationID != nil:
		return assignment.RotationTarget(*r.TgtRotationID), true
	case r.TgtEscalationPolID != nil:
		return assignment.EscalationPolicyTarget(*r.TgtEscalationPolID), true
	case r.TgtUserID != nil:
		return assignment.UserTarget(*r.TgtUserID), true
	}
	return nil, false
}

func newFavRow(id int64, userID string, tgt assignment.Target) favRow {
	r := favRow{ID: id, UserID: userID}
	v := tgt.TargetID()
	switch tgt.TargetType() {
	case assignment.TargetTypeService:
		r.TgtServiceID = &v
	case assignment.TargetTypeSchedule:
		r.TgtScheduleID = &v
	case assignment.TargetTypeRotation:
		r.TgtRotationID = &v
	case assignment.TargetTypeEscalationPolicy:
		r.TgtEscalationPolID = &v
	case assignment.TargetTypeUser:
		r.TgtUserID = &v
	}
	return r
}

// targetKey is a row's key in the unique index of user and target, which
// stands for the five (user_id, tgt_...) unique constraints.
func targetKey(userID string, tgt assignment.Target) string {
	return userID + "/" + tgt.TargetType().String() + "/" + tgt.TargetID()
}

// DeltaTables are the favorites store's tables on the Delta backend.
type DeltaTables struct {
	favorites *filestore.Table[favRow]
	byUser    *filestore.Index[favRow]
	byTarget  *filestore.Unique[favRow]
}

// DefineDeltaTables adds user_favorites to the Delta backend's schema, with
// its foreign keys to users. Those to services, schedules, rotations and
// escalation policies are added when those tables are ported.
func DefineDeltaTables(s *filestore.Schema, users *user.DeltaTables) *DeltaTables {
	t := &DeltaTables{}
	t.favorites = filestore.NewTable(s, "user_favorites", func(r favRow) string { return filestore.IntKey(r.ID) })
	t.byUser = t.favorites.Index("user_favorites_user_id", func(r favRow) []string { return []string{r.UserID} })
	t.byTarget = t.favorites.Unique("user_favorites_user_id_tgt_key", func(r favRow) (string, bool) {
		tgt, ok := r.target()
		if !ok {
			return "", false
		}
		return targetKey(r.UserID, tgt), true
	})
	filestore.References(t.favorites, "user_favorites_user_id_fkey", users.Users,
		func(r favRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
	filestore.References(t.favorites, "user_favorites_tgt_user_id_fkey", users.Users,
		func(r favRow) (string, bool) { return filestore.Ref(r.TgtUserID) }, filestore.Cascade, nil)
	return t
}

// IsUserFavorite reports, in r, whether userID has targetUserID as a
// favorite. It is the user store's user.FavoriteFunc.
func (t *DeltaTables) IsUserFavorite(r filestore.Reader, userID, targetUserID string) bool {
	_, ok := t.byTarget.Get(r, targetKey(userID, assignment.UserTarget(targetUserID)))
	return ok
}

// NewDeltaStore returns a favorites store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables) *Store {
	return &Store{delta: &deltaFavorites{db: db, DeltaTables: tables}}
}

type deltaFavorites struct {
	db txn.DB
	*DeltaTables
}

// set adds a favorite, or does nothing if it is one (ON CONFLICT DO
// NOTHING).
func (d *deltaFavorites) set(ctx context.Context, tx txn.Tx, userID string, tgt assignment.Target) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if _, ok := d.byTarget.Get(s.Reader, targetKey(userID, tgt)); ok {
			return nil
		}
		id, err := s.Tx.NextID("user_favorites_id_seq")
		if err != nil {
			return err
		}
		return d.favorites.Insert(s.Tx, newFavRow(id, userID, tgt), "user_favorites_uniq_id")
	})
}

// unset removes the user's favorite. (The Postgres query's AND/OR
// precedence removes everyone's favorite of a schedule, rotation,
// escalation policy or user; this removes only the user's own.)
func (d *deltaFavorites) unset(ctx context.Context, tx txn.Tx, userID string, tgt assignment.Target) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.byTarget.Get(s.Reader, targetKey(userID, tgt))
		if !ok {
			return nil
		}
		_, err := d.favorites.Delete(s.Tx, filestore.IntKey(row.ID))
		return err
	})
}

// findAll returns the user's favorites of the allowed types, oldest first.
func (d *deltaFavorites) findAll(ctx context.Context, tx txn.Tx, userID string, allow func(assignment.TargetType) bool) ([]assignment.Target, error) {
	targets := []assignment.Target{} // as the Postgres store returns
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		targets = []assignment.Target{}
		d.byUser.Ascend(s.Reader, userID, func(row favRow) bool {
			if tgt, ok := row.target(); ok && allow(tgt.TargetType()) {
				targets = append(targets, tgt)
			}
			return true
		})
		return nil
	})
	return targets, err
}
