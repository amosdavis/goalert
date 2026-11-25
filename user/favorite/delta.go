package favorite

import (
	"context"

	"github.com/google/uuid"
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
	r := favRow{ID: id, UserID: dbID(userID)}
	v := dbID(tgt.TargetID())
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
	return dbID(userID) + "/" + tgt.TargetType().String() + "/" + dbID(tgt.TargetID())
}

// dbID returns a valid uuid the way Postgres stores it, so that ids match
// however they are written.
func dbID(id string) string {
	u, err := uuid.Parse(id)
	if err != nil {
		return id
	}
	return u.String()
}

// DeltaTables are the favorites store's tables on the Delta backend.
type DeltaTables struct {
	favorites *filestore.Table[favRow]
	byUser    *filestore.Index[favRow]
	byTarget  *filestore.Unique[favRow]
}

// DefineDeltaTables adds user_favorites to the Delta backend's schema, with
// its foreign keys to users.
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

// DeclareDeltaForeignKeys declares this package's foreign keys to services,
// schedules, rotations and escalation policies.
func DeclareDeltaForeignKeys[Svc, Sched, Rot, EP any](t *DeltaTables, services *filestore.Table[Svc], schedules *filestore.Table[Sched], rotations *filestore.Table[Rot], policies *filestore.Table[EP]) {
	filestore.References(t.favorites, "user_favorites_tgt_service_id_fkey", services,
		func(r favRow) (string, bool) { return filestore.Ref(r.TgtServiceID) }, filestore.Cascade, nil)
	filestore.References(t.favorites, "user_favorites_tgt_schedule_id_fkey", schedules,
		func(r favRow) (string, bool) { return filestore.Ref(r.TgtScheduleID) }, filestore.Cascade, nil)
	filestore.References(t.favorites, "user_favorites_tgt_rotation_id_fkey", rotations,
		func(r favRow) (string, bool) { return filestore.Ref(r.TgtRotationID) }, filestore.Cascade, nil)
	filestore.References(t.favorites, "user_favorites_tgt_escalation_policy_id_fkey", policies,
		func(r favRow) (string, bool) { return filestore.Ref(r.TgtEscalationPolID) }, filestore.Cascade, nil)
}

// IsFavorite reports, in r, whether userID has the target as a favorite.
func (t *DeltaTables) IsFavorite(r filestore.Reader, userID string, tgt assignment.Target) bool {
	_, ok := t.byTarget.Get(r, targetKey(userID, tgt))
	return ok
}

// The stores of the targets take one of these each.

// IsUserFavorite is IsFavorite for a user.
func (t *DeltaTables) IsUserFavorite(r filestore.Reader, userID, id string) bool {
	return t.IsFavorite(r, userID, assignment.UserTarget(id))
}

// IsScheduleFavorite is IsFavorite for a schedule.
func (t *DeltaTables) IsScheduleFavorite(r filestore.Reader, userID, id string) bool {
	return t.IsFavorite(r, userID, assignment.ScheduleTarget(id))
}

// IsRotationFavorite is IsFavorite for a rotation.
func (t *DeltaTables) IsRotationFavorite(r filestore.Reader, userID, id string) bool {
	return t.IsFavorite(r, userID, assignment.RotationTarget(id))
}

// IsEscalationPolicyFavorite is IsFavorite for an escalation policy.
func (t *DeltaTables) IsEscalationPolicyFavorite(r filestore.Reader, userID, id string) bool {
	return t.IsFavorite(r, userID, assignment.EscalationPolicyTarget(id))
}

// FavoriteServices returns, in r, the IDs of the services that userID has as
// favorites.
func (t *DeltaTables) FavoriteServices(r filestore.Reader, userID string) []string {
	var ids []string
	t.byUser.Ascend(r, dbID(userID), func(row favRow) bool {
		if row.TgtServiceID != nil {
			ids = append(ids, *row.TgtServiceID)
		}
		return true
	})
	return ids
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

// unset removes the user's favorite.
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
		d.byUser.Ascend(s.Reader, dbID(userID), func(row favRow) bool {
			if tgt, ok := row.target(); ok && allow(tgt.TargetType()) {
				targets = append(targets, tgt)
			}
			return true
		})
		return nil
	})
	return targets, err
}
