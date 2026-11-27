package rotation

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util"
	"github.com/target/goalert/validation"
)

// RotationRow is a rotations row on the Delta backend.
type RotationRow struct {
	ID               string     `parquet:"id"`
	Name             string     `parquet:"name"`
	Description      string     `parquet:"description"`
	Type             string     `parquet:"type"`
	Start            time.Time  `parquet:"start_time,timestamp(microsecond:utc)"`
	ShiftLength      int64      `parquet:"shift_length"`
	TimeZone         string     `parquet:"time_zone"`
	ParticipantCount int32      `parquet:"participant_count"`
	LastProcessed    *time.Time `parquet:"last_processed,optional,timestamp(microsecond:utc)"`
}

// ParticipantRow is a rotation_participants row on the Delta backend.
type ParticipantRow struct {
	ID         string `parquet:"id"`
	Position   int32  `parquet:"position"`
	RotationID string `parquet:"rotation_id"`
	UserID     string `parquet:"user_id"`
}

// StateRow is a rotation_state row on the Delta backend.
type StateRow struct {
	ID            int64     `parquet:"id"`
	Position      int32     `parquet:"position"`
	RotationID    string    `parquet:"rotation_id"`
	ParticipantID string    `parquet:"rotation_participant_id"`
	ShiftStart    time.Time `parquet:"shift_start,timestamp(microsecond:utc)"`
	Version       int32     `parquet:"version"`
}

// stateVersion is the version column's default.
const stateVersion = 2

// DeltaTables are the rotation store's tables on the Delta backend. Other
// packages declare their foreign keys to Rotations; the engine works with
// all of them.
//
// Write participants and state only through this package's functions: they
// do what the Postgres triggers did. A write to the tables themselves does
// not.
type DeltaTables struct {
	Rotations    *filestore.Table[RotationRow]    // keyed by id
	Participants *filestore.Table[ParticipantRow] // keyed by id
	State        *filestore.Table[StateRow]       // keyed by rotation_id

	ParticipantsByRotation *filestore.Index[ParticipantRow]
	ParticipantsByUser     *filestore.Index[ParticipantRow]

	byPosition *filestore.Unique[ParticipantRow] // by positionKey
}

func positionKey(rotationID string, position int) string {
	return rotationID + "/" + strconv.Itoa(position)
}

// DefineDeltaTables adds the rotation store's tables to the Delta backend's
// schema: rotations, rotation_participants and rotation_state, with the
// foreign keys between them.
//
// rotation_participants_rotation_id_position_key is checked when Postgres
// commits (DEFERRABLE INITIALLY DEFERRED). Here it holds at every step: the
// positions are changed in an order that never gives two participants the
// same one.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Rotations:    filestore.NewTable(s, "rotations", func(r RotationRow) string { return r.ID }),
		Participants: filestore.NewTable(s, "rotation_participants", func(p ParticipantRow) string { return p.ID }),
		State:        filestore.NewTable(s, "rotation_state", func(st StateRow) string { return st.RotationID }),
	}

	t.Rotations.Unique("rotations_name_unique", func(r RotationRow) (string, bool) { return r.Name, true })
	t.Rotations.Unique("rotations_name", func(r RotationRow) (string, bool) { return strings.ToLower(r.Name), true })

	t.ParticipantsByRotation = t.Participants.Index("idx_participant_rotation", func(p ParticipantRow) []string { return []string{p.RotationID} })
	t.ParticipantsByUser = t.Participants.Index("rotation_participants_user_id", func(p ParticipantRow) []string { return []string{p.UserID} })
	t.byPosition = t.Participants.Unique("rotation_participants_rotation_id_position_key", func(p ParticipantRow) (string, bool) {
		return positionKey(p.RotationID, int(p.Position)), true
	})

	t.State.Unique("rotation_state_uniq_id", func(st StateRow) (string, bool) { return filestore.IntKey(st.ID), true })

	// The order matters when a rotation is deleted: its state goes first,
	// since the state refers to a participant (Restrict).
	filestore.References(t.State, "rotation_state_rotation_id_fkey", t.Rotations,
		func(st StateRow) (string, bool) { return st.RotationID, true }, filestore.Cascade, nil)
	filestore.References(t.Participants, "rotation_participants_rotation_id_fkey", t.Rotations,
		func(p ParticipantRow) (string, bool) { return p.RotationID, true }, filestore.Cascade, nil)
	filestore.References(t.State, "rotation_state_rotation_participant_id_fkey", t.Participants,
		func(st StateRow) (string, bool) { return st.ParticipantID, true }, filestore.Restrict, nil)
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
//
// rotation_participants_user_id_fkey cascades on Postgres, where triggers
// then close the gap in the positions and advance the rotation. A cascade
// here would run none of that and leave a broken rotation (F5), so a user
// who still takes part in one can't be deleted: Store.RemoveUserDelta takes
// them out first, as the Postgres user store does itself.
func DeclareDeltaForeignKeys[U any](t *DeltaTables, users *filestore.Table[U]) {
	filestore.References(t.Participants, "rotation_participants_user_id_fkey", users,
		func(p ParticipantRow) (string, bool) { return p.UserID, true }, filestore.Restrict, nil)
}

// OrderedParticipants returns a rotation's participants in r, in order of
// position.
func (t *DeltaTables) OrderedParticipants(r filestore.Reader, rotationID string) []ParticipantRow {
	var parts []ParticipantRow
	t.ParticipantsByRotation.Ascend(r, rotationID, func(p ParticipantRow) bool {
		parts = append(parts, p)
		return true
	})
	slices.SortFunc(parts, func(a, b ParticipantRow) int { return cmp.Compare(a.Position, b.Position) })
	return parts
}

// UserRotationIDs returns the rotations that a user takes part in, in r.
func (t *DeltaTables) UserRotationIDs(r filestore.Reader, userID string) []string {
	var ids []string
	t.ParticipantsByUser.Ascend(r, userID, func(p ParticipantRow) bool {
		ids = append(ids, p.RotationID)
		return true
	})
	slices.Sort(ids)
	return slices.Compact(ids)
}

// ActiveUserID returns the user a rotation's state makes active in r, and
// false if it has none.
func (t *DeltaTables) ActiveUserID(r filestore.Reader, rotationID string) (string, bool) {
	st, ok := t.State.Get(r, rotationID)
	if !ok {
		return "", false
	}
	p, ok := t.Participants.Get(r, st.ParticipantID)
	return p.UserID, ok
}

// FavoriteFunc reports, in r, whether userID has a rotation as a favorite
// (user_favorites).
type FavoriteFunc func(r filestore.Reader, userID, rotationID string) bool

// LimitFunc returns a limit in r (config_limits), or -1 if there is none.
type LimitFunc func(r filestore.Reader) int

// ChangedFunc adds the rotation manager's job for a rotation, in the
// transaction that changed it (F2). It stands for fn_job_rotation, which the
// trg_track_rotation_* triggers run, and may be called several times for a
// rotation in one transaction; like fn_util_river_job, it should then add one
// job.
type ChangedFunc func(tx *filestore.Tx, rotationID string) error

// NewDeltaStore returns a rotation store on the Delta backend.
// maxParticipants is the participants_per_rotation limit. isFavorite may be
// nil (no rotation is a favorite), and so may changed (nothing follows
// rotation changes).
func NewDeltaStore(db txn.DB, tables *DeltaTables, isFavorite FavoriteFunc, maxParticipants LimitFunc, changed ChangedFunc) *Store {
	if isFavorite == nil {
		isFavorite = func(filestore.Reader, string, string) bool { return false }
	}
	if maxParticipants == nil {
		panic("rotation: NewDeltaStore needs the limit on participants per rotation") // or it would not be enforced (F8)
	}
	if changed == nil {
		changed = func(*filestore.Tx, string) error { return nil }
	}
	return &Store{delta: &deltaRotations{
		db: db, DeltaTables: tables, isFavorite: isFavorite, maxParticipants: maxParticipants, changed: changed,
	}}
}

// RemoveUserDelta takes a user out of every rotation, for the user store to
// call before it deletes the user, in the same transaction. Whoever else is
// active stays active, however often the user takes part (F5). If the user
// is active, the next remaining participant after them takes over, the first
// one after the last. A rotation left without participants ends. The time
// the shift started stays, so the next handoff does too.
func (s *Store) RemoveUserDelta(tx *filestore.Tx, userID string) error {
	return s.delta.removeUser(tx, dbID(userID))
}

type deltaRotations struct {
	db txn.DB
	*DeltaTables
	isFavorite      FavoriteFunc
	maxParticipants LimitFunc
	changed         ChangedFunc
}

// dbID returns a valid uuid the way Postgres stores it, so that ids match
// however they are written.
func dbID(id string) string { return strings.ToLower(id) }

// set sets the columns that the store's INSERT and UPDATE set.
func (r *RotationRow) set(rot *Rotation) {
	r.Name, r.Description, r.Type = rot.Name, rot.Description, string(rot.Type)
	r.Start, r.ShiftLength = rot.Start, int64(rot.ShiftLength)
	r.TimeZone = rot.Start.Location().String()
}

// check checks the rotations table's CHECK constraint.
func (r RotationRow) check() error {
	if r.ShiftLength <= 0 {
		return &filestore.CheckError{Table: "rotations", Constraint: "rotations_shift_length_check"}
	}
	return nil
}

func (r RotationRow) rotation(isFavorite bool) (*Rotation, error) {
	loc, err := util.LoadLocation(r.TimeZone)
	if err != nil {
		return nil, err
	}
	return &Rotation{
		ID: r.ID, Name: r.Name, Description: r.Description, Type: Type(r.Type),
		Start: r.Start.In(loc), ShiftLength: int(r.ShiftLength), isUserFavorite: isFavorite,
	}, nil
}

func (p ParticipantRow) participant() Participant {
	return Participant{ID: p.ID, RotationID: p.RotationID, Position: int(p.Position), Target: assignment.UserTarget(p.UserID)}
}

// favorite reports whether the rotation is a favorite of ctx's user, like
// the Postgres store's join with permission.UserNullUUID.
func (d *deltaRotations) favorite(ctx context.Context, r filestore.Reader, rotationID string) bool {
	userID := permission.UserNullUUID(ctx)
	return userID.Valid && d.isFavorite(r, userID.UUID.String(), rotationID)
}

func (d *deltaRotations) create(ctx context.Context, tx txn.Tx, rot *Rotation) error {
	row := RotationRow{ID: dbID(rot.ID)}
	row.set(rot)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if err := row.check(); err != nil {
			return err
		}
		if err := d.Rotations.Insert(s.Tx, row, "rotations_pkey"); err != nil {
			return err
		}
		return d.changed(s.Tx, row.ID)
	})
}

// update also starts the active shift anew, as the Postgres store's does.
func (d *deltaRotations) update(ctx context.Context, tx txn.Tx, rot *Rotation) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Rotations.Get(s.Reader, dbID(rot.ID))
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.set(rot)
		if err := row.check(); err != nil {
			return err
		}
		if err := d.Rotations.Put(s.Tx, row); err != nil {
			return err
		}
		if st, ok := d.State.Get(s.Reader, row.ID); ok {
			st.ShiftStart = s.Now
			if err := d.State.Put(s.Tx, st); err != nil {
				return err
			}
		}
		return d.changed(s.Tx, row.ID)
	})
}

// find returns sql.ErrNoRows for a missing rotation, as the Postgres store
// does. With forUpdate, the favorite flag is false, as there.
func (d *deltaRotations) find(ctx context.Context, tx txn.Tx, id string, forUpdate bool) (*Rotation, error) {
	var row RotationRow
	var fav bool
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var ok bool
		row, ok = d.Rotations.Get(s.Reader, dbID(id))
		if !ok {
			return sql.ErrNoRows
		}
		fav = !forUpdate && d.favorite(ctx, s.Reader, row.ID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return row.rotation(fav)
}

// foundRotation is a rotation and whether it is a favorite.
type foundRotation struct {
	row RotationRow
	fav bool
}

func toRotations(found []foundRotation, into []Rotation) ([]Rotation, error) {
	for _, f := range found {
		rot, err := f.row.rotation(f.fav)
		if err != nil {
			return nil, err
		}
		into = append(into, *rot)
	}
	return into, nil
}

func (d *deltaRotations) findMany(ctx context.Context, ids []string) ([]Rotation, error) {
	var found []foundRotation
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		found = nil
		seen := make(map[string]bool, len(ids))
		for _, id := range ids {
			id = dbID(id)
			if seen[id] {
				continue // like `where id = any($1)`
			}
			seen[id] = true
			if row, ok := d.Rotations.Get(s.Reader, id); ok {
				found = append(found, foundRotation{row: row, fav: d.favorite(ctx, s.Reader, id)})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return toRotations(found, make([]Rotation, 0, len(ids)))
}

func (d *deltaRotations) participantCount(ctx context.Context, id string) (int, error) {
	count := -1
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Rotations.Get(s.Reader, dbID(id))
		if !ok {
			return sql.ErrNoRows
		}
		count = int(row.ParticipantCount)
		return nil
	})
	return count, err
}

// deleteMany deletes rotations. Their state and participants go with them
// (ON DELETE CASCADE); nothing is left for the participants' triggers to
// repair, except that they report the rotation as changed.
func (d *deltaRotations) deleteMany(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			id = dbID(id)
			hadParts := d.ParticipantsByRotation.Count(s.Reader, id) > 0
			if _, err := d.Rotations.Delete(s.Tx, id); err != nil {
				return err
			}
			if !hadParts {
				continue
			}
			if err := d.changed(s.Tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaRotations) findAllParticipants(ctx context.Context, tx txn.Tx, rotationID string) ([]Participant, error) {
	var res []Participant
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		res = nil
		for _, p := range d.OrderedParticipants(s.Reader, dbID(rotationID)) {
			res = append(res, p.participant())
		}
		return nil
	})
	return res, err
}

// findParticipant returns a participant with only its id set along with
// sql.ErrNoRows for a missing one, as the Postgres store does.
func (d *deltaRotations) findParticipant(ctx context.Context, id string) (*Participant, error) {
	p := Participant{ID: id}
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Participants.Get(s.Reader, dbID(id))
		if !ok {
			return sql.ErrNoRows
		}
		p = row.participant()
		p.ID = id
		return nil
	})
	return &p, err
}

func (d *deltaRotations) state(ctx context.Context, tx txn.Tx, rotationID string) (*State, error) {
	var st State
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.State.Get(s.Reader, dbID(rotationID))
		if !ok {
			return ErrNoState
		}
		st = State{RotationID: rotationID, ParticipantID: row.ParticipantID, Position: int(row.Position), ShiftStart: row.ShiftStart}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// deleteState ends a rotation. Like the trigger, which runs only on updates,
// it does not report the rotation as changed.
func (d *deltaRotations) deleteState(ctx context.Context, tx txn.Tx, rotationID string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		_, err := d.State.Delete(s.Tx, dbID(rotationID))
		return err
	})
}

// setActiveIndex makes the participant at position active. Without a state
// it does nothing (an UPDATE of no rows). With one, a position that nobody
// has is refused: on Postgres it sets the state's participant to NULL, and
// that column's NOT NULL (SQLSTATE 23502) becomes the same message.
func (d *deltaRotations) setActiveIndex(ctx context.Context, tx txn.Tx, rotationID string, position int) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		st, ok := d.State.Get(s.Reader, dbID(rotationID))
		if !ok {
			return nil
		}
		p, ok := d.byPosition.Get(s.Reader, positionKey(st.RotationID, position))
		if !ok {
			return validation.NewFieldError("ActiveUserIndex", "invalid index for rotation")
		}
		if err := d.setActive(s.Tx, st, p); err != nil {
			return err
		}
		return d.changed(s.Tx, st.RotationID)
	})
}

// setActive makes p the active participant, at its position
// (trg_set_rot_state_pos_on_active_change). The shift's start stays.
func (d *deltaRotations) setActive(tx *filestore.Tx, st StateRow, p ParticipantRow) error {
	st.ParticipantID, st.Position = p.ID, p.Position
	return d.State.Put(tx, st)
}

func (d *deltaRotations) addUsers(ctx context.Context, tx txn.Tx, rotationID string, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, userID := range userIDs {
			err := d.addParticipant(s, uuid.NewString(), dbID(rotationID), dbID(userID))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// DeltaInsertParticipant adds a participant to the end of its rotation, as an
// INSERT into Postgres does, with its triggers. The store's own methods do so
// too.
func (s *Store) DeltaInsertParticipant(tx *filestore.Tx, p ParticipantRow) error {
	return s.delta.addParticipant(deltatxn.Scope{Tx: tx, Reader: tx, Now: tx.Now()}, p.ID, p.RotationID, p.UserID)
}

// addParticipant adds a user to the end of a rotation, and does what the
// triggers on an insert did, in their order.
func (d *deltaRotations) addParticipant(s deltatxn.Scope, id, rotationID, userID string) error {
	// trg_inc_rot_part_position_on_insert
	count := d.ParticipantsByRotation.Count(s.Reader, rotationID) + 1
	p := ParticipantRow{ID: id, RotationID: rotationID, UserID: userID, Position: int32(count - 1)}
	if err := d.Participants.Insert(s.Tx, p, "rotation_participants_pkey"); err != nil {
		return err
	}
	if err := d.addToCount(s.Tx, rotationID, 1); err != nil {
		return err // trg_incr_part_count_on_add
	}
	// trg_enforce_rotation_participant_limit
	if limit := d.maxParticipants(s.Reader); limit != -1 && count > limit {
		return &filestore.CheckError{
			Table: "rotation_participants", Constraint: "participants_per_rotation_limit",
			Message: "limit exceeded", HintText: "max=" + strconv.Itoa(limit),
		}
	}
	// trg_start_rotation_on_first_part_add: any participant starts a rotation
	// that has no state, from its first participant.
	if _, ok := d.State.Get(s.Reader, rotationID); !ok {
		first, ok := d.byPosition.Get(s.Reader, positionKey(rotationID, 0))
		if !ok {
			return errNoFirst
		}
		stateID, err := s.Tx.NextID("rotation_state_id_seq")
		if err != nil {
			return err
		}
		err = d.State.Insert(s.Tx, StateRow{
			ID: stateID, RotationID: rotationID, ParticipantID: first.ID, ShiftStart: s.Now, Version: stateVersion,
		}, "rotation_state_pkey")
		if err != nil {
			return err
		}
	}
	return d.changed(s.Tx, rotationID)
}

var errNoFirst = errors.New("rotation has participants, but none at position 0")

// addToCount keeps rotations.participant_count (trg_incr_part_count_on_add,
// trg_10_decr_part_count_on_del).
func (d *deltaRotations) addToCount(tx *filestore.Tx, rotationID string, n int32) error {
	rot, ok := d.Rotations.Get(tx, rotationID)
	if !ok {
		return nil
	}
	rot.ParticipantCount += n
	return d.Rotations.Put(tx, rot)
}

// updateParticipant replaces a participant, and does what the triggers on an
// update did. The caller checks for gaps once it is done (checkNoGaps).
func (d *deltaRotations) updateParticipant(tx *filestore.Tx, p ParticipantRow) error {
	old, ok := d.Participants.Get(tx, p.ID)
	if !ok {
		return nil
	}
	if old.RotationID != p.RotationID {
		return errGap("must not change rotation_id of existing participant")
	}
	if err := d.Participants.Put(tx, p); err != nil {
		return err
	}
	if old.Position == p.Position {
		return nil
	}
	// trg_set_rot_state_pos_on_part_reorder
	st, ok := d.State.Get(tx, p.RotationID)
	if !ok || st.ParticipantID != p.ID {
		return nil
	}
	return d.setActive(tx, st, p)
}

// checkNoGaps refuses a gap in the positions, which start at 0. Postgres
// checks it when it commits (trg_enforce_rot_part_position_no_gaps is
// DEFERRABLE INITIALLY DEFERRED); here every change to a rotation's
// participants checks it once it is complete, since each leaves none.
func (d *deltaRotations) checkNoGaps(r filestore.Reader, rotationID string) error {
	for i, p := range d.OrderedParticipants(r, rotationID) {
		if int(p.Position) != i {
			return errGap("must not have gap in participant positions")
		}
	}
	return nil
}

// errGap is what fn_enforce_rot_part_position_no_gaps raises.
func errGap(msg string) error {
	return &filestore.CheckError{Table: "rotation_participants", Constraint: "trg_enforce_rot_part_position_no_gaps", Message: msg}
}

// deleteParticipant deletes one participant, and does what the triggers on a
// delete did, in their order.
func (d *deltaRotations) deleteParticipant(tx *filestore.Tx, id string) error {
	p, ok := d.Participants.Get(tx, id)
	if !ok {
		return nil
	}
	if err := d.addToCount(tx, p.RotationID, -1); err != nil {
		return err // trg_10_decr_part_count_on_del
	}
	// trg_30_advance_or_end_rot_on_part_del: if it is active, the next one
	// takes over, or the first one after the last.
	if st, ok := d.State.Get(tx, p.RotationID); ok && st.ParticipantID == p.ID {
		next, ok := d.byPosition.Get(tx, positionKey(p.RotationID, int(p.Position)+1))
		if !ok && p.Position != 0 {
			next, ok = d.byPosition.Get(tx, positionKey(p.RotationID, 0))
		}
		var err error
		if ok {
			err = d.setActive(tx, st, next)
		} else {
			_, err = d.State.Delete(tx, st.RotationID)
		}
		if err != nil {
			return err
		}
	}
	if _, err := d.Participants.Delete(tx, p.ID); err != nil {
		return err
	}
	// trg_20_decr_rot_part_position_on_delete, lowest first so that each
	// takes the position just freed.
	for _, q := range d.OrderedParticipants(tx, p.RotationID) {
		if q.Position <= p.Position {
			continue
		}
		q.Position--
		if err := d.updateParticipant(tx, q); err != nil {
			return err
		}
	}
	return nil
}

// deleteParts deletes participants one at a time, each with its triggers.
// Any order gives the same result: if the active one goes, the next one that
// stays takes over. The last position goes first, which moves the fewest
// others. (One Postgres DELETE of several rows runs its triggers in no set
// order, and can fail on a gap or end a rotation that has participants left.
// Deleting the last ones, as the GraphQL layer does, gives the same result on
// both.)
func (d *deltaRotations) deleteParts(tx *filestore.Tx, parts []ParticipantRow) error {
	slices.SortFunc(parts, func(a, b ParticipantRow) int {
		return cmp.Or(cmp.Compare(a.RotationID, b.RotationID), cmp.Compare(b.Position, a.Position))
	})
	for _, p := range parts {
		if err := d.deleteParticipant(tx, p.ID); err != nil {
			return err
		}
	}
	for i, p := range parts {
		if i > 0 && parts[i-1].RotationID == p.RotationID {
			continue
		}
		if err := d.checkNoGaps(tx, p.RotationID); err != nil {
			return err
		}
		if err := d.changed(tx, p.RotationID); err != nil {
			return err
		}
	}
	return nil
}

func (d *deltaRotations) deleteParticipants(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var parts []ParticipantRow
		for _, id := range ids {
			if p, ok := d.Participants.Get(s.Reader, dbID(id)); ok {
				parts = append(parts, p)
			}
		}
		return d.deleteParts(s.Tx, parts)
	})
}

func (d *deltaRotations) updateParticipantUserID(ctx context.Context, tx txn.Tx, id, userID string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		p, ok := d.Participants.Get(s.Reader, dbID(id))
		if !ok {
			return nil
		}
		p.UserID = dbID(userID)
		if err := d.updateParticipant(s.Tx, p); err != nil {
			return err
		}
		if err := d.checkNoGaps(s.Reader, p.RotationID); err != nil {
			return err
		}
		return d.changed(s.Tx, p.RotationID)
	})
}

// removeUser deletes the user's participants. The Postgres user store moves
// the remaining users to the first participants and deletes the last ones
// instead; that gives the same users in the same order, since a
// participant's id is not visible outside.
func (d *deltaRotations) removeUser(tx *filestore.Tx, userID string) error {
	var parts []ParticipantRow
	d.ParticipantsByUser.Ascend(tx, userID, func(p ParticipantRow) bool {
		parts = append(parts, p)
		return true
	})
	if err := d.deleteParts(tx, parts); err != nil {
		return fmt.Errorf("remove user '%s' from rotations: %w", userID, err)
	}
	return nil
}

// search finds rotations like searchTemplate does. Names sort by their
// lowercase bytes, where Postgres uses the database's collation.
func (d *deltaRotations) search(ctx context.Context, opts *renderData) ([]Rotation, error) {
	omit := make(map[string]bool, len(opts.Omit))
	for _, id := range opts.Omit {
		omit[dbID(id)] = true
	}
	favUserID, afterName := dbID(opts.FavoritesUserID), strings.ToLower(opts.After.Name)
	match := newSearchMatcher(opts.Search)

	var found []foundRotation
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		found = nil
		consider := func(row RotationRow) {
			if omit[row.ID] || !match(row) {
				return
			}
			fav := favUserID != "" && d.isFavorite(s.Reader, favUserID, row.ID)
			if opts.FavoritesOnly && !fav {
				return
			}
			if afterName != "" {
				after := strings.ToLower(row.Name) > afterName
				if opts.FavoritesFirst && fav != opts.After.IsFavorite {
					after = !fav // the others come after the favorites
				}
				if !after {
					return
				}
			}
			found = append(found, foundRotation{row: row, fav: fav})
		}
		if len(opts.Only) == 0 {
			d.Rotations.Ascend(s.Reader, func(row RotationRow) bool {
				consider(row)
				return true
			})
			return nil
		}
		seen := make(map[string]bool, len(opts.Only))
		for _, id := range opts.Only {
			id = dbID(id)
			if row, ok := d.Rotations.Get(s.Reader, id); ok && !seen[id] {
				seen[id] = true
				consider(row)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(found, func(a, b foundRotation) int {
		if opts.FavoritesFirst && a.fav != b.fav {
			if a.fav {
				return -1
			}
			return 1
		}
		return cmp.Compare(strings.ToLower(a.row.Name), strings.ToLower(b.row.Name))
	})
	if len(found) > opts.Limit {
		found = found[:opts.Limit]
	}
	return toRotations(found, nil)
}

// newSearchMatcher returns whether a rotation matches a search: its name has
// words that begin with the search's words, in order (as Postgres's
// orderedPrefixSearch had it), or its description or name contains the search
// (contains). An empty search matches everything.
func newSearchMatcher(query string) func(row RotationRow) bool {
	if query == "" {
		return func(RotationRow) bool { return true }
	}
	lower := strings.ToLower(query)
	prefixes := words(lower)
	if len(prefixes) == 0 {
		prefixes = []string{""} // the pattern `\m`, which any word matches
	}
	return func(row RotationRow) bool {
		name := strings.ToLower(row.Name)
		if strings.Contains(name, lower) || strings.Contains(strings.ToLower(row.Description), lower) {
			return true
		}
		i := 0
		for _, w := range words(name) {
			if strings.HasPrefix(w, prefixes[i]) {
				i++
				if i == len(prefixes) {
					return true
				}
			}
		}
		return false
	}
}

// words splits s into its runs of letters and digits.
func words(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}
