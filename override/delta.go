package override

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// OverrideRow is a user_overrides row on the Delta backend.
type OverrideRow struct {
	ID           string    `parquet:"id"`
	AddUserID    *string   `parquet:"add_user_id,optional"`
	RemoveUserID *string   `parquet:"remove_user_id,optional"`
	Start        time.Time `parquet:"start_time,timestamp(microsecond:utc)"`
	End          time.Time `parquet:"end_time,timestamp(microsecond:utc)"`
	ScheduleID   string    `parquet:"tgt_schedule_id"`
}

// users returns the users that the override is about, each once.
func (o OverrideRow) users() []string {
	var ids []string
	if o.AddUserID != nil {
		ids = append(ids, *o.AddUserID)
	}
	if o.RemoveUserID != nil && (o.AddUserID == nil || *o.AddUserID != *o.RemoveUserID) {
		ids = append(ids, *o.RemoveUserID)
	}
	return ids
}

// check checks the table's CHECK constraints, in the order Postgres does.
func (o OverrideRow) check() error {
	var constraint string
	switch {
	case !o.End.After(o.Start):
		constraint = "user_overrides_check"
	case o.AddUserID == nil && o.RemoveUserID == nil:
		constraint = "user_overrides_check1"
	case o.AddUserID != nil && o.RemoveUserID != nil && *o.AddUserID == *o.RemoveUserID:
		constraint = "user_overrides_check2"
	default:
		return nil
	}
	return &filestore.CheckError{Table: "user_overrides", Constraint: constraint}
}

func (o OverrideRow) override() UserOverride {
	u := UserOverride{ID: o.ID, Start: o.Start, End: o.End, Target: assignment.ScheduleTarget(o.ScheduleID)}
	if o.AddUserID != nil {
		u.AddUserID = *o.AddUserID
	}
	if o.RemoveUserID != nil {
		u.RemoveUserID = *o.RemoveUserID
	}
	return u
}

// DeltaTables are the override store's tables on the Delta backend. The
// engine works with Overrides too; write to it only through the store, which
// enforces the rules.
type DeltaTables struct {
	Overrides *filestore.Table[OverrideRow] // keyed by id

	BySchedule *filestore.Index[OverrideRow]
	ByUser     *filestore.Index[OverrideRow] // the added and the removed user

	byScheduleUser *filestore.Index[OverrideRow] // by scheduleUserKey
}

func scheduleUserKey(scheduleID, userID string) string { return scheduleID + "/" + userID }

// DefineDeltaTables adds the override store's table to the Delta backend's
// schema: user_overrides, keyed by id (user_overrides_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Overrides: filestore.NewTable(s, "user_overrides", func(o OverrideRow) string { return o.ID }),
	}
	t.BySchedule = t.Overrides.Index("idx_user_overrides_schedule", func(o OverrideRow) []string { return []string{o.ScheduleID} })
	t.ByUser = t.Overrides.Index("user_overrides_user_id", func(o OverrideRow) []string { return o.users() })
	t.byScheduleUser = t.Overrides.Index("user_overrides_schedule_user_id", func(o OverrideRow) []string {
		keys := o.users()
		for i, id := range keys {
			keys[i] = scheduleUserKey(o.ScheduleID, id)
		}
		return keys
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[U, S any](t *DeltaTables, users *filestore.Table[U], schedules *filestore.Table[S]) {
	filestore.References(t.Overrides, "user_overrides_add_user_id_fkey", users,
		func(o OverrideRow) (string, bool) { return filestore.Ref(o.AddUserID) }, filestore.Cascade, nil)
	filestore.References(t.Overrides, "user_overrides_remove_user_id_fkey", users,
		func(o OverrideRow) (string, bool) { return filestore.Ref(o.RemoveUserID) }, filestore.Cascade, nil)
	filestore.References(t.Overrides, "user_overrides_tgt_schedule_id_fkey", schedules,
		func(o OverrideRow) (string, bool) { return o.ScheduleID, true }, filestore.Cascade, nil)
}

// FindOverlapping returns a schedule's overrides in r that overlap the time
// from start to end, as Postgres's OVERLAPS has it, by start time.
func (t *DeltaTables) FindOverlapping(r filestore.Reader, scheduleID string, start, end time.Time) []UserOverride {
	var rows []OverrideRow
	t.BySchedule.Ascend(r, scheduleID, func(o OverrideRow) bool {
		if overlaps(o.Start, o.End, start, end) {
			rows = append(rows, o)
		}
		return true
	})
	slices.SortFunc(rows, compareRows)
	var res []UserOverride
	for _, o := range rows {
		res = append(res, o.override())
	}
	return res
}

// compareRows orders overrides as the search does. Uuids compare the same as
// their text.
func compareRows(a, b OverrideRow) int {
	return cmp.Or(a.Start.Compare(b.Start), a.End.Compare(b.End), cmp.Compare(a.ID, b.ID))
}

// overlaps is Postgres's (start1, end1) OVERLAPS (start2, end2): a period
// includes its start but not its end, unless they are the same time, and can
// be given backwards.
func overlaps(start1, end1, start2, end2 time.Time) bool {
	if end1.Before(start1) {
		start1, end1 = end1, start1
	}
	if end2.Before(start2) {
		start2, end2 = end2, start2
	}
	switch start1.Compare(start2) {
	case -1:
		return start2.Before(end1)
	case 1:
		return start1.Before(end2)
	}
	return true
}

// LimitFunc returns a limit in r (config_limits), or -1 if there is none.
type LimitFunc func(r filestore.Reader) int

// NewDeltaStore returns an override store on the Delta backend.
// maxPerSchedule is the user_overrides_per_schedule limit.
func NewDeltaStore(db txn.DB, tables *DeltaTables, maxPerSchedule LimitFunc) *Store {
	if maxPerSchedule == nil {
		panic("override: NewDeltaStore needs the limit on overrides per schedule") // or it would not be enforced (F8)
	}
	return &Store{delta: &deltaOverrides{db: db, DeltaTables: tables, maxPerSchedule: maxPerSchedule}}
}

type deltaOverrides struct {
	db txn.DB
	*DeltaTables
	maxPerSchedule LimitFunc
}

// dbID returns a valid uuid the way Postgres stores it, so that ids match
// however they are written.
func dbID(id string) string { return strings.ToLower(id) }

// usec drops what Postgres timestamps can't hold.
func usec(t time.Time) time.Time { return t.Truncate(time.Microsecond) }

func toRow(o *UserOverride) OverrideRow {
	row := OverrideRow{ID: dbID(o.ID), Start: usec(o.Start), End: usec(o.End), ScheduleID: dbID(o.Target.TargetID())}
	if o.AddUserID != "" {
		id := dbID(o.AddUserID)
		row.AddUserID = &id
	}
	if o.RemoveUserID != "" {
		id := dbID(o.RemoveUserID)
		row.RemoveUserID = &id
	}
	return row
}

func (d *deltaOverrides) create(ctx context.Context, tx txn.Tx, o *UserOverride) error {
	row := toRow(o)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if err := row.check(); err != nil {
			return err
		}
		if err := d.Overrides.Insert(s.Tx, row, "user_overrides_pkey"); err != nil {
			return err
		}
		if err := d.checkConflict(s.Reader, row); err != nil {
			return err
		}
		return d.checkLimit(s, row.ScheduleID)
	})
}

func (d *deltaOverrides) update(ctx context.Context, tx txn.Tx, o *UserOverride) error {
	row := toRow(o)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if _, ok := d.Overrides.Get(s.Reader, row.ID); !ok {
			return nil // an UPDATE of no rows
		}
		if err := row.check(); err != nil {
			return err
		}
		if err := d.Overrides.Put(s.Tx, row); err != nil {
			return err
		}
		return d.checkConflict(s.Reader, row)
	})
}

// checkConflict refuses a second override of a user on a schedule at the
// same time (fn_enforce_user_overide_no_conflict). Postgres locks the table
// for it. Here, if another pod commits a conflicting override first, this
// transaction runs again and finds it (F4).
func (d *deltaOverrides) checkConflict(r filestore.Reader, row OverrideRow) error {
	var conflict string
	for _, userID := range row.users() {
		d.byScheduleUser.Ascend(r, scheduleUserKey(row.ScheduleID, userID), func(o OverrideRow) bool {
			if o.ID != row.ID && overlaps(o.Start, o.End, row.Start, row.End) {
				conflict = o.ID
			}
			return conflict == ""
		})
		if conflict != "" {
			return &filestore.CheckError{
				Table: "user_overrides", Constraint: "user_override_no_conflict_allowed",
				Message: "override conflict", HintText: "CONFLICTING_ID=" + conflict,
			}
		}
	}
	return nil
}

// checkLimit checks how many overrides a schedule has that have not ended
// (fn_enforce_user_override_schedule_limit).
func (d *deltaOverrides) checkLimit(s deltatxn.Scope, scheduleID string) error {
	limit := d.maxPerSchedule(s.Reader)
	if limit == -1 {
		return nil
	}
	count := 0
	d.BySchedule.Ascend(s.Reader, scheduleID, func(o OverrideRow) bool {
		if o.End.After(s.Now) {
			count++
		}
		return true
	})
	if count <= limit {
		return nil
	}
	return &filestore.CheckError{
		Table: "user_overrides", Constraint: "user_overrides_per_schedule_limit",
		Message: "limit exceeded", HintText: "max=" + strconv.Itoa(limit),
	}
}

func (d *deltaOverrides) delete(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Overrides.Delete(s.Tx, dbID(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// findOne returns nil for a missing override, as the Postgres store does.
func (d *deltaOverrides) findOne(ctx context.Context, tx txn.Tx, id string) (*UserOverride, error) {
	var res *UserOverride
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		res = nil
		if row, ok := d.Overrides.Get(s.Reader, dbID(id)); ok {
			o := row.override()
			res = &o
		}
		return nil
	})
	return res, err
}

func (d *deltaOverrides) findAll(ctx context.Context, start, end time.Time, t assignment.Target) ([]UserOverride, error) {
	var res []UserOverride
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		res = d.FindOverlapping(s.Reader, dbID(t.TargetID()), usec(start), usec(end))
		return nil
	})
	for i := range res {
		res[i].Target = t
	}
	return res, err
}

// searchMax is the query's LIMIT.
const searchMax = 150

// search finds overrides like the query OverrideSearch does.
func (d *deltaOverrides) search(ctx context.Context, tx txn.Tx, arg gadb.OverrideSearchParams, limit int) ([]UserOverride, error) {
	anyUser, addUser, removeUser, omit := idSet(arg.AnyUserID), idSet(arg.AddUserID), idSet(arg.RemoveUserID), idSet(arg.Omit)
	after := arg.AfterID.UUID.String()
	searchStart, searchEnd := usec(arg.SearchStart.Time), usec(arg.SearchEnd.Time)
	in := func(set map[string]bool, id *string) bool { return id != nil && set[*id] }

	var rows []OverrideRow
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		rows = nil
		var afterRow OverrideRow
		if arg.AfterID.Valid {
			var ok bool
			if afterRow, ok = d.Overrides.Get(s.Reader, after); !ok {
				return nil // nothing comes after a missing override
			}
		}
		seen := make(map[string]bool) // an override can be found by both its users
		consider := func(o OverrideRow) bool {
			switch {
			case seen[o.ID],
				omit[o.ID],
				arg.ScheduleID.Valid && o.ScheduleID != arg.ScheduleID.UUID.String(),
				len(anyUser) > 0 && !in(anyUser, o.AddUserID) && !in(anyUser, o.RemoveUserID),
				len(addUser) > 0 && !in(addUser, o.AddUserID),
				len(removeUser) > 0 && !in(removeUser, o.RemoveUserID),
				arg.SearchStart.Valid && !o.End.After(searchStart),
				arg.SearchEnd.Valid && o.Start.After(searchEnd),
				arg.AfterID.Valid && compareRows(o, afterRow) <= 0:
			default:
				seen[o.ID] = true
				rows = append(rows, o)
			}
			return true
		}

		users := anyUser
		if len(users) == 0 {
			users = addUser
		}
		if len(users) == 0 {
			users = removeUser
		}
		switch {
		case arg.ScheduleID.Valid:
			d.BySchedule.Ascend(s.Reader, arg.ScheduleID.UUID.String(), consider)
		case len(users) > 0:
			for id := range users {
				d.ByUser.Ascend(s.Reader, id, consider)
			}
		default:
			d.Overrides.Ascend(s.Reader, consider)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(rows, compareRows)
	if limit <= 0 || limit > searchMax {
		limit = searchMax
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	result := make([]UserOverride, len(rows))
	for i, o := range rows {
		result[i] = o.override()
	}
	return result, nil
}

func idSet(ids []uuid.UUID) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id.String()] = true
	}
	return set
}
