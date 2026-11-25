package schedule

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/jsonutil"
)

// scheduleRow is a schedules row on the Delta backend.
type scheduleRow struct {
	ID            string     `parquet:"id"`
	Name          string     `parquet:"name"`
	Description   string     `parquet:"description"`
	TimeZone      string     `parquet:"time_zone"`
	LastProcessed *time.Time `parquet:"last_processed,optional,timestamp(microsecond:utc)"`
}

// dataRow is a schedule_data row on the Delta backend: the JSON document
// (Data) with a schedule's temporary schedules and on-call notification
// rules.
type dataRow struct {
	ScheduleID    string     `parquet:"schedule_id"`
	ID            int64      `parquet:"id"`
	Data          []byte     `parquet:"data"`
	LastCleanupAt *time.Time `parquet:"last_cleanup_at,optional,timestamp(microsecond:utc)"`
}

// DeltaTables are the schedule store's tables on the Delta backend. Other
// packages declare their foreign keys to Schedules.
type DeltaTables struct {
	Schedules *filestore.Table[scheduleRow]
	Data      *filestore.Table[dataRow]
}

// DefineDeltaTables adds the schedule store's tables to the Delta backend's
// schema: schedules, keyed by id (schedules_pkey), and schedule_data, keyed
// by schedule_id (schedule_data_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		Schedules: filestore.NewTable(s, "schedules", func(r scheduleRow) string { return r.ID }),
		Data:      filestore.NewTable(s, "schedule_data", func(r dataRow) string { return r.ScheduleID }),
	}
	t.Schedules.Unique("schedules_name_key", func(r scheduleRow) (string, bool) { return r.Name, true })
	t.Schedules.Unique("schedules_name", func(r scheduleRow) (string, bool) { return strings.ToLower(r.Name), true })
	t.Data.Unique("schedule_data_id_key", func(r dataRow) (string, bool) { return filestore.IntKey(r.ID), true })
	filestore.References(t.Data, "schedule_data_schedule_id_fkey", t.Schedules,
		func(r dataRow) (string, bool) { return r.ScheduleID, true }, filestore.Cascade, nil)
	return t
}

// TimeZone returns the name of a schedule's time zone, and whether the
// schedule exists.
func (t *DeltaTables) TimeZone(r filestore.Reader, scheduleID string) (string, bool) {
	row, ok := t.Schedules.Get(r, scheduleID)
	return row.TimeZone, ok
}

// EachSchedule calls fn for every schedule in r, by id, until it returns false.
func (t *DeltaTables) EachSchedule(r filestore.Reader, fn func(id, timeZone string) bool) {
	t.Schedules.Ascend(r, func(row scheduleRow) bool { return fn(row.ID, row.TimeZone) })
}

// EachData calls fn for every schedule in r that has data, by id, until it
// returns false. lastCleanup is nil if its data was never cleaned up.
func (t *DeltaTables) EachData(r filestore.Reader, fn func(scheduleID string, lastCleanup *time.Time) bool) {
	t.Data.Ascend(r, func(row dataRow) bool { return fn(row.ScheduleID, row.LastCleanupAt) })
}

// ScheduleData returns a schedule's data in r. A schedule without any has an
// empty one.
func (t *DeltaTables) ScheduleData(r filestore.Reader, scheduleID uuid.UUID) (*Data, error) {
	row, _ := t.Data.Get(r, scheduleID.String())
	return parseData(row.Data)
}

func parseData(raw []byte) (*Data, error) {
	var data Data
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

// UpdateData changes a schedule's data with apply, and adds its row if there
// is none. It reads and writes in one transaction, so it is made on top of
// every earlier change (F4), and it keeps the fields of the document that
// Data does not have. apply runs again when the transaction does, so it must
// not change what it captured.
func (t *DeltaTables) UpdateData(tx *filestore.Tx, scheduleID uuid.UUID, apply func(data *Data) error) error {
	row, ok := t.Data.Get(tx, scheduleID.String())
	if !ok {
		id, err := tx.NextID("schedule_data_id_seq")
		if err != nil {
			return err
		}
		row = dataRow{ScheduleID: scheduleID.String(), ID: id}
	}
	data, err := parseData(row.Data)
	if err != nil {
		return err
	}
	if err := apply(data); err != nil {
		return err
	}
	row.Data, err = jsonutil.Apply(row.Data, data)
	if err != nil {
		return err
	}
	return t.Data.Put(tx, row)
}

// DeltaLookups are what the schedule store needs from other packages'
// tables. A method that needs a lookup that is missing returns
// deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// IsFavorite reports whether userID has the schedule as a favorite
	// (user_favorites). It may be nil, in which case no schedule is one.
	IsFavorite func(r filestore.Reader, userID, scheduleID string) bool

	// UserExists reports whether there is a user with the id (users).
	UserExists func(r filestore.Reader, userID string) bool

	// SchedulesOfUser returns the ids of the schedules with a rule for the
	// user, or for a rotation that the user takes part in (schedule_rules,
	// rotation_participants).
	SchedulesOfUser func(r filestore.Reader, userID string) []string
}

// NewDeltaStore returns a schedule store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, lookups DeltaLookups) *Store {
	return &Store{delta: &deltaSchedules{db: db, DeltaTables: tables, lookups: lookups}}
}

type deltaSchedules struct {
	db txn.DB
	*DeltaTables
	lookups DeltaLookups
}

func (d *deltaSchedules) isFavorite(r filestore.Reader, userID, scheduleID string) bool {
	return d.lookups.IsFavorite != nil && userID != "" && d.lookups.IsFavorite(r, userID, scheduleID)
}

func (r scheduleRow) schedule(isFavorite bool) (Schedule, error) {
	loc, err := util.LoadLocation(r.TimeZone)
	if err != nil {
		return Schedule{}, err
	}
	return Schedule{ID: r.ID, Name: r.Name, Description: r.Description, TimeZone: loc, isUserFavorite: isFavorite}, nil
}

// currentUser is the id of the context's user, or "" for none.
func currentUser(ctx context.Context) string {
	id := permission.UserNullUUID(ctx)
	if !id.Valid {
		return ""
	}
	return id.UUID.String()
}

// findMany returns the schedules that exist, each once, like `where id =
// any($1)`, in the order of ids.
func (d *deltaSchedules) findMany(ctx context.Context, tx txn.Tx, ids []uuid.UUID) ([]Schedule, error) {
	var result []Schedule
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		result = make([]Schedule, 0, len(ids))
		me := currentUser(ctx)
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			row, ok := d.Schedules.Get(s.Reader, id.String())
			if !ok {
				continue
			}
			sched, err := row.schedule(d.isFavorite(s.Reader, me, row.ID))
			if err != nil {
				return err
			}
			result = append(result, sched)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// findManyByUser returns the schedules that can have the user on call
// through a rule, by id. Like the Postgres store, it leaves out the time
// zone.
func (d *deltaSchedules) findManyByUser(ctx context.Context, tx txn.Tx, userID uuid.NullUUID) ([]Schedule, error) {
	if !userID.Valid {
		return nil, nil // like `tgt_user_id = NULL`
	}
	if d.lookups.SchedulesOfUser == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var result []Schedule
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		result = nil
		ids := slices.Clone(d.lookups.SchedulesOfUser(s.Reader, userID.UUID.String()))
		slices.Sort(ids)
		for _, id := range slices.Compact(ids) {
			if row, ok := d.Schedules.Get(s.Reader, id); ok {
				result = append(result, Schedule{ID: row.ID, Name: row.Name, Description: row.Description})
			}
		}
		return nil
	})
	return result, err
}

func (d *deltaSchedules) create(ctx context.Context, tx txn.Tx, id uuid.UUID, n *Schedule) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.Schedules.Insert(s.Tx, scheduleRow{
			ID: id.String(), Name: n.Name, Description: n.Description, TimeZone: n.TimeZone.String(),
		}, "schedules_pkey")
	})
}

func (d *deltaSchedules) update(ctx context.Context, tx txn.Tx, id uuid.UUID, n *Schedule) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Schedules.Get(s.Reader, id.String())
		if !ok {
			return nil // an UPDATE of no rows
		}
		row.Name, row.Description, row.TimeZone = n.Name, n.Description, n.TimeZone.String()
		return d.Schedules.Put(s.Tx, row)
	})
}

// findAll returns every schedule, by id.
func (d *deltaSchedules) findAll(ctx context.Context) ([]Schedule, error) {
	var result []Schedule
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		var err error
		d.Schedules.Ascend(s.Reader, func(row scheduleRow) bool {
			var sched Schedule
			if sched, err = row.schedule(false); err != nil {
				err = fmt.Errorf("parse scanned time zone: %w", err)
				return false
			}
			result = append(result, sched)
			return true
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// findOne returns sql.ErrNoRows for a missing schedule, as the Postgres
// store does. The favorite flag is the context's user's, if withFavorite.
// Reading for an update needs no lock: if another pod changes the schedule
// first, the transaction runs again (F4).
func (d *deltaSchedules) findOne(ctx context.Context, tx txn.Tx, id uuid.UUID, withFavorite bool) (*Schedule, error) {
	var sched Schedule
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Schedules.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		var err error
		sched, err = row.schedule(withFavorite && d.isFavorite(s.Reader, currentUser(ctx), row.ID))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &sched, nil
}

// deleteMany deletes the schedules that exist. What refers to them goes with
// them, through the foreign keys (F7).
func (d *deltaSchedules) deleteMany(ctx context.Context, tx txn.Tx, ids []uuid.UUID) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if _, err := d.Schedules.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaSchedules) scheduleData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID) (*Data, error) {
	var data *Data
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) (err error) {
		data, err = d.ScheduleData(s.Reader, scheduleID)
		return err
	})
	return data, err
}

func (d *deltaSchedules) updateData(ctx context.Context, tx txn.Tx, scheduleID uuid.UUID, apply func(data *Data) error) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		return d.UpdateData(s.Tx, scheduleID, apply)
	})
}

// userChecker tells which users exist in a reader.
type userChecker struct {
	r      filestore.Reader
	exists func(r filestore.Reader, userID string) bool
}

var _ user.ExistanceChecker = userChecker{}

func (c userChecker) UserExistsUUID(id uuid.UUID) bool { return c.exists(c.r, id.String()) }
func (c userChecker) UserExistsString(idStr string) bool {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return false
	}
	return c.UserExistsUUID(id)
}

// userExists returns a checker of the users in tx, or in this pod's current
// view if tx is nil.
func (d *deltaSchedules) userExists(ctx context.Context, tx txn.Tx) (user.ExistanceChecker, error) {
	if d.lookups.UserExists == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var c userChecker
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		c = userChecker{r: s.Reader, exists: d.lookups.UserExists}
		return nil
	})
	return c, err
}
