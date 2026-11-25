package oncall

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/override"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/schedule/rule"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util"
)

// ScheduleOnCallRow is a schedule_on_call_users row on the Delta backend: a
// user was on call for a schedule from the start to the end time, or still is
// if there is no end time (an open row).
type ScheduleOnCallRow struct {
	ID         int64      `parquet:"id"`
	ScheduleID string     `parquet:"schedule_id"`
	UserID     string     `parquet:"user_id"`
	StartTime  time.Time  `parquet:"start_time,timestamp(microsecond:utc)"`
	EndTime    *time.Time `parquet:"end_time,optional,timestamp(microsecond:utc)"`
}

// StepOnCallRow is an ep_step_on_call_users row on the Delta backend: like
// ScheduleOnCallRow, for a step of an escalation policy.
type StepOnCallRow struct {
	ID        int64      `parquet:"id"`
	StepID    string     `parquet:"ep_step_id"`
	UserID    string     `parquet:"user_id"`
	StartTime time.Time  `parquet:"start_time,timestamp(microsecond:utc)"`
	EndTime   *time.Time `parquet:"end_time,optional,timestamp(microsecond:utc)"`
}

// allOpen is the index key of every open row, next to keys that are uuids.
const allOpen = "open"

// DeltaTables are the on-call tables on the Delta backend. The engine writes
// them, with the Start and End methods, which keep their constraints.
type DeltaTables struct {
	ScheduleOnCall *filestore.Table[ScheduleOnCallRow]
	StepOnCall     *filestore.Table[StepOnCallRow]

	schedOpen       *filestore.Unique[ScheduleOnCallRow] // open rows by schedule and user
	schedOpenBy     *filestore.Index[ScheduleOnCallRow]  // open rows by schedule, and all
	schedBySchedule *filestore.Index[ScheduleOnCallRow]

	stepOpen   *filestore.Unique[StepOnCallRow] // open rows by user and step
	stepOpenBy *filestore.Index[StepOnCallRow]  // open rows by step, by user, and all
}

// DefineDeltaTables adds the on-call tables to the Delta backend's schema:
// schedule_on_call_users, keyed by id (schedule_on_call_users_uniq_id), and
// ep_step_on_call_users, keyed by id (ep_step_on_call_users_uniq_id).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		ScheduleOnCall: filestore.NewTable(s, "schedule_on_call_users", func(r ScheduleOnCallRow) string { return filestore.IntKey(r.ID) }),
		StepOnCall:     filestore.NewTable(s, "ep_step_on_call_users", func(r StepOnCallRow) string { return filestore.IntKey(r.ID) }),
	}

	t.schedOpen = t.ScheduleOnCall.Unique("idx_schedule_on_call_once", func(r ScheduleOnCallRow) (string, bool) {
		return pairKey(r.ScheduleID, r.UserID), r.EndTime == nil
	})
	t.schedOpenBy = t.ScheduleOnCall.Index("schedule_on_call_users_open", func(r ScheduleOnCallRow) []string {
		if r.EndTime != nil {
			return nil
		}
		return []string{allOpen, r.ScheduleID}
	})
	t.schedBySchedule = t.ScheduleOnCall.Index("idx_sched_oncall_times", func(r ScheduleOnCallRow) []string { return []string{r.ScheduleID} })

	t.stepOpen = t.StepOnCall.Unique("idx_ep_step_on_call", func(r StepOnCallRow) (string, bool) {
		return pairKey(r.UserID, r.StepID), r.EndTime == nil
	})
	t.stepOpenBy = t.StepOnCall.Index("ep_step_on_call_users_open", func(r StepOnCallRow) []string {
		if r.EndTime != nil {
			return nil
		}
		return []string{allOpen, stepKey(r.StepID), userKey(r.UserID)}
	})
	return t
}

func pairKey(a, b string) string   { return a + "/" + b }
func stepKey(stepID string) string { return "step/" + stepID }
func userKey(userID string) string { return "user/" + userID }

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables; steps is escalation_policy_steps.
func DeclareDeltaForeignKeys[S, P, U any](t *DeltaTables, schedules *filestore.Table[S], steps *filestore.Table[P], users *filestore.Table[U]) {
	filestore.References(t.ScheduleOnCall, "schedule_on_call_users_schedule_id_fkey", schedules,
		func(r ScheduleOnCallRow) (string, bool) { return r.ScheduleID, true }, filestore.Cascade, nil)
	filestore.References(t.ScheduleOnCall, "schedule_on_call_users_user_id_fkey", users,
		func(r ScheduleOnCallRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
	filestore.References(t.StepOnCall, "ep_step_on_call_users_ep_step_id_fkey", steps,
		func(r StepOnCallRow) (string, bool) { return r.StepID, true }, filestore.Cascade, nil)
	filestore.References(t.StepOnCall, "ep_step_on_call_users_user_id_fkey", users,
		func(r StepOnCallRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// PutScheduleOnCall writes a schedule_on_call_users row. Use it rather than
// the table's Put: it holds schedule_on_call_users_check, that a shift ends
// after it starts (F8). Rows keep times to the microsecond.
func (t *DeltaTables) PutScheduleOnCall(tx *filestore.Tx, row ScheduleOnCallRow) error {
	if row.EndTime != nil && !row.EndTime.Truncate(time.Microsecond).After(row.StartTime.Truncate(time.Microsecond)) {
		return &filestore.CheckError{Table: "schedule_on_call_users", Constraint: "schedule_on_call_users_check"}
	}
	return t.ScheduleOnCall.Put(tx, row)
}

// StartScheduleOnCall records that the user goes on call for the schedule
// now. One who already is on call breaks idx_schedule_on_call_once, as on
// Postgres.
func (t *DeltaTables) StartScheduleOnCall(tx *filestore.Tx, scheduleID, userID string) error {
	id, err := tx.NextID("schedule_on_call_users_id_seq")
	if err != nil {
		return err
	}
	return t.ScheduleOnCall.Insert(tx, ScheduleOnCallRow{
		ID: id, ScheduleID: scheduleID, UserID: userID, StartTime: tx.Now(),
	}, "schedule_on_call_users_uniq_id")
}

// EndScheduleOnCall records that the user goes off call for the schedule now.
// It does nothing for a user who isn't on call. A shift that started at this very
// time, as one can when another pod's clock was ahead (F13), gets the shortest
// length there is.
func (t *DeltaTables) EndScheduleOnCall(tx *filestore.Tx, scheduleID, userID string) error {
	row, ok := t.schedOpen.Get(tx, pairKey(scheduleID, userID))
	if !ok {
		return nil
	}
	end := tx.Now()
	if !end.After(row.StartTime) {
		end = row.StartTime.Add(time.Microsecond)
	}
	row.EndTime = &end
	return t.PutScheduleOnCall(tx, row)
}

// StartStepOnCall records that the user goes on call for the escalation
// policy step now, and reports whether that is new: for one who already is on
// call it does nothing (ON CONFLICT DO NOTHING).
func (t *DeltaTables) StartStepOnCall(tx *filestore.Tx, stepID, userID string) (bool, error) {
	if _, ok := t.stepOpen.Get(tx, pairKey(userID, stepID)); ok {
		return false, nil
	}
	id, err := tx.NextID("ep_step_on_call_users_id_seq")
	if err != nil {
		return false, err
	}
	err = t.StepOnCall.Insert(tx, StepOnCallRow{ID: id, StepID: stepID, UserID: userID, StartTime: tx.Now()}, "ep_step_on_call_users_uniq_id")
	return err == nil, err
}

// EndStepOnCall records that the user goes off call for the escalation policy
// step now. It does nothing for a user who isn't on call.
func (t *DeltaTables) EndStepOnCall(tx *filestore.Tx, stepID, userID string) error {
	row, ok := t.stepOpen.Get(tx, pairKey(userID, stepID))
	if !ok {
		return nil
	}
	now := tx.Now()
	row.EndTime = &now
	return t.StepOnCall.Put(tx, row)
}

// EachOpenSchedule calls fn for every open schedule_on_call_users row, oldest
// first, until it returns false.
func (t *DeltaTables) EachOpenSchedule(r filestore.Reader, fn func(ScheduleOnCallRow) bool) {
	t.schedOpenBy.Ascend(r, allOpen, fn)
}

// EachOpenStep calls fn for every open ep_step_on_call_users row, oldest
// first, until it returns false.
func (t *DeltaTables) EachOpenStep(r filestore.Reader, fn func(StepOnCallRow) bool) {
	t.stepOpenBy.Ascend(r, allOpen, fn)
}

// ScheduleUsers returns the ids of the users on call for a schedule, each
// once, oldest shift first.
func (t *DeltaTables) ScheduleUsers(r filestore.Reader, scheduleID string) []string {
	var ids []string
	t.schedOpenBy.Ascend(r, scheduleID, func(row ScheduleOnCallRow) bool {
		ids = append(ids, row.UserID)
		return true
	})
	return ids
}

// StepUsers returns the ids of the users on call for an escalation policy
// step, each once, oldest shift first.
func (t *DeltaTables) StepUsers(r filestore.Reader, stepID string) []string {
	var ids []string
	t.stepOpenBy.Ascend(r, stepKey(stepID), func(row StepOnCallRow) bool {
		ids = append(ids, row.UserID)
		return true
	})
	return ids
}

// UserSteps returns the ids of the escalation policy steps that a user is on
// call for, each once, oldest shift first.
func (t *DeltaTables) UserSteps(r filestore.Reader, userID string) []string {
	var ids []string
	t.stepOpenBy.Ascend(r, userKey(userID), func(row StepOnCallRow) bool {
		ids = append(ids, row.StepID)
		return true
	})
	return ids
}

// DeltaStep is a step of a service's escalation policy.
type DeltaStep struct {
	ID     string
	Number int
}

// DeltaRotation is a rotation with its state and participants.
type DeltaRotation struct {
	Type        rotation.Type
	Start       time.Time
	ShiftLength int
	TimeZone    string

	Position   int       // rotation_state.position
	ShiftStart time.Time // rotation_state.shift_start

	UserIDs []string // the participants' users, by position
}

// DeltaLookups are what the on-call store needs from other packages' tables.
// A method that needs a lookup that is missing returns deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// ScheduleTimeZone returns the name of a schedule's time zone, and
	// whether the schedule exists (schedules).
	ScheduleTimeZone func(r filestore.Reader, scheduleID string) (string, bool)

	// UserName returns a user's name, and whether the user exists (users).
	UserName func(r filestore.Reader, userID string) (string, bool)

	// ServiceSteps returns the steps of a service's escalation policy, or
	// none if there is no such service (services, escalation_policy_steps).
	ServiceSteps func(r filestore.Reader, serviceID string) []DeltaStep

	// Rotation returns a rotation, and false if it doesn't exist or has no
	// state, which means it has no participants (rotations, rotation_state,
	// rotation_participants).
	Rotation func(r filestore.Reader, rotationID string) (DeltaRotation, bool)

	// Overrides returns a schedule's overrides, at least those that have not
	// ended (user_overrides). Their targets are not used.
	Overrides func(r filestore.Reader, scheduleID string) []override.UserOverride
}

// NewDeltaStore returns an on-call store on the Delta backend. ruleStore and
// schedStore are the Delta backend's too.
func NewDeltaStore(db txn.DB, tables *DeltaTables, ruleStore *rule.Store, schedStore *schedule.Store, lookups DeltaLookups) *Store {
	return &Store{
		delta:      &deltaOnCall{db: db, DeltaTables: tables, lookups: lookups},
		ruleStore:  ruleStore,
		schedStore: schedStore,
		histLim:    make(chan struct{}, 3),
	}
}

type deltaOnCall struct {
	db txn.DB
	*DeltaTables
	lookups DeltaLookups
}

func (d *deltaOnCall) usersByService(ctx context.Context, serviceID string) ([]ServiceOnCallUser, error) {
	if d.lookups.ServiceSteps == nil || d.lookups.UserName == nil {
		return nil, deltatxn.ErrNotPorted
	}
	id, err := uuid.Parse(serviceID)
	if err != nil {
		return nil, err
	}
	var result []ServiceOnCallUser
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		type user struct {
			ServiceOnCallUser
			row StepOnCallRow
		}
		var users []user
		for _, step := range d.lookups.ServiceSteps(s.Reader, id.String()) {
			d.stepOpenBy.Ascend(s.Reader, stepKey(step.ID), func(row StepOnCallRow) bool {
				if name, ok := d.lookups.UserName(s.Reader, row.UserID); ok {
					users = append(users, user{ServiceOnCallUser{StepNumber: step.Number, UserID: row.UserID, UserName: name}, row})
				}
				return true
			})
		}
		slices.SortFunc(users, func(a, b user) int { // order by step_number, start_time
			return cmp.Or(cmp.Compare(a.StepNumber, b.StepNumber), a.row.StartTime.Compare(b.row.StartTime), cmp.Compare(a.row.ID, b.row.ID))
		})
		for _, u := range users {
			result = append(result, u.ServiceOnCallUser)
		}
		return nil
	})
	return result, err
}

func (d *deltaOnCall) usersBySchedule(ctx context.Context, scheduleID string) ([]ScheduleOnCallUser, error) {
	if d.lookups.UserName == nil {
		return nil, deltatxn.ErrNotPorted
	}
	id, err := uuid.Parse(scheduleID)
	if err != nil {
		return nil, err
	}
	var result []ScheduleOnCallUser
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		for _, userID := range d.ScheduleUsers(s.Reader, id.String()) {
			if name, ok := d.lookups.UserName(s.Reader, userID); ok {
				result = append(result, ScheduleOnCallUser{ID: userID, Name: name})
			}
		}
		return nil
	})
	return result, err
}

// errRange is Postgres's error for a range that ends before it starts.
var errRange = errors.New("range lower bound must be less than or equal to range upper bound")

// shifts returns the on-call history that overlaps the time from start to
// end, like the schedOnCall query: shifts of up to a minute are left out.
func (t *DeltaTables) shifts(r filestore.Reader, scheduleID string, start, end time.Time) ([]Shift, error) {
	if start.After(end) {
		return nil, errRange
	}
	if !start.Before(end) {
		return nil, nil // an empty range overlaps nothing
	}
	var shifts []Shift
	t.schedBySchedule.Ascend(r, scheduleID, func(row ScheduleOnCallRow) bool {
		if !row.StartTime.Before(end) {
			return true
		}
		s := Shift{UserID: row.UserID, Start: row.StartTime}
		if row.EndTime != nil {
			if !start.Before(*row.EndTime) || row.EndTime.Sub(row.StartTime) <= time.Minute {
				return true
			}
			s.End = *row.EndTime
		}
		shifts = append(shifts, s)
		return true
	})
	return shifts, nil
}

// overlaps is the SQL operator OVERLAPS, which takes the bounds of a period
// in either order, and an empty period as one instant.
func overlaps(aStart, aEnd, bStart, bEnd time.Time) bool {
	if aEnd.Before(aStart) {
		aStart, aEnd = aEnd, aStart
	}
	if bEnd.Before(bStart) {
		bStart, bEnd = bEnd, bStart
	}
	switch aStart.Compare(bStart) {
	case 1:
		return aStart.Before(bEnd)
	case -1:
		return bStart.Before(aEnd)
	}
	return true
}

// resolved returns the rotation as the calculation takes it.
func (rot DeltaRotation) resolved(id string) (*ResolvedRotation, error) {
	loc, err := util.LoadLocation(rot.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("load time zone info: %w", err)
	}
	return &ResolvedRotation{
		Rotation:     rotation.Rotation{ID: id, Type: rot.Type, Start: rot.Start.In(loc), ShiftLength: rot.ShiftLength},
		CurrentIndex: rot.Position,
		CurrentStart: rot.ShiftStart,
		Users:        slices.Clone(rot.UserIDs),
	}, nil
}

// historyState reads what the shifts are calculated from, all from one view
// (F10), like the repeatable-read transaction on Postgres.
func (d *deltaOnCall) historyState(ctx context.Context, store *Store, scheduleID string, start, end time.Time) (*state, error) {
	if d.lookups.ScheduleTimeZone == nil || d.lookups.Rotation == nil || d.lookups.Overrides == nil {
		return nil, deltatxn.ErrNotPorted
	}
	schedID, err := uuid.Parse(scheduleID)
	if err != nil {
		return nil, fmt.Errorf("parse schedule ID: %w", err)
	}
	var st *state
	err = d.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		return deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
			next := &state{now: s.Now}

			tz, ok := d.lookups.ScheduleTimeZone(s.Reader, schedID.String())
			if !ok {
				return fmt.Errorf("lookup schedule time zone: %w", sql.ErrNoRows)
			}

			rawRules, err := store.ruleStore.FindAllTx(ctx, tx, schedID.String())
			if err != nil {
				return fmt.Errorf("lookup schedule rules: %w", err)
			}
			rots := make(map[string]*ResolvedRotation)
			for _, r := range rawRules {
				if r.Target.TargetType() != assignment.TargetTypeRotation || rots[r.Target.TargetID()] != nil {
					continue
				}
				id := r.Target.TargetID()
				rot, ok := d.lookups.Rotation(s.Reader, id)
				if !ok {
					continue
				}
				if rots[id], err = rot.resolved(id); err != nil {
					return err
				}
			}
			next.rules = resolveRules(rawRules, rots)

			next.history, err = d.shifts(s.Reader, schedID.String(), start, end)
			if err != nil {
				return fmt.Errorf("lookup on-call history: %w", err)
			}

			for _, ov := range d.lookups.Overrides(s.Reader, schedID.String()) {
				if ov.End.After(s.Now) && overlaps(start, end, ov.Start, ov.End) {
					next.overrides = append(next.overrides, ov)
				}
			}
			slices.SortFunc(next.overrides, func(a, b override.UserOverride) int {
				return cmp.Or(a.Start.Compare(b.Start), strings.Compare(a.ID, b.ID))
			})

			next.tempScheds, err = store.schedStore.TemporarySchedules(ctx, tx, schedID)
			if err != nil {
				return fmt.Errorf("lookup temporary schedules: %w", err)
			}
			next.loc, err = util.LoadLocation(tz)
			if err != nil {
				return fmt.Errorf("load time zone info: %w", err)
			}
			st = next
			return nil
		})
	})
	return st, err
}
