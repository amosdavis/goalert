package rule

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/timeutil"
)

// ruleRow is a schedule_rules row on the Delta backend; exactly one target
// is set. The start and end times are the text of a Postgres time of day,
// such as "15:04:05", in the schedule's time zone.
type ruleRow struct {
	ID            string    `parquet:"id"`
	ScheduleID    string    `parquet:"schedule_id"`
	Sunday        bool      `parquet:"sunday"`
	Monday        bool      `parquet:"monday"`
	Tuesday       bool      `parquet:"tuesday"`
	Wednesday     bool      `parquet:"wednesday"`
	Thursday      bool      `parquet:"thursday"`
	Friday        bool      `parquet:"friday"`
	Saturday      bool      `parquet:"saturday"`
	StartTime     string    `parquet:"start_time"`
	EndTime       string    `parquet:"end_time"`
	TgtUserID     *string   `parquet:"tgt_user_id,optional"`
	TgtRotationID *string   `parquet:"tgt_rotation_id,optional"`
	IsActive      bool      `parquet:"is_active"`
	CreatedAt     time.Time `parquet:"created_at,timestamp(microsecond:utc)"`
}

// DeltaTables are the rule store's tables on the Delta backend.
type DeltaTables struct {
	Rules      *filestore.Table[ruleRow]
	bySchedule *filestore.Index[ruleRow]
	byTarget   *filestore.Index[ruleRow]
}

// DefineDeltaTables adds the rule store's tables to the Delta backend's
// schema: schedule_rules, keyed by id (schedule_rules_pkey).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Rules: filestore.NewTable(s, "schedule_rules", func(r ruleRow) string { return r.ID })}
	t.bySchedule = t.Rules.Index("idx_rule_schedule", func(r ruleRow) []string { return []string{r.ScheduleID} })
	t.byTarget = t.Rules.Index("idx_schedule_rules_tgt", func(r ruleRow) []string {
		if tgt, err := r.target(); err == nil {
			return []string{targetKey(tgt)}
		}
		return nil
	})
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[S, R, U any](t *DeltaTables, schedules *filestore.Table[S], rotations *filestore.Table[R], users *filestore.Table[U]) {
	filestore.References(t.Rules, "schedule_rules_schedule_id_fkey", schedules,
		func(r ruleRow) (string, bool) { return r.ScheduleID, true }, filestore.Cascade, nil)
	filestore.References(t.Rules, "schedule_rules_tgt_rotation_id_fkey", rotations,
		func(r ruleRow) (string, bool) { return filestore.Ref(r.TgtRotationID) }, filestore.Cascade, nil)
	filestore.References(t.Rules, "schedule_rules_tgt_user_id_fkey", users,
		func(r ruleRow) (string, bool) { return filestore.Ref(r.TgtUserID) }, filestore.Cascade, nil)
}

func targetKey(tgt assignment.Target) string {
	return tgt.TargetType().String() + "/" + tgt.TargetID()
}

// target is the row's user or rotation, with the same error as the Postgres
// store for a row without one.
func (r ruleRow) target() (assignment.Target, error) {
	switch {
	case r.TgtUserID != nil:
		return assignment.UserTarget(*r.TgtUserID), nil
	case r.TgtRotationID != nil:
		return assignment.RotationTarget(*r.TgtRotationID), nil
	}
	return nil, errNoKnownTarget
}

// Rule is the row as the store returns it: without CreatedAt, which the
// Postgres store doesn't read either.
func (r ruleRow) Rule() (Rule, error) {
	n := Rule{ID: r.ID, ScheduleID: r.ScheduleID}
	for day, on := range []bool{r.Sunday, r.Monday, r.Tuesday, r.Wednesday, r.Thursday, r.Friday, r.Saturday} {
		n.SetDay(time.Weekday(day), on)
	}
	var err error
	if n.Start, err = timeutil.ParseClock(r.StartTime); err != nil {
		return Rule{}, fmt.Errorf("start time: %w", err)
	}
	if n.End, err = timeutil.ParseClock(r.EndTime); err != nil {
		return Rule{}, fmt.Errorf("end time: %w", err)
	}
	if n.Target, err = r.target(); err != nil {
		return Rule{}, err
	}
	return n, nil
}

// clockColumn is c as a row has it. Postgres refuses a time of day it can't
// hold when the rule is written, and so does this.
func clockColumn(c timeutil.Clock) (string, error) {
	if c < 0 || time.Duration(c) >= 24*time.Hour {
		return "", fmt.Errorf("invalid time of day %q", c.String())
	}
	return c.String() + ":00", nil
}

// setRule sets the columns that the store writes, like readFields.
func (r *ruleRow) setRule(n *Rule) error {
	schedID, err := uuid.Parse(n.ScheduleID)
	if err != nil {
		return err
	}
	r.ScheduleID = schedID.String()
	r.Sunday, r.Monday, r.Tuesday = n.Day(time.Sunday), n.Day(time.Monday), n.Day(time.Tuesday)
	r.Wednesday, r.Thursday = n.Day(time.Wednesday), n.Day(time.Thursday)
	r.Friday, r.Saturday = n.Day(time.Friday), n.Day(time.Saturday)
	if r.StartTime, err = clockColumn(n.Start); err != nil {
		return err
	}
	if r.EndTime, err = clockColumn(n.End); err != nil {
		return err
	}

	r.TgtUserID, r.TgtRotationID = nil, nil
	typ := n.Target.TargetType()
	if typ != assignment.TargetTypeUser && typ != assignment.TargetTypeRotation {
		return nil // no target, which the check constraint refuses
	}
	tgtID, err := uuid.Parse(n.Target.TargetID())
	if err != nil {
		return err
	}
	id := tgtID.String()
	if typ == assignment.TargetTypeUser {
		r.TgtUserID = &id
	} else {
		r.TgtRotationID = &id
	}
	return nil
}

// check is schedule_rules_check: exactly one target.
func (r ruleRow) check() error {
	if (r.TgtUserID == nil) == (r.TgtRotationID == nil) {
		return &filestore.CheckError{Table: "schedule_rules", Constraint: "schedule_rules_check"}
	}
	return nil
}

// ScheduleIDsByTarget returns the ids of the schedules with a rule for any
// of the targets, sorted and each once.
func (t *DeltaTables) ScheduleIDsByTarget(r filestore.Reader, targets ...assignment.Target) []string {
	var ids []string
	for _, tgt := range targets {
		t.byTarget.Ascend(r, targetKey(tgt), func(row ruleRow) bool {
			ids = append(ids, row.ScheduleID)
			return true
		})
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// DeltaLimits return the configured limits (config_limits), or -1 for none.
// Adding a rule without them returns deltatxn.ErrNotPorted.
type DeltaLimits struct {
	RulesPerSchedule   func(r filestore.Reader) int
	TargetsPerSchedule func(r filestore.Reader) int
}

// NewDeltaStore returns a rule store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, limits DeltaLimits) *Store {
	return &Store{delta: &deltaRules{db: db, DeltaTables: tables, limits: limits}}
}

type deltaRules struct {
	db txn.DB
	*DeltaTables
	limits DeltaLimits
}

// checkLimits stands for trg_enforce_schedule_rule_limit and
// trg_enforce_schedule_target_limit, which count the new row too. It runs
// before the row is written, so nothing is left behind if it fails.
func (d *deltaRules) checkLimits(r filestore.Reader, row ruleRow) error {
	if d.limits.RulesPerSchedule == nil || d.limits.TargetsPerSchedule == nil {
		return deltatxn.ErrNotPorted
	}
	rules := 1
	targets := make(map[string]bool)
	add := func(row ruleRow) {
		if tgt, err := row.target(); err == nil {
			targets[targetKey(tgt)] = true
		}
	}
	add(row)
	d.bySchedule.Ascend(r, row.ScheduleID, func(row ruleRow) bool {
		rules++
		add(row)
		return true
	})
	if err := checkLimit("rules_per_schedule_limit", d.limits.RulesPerSchedule(r), rules); err != nil {
		return err
	}
	return checkLimit("targets_per_schedule_limit", d.limits.TargetsPerSchedule(r), len(targets))
}

func checkLimit(constraint string, max, count int) error {
	if max == -1 || count <= max {
		return nil
	}
	return &filestore.CheckError{
		Table: "schedule_rules", Constraint: constraint,
		Message: "limit exceeded", HintText: fmt.Sprintf("max=%d", max),
	}
}

func (d *deltaRules) add(ctx context.Context, tx txn.Tx, n *Rule) error {
	row := ruleRow{ID: n.ID}
	if err := row.setRule(n); err != nil {
		return err
	}
	if err := row.check(); err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if err := d.checkLimits(s.Reader, row); err != nil {
			return err
		}
		added := row
		added.CreatedAt = s.Now
		return d.Rules.Insert(s.Tx, added, "schedule_rules_pkey")
	})
}

// update sets what the Postgres store's update does. The limits hold only
// for new rules, as there.
func (d *deltaRules) update(ctx context.Context, tx txn.Tx, n *Rule) error {
	id, err := uuid.Parse(n.ID)
	if err != nil {
		return err
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Rules.Get(s.Reader, id.String())
		if err := row.setRule(n); err != nil {
			return err // also without a row, like the query's casts
		}
		if !ok {
			return nil // an UPDATE of no rows
		}
		if err := row.check(); err != nil {
			return err
		}
		return d.Rules.Put(s.Tx, row)
	})
}

func (d *deltaRules) deleteMany(ctx context.Context, tx txn.Tx, ids []string) error {
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, idStr := range ids {
			id, err := uuid.Parse(idStr)
			if err != nil {
				return err
			}
			if _, err := d.Rules.Delete(s.Tx, id.String()); err != nil {
				return err
			}
		}
		return nil
	})
}

// findBySchedule returns a schedule's rules, oldest first, that match keep.
func (d *deltaRules) findBySchedule(ctx context.Context, tx txn.Tx, scheduleID string, keep func(ruleRow) bool) ([]Rule, error) {
	schedID, err := uuid.Parse(scheduleID)
	if err != nil {
		return nil, err
	}
	var result []Rule
	err = deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		result = nil
		var rows []ruleRow
		d.bySchedule.Ascend(s.Reader, schedID.String(), func(row ruleRow) bool {
			if keep(row) {
				rows = append(rows, row)
			}
			return true
		})
		slices.SortFunc(rows, func(a, b ruleRow) int { // order by created_at, id
			if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
				return c
			}
			return strings.Compare(a.ID, b.ID)
		})
		for _, row := range rows {
			r, err := row.Rule()
			if err != nil {
				return err
			}
			result = append(result, r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *deltaRules) findAll(ctx context.Context, tx txn.Tx, scheduleID string) ([]Rule, error) {
	return d.findBySchedule(ctx, tx, scheduleID, func(ruleRow) bool { return true })
}

func (d *deltaRules) findByTarget(ctx context.Context, tx txn.Tx, scheduleID string, target assignment.Target) ([]Rule, error) {
	id, err := uuid.Parse(target.TargetID())
	if err != nil {
		return nil, err
	}
	want := id.String()
	return d.findBySchedule(ctx, tx, scheduleID, func(row ruleRow) bool {
		ref := row.TgtUserID
		if target.TargetType() == assignment.TargetTypeRotation {
			ref = row.TgtRotationID
		}
		return ref != nil && *ref == want
	})
}
