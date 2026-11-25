package calsub

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
	"github.com/target/goalert/validation/validate"
)

// subRow is a user_calendar_subscriptions row on the Delta backend.
type subRow struct {
	ID         string     `parquet:"id"`
	Name       string     `parquet:"name"`
	UserID     string     `parquet:"user_id"`
	ScheduleID string     `parquet:"schedule_id"`
	Config     []byte     `parquet:"config"`
	Disabled   bool       `parquet:"disabled"`
	CreatedAt  time.Time  `parquet:"created_at,timestamp(microsecond:utc)"`
	LastAccess *time.Time `parquet:"last_access,optional,timestamp(microsecond:utc)"`
	LastUpdate time.Time  `parquet:"last_update,timestamp(microsecond:utc)"`
}

// accessInterval is how old the recorded last access of a subscription must
// be before another one is recorded.
const accessInterval = time.Minute

// DeltaTables are the calendar subscription store's tables on the Delta
// backend.
type DeltaTables struct {
	Subscriptions *filestore.Table[subRow]

	byUser *filestore.Index[subRow]
}

// DefineDeltaTables adds the calendar subscription store's tables to the
// Delta backend's schema: user_calendar_subscriptions, keyed by id
// (user_calendar_subscriptions_pkey). Its rows are small, and a write of last
// accesses changes many of them, so it has few buckets (F39).
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.Subscriptions = filestore.NewTable(s, "user_calendar_subscriptions", func(r subRow) string { return r.ID }).Buckets(4)
	t.Subscriptions.Unique("user_calendar_subscriptions_name_schedule_id_user_id_key", func(r subRow) (string, bool) {
		return r.ScheduleID + "/" + r.UserID + "/" + r.Name, true
	})
	t.byUser = t.Subscriptions.Index("user_calendar_subscriptions_user_id", func(r subRow) []string { return []string{r.UserID} })
	return t
}

// DeclareDeltaForeignKeys declares the calendar subscription store's foreign
// keys to users and schedules.
func DeclareDeltaForeignKeys[U, S any](t *DeltaTables, users *filestore.Table[U], schedules *filestore.Table[S]) {
	filestore.References(t.Subscriptions, "user_calendar_subscriptions_schedule_id_fkey", schedules,
		func(r subRow) (string, bool) { return r.ScheduleID, true }, filestore.Cascade, nil)
	filestore.References(t.Subscriptions, "user_calendar_subscriptions_user_id_fkey", users,
		func(r subRow) (string, bool) { return r.UserID, true }, filestore.Cascade, nil)
}

// DisableUnused disables up to limit subscriptions that were neither accessed
// nor updated since before, in order of their IDs, and returns how many. It
// is the cleanup manager's CleanupMgrDisableOldCalSub.
func (t *DeltaTables) DisableUnused(tx *filestore.Tx, before time.Time, limit int) (int, error) {
	var unused []subRow
	t.Subscriptions.Ascend(tx, func(r subRow) bool {
		if len(unused) >= limit {
			return false
		}
		if !r.Disabled && r.LastUpdate.Before(before) && (r.LastAccess == nil || r.LastAccess.Before(before)) {
			unused = append(unused, r)
		}
		return true
	})
	for _, r := range unused {
		r.Disabled = true
		if err := t.Subscriptions.Put(tx, r); err != nil {
			return 0, err
		}
	}
	return len(unused), nil
}

// DeltaLookups are what the store needs of other packages' tables, in r.
type DeltaLookups struct {
	// ScheduleName returns the name of a schedule (schedules), and false if
	// there is no such schedule.
	ScheduleName func(r filestore.Reader, scheduleID string) (string, bool)
	// UserName returns the name of a user (users), and false if there is no
	// such user.
	UserName func(r filestore.Reader, userID string) (string, bool)
	// MaxPerUser returns how many subscriptions a user may have
	// (config_limits, limit.CalendarSubscriptionsPerUser), -1 for no limit.
	MaxPerUser func(r filestore.Reader) int
}

// NewDeltaStore returns a calendar subscription store on the Delta backend.
// Its Shutdown writes the last accesses that aren't written yet.
func NewDeltaStore(db txn.DB, tables *DeltaTables, apiKeyring keyring.Keyring, oc *oncall.Store, lookups DeltaLookups) *Store {
	d := &deltaStore{
		db: db, DeltaTables: tables, lookups: lookups,
		check: authdelta.NewChecker(db, authdelta.MaxStale),
	}
	d.access = authdelta.NewRecorder(authdelta.RecordInterval, "calendar subscription access", d.writeAccess)
	return &Store{keys: apiKeyring, oc: oc, delta: d}
}

// Shutdown writes the last accesses that aren't written yet.
func (s *Store) Shutdown(ctx context.Context) error {
	if s == nil || s.delta == nil {
		return nil
	}
	return s.delta.access.Flush(ctx)
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
	lookups DeltaLookups
	check   *authdelta.Checker
	access  *authdelta.Recorder[time.Time]
}

func (r subRow) subscription() (*Subscription, error) {
	cs := Subscription{ID: r.ID, Name: r.Name, UserID: r.UserID, Disabled: r.Disabled, ScheduleID: r.ScheduleID}
	if r.LastAccess != nil {
		cs.LastAccess = *r.LastAccess
	}
	if err := json.Unmarshal(r.Config, &cs.Config); err != nil {
		return nil, err
	}
	return &cs, nil
}

// authUser returns the user of a subscription that isn't disabled and was
// created at that time (to the second), or sql.ErrNoRows. The access is
// recorded, but not committed: a pod writes the accesses of all
// subscriptions together, at most once a minute.
func (d *deltaStore) authUser(ctx context.Context, id uuid.UUID, createdAt time.Time) (uuid.UUID, error) {
	var row subRow
	var now time.Time
	err := d.check.Read(ctx, func(_ txn.Tx, s deltatxn.Scope) error {
		var ok bool
		row, ok = d.Subscriptions.Get(s.Reader, id.String())
		if !ok || row.Disabled || !row.CreatedAt.Truncate(time.Second).Equal(createdAt) {
			return sql.ErrNoRows
		}
		now = s.Now
		return nil
	})
	if err != nil {
		return uuid.UUID{}, err
	}
	userID, err := uuid.Parse(row.UserID)
	if err != nil {
		return uuid.UUID{}, err
	}

	last := row.LastAccess
	if at, ok := d.access.Pending(row.ID); ok {
		last = &at
	}
	if last == nil || now.Sub(*last) >= accessInterval {
		d.access.Add(row.ID, now)
	}
	return userID, nil
}

// writeAccess writes last accesses, unless another pod has written a later
// one. A subscription deleted meanwhile stays deleted (F29).
func (d *deltaStore) writeAccess(ctx context.Context, at map[string]time.Time) error {
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		for _, id := range slices.Sorted(maps.Keys(at)) {
			t := at[id]
			row, ok := d.Subscriptions.Get(s.Reader, id)
			if !ok || (row.LastAccess != nil && !row.LastAccess.Before(t)) {
				continue
			}
			row.LastAccess = &t
			if err := d.Subscriptions.Put(s.Tx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *deltaStore) findOne(ctx context.Context, tx txn.Tx, id string) (*Subscription, error) {
	var row subRow
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		var ok bool
		row, ok = d.Subscriptions.Get(s.Reader, strings.ToLower(id))
		if !ok {
			return sql.ErrNoRows
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, validation.NewFieldError("ID", "not found")
	}
	if err != nil {
		return nil, err
	}
	return row.subscription()
}

// findAllByUser returns the user's subscriptions, oldest first.
func (d *deltaStore) findAllByUser(ctx context.Context, userID string) ([]Subscription, error) {
	var rows []subRow
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		d.byUser.Ascend(s.Reader, strings.ToLower(userID), func(row subRow) bool {
			rows = append(rows, row)
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b subRow) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	subs := make([]Subscription, len(rows))
	for i, row := range rows {
		cs, err := row.subscription()
		if err != nil {
			return nil, err
		}
		subs[i] = *cs
	}
	return subs, nil
}

// create adds a subscription and returns when it was created. Like the
// trigger trg_enforce_calendar_subscriptions_per_user_limit, it refuses one
// more than the user may have.
func (d *deltaStore) create(ctx context.Context, tx txn.Tx, cs *Subscription, config []byte) (time.Time, error) {
	scheduleID, err := validate.ParseUUID("ScheduleID", cs.ScheduleID)
	if err != nil {
		return time.Time{}, err
	}
	row := subRow{
		ID: strings.ToLower(cs.ID), Name: cs.Name, UserID: strings.ToLower(cs.UserID),
		ScheduleID: scheduleID.String(), Config: config, Disabled: cs.Disabled,
	}
	err = deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row.CreatedAt, row.LastUpdate = s.Now, s.Now
		if err := d.Subscriptions.Insert(s.Tx, row, "user_calendar_subscriptions_pkey"); err != nil {
			return err
		}
		limit := d.lookups.MaxPerUser(s.Reader)
		if limit != -1 && d.byUser.Count(s.Reader, row.UserID) > limit {
			return &filestore.CheckError{
				Table: "user_calendar_subscriptions", Constraint: "calendar_subscriptions_per_user_limit",
				Message: "limit exceeded", HintText: fmt.Sprintf("max=%d", limit),
			}
		}
		return nil
	})
	return row.CreatedAt, err
}

// update sets the name, config and whether it is disabled, if the
// subscription is the user's.
func (d *deltaStore) update(ctx context.Context, tx txn.Tx, cs *Subscription, config []byte) error {
	id, userID := strings.ToLower(cs.ID), strings.ToLower(cs.UserID)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		row, ok := d.Subscriptions.Get(s.Reader, id)
		if !ok || row.UserID != userID {
			return nil
		}
		row.Name, row.Disabled, row.Config, row.LastUpdate = cs.Name, cs.Disabled, config, s.Now
		return d.Subscriptions.Put(s.Tx, row)
	})
}

// deleteMany deletes the subscriptions that are the user's.
func (d *deltaStore) deleteMany(ctx context.Context, tx txn.Tx, userID string, ids []string) error {
	userID = strings.ToLower(userID)
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		for _, id := range ids {
			row, ok := d.Subscriptions.Get(s.Reader, strings.ToLower(id))
			if !ok || row.UserID != userID {
				continue
			}
			if _, err := d.Subscriptions.Delete(s.Tx, row.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// renderInfo returns what a subscription is rendered from, or sql.ErrNoRows.
func (d *deltaStore) renderInfo(ctx context.Context, id uuid.UUID) (renderInfo, error) {
	var row subRow
	var info renderInfo
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var ok bool
		row, ok = d.Subscriptions.Get(s.Reader, id.String())
		if !ok {
			return sql.ErrNoRows
		}
		name, ok := d.lookups.ScheduleName(s.Reader, row.ScheduleID)
		if !ok {
			return sql.ErrNoRows
		}
		info = renderInfo{Now: s.Now, ScheduleName: name, Config: row.Config}
		return nil
	})
	if err != nil {
		return renderInfo{}, err
	}
	if info.ScheduleID, err = uuid.Parse(row.ScheduleID); err != nil {
		return renderInfo{}, err
	}
	if info.UserID, err = uuid.Parse(row.UserID); err != nil {
		return renderInfo{}, err
	}
	return info, nil
}

// userNames sets the names of the users that exist.
func (d *deltaStore) userNames(ctx context.Context, names map[string]string) error {
	return deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		for id := range names {
			if name, ok := d.lookups.UserName(s.Reader, id); ok {
				names[id] = name
			}
		}
		return nil
	})
}
