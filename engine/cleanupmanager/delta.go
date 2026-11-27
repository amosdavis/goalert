package cleanupmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/config"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/override"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/jsonutil"
)

// DeltaDB deletes what is not needed any more on the Delta backend.
type DeltaDB struct {
	db     *filestore.DeltaDB
	t      *deltastores.Tables
	alerts *alert.Store
	runner *enginetables.Runner
	logger *slog.Logger
}

// NewDeltaDB returns a cleanup manager on the Delta backend. alerts is the Delta
// backend's too.
func NewDeltaDB(db *filestore.DeltaDB, t *deltastores.Tables, alerts *alert.Store, runner *enginetables.Runner, logger *slog.Logger) *DeltaDB {
	return &DeltaDB{db: db, t: t, alerts: alerts, runner: runner, logger: logger}
}

const (
	// deltaBatch is how many rows a transaction deletes, and scanBatch how many it
	// looks at, so that none holds up the engine's for long (F15).
	deltaBatch = 100
	scanBatch  = 5000
	// batchDelay is the time between the transactions of a task.
	batchDelay = 100 * time.Millisecond

	day = 24 * time.Hour
)

// Tasks are what is to be done, and how often.
func (db *DeltaDB) Tasks() []enginetables.Task {
	return []enginetables.Task{
		{Name: "cleanup-alerts", Every: time.Hour, Run: db.cleanupAlerts},
		{Name: "cleanup-shifts", Every: day, Run: db.cleanupShifts},
		{Name: "cleanup-schedule-data", Every: day, Run: db.cleanupScheduleData},
		{Name: "cleanup-api-keys", Every: day, Run: db.cleanupAPIKeys},
		{Name: "cleanup-alert-logs", Every: 7 * day, Run: db.cleanupAlertLogs},
		{Name: "cleanup-storage", Every: 6 * time.Hour, Run: db.cleanupStorage},
	}
}

// whileWork runs transactions until one is done.
func (db *DeltaDB) whileWork(ctx context.Context, name string, run func(ctx context.Context, tx *filestore.Tx) (done bool, err error)) error {
	for {
		var done bool
		err := db.runner.Do(ctx, enginetables.Step{Name: name, Run: func(ctx context.Context, tx *filestore.Tx) (err error) {
			done, err = run(ctx, tx)
			return err
		}})
		if err != nil || done {
			return err
		}
		if err := util.ContextSleep(ctx, batchDelay); err != nil {
			return err
		}
	}
}

// deleteAll deletes the rows that find returns, so many at a time.
func (db *DeltaDB) deleteAll(ctx context.Context, name string, find func(tx *filestore.Tx, add func(key string) bool), remove func(tx *filestore.Tx, key string) (bool, error)) error {
	return db.whileWork(ctx, name, func(ctx context.Context, tx *filestore.Tx) (bool, error) {
		var keys []string
		find(tx, func(key string) bool {
			keys = append(keys, key)
			return len(keys) < deltaBatch
		})
		for _, key := range keys {
			if _, err := remove(tx, key); err != nil {
				return false, err
			}
		}
		return len(keys) < deltaBatch, nil
	})
}

func daysAgo(tx *filestore.Tx, days int) time.Time { return tx.Now().Add(-time.Duration(days) * day) }

// cleanupAlerts closes the alerts that nothing happened to for long, and deletes
// those that were closed long ago.
func (db *DeltaDB) cleanupAlerts(ctx context.Context) error {
	cfg := config.FromContext(ctx).Maintenance
	if cfg.AlertAutoCloseDays > 0 {
		err := db.whileWork(ctx, "auto close alerts", func(ctx context.Context, tx *filestore.Tx) (bool, error) {
			ids := db.staleAlerts(tx, daysAgo(tx, cfg.AlertAutoCloseDays), cfg.AutoCloseAckedAlerts)
			err := db.alerts.DeltaCloseMany(ctx, deltatxn.Of(tx), ids, alertlog.AutoClose{AlertAutoCloseDays: cfg.AlertAutoCloseDays})
			return len(ids) < deltaBatch, err
		})
		if err != nil {
			return err
		}
	}
	if cfg.AlertCleanupDays <= 0 {
		return nil
	}
	return db.deleteAll(ctx, "delete old alerts", func(tx *filestore.Tx, add func(string) bool) {
		cutoff := daysAgo(tx, cfg.AlertCleanupDays)
		db.t.Alert.AscendCreated(tx, time.Time{}, cutoff, func(a alert.DeltaAlert) bool {
			if a.Status == string(alert.StatusClosed) && a.CreatedAt.Before(cutoff) {
				return add(filestore.IntKey(a.ID))
			}
			return true
		})
	}, db.t.Alert.Alerts.Delete)
}

// staleAlerts are open alerts that nothing happened to since a time. A change of
// their policy is nothing that happened to them.
func (db *DeltaDB) staleAlerts(tx *filestore.Tx, since time.Time, acked bool) (ids []int) {
	find := func(a alert.DeltaAlert) bool {
		if a.CreatedAt.After(since) {
			return true
		}
		active := false
		db.t.AlertLog.AscendAlert(tx, a.ID, func(l alertlog.DeltaLog) bool {
			active = l.Timestamp != nil && l.Timestamp.After(since) && l.Event != string(alertlog.TypePolicyUpdated)
			return !active
		})
		if !active {
			ids = append(ids, int(a.ID))
		}
		return len(ids) < deltaBatch
	}
	db.t.Alert.AscendOpen(tx, alert.StatusTriggered, find)
	if acked && len(ids) < deltaBatch {
		db.t.Alert.AscendOpen(tx, alert.StatusActive, find)
	}
	return ids
}

// cleanupShifts deletes old shifts and overrides.
func (db *DeltaDB) cleanupShifts(ctx context.Context) error {
	days := config.FromContext(ctx).Maintenance.ScheduleCleanupDays
	if days <= 0 {
		return nil
	}
	err := db.deleteAll(ctx, "delete old shifts", func(tx *filestore.Tx, add func(string) bool) {
		cutoff := daysAgo(tx, days)
		db.t.OnCall.ScheduleOnCall.Ascend(tx, func(r oncall.ScheduleOnCallRow) bool {
			return r.EndTime == nil || !r.EndTime.Before(cutoff) || add(filestore.IntKey(r.ID))
		})
	}, db.t.OnCall.ScheduleOnCall.Delete)
	if err != nil {
		return err
	}
	err = db.deleteAll(ctx, "delete old overrides", func(tx *filestore.Tx, add func(string) bool) {
		cutoff := daysAgo(tx, days)
		db.t.Override.Overrides.Ascend(tx, func(o override.OverrideRow) bool {
			return !o.End.Before(cutoff) || add(o.ID)
		})
	}, db.t.Override.Overrides.Delete)
	if err != nil {
		return err
	}
	return db.deleteAll(ctx, "delete old step shifts", func(tx *filestore.Tx, add func(string) bool) {
		cutoff := daysAgo(tx, days)
		db.t.OnCall.StepOnCall.Ascend(tx, func(r oncall.StepOnCallRow) bool {
			return r.EndTime == nil || !r.EndTime.Before(cutoff) || add(filestore.IntKey(r.ID))
		})
	}, db.t.OnCall.StepOnCall.Delete)
}

// scheduleDataDays is how often a schedule's data is cleaned up.
const scheduleDataDays = 30

// cleanupScheduleData removes the temporary schedules that are over, and the
// shifts of users that no longer exist.
func (db *DeltaDB) cleanupScheduleData(ctx context.Context) error {
	if config.FromContext(ctx).Maintenance.ScheduleCleanupDays <= 0 {
		return nil
	}
	return db.whileWork(ctx, "cleanup schedule data", func(ctx context.Context, tx *filestore.Tx) (bool, error) {
		var ids []string
		cutoff := daysAgo(tx, scheduleDataDays)
		db.t.Schedule.EachData(tx, func(id string, last *time.Time) bool {
			if last == nil || !last.After(cutoff) {
				ids = append(ids, id)
			}
			return len(ids) < deltaBatch
		})
		err := enginetables.Each(tx, ids, func(id string) error { return db.cleanupOne(ctx, tx, id) })
		if err != nil {
			// It is looked at again in a month, like the others.
			db.logger.ErrorContext(ctx, "Clean up schedule data.", slog.Any("error", err))
		}
		return len(ids) < deltaBatch, nil
	})
}

func (db *DeltaDB) cleanupOne(ctx context.Context, tx *filestore.Tx, id string) error {
	row, ok := db.t.Schedule.Data.Get(tx, id)
	if !ok {
		return nil
	}
	now := tx.Now()
	row.LastCleanupAt = &now
	var data schedule.Data
	if err := json.Unmarshal(row.Data, &data); err != nil {
		db.logger.ErrorContext(ctx, "Unmarshal schedule data.", slog.String("schedule_id", id), slog.Any("error", err))
		return db.t.Schedule.Data.Put(tx, row)
	}
	var valid []uuid.UUID
	for _, userID := range collectUsers(data) {
		if _, ok := db.t.User.Users.Get(tx, userID.String()); ok {
			valid = append(valid, userID)
		}
	}
	if cleanupData(&data, valid, now) {
		raw, err := jsonutil.Apply(row.Data, data) // keeps what Data does not have
		if err != nil {
			return fmt.Errorf("schedule %s: %w", id, err)
		}
		row.Data = raw
		db.logger.InfoContext(ctx, "Updated schedule data.", slog.String("schedule_id", id))
	}
	return db.t.Schedule.Data.Put(tx, row)
}

// sessionDays is how long a session lasts that is not used. It is defined by how
// the keyring of sessions works, and is not influenced by the configuration.
const sessionDays = 30

// cleanupAPIKeys deletes old sessions.
func (db *DeltaDB) cleanupAPIKeys(ctx context.Context) error {
	return db.whileWork(ctx, "delete old user sessions", func(_ context.Context, tx *filestore.Tx) (bool, error) {
		n, err := db.t.Auth.DeleteOld(tx, daysAgo(tx, sessionDays), deltaBatch)
		return n < deltaBatch, err
	})
}

// cleanupAlertLogs deletes the log entries of alerts that were deleted.
func (db *DeltaDB) cleanupAlertLogs(ctx context.Context) error {
	for from := ""; ; {
		var next string
		err := db.runner.Do(ctx, enginetables.Step{Name: "cleanup alert logs", Run: func(_ context.Context, tx *filestore.Tx) error {
			next = ""
			seen := 0
			var orphans []string
			db.t.AlertLog.Logs.AscendFrom(tx, from, func(l alertlog.DeltaLog) bool {
				if seen == scanBatch {
					next = filestore.IntKey(l.ID)
					return false
				}
				seen++
				if l.AlertID != nil {
					if _, ok := db.t.Alert.Alerts.Get(tx, filestore.IntKey(*l.AlertID)); ok {
						return true
					}
				}
				orphans = append(orphans, filestore.IntKey(l.ID))
				return true
			})
			for _, key := range orphans {
				if _, err := db.t.AlertLog.Logs.Delete(tx, key); err != nil {
					return err
				}
			}
			return nil
		}})
		if err != nil || next == "" {
			return err
		}
		from = next
		if err := util.ContextSleep(ctx, batchDelay); err != nil {
			return err
		}
	}
}

// cleanupStorage deletes the commits and files that no pod needs any more (F16).
func (db *DeltaDB) cleanupStorage(ctx context.Context) error {
	if !db.runner.Holds() {
		return enginetables.ErrNotHolder
	}
	commits, err := db.db.CleanLog(ctx)
	if err != nil {
		return fmt.Errorf("clean up the log: %w", err)
	}
	files, err := db.db.Vacuum(ctx)
	if err != nil {
		return fmt.Errorf("delete unused files: %w", err)
	}
	db.logger.DebugContext(ctx, "Cleaned up storage.", slog.Int("commits", commits), slog.Int("files", files))
	return nil
}
