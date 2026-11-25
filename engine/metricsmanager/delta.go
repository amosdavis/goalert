package metricsmanager

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
)

// DeltaDB updates metrics on the Delta backend.
type DeltaDB struct{ t *deltastores.Tables }

// NewDeltaDB returns a metrics manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables) *DeltaDB { return &DeltaDB{t: t} }

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.MetricsManager" }

const (
	stateName = "metrics"
	// maxLogs is how many closed alerts are measured in a cycle.
	maxLogs = 500
)

// UpdateAll measures the alerts that were closed since the last time.
//
// The state has the time and id of the last log entry that was looked at. A
// transaction sees every entry that was made before it, so there is no need to
// wait until those of other transactions have settled, as there was on Postgres.
// Commit times never go back (F13), and ids tell those of one commit apart.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	var state State
	if err := db.t.Engine.LoadState(tx, stateName, &state); err != nil {
		return fmt.Errorf("load state: %w", err)
	}

	// Days with nothing new are skipped, and a day's entries are in order of their
	// ids, so every one of them is needed to find the next in order of time.
	var logs []alertlog.DeltaLog
	day := time.Time{}
	db.t.AlertLog.AscendClosed(tx, state.V2.LastLogTime, tx.Now(), func(l alertlog.DeltaLog) bool {
		if d := l.Timestamp.UTC().Truncate(24 * time.Hour); !d.Equal(day) {
			if len(logs) >= maxLogs {
				return false
			}
			day = d
		}
		c := cmp.Or(l.Timestamp.Compare(state.V2.LastLogTime), cmp.Compare(l.ID, int64(state.V2.LastLogID)))
		if c > 0 && l.AlertID != nil && !l.Timestamp.After(tx.Now()) {
			logs = append(logs, l)
		}
		return true
	})
	if len(logs) == 0 {
		return nil
	}
	slices.SortFunc(logs, func(a, b alertlog.DeltaLog) int {
		return cmp.Or(a.Timestamp.Compare(*b.Timestamp), cmp.Compare(a.ID, b.ID))
	})
	if len(logs) > maxLogs {
		logs = logs[:maxLogs]
	}
	for _, l := range logs {
		if err := db.measure(tx, *l.AlertID); err != nil {
			return fmt.Errorf("insert metrics: %w", err)
		}
	}
	last := logs[len(logs)-1]
	state.V2.LastLogTime, state.V2.LastLogID = *last.Timestamp, int(last.ID)
	if err := db.t.Engine.SaveState(tx, stateName, &state); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	return nil
}

// measure adds an alert's metrics, unless it has some.
func (db *DeltaDB) measure(tx *filestore.Tx, alertID int64) error {
	a, ok := db.t.Alert.Alerts.Get(tx, filestore.IntKey(alertID))
	if !ok || a.ServiceID == nil {
		return nil
	}
	since := func(t *time.Time) *int64 {
		if t == nil {
			return nil
		}
		d := t.Sub(a.CreatedAt).Microseconds()
		return &d
	}
	// first is the time of the first entry of a type, and how many there are.
	first := func(typ alertlog.Type) (at *time.Time, n int) {
		db.t.AlertLog.AscendEvent(tx, alertID, typ, func(l alertlog.DeltaLog) bool {
			n++
			if l.Timestamp != nil && (at == nil || l.Timestamp.Before(*at)) {
				at = l.Timestamp
			}
			return true
		})
		return at, n
	}
	acked, _ := first(alertlog.TypeAcknowledged)
	closed, _ := first(alertlog.TypeClosed)
	_, escalated := first(alertlog.TypeEscalated)
	if closed == nil {
		return nil
	}
	return db.t.AlertMetrics.Insert(deltatxn.Scope{Tx: tx, Reader: tx, Now: tx.Now()}, alertmetrics.DeltaMetric{
		AlertID: alertID, ServiceID: *a.ServiceID,
		TimeToAck: since(acked), TimeToClose: since(closed), Escalated: escalated > 1, ClosedAt: *closed,
	})
}
