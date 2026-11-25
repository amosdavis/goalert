package heartbeatmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/heartbeat"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation/validate"
)

// DeltaDB processes heartbeats on the Delta backend.
type DeltaDB struct {
	t      *deltastores.Tables
	alerts *alert.Store
}

// NewDeltaDB returns a heartbeat manager on the Delta backend. alerts is the
// Delta backend's too.
func NewDeltaDB(t *deltastores.Tables, alerts *alert.Store) *DeltaDB {
	return &DeltaDB{t: t, alerts: alerts}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.HeartbeatManager" }

// maxMonitors is how many monitors of each kind are handled in a cycle.
const maxMonitors = 250

// UpdateAll opens an alert for each monitor that had no heartbeat in time, and
// closes it for each that has one again. A monitor that can't be handled is
// logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	expired := db.t.Heartbeat.Expired(tx, tx.Now(), maxMonitors)
	recovered := db.t.Heartbeat.Recovered(tx, tx.Now(), maxMonitors)
	err := enginetables.Each(tx, expired, func(m heartbeat.DeltaMonitor) error {
		return errors.Wrapf(db.expire(ctx, tx, m), "heartbeat monitor %s: open alert", m.ID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	err = enginetables.Each(tx, recovered, func(m heartbeat.DeltaMonitor) error {
		return errors.Wrapf(db.recover(ctx, tx, m), "heartbeat monitor %s: close alert", m.ID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func monitorContext(ctx context.Context, m heartbeat.DeltaMonitor) context.Context {
	return row{ID: m.ID, ServiceID: m.ServiceID}.Context(ctx)
}

func monitorAlert(m heartbeat.DeltaMonitor, status alert.Status) *alert.Alert {
	return &alert.Alert{
		Status:    status,
		ServiceID: m.ServiceID,
		Dedup:     &alert.DedupID{Type: alert.DedupTypeHeartbeat, Version: 1, Payload: m.ID},
	}
}

func (db *DeltaDB) expire(ctx context.Context, tx *filestore.Tx, m heartbeat.DeltaMonitor) error {
	if err := db.t.Heartbeat.SetState(tx, m.ID, heartbeat.StateUnhealthy); err != nil {
		return err
	}
	if m.Muted != "" {
		return nil
	}
	details := "Last heartbeat: " + m.LastHeartbeat.Format(time.UnixDate)
	if m.AdditionalDetails != "" {
		details += "\n\n" + m.AdditionalDetails
	}
	a := monitorAlert(m, alert.StatusTriggered)
	a.Summary = fmt.Sprintf("Heartbeat monitor '%s' expired.", m.Name)
	a.Details = validate.SanitizeText(details, alert.MaxDetailsLength)
	_, _, err := db.alerts.CreateOrUpdateTx(monitorContext(ctx, m), deltatxn.Of(tx), a)
	return err
}

func (db *DeltaDB) recover(ctx context.Context, tx *filestore.Tx, m heartbeat.DeltaMonitor) error {
	if err := db.t.Heartbeat.SetState(tx, m.ID, heartbeat.StateHealthy); err != nil {
		return err
	}
	_, _, err := db.alerts.CreateOrUpdateTx(monitorContext(ctx, m), deltatxn.Of(tx), monitorAlert(m, alert.StatusClosed))
	return err
}
