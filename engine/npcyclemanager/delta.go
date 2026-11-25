package npcyclemanager

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/user/notificationrule"
	"github.com/target/goalert/util/log"
)

// DeltaDB notifies users by their rules on the Delta backend.
type DeltaDB struct {
	t   *deltastores.Tables
	log *alertlog.Store
}

// NewDeltaDB returns a notification cycle manager on the Delta backend. log is
// the Delta backend's too.
func NewDeltaDB(t *deltastores.Tables, log *alertlog.Store) *DeltaDB {
	return &DeltaDB{t: t, log: log}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.NotificationCycleManager" }

const (
	// maxCycles is how many notification cycles are worked on in an engine
	// cycle, so that it stays short (F15). The rest follow in the next.
	maxCycles = 5000
	// tickInterval is how often each is looked at.
	tickInterval = time.Minute
)

// UpdateAll queues the messages of the rules whose delay is over since a cycle
// was last looked at, and ends the cycles of alerts that don't need them any
// more. A cycle that can't be handled is logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	last := tx.Now().Add(-tickInterval)
	var cycles []enginetables.CycleRow
	db.t.Engine.Cycles.Ascend(tx, func(c enginetables.CycleRow) bool {
		if c.LastTick == nil || c.LastTick.Before(last) {
			cycles = append(cycles, c)
		}
		return true
	})
	slices.SortFunc(cycles, func(a, b enginetables.CycleRow) int {
		return cmp.Or(tick(a).Compare(tick(b)), a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.ID, b.ID))
	})
	if len(cycles) > maxCycles {
		cycles = cycles[:maxCycles]
	}
	err := enginetables.Each(tx, cycles, func(c enginetables.CycleRow) error {
		return errors.Wrapf(db.update(ctx, tx, c), "notify user %s of alert %d", c.UserID, c.AlertID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

// tick is when a cycle was last looked at, and before all others if never.
func tick(c enginetables.CycleRow) time.Time {
	if c.LastTick == nil {
		return time.Time{}
	}
	return *c.LastTick
}

func (db *DeltaDB) update(ctx context.Context, tx *filestore.Tx, c enginetables.CycleRow) error {
	a, ok := db.t.Alert.Alerts.Get(tx, filestore.IntKey(c.AlertID))
	if !ok || db.over(tx, c, alert.Status(a.Status)) {
		_, err := db.t.Engine.Cycles.Delete(tx, c.ID)
		return errors.Wrap(err, "end cycle")
	}

	queued, err := db.queue(tx, c, a)
	if err != nil {
		return err
	}
	if c.LastTick == nil && !queued {
		ctx := permission.UserSourceContext(ctx, c.UserID, permission.RoleUser, &permission.SourceInfo{
			Type: permission.SourceTypeContactMethod,
			// no ID available, since notification couldn't be sent
		})
		err = db.log.LogTx(ctx, deltatxn.Of(tx), int(c.AlertID), alertlog.TypeNoNotificationSent, nil)
		if err != nil {
			return errors.Wrap(err, "log no notifications sent")
		}
	}
	now := tx.Now()
	c.LastTick = &now
	return errors.Wrap(db.t.Engine.Cycles.Put(tx, c), "update cycle")
}

// over reports whether an alert's user need not be notified any more.
func (db *DeltaDB) over(tx *filestore.Tx, c enginetables.CycleRow, status alert.Status) bool {
	switch {
	case status == alert.StatusTriggered:
		return false
	case status == alert.StatusClosed, !c.MultiAck:
		return true
	}
	// Somebody has acknowledged it, and everybody on the step has to: go on until
	// this user has.
	acked := false
	db.t.AlertLog.AscendEvent(tx, c.AlertID, alertlog.TypeAcknowledged, func(l alertlog.DeltaLog) bool {
		acked = l.SubUserID != nil && *l.SubUserID == c.UserID && l.Timestamp != nil && l.Timestamp.After(c.StartedAt)
		return !acked
	})
	return acked
}

// queue queues a message for each contact method that a rule says to use by now,
// and reports whether there is one.
func (db *DeltaDB) queue(tx *filestore.Tx, c enginetables.CycleRow, a alert.DeltaAlert) (bool, error) {
	if a.ServiceID == nil {
		return false, nil
	}
	svc, ok := db.t.Service.Services.Get(tx, *a.ServiceID)
	if !ok {
		return false, nil
	}
	age := tx.Now().Sub(c.StartedAt)
	var methods []string
	db.t.NotificationRule.ByUser.Ascend(tx, c.UserID, func(r notificationrule.Row) bool {
		delay := time.Duration(r.DelayMinutes) * time.Minute
		if delay <= age && (c.LastTick == nil || delay > c.LastTick.Sub(c.StartedAt)) && !slices.Contains(methods, r.ContactMethodID) {
			methods = append(methods, r.ContactMethodID)
		}
		return true
	})
	for _, methodID := range methods {
		_, err := db.t.Notification.InsertMessage(tx, notification.MessageRow{
			MessageType:     string(gadb.EnumOutgoingMessagesTypeAlertNotification),
			ContactMethodID: &methodID, UserID: &c.UserID, AlertID: &c.AlertID, CycleID: &c.ID,
			ServiceID: a.ServiceID, EscalationPolicyID: &svc.EscalationPolicyID,
		})
		if err != nil {
			return false, errors.Wrap(err, "queue message")
		}
	}
	return len(methods) > 0, nil
}
