package statusmgr

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/util/log"
)

// DeltaDB sends status updates on the Delta backend.
type DeltaDB struct {
	t   *deltastores.Tables
	reg *nfydest.Registry
}

// NewDeltaDB returns a status update manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables, reg *nfydest.Registry) *DeltaDB {
	return &DeltaDB{t: t, reg: reg}
}

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.StatusUpdateManager" }

const (
	// subscriptionLifetime is how long a subscription that nothing happened to is
	// kept.
	subscriptionLifetime = 7 * 24 * time.Hour
	// maxUpdates is how many status updates are queued in a cycle, so that it
	// stays short (F15). The rest follow in the next.
	maxUpdates = 1000
)

// UpdateAll queues a status update for each subscription to an alert whose
// status is not the last one that was sent, and ends old subscriptions. One that
// can't be handled is logged, and stays as it is.
//
// It looks at every subscription in every cycle (F52). There is one for each
// destination that was notified of an open alert.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	cutoff := tx.Now().Add(-subscriptionLifetime)
	var stale, changed []alert.DeltaSubscription
	db.t.Alert.Subscriptions.Ascend(tx, func(sub alert.DeltaSubscription) bool {
		if sub.UpdatedAt.Before(cutoff) {
			stale = append(stale, sub)
			return true
		}
		status, ok := db.t.Alert.Status(tx, sub.AlertID)
		if ok && string(status) != sub.LastAlertStatus && len(changed) < maxUpdates {
			changed = append(changed, sub)
		}
		return true
	})
	for _, sub := range stale {
		if err := db.end(tx, sub); err != nil {
			return err
		}
	}
	err := enginetables.Each(tx, changed, func(sub alert.DeltaSubscription) error {
		return errors.Wrapf(db.update(ctx, tx, sub), "status update of alert %d", sub.AlertID)
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func (db *DeltaDB) end(tx *filestore.Tx, sub alert.DeltaSubscription) error {
	_, err := db.t.Alert.Subscriptions.Delete(tx, filestore.IntKey(sub.ID))
	return err
}

func (db *DeltaDB) update(ctx context.Context, tx *filestore.Tx, sub alert.DeltaSubscription) error {
	status, _ := db.t.Alert.Status(tx, sub.AlertID)
	var event alertlog.Type
	switch status {
	case alert.StatusTriggered:
		event = alertlog.TypeEscalated
	case alert.StatusActive:
		event = alertlog.TypeAcknowledged
	case alert.StatusClosed:
		event = alertlog.TypeClosed
	}
	// The newest entry of that kind is what is sent.
	var entry *alertlog.DeltaLog
	db.t.AlertLog.AscendEvent(tx, sub.AlertID, event, func(l alertlog.DeltaLog) bool {
		entry = &l
		return true
	})

	msg := notification.MessageRow{
		MessageType: string(gadb.EnumOutgoingMessagesTypeAlertStatusUpdate),
		AlertID:     &sub.AlertID, ContactMethodID: sub.ContactMethodID, ChannelID: sub.ChannelID,
	}
	send := true
	switch {
	case entry == nil:
		// no log entry, log error but continue
		log.Log(ctx, fmt.Errorf("no log entry found for alert #%d status update (%s), skipping", sub.AlertID, event))
		send = false
	case sub.ContactMethodID != nil:
		method, ok := db.t.ContactMethod.Methods.Get(tx, *sub.ContactMethodID)
		if !ok || method.Disabled {
			return db.end(tx, sub)
		}
		dest, err := method.DestV1()
		if err != nil {
			return fmt.Errorf("lookup contact method info: %w", err)
		}
		// What a type supports is known without asking anybody.
		info, err := db.reg.TypeInfo(ctx, dest.Type)
		if err != nil {
			return fmt.Errorf("lookup contact method type info: %w", err)
		}
		if !info.SupportsStatusUpdates {
			return db.end(tx, sub)
		}
		// We don't want to update a user for their own actions, unless the
		// contact method is forced to always send updates.
		send = info.StatusUpdatesRequired || entry.SubUserID == nil || *entry.SubUserID != method.UserID
		msg.UserID = &method.UserID
	}
	if send {
		msg.AlertLogID = &entry.ID
		if _, err := db.t.Notification.InsertMessage(tx, msg); err != nil {
			return fmt.Errorf("send status update message: %w", err)
		}
	}
	if status == alert.StatusClosed {
		return db.end(tx, sub)
	}
	sub.LastAlertStatus, sub.UpdatedAt = string(status), tx.Now()
	return db.t.Alert.Subscriptions.Put(tx, sub)
}
