package signalmgr

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/util/log"
)

// DeltaDB schedules outgoing signal messages on the Delta backend.
type DeltaDB struct{ t *deltastores.Tables }

// NewDeltaDB returns a signal manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables) *DeltaDB { return &DeltaDB{t: t} }

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.SignalManager" }

const (
	// maxSignals is how many signals get a message in a cycle.
	maxSignals = 1000
	// signalLifetime is how long a signal waits for one.
	signalLifetime = time.Hour
)

// UpdateAll queues a message for the oldest signal of each service and
// destination that has none waiting, and deletes the signals that waited too
// long. One that can't be handled is logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	type dest struct{ serviceID, channelID string }
	waiting := make(map[dest]bool)
	typ := string(gadb.EnumOutgoingMessagesTypeSignalMessage)
	db.t.Notification.MessagesPending.Ascend(tx, notification.PendingTypeKey(gadb.EnumOutgoingMessagesTypeSignalMessage), func(m notification.MessageRow) bool {
		if m.ServiceID != nil && m.ChannelID != nil {
			waiting[dest{*m.ServiceID, *m.ChannelID}] = true
		}
		return true
	})
	cutoff := tx.Now().Add(-signalLifetime)
	var stale, next []integrationkey.DeltaSignal
	db.t.IntegrationKey.EachUnsent(tx, func(s integrationkey.DeltaSignal) bool {
		d := dest{s.ServiceID, s.DestID}
		switch {
		case s.CreatedAt.Before(cutoff):
			stale = append(stale, s)
		case !waiting[d] && len(next) < maxSignals:
			waiting[d] = true
			next = append(next, s)
		}
		return true
	})
	err := enginetables.Each(tx, stale, func(s integrationkey.DeltaSignal) error {
		return errors.Wrap(db.t.IntegrationKey.DeleteSignal(tx, s.ID), "delete stale signal")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	err = enginetables.Each(tx, next, func(s integrationkey.DeltaSignal) error {
		m, err := db.t.Notification.InsertMessage(tx, notification.MessageRow{
			MessageType: typ, ServiceID: &s.ServiceID, ChannelID: &s.DestID,
		})
		if err != nil {
			return errors.Wrapf(err, "signal of service %s: insert message", s.ServiceID)
		}
		return errors.Wrap(db.t.IntegrationKey.SetSignalMessage(tx, s.ID, m.ID), "update signal")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}
