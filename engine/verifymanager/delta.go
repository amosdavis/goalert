package verifymanager

import (
	"context"

	"github.com/pkg/errors"
	"github.com/target/goalert/deltastores"
	"github.com/target/goalert/engine/enginetables"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/util/log"
)

// DeltaDB manages verification codes on the Delta backend.
type DeltaDB struct{ t *deltastores.Tables }

// NewDeltaDB returns a verification code manager on the Delta backend.
func NewDeltaDB(t *deltastores.Tables) *DeltaDB { return &DeltaDB{t: t} }

// Name returns the name of the module.
func (db *DeltaDB) Name() string { return "Engine.VerificationManager" }

// maxCodes is how many codes of each kind are handled in a cycle.
const maxCodes = 100

// UpdateAll deletes the codes that have expired, with the contact methods that
// were waiting for them, and queues the messages with those that were not sent
// yet. One that can't be handled is logged, and stays as it is.
func (db *DeltaDB) UpdateAll(ctx context.Context, tx *filestore.Tx) error {
	if err := permission.LimitCheckAny(ctx, permission.System); err != nil {
		return err
	}
	var expired, unsent []notification.VerificationCodeRow
	db.t.Notification.Codes.Ascend(tx, func(c notification.VerificationCodeRow) bool {
		switch {
		case !tx.Now().Before(c.ExpiresAt):
			if len(expired) < maxCodes {
				expired = append(expired, c)
			}
		case !c.Sent && len(unsent) < maxCodes:
			unsent = append(unsent, c)
		}
		return true
	})
	err := enginetables.Each(tx, expired, func(c notification.VerificationCodeRow) error {
		return errors.Wrap(db.expire(tx, c), "cleanup expired code")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	err = enginetables.Each(tx, unsent, func(c notification.VerificationCodeRow) error {
		return errors.Wrap(db.send(tx, c), "send verification code")
	})
	if err != nil {
		log.Log(ctx, err)
	}
	return nil
}

func (db *DeltaDB) expire(tx *filestore.Tx, c notification.VerificationCodeRow) error {
	if m, ok := db.t.ContactMethod.Methods.Get(tx, c.ContactMethodID); ok && m.Pending {
		if _, err := db.t.ContactMethod.Methods.Delete(tx, m.ID); err != nil {
			return err
		}
	}
	_, err := db.t.Notification.Codes.Delete(tx, c.ID)
	return err
}

func (db *DeltaDB) send(tx *filestore.Tx, c notification.VerificationCodeRow) error {
	m, ok := db.t.ContactMethod.Methods.Get(tx, c.ContactMethodID)
	if !ok {
		return nil
	}
	_, err := db.t.Notification.InsertMessage(tx, notification.MessageRow{
		MessageType:     string(gadb.EnumOutgoingMessagesTypeVerificationMessage),
		ContactMethodID: &m.ID, UserID: &m.UserID, UserVerificationCodeID: &c.ID,
	})
	if err != nil {
		return err
	}
	c.Sent = true
	return db.t.Notification.Codes.Put(tx, c)
}
