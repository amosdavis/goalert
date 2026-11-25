package notification

import (
	"context"
	cRand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// codeLifetime is how long a verification code is valid.
const codeLifetime = 15 * time.Minute

// VerificationCodeRow is a user_verification_codes row on the Delta backend.
// The code is secret: it must not be part of a key, which errors show (F20).
type VerificationCodeRow struct {
	ID              string    `parquet:"id"`
	ContactMethodID string    `parquet:"contact_method_id"`
	Code            int32     `parquet:"code"`
	ExpiresAt       time.Time `parquet:"expires_at,timestamp(microsecond:utc)"`
	Sent            bool      `parquet:"sent"`
}

// DeltaTables are the notification store's tables on the Delta backend.
type DeltaTables struct {
	// Codes is user_verification_codes, by id.
	Codes *filestore.Table[VerificationCodeRow]
	// CodesByMethod finds a contact method's verification code.
	CodesByMethod *filestore.Unique[VerificationCodeRow]

	// Messages is outgoing_messages, by id. Other packages declare their
	// foreign keys to it, and read it. They write it only through
	// InsertMessage, SetMessageStatus and UpdateMessage, which apply its
	// checks and triggers, or delete from it.
	Messages *filestore.Table[MessageRow]
	// History is message_status_history, by id, which those methods write.
	History *filestore.Table[StatusHistoryRow]

	// MessagesUnfinished finds, by last_status, the messages the engine still
	// has something to do with: pending, sending and queued_remotely ones,
	// and failed ones with a next_retry_at or a cycle_id.
	MessagesUnfinished *filestore.Index[MessageRow]
	// MessagesPending finds the pending messages by PendingTypeKey and by
	// PendingDestKey.
	MessagesPending *filestore.Index[MessageRow]
	// MessagesByAlert finds messages by filestore.IntKey of their alert_id.
	MessagesByAlert *filestore.Index[MessageRow]
	// MessagesByCycle finds messages by cycle_id.
	MessagesByCycle *filestore.Index[MessageRow]
	// MessagesByProviderMsgID finds the message with a provider_msg_id, as
	// ProviderMsgIDColumn gives it.
	MessagesByProviderMsgID *filestore.Unique[MessageRow]

	byMethodType     *filestore.Index[MessageRow] // by methodTypeKey
	byCreated        timeIndex
	bySent           timeIndex
	historyByMessage *filestore.Index[StatusHistoryRow]
}

// DefineDeltaTables adds the notification store's tables to the Delta
// backend's schema: user_verification_codes, keyed by id
// (user_verification_codes_pkey), outgoing_messages and
// message_status_history.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	t.Codes = filestore.NewTable(s, "user_verification_codes", func(c VerificationCodeRow) string { return c.ID })
	t.CodesByMethod = t.Codes.Unique("user_verification_codes_contact_method_id_key", func(c VerificationCodeRow) (string, bool) {
		return c.ContactMethodID, true
	})
	t.defineMessages(s)
	filestore.References(t.Messages, "outgoing_messages_user_verification_code_id_fkey", t.Codes,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.UserVerificationCodeID) }, filestore.Cascade, nil)
	return t
}

func intRef(id *int64) (string, bool) {
	if id == nil {
		return "", false
	}
	return filestore.IntKey(*id), true
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables. The keys of alerts and alert_logs are filestore.IntKey of
// their ids.
func DeclareDeltaForeignKeys[A, L, N, C, P, D, S, U any](t *DeltaTables,
	alerts *filestore.Table[A], alertLogs *filestore.Table[L],
	channels *filestore.Table[N], contactMethods *filestore.Table[C],
	policies *filestore.Table[P], schedules *filestore.Table[D],
	services *filestore.Table[S], users *filestore.Table[U],
) {
	filestore.References(t.Codes, "user_verification_codes_contact_method_id_fkey", contactMethods,
		func(c VerificationCodeRow) (string, bool) { return c.ContactMethodID, true }, filestore.Cascade, nil)

	filestore.References(t.Messages, "outgoing_messages_alert_id_fkey", alerts,
		func(m MessageRow) (string, bool) { return intRef(m.AlertID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_alert_log_id_fkey", alertLogs,
		func(m MessageRow) (string, bool) { return intRef(m.AlertLogID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_channel_id_fkey", channels,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.ChannelID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_contact_method_id_fkey", contactMethods,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.ContactMethodID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_escalation_policy_id_fkey", policies,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.EscalationPolicyID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_schedule_id_fkey", schedules,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.ScheduleID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_service_id_fkey", services,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.ServiceID) }, filestore.Cascade, nil)
	filestore.References(t.Messages, "outgoing_messages_user_id_fkey", users,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.UserID) }, filestore.Cascade, nil)
}

// DeclareDeltaCycleForeignKey declares the foreign key of outgoing_messages
// to the engine's notification_policy_cycles, which is ported after this
// package.
func DeclareDeltaCycleForeignKey[Y any](t *DeltaTables, cycles *filestore.Table[Y]) {
	filestore.References(t.Messages, "outgoing_messages_cycle_id_fkey", cycles,
		func(m MessageRow) (string, bool) { return filestore.Ref(m.CycleID) }, filestore.Cascade, nil)
}

// DeltaLookups are what the notification store needs of other packages'
// tables on the Delta backend. Every one is required. Those that read return
// false, or sql.ErrNoRows, for a missing row.
type DeltaLookups struct {
	// Of user_contact_methods.
	MethodOwner     func(r filestore.Reader, cmID string) (userID string, ok bool)
	MethodDest      func(r filestore.Reader, cmID string) (gadb.DestV1, error)
	MethodDisabled  func(r filestore.Reader, cmID string) (disabled, ok bool)
	MethodTypeValue func(r filestore.Reader, cmID string) (typ, value string, ok bool)
	// SetMethodDisabled sets disabled, as trg_cm_set_not_pending_on_verify
	// wants it, and reports whether the contact method exists.
	SetMethodDisabled func(tx *filestore.Tx, cmID string, disabled bool) (bool, error)
	// SetMethodLastTestVerify sets last_test_verify_at to now, unless it is
	// minAge old or less, and reports whether it did.
	SetMethodLastTestVerify func(tx *filestore.Tx, cmID string, minAge time.Duration) (bool, error)

	// Of notification_channels.
	ChannelDest     func(r filestore.Reader, channelID string) (gadb.DestV1, error)
	ChannelNameType func(r filestore.Reader, channelID string) (name, typ string, ok bool)

	// Of users and services.
	UserName    func(r filestore.Reader, userID string) (string, bool)
	ServiceName func(r filestore.Reader, serviceID string) (string, bool)
}

func (l DeltaLookups) complete() bool {
	return l.MethodOwner != nil && l.MethodDest != nil && l.MethodDisabled != nil && l.MethodTypeValue != nil &&
		l.SetMethodDisabled != nil && l.SetMethodLastTestVerify != nil &&
		l.ChannelDest != nil && l.ChannelNameType != nil && l.UserName != nil && l.ServiceName != nil
}

// NewDeltaStore returns a notification store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, lookups DeltaLookups) *Store {
	if !lookups.complete() {
		panic("notification: NewDeltaStore needs every lookup") // now, not at the first page
	}
	return &Store{delta: &deltaStore{db: db, DeltaTables: tables, look: lookups}}
}

type deltaStore struct {
	db txn.DB
	*DeltaTables
	look DeltaLookups
}

// canonicalID is a valid UUID as the tables hold it. Callers can pass others
// in, such as uppercase ones.
func canonicalID(id string) (string, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// nullDest is the result of a destination lookup: not valid for a missing
// row, like an outer join.
func nullDest(dest gadb.DestV1, err error) (gadb.NullDestV1, error) {
	if errors.Is(err, sql.ErrNoRows) {
		return gadb.NullDestV1{}, nil
	}
	if err != nil {
		return gadb.NullDestV1{}, err
	}
	return gadb.NullDestV1{Valid: true, DestV1: dest}, nil
}

func (d *deltaStore) sendResult(r filestore.Reader, m MessageRow) (*SendResult, error) {
	msg, err := m.outgoingMessage()
	if err != nil {
		return nil, err
	}
	var cm, ch gadb.NullDestV1
	if m.ContactMethodID != nil {
		if cm, err = nullDest(d.look.MethodDest(r, *m.ContactMethodID)); err != nil {
			return nil, err
		}
	}
	if m.ChannelID != nil {
		if ch, err = nullDest(d.look.ChannelDest(r, *m.ChannelID)); err != nil {
			return nil, err
		}
	}
	return outgoingMessageToSendResult(msg, cm, ch)
}

// originalMessageStatus is for the alert's message to dst that was sent
// first, before any not sent yet.
func (d *deltaStore) originalMessageStatus(ctx context.Context, alertID int, dst DestID) (*SendResult, error) {
	var res *SendResult
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var first MessageRow
		found := false
		d.MessagesByAlert.Ascend(s.Reader, filestore.IntKey(int64(alertID)), func(m MessageRow) bool {
			toMethod := dst.CMID.Valid && m.ContactMethodID != nil && *m.ContactMethodID == dst.CMID.UUID.String()
			toChannel := dst.NCID.Valid && m.ChannelID != nil && *m.ChannelID == dst.NCID.UUID.String()
			if m.MessageType != typeAlert || !toMethod && !toChannel {
				return true
			}
			earlier := m.SentAt != nil && (first.SentAt == nil || m.SentAt.Before(*first.SentAt))
			if !found || earlier {
				first, found = m, true
			}
			return true
		})
		res = nil
		if !found {
			return nil
		}
		var err error
		res, err = d.sendResult(s.Reader, first)
		return err
	})
	return res, err
}

func (d *deltaStore) methodOwner(ctx context.Context, cmID string) (string, error) {
	cmID, err := canonicalID(cmID)
	if err != nil {
		return "", err
	}
	var userID string
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var ok bool
		if userID, ok = d.look.MethodOwner(s.Reader, cmID); !ok {
			return sql.ErrNoRows
		}
		return nil
	})
	return userID, err
}

func (d *deltaStore) code(ctx context.Context, id string) (int, error) {
	id, err := canonicalID(id)
	if err != nil {
		return 0, err
	}
	var code int
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		row, ok := d.Codes.Get(s.Reader, id)
		if !ok {
			return sql.ErrNoRows
		}
		code = int(row.Code)
		return nil
	})
	return code, err
}

// sendTest queues a test message. Two pods asked at once send one: the
// transaction that loses runs again, and the contact method's
// last_test_verify_at refuses it (F4).
func (d *deltaStore) sendTest(ctx context.Context, cmID string) error {
	cmID, err := canonicalID(cmID)
	if err != nil {
		return err
	}
	msgID := uuid.NewString()
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		disabled, ok := d.look.MethodDisabled(s.Reader, cmID)
		if !ok {
			return sql.ErrNoRows
		}
		if disabled {
			return validation.NewFieldError("ContactMethod", "contact method disabled")
		}
		ok, err := d.look.SetMethodLastTestVerify(s.Tx, cmID, minTimeBetweenTests)
		if err != nil {
			return err
		}
		if !ok {
			return validation.NewFieldError("ContactMethod", "test message rate-limit exceeded")
		}
		userID, ok := d.look.MethodOwner(s.Reader, cmID)
		if !ok {
			return sql.ErrNoRows
		}
		_, err = d.InsertMessage(s.Tx, MessageRow{ID: msgID, MessageType: typeTest, ContactMethodID: &cmID, UserID: &userID})
		return err
	})
}

// newCode returns a random six-digit verification code.
func newCode() (int32, error) {
	n, err := cRand.Int(cRand.Reader, big.NewInt(900000))
	if err != nil {
		return 0, fmt.Errorf("generate verification code: %w", err)
	}
	return int32(n.Int64()) + 100000, nil
}

// sendVerification gives a contact method a verification code, which the
// engine then sends. A contact method has one code: while it is valid, asking
// again sends the same one, and renews it.
func (d *deltaStore) sendVerification(ctx context.Context, cmID string) error {
	cmID, err := canonicalID(cmID)
	if err != nil {
		return err
	}
	code, err := newCode()
	if err != nil {
		return err
	}
	id := uuid.NewString()
	return deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		ok, err := d.look.SetMethodLastTestVerify(s.Tx, cmID, minTimeBetweenTests)
		if err != nil {
			return err
		}
		if !ok {
			return validation.NewFieldError("ContactMethod", fmt.Sprintf("Too many messages! Please try again in %.0f minute(s)", minTimeBetweenTests.Minutes()))
		}

		expires := s.Now.Add(codeLifetime)
		row, ok := d.CodesByMethod.Get(s.Reader, cmID)
		if ok && s.Now.Before(row.ExpiresAt) {
			row.Sent, row.ExpiresAt = false, expires
			return d.Codes.Put(s.Tx, row)
		}
		if ok {
			if _, err := d.Codes.Delete(s.Tx, row.ID); err != nil {
				return err
			}
		}
		return d.Codes.Insert(s.Tx, VerificationCodeRow{ID: id, ContactMethodID: cmID, Code: code, ExpiresAt: expires}, "user_verification_codes_pkey")
	})
}

// verify enables a contact method if code is its valid verification code,
// uses the code up, and reports whether it was. If two pods verify the same
// code at once, the transaction that loses runs again and finds it gone, so
// only one succeeds (F4).
func (d *deltaStore) verify(ctx context.Context, cmID string, code int) (bool, error) {
	cmID, err := canonicalID(cmID)
	if err != nil {
		return false, err
	}
	var verified bool
	err = deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		verified = false
		row, ok := d.CodesByMethod.Get(s.Reader, cmID)
		if !ok || int(row.Code) != code || !s.Now.Before(row.ExpiresAt) {
			return nil
		}
		// The messages that carried the code go with it, through the foreign
		// key.
		if _, err := d.Codes.Delete(s.Tx, row.ID); err != nil {
			return err
		}
		var err error
		verified, err = d.look.SetMethodDisabled(s.Tx, cmID, false)
		return err
	})
	return verified, err
}

// findManyMessageStatuses returns each message once, like `where id =
// any($1)`.
func (d *deltaStore) findManyMessageStatuses(ctx context.Context, ids []uuid.UUID) ([]SendResult, error) {
	var result []SendResult
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil
		seen := make(map[uuid.UUID]bool, len(ids))
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			m, ok := d.Messages.Get(s.Reader, id.String())
			if !ok {
				continue
			}
			res, err := d.sendResult(s.Reader, m)
			if err != nil {
				return err
			}
			result = append(result, *res)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// lastMessageStatus is for the newest message of a type to a contact method,
// created at or after from, and nil if there is none.
func (d *deltaStore) lastMessageStatus(ctx context.Context, typ gadb.EnumOutgoingMessagesType, cmID uuid.UUID, from time.Time) (*SendResult, time.Time, error) {
	from = from.Truncate(time.Microsecond)
	var res *SendResult
	var created time.Time
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		res, created = nil, time.Time{}
		var last MessageRow
		found := false
		d.byMethodType.Ascend(s.Reader, methodTypeKey(cmID.String(), string(typ)), func(m MessageRow) bool {
			if !m.CreatedAt.Before(from) && (!found || m.CreatedAt.After(last.CreatedAt)) {
				last, found = m, true
			}
			return true
		})
		if !found {
			return nil
		}
		var err error
		res, err = d.sendResult(s.Reader, last)
		created = last.CreatedAt
		return err
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	return res, created, nil
}

// messageStatusHistory returns a message's status changes, newest first.
func (d *deltaStore) messageStatusHistory(ctx context.Context, messageID uuid.UUID) ([]gadb.MessageStatusHistory, error) {
	var hist []gadb.MessageStatusHistory
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		hist = nil
		d.historyByMessage.Ascend(s.Reader, messageID.String(), func(h StatusHistoryRow) bool {
			hist = append(hist, gadb.MessageStatusHistory{
				ID:            h.ID,
				MessageID:     messageID,
				Status:        gadb.EnumOutgoingMessagesStatus(h.Status),
				StatusDetails: h.StatusDetails,
				Timestamp:     h.Timestamp,
			})
			return true
		})
		sortHistory(hist)
		return nil
	})
	return hist, err
}
