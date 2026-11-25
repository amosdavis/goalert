package twilio

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// smsCallbackRow is a twilio_sms_callbacks row on the Delta backend: the
// alert or service that a reply code in an SMS to a number stands for.
type smsCallbackRow struct {
	ID          int64     `parquet:"id"`
	PhoneNumber string    `parquet:"phone_number"`
	Code        int32     `parquet:"code"`
	CallbackID  string    `parquet:"callback_id"`
	AlertID     *int64    `parquet:"alert_id,optional"`
	ServiceID   *string   `parquet:"service_id,optional"`
	SentAt      time.Time `parquet:"sent_at,timestamp(microsecond:utc)"`
}

// errorRow is a twilio_sms_errors or twilio_voice_errors row on the Delta
// backend.
type errorRow struct {
	ID           int64     `parquet:"id"`
	PhoneNumber  string    `parquet:"phone_number"`
	ErrorMessage string    `parquet:"error_message"`
	Outgoing     bool      `parquet:"outgoing"`
	OccurredAt   time.Time `parquet:"occurred_at,timestamp(microsecond:utc)"`
}

// phoneKey is an index key for a phone number, which keeps the number out of
// error messages (F20).
func phoneKey(number string) string {
	sum := sha256.Sum256([]byte(number))
	return hex.EncodeToString(sum[:])
}

func codeKey(number string, code int) string { return phoneKey(number) + "/" + strconv.Itoa(code) }

// DeltaTables are this package's tables on the Delta backend.
type DeltaTables struct {
	callbacks *filestore.Table[smsCallbackRow]
	byPhone   *filestore.Index[smsCallbackRow]
	byCode    *filestore.Unique[smsCallbackRow]
}

// DefineDeltaTables adds this package's tables to the Delta backend's
// schema: twilio_sms_callbacks, keyed by id (twilio_sms_callbacks_uniq_id),
// and twilio_sms_errors and twilio_voice_errors, keyed by id
// (twilio_sms_errors_uniq_id and twilio_voice_errors_uniq_id), which nothing
// reads or writes.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{
		callbacks: filestore.NewTable(s, "twilio_sms_callbacks", func(r smsCallbackRow) string { return filestore.IntKey(r.ID) }),
	}
	t.byPhone = t.callbacks.Index("twilio_sms_callbacks_phone_number", func(r smsCallbackRow) []string {
		return []string{phoneKey(r.PhoneNumber)}
	})
	t.byCode = t.callbacks.Unique("idx_twilio_sms_codes", func(r smsCallbackRow) (string, bool) {
		return codeKey(r.PhoneNumber, int(r.Code)), true
	})
	for _, name := range []string{"twilio_sms_errors", "twilio_voice_errors"} {
		filestore.NewTable(s, name, func(r errorRow) string { return filestore.IntKey(r.ID) })
	}
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables. The keys of alerts are filestore.IntKey of their ids.
func DeclareDeltaForeignKeys[A, S any](t *DeltaTables, alerts *filestore.Table[A], services *filestore.Table[S]) {
	filestore.References(t.callbacks, "twilio_sms_callbacks_alert_id_fkey", alerts,
		func(r smsCallbackRow) (string, bool) {
			if r.AlertID == nil {
				return "", false
			}
			return filestore.IntKey(*r.AlertID), true
		}, filestore.Cascade, nil)
	filestore.References(t.callbacks, "twilio_sms_callbacks_service_id_fkey", services,
		func(r smsCallbackRow) (string, bool) { return filestore.Ref(r.ServiceID) }, filestore.Cascade, nil)
}

// AlertOpenFunc reports whether, in r, an alert exists and is not closed.
type AlertOpenFunc func(r filestore.Reader, alertID int64) bool

// ServiceNameFunc returns, in r, a service's name, and false if there is no
// such service.
type ServiceNameFunc func(r filestore.Reader, serviceID string) (name string, ok bool)

// NewDeltaSMS is NewSMS on the Delta backend.
func NewDeltaSMS(db txn.DB, tables *DeltaTables, c *Config, alertOpen AlertOpenFunc, serviceName ServiceNameFunc) *SMS {
	if alertOpen == nil || serviceName == nil {
		panic("twilio: NewDeltaSMS needs every lookup") // now, not at the first page
	}
	return newSMS(&dbSMS{delta: &deltaSMS{db: db, DeltaTables: tables, alertOpen: alertOpen, serviceName: serviceName}}, c)
}

type deltaSMS struct {
	db txn.DB
	*DeltaTables
	alertOpen   AlertOpenFunc
	serviceName ServiceNameFunc
}

// insert returns the reply code of an alert, or a service, in messages to a
// number: the one it already has, or the first that no open alert and no
// service has. If two pods allocate codes for a number at once, the
// transaction that loses runs again and finds the other's code in use, so
// they never give the same code to two alerts (F4, F8).
func (d *deltaSMS) insert(ctx context.Context, phoneNumber, callbackID string, alertID int, serviceID string) (int, error) {
	cbID, err := uuid.Parse(callbackID)
	if err != nil {
		return 0, fmt.Errorf("twilio_sms_callbacks: callback_id: %w", err)
	}
	row := smsCallbackRow{PhoneNumber: phoneNumber, CallbackID: cbID.String()}
	if alertID != 0 {
		id := int64(alertID)
		row.AlertID = &id
	}
	if serviceID != "" {
		id, err := uuid.Parse(serviceID)
		if err != nil {
			return 0, fmt.Errorf("twilio_sms_callbacks: service_id: %w", err)
		}
		svcID := id.String()
		row.ServiceID = &svcID
	}

	var code int
	err = deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		var mine *smsCallbackRow
		inUse := make(map[int]bool)
		d.byPhone.Ascend(s.Reader, phoneKey(phoneNumber), func(old smsCallbackRow) bool {
			open := old.AlertID != nil && d.alertOpen(s.Reader, *old.AlertID)
			if old.ServiceID == nil && !open {
				return true
			}
			inUse[int(old.Code)] = true
			sameService := row.ServiceID != nil && old.ServiceID != nil && *old.ServiceID == *row.ServiceID
			sameAlert := row.AlertID != nil && open && *old.AlertID == *row.AlertID
			if mine == nil && (sameService || sameAlert) {
				mine = &old
			}
			return true
		})

		// A reply is to this message, so its callback and time are kept even
		// when the code is not new: an escalation is as of the message, and a
		// reply without a code is for the latest.
		next := row
		next.SentAt = s.Now
		if mine != nil {
			code = int(mine.Code)
			next.ID, next.Code = mine.ID, mine.Code
			next.AlertID, next.ServiceID = mine.AlertID, mine.ServiceID
			return d.callbacks.Put(s.Tx, next)
		}

		code = 1
		if serviceID != "" {
			code = 100
		}
		for inUse[code] {
			code++
		}
		next.Code = int32(code)
		if old, ok := d.byCode.Get(s.Reader, codeKey(phoneNumber, code)); ok {
			// ON CONFLICT (phone_number, code) DO UPDATE: nothing open has
			// the code any more.
			next.ID = old.ID
			return d.callbacks.Put(s.Tx, next)
		}
		id, err := s.Tx.NextID("twilio_sms_callbacks_id_seq")
		if err != nil {
			return err
		}
		next.ID = id
		return d.callbacks.Insert(s.Tx, next, "twilio_sms_callbacks_uniq_id")
	})
	if err != nil {
		return 0, err
	}
	return code, nil
}

// lookup returns what find finds, or sql.ErrNoRows.
func (d *deltaSMS) lookup(ctx context.Context, find func(r filestore.Reader) (codeInfo, bool)) (*codeInfo, error) {
	info := &codeInfo{}
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		found, ok := find(s.Reader)
		if !ok {
			return sql.ErrNoRows
		}
		*info = found
		return nil
	})
	return info, err
}

func (r smsCallbackRow) info() codeInfo {
	info := codeInfo{CallbackID: r.CallbackID}
	if r.AlertID != nil {
		info.AlertID = int(*r.AlertID)
	}
	return info
}

// latest returns the row that was sent last, of those to a number that match.
func (d *deltaSMS) latest(r filestore.Reader, phoneNumber string, match func(smsCallbackRow) bool) (codeInfo, bool) {
	var last smsCallbackRow
	found := false
	d.byPhone.Ascend(r, phoneKey(phoneNumber), func(row smsCallbackRow) bool {
		if match(row) && (!found || !row.SentAt.Before(last.SentAt)) {
			last, found = row, true
		}
		return true
	})
	return last.info(), found
}

// lookupByCode finds an alert by its code, or with code 0, the number's
// latest alert. A service's code is not an alert's: lookupSvcByCode finds it.
func (d *deltaSMS) lookupByCode(ctx context.Context, phoneNumber string, code int) (*codeInfo, error) {
	return d.lookup(ctx, func(r filestore.Reader) (codeInfo, bool) {
		if code == 0 {
			return d.latest(r, phoneNumber, func(row smsCallbackRow) bool { return row.AlertID != nil })
		}
		row, ok := d.byCode.Get(r, codeKey(phoneNumber, code))
		return row.info(), ok && row.AlertID != nil
	})
}

func (d *deltaSMS) lookupByAlertID(ctx context.Context, phoneNumber string, alertID int) (*codeInfo, error) {
	return d.lookup(ctx, func(r filestore.Reader) (codeInfo, bool) {
		return d.latest(r, phoneNumber, func(row smsCallbackRow) bool { return row.AlertID != nil && *row.AlertID == int64(alertID) })
	})
}

func (d *deltaSMS) lookupSvcByCode(ctx context.Context, phoneNumber string, code int) (*codeInfo, error) {
	return d.lookup(ctx, func(r filestore.Reader) (codeInfo, bool) {
		row, ok := d.byCode.Get(r, codeKey(phoneNumber, code))
		if !ok || row.ServiceID == nil {
			return codeInfo{}, false
		}
		name, ok := d.serviceName(r, *row.ServiceID)
		return codeInfo{CallbackID: row.CallbackID, ServiceName: name}, ok
	})
}
