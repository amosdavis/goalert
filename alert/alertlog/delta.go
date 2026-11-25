package alertlog

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/alert/internal/deltaquery"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/integrationkey"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// DeltaLog is an alert_logs row on the Delta backend.
type DeltaLog struct {
	ID                  int64      `parquet:"id"`
	AlertID             *int64     `parquet:"alert_id,optional"`
	Timestamp           *time.Time `parquet:"timestamp,optional,timestamp(microsecond:utc)"`
	Event               string     `parquet:"event"`
	Message             string     `parquet:"message"`
	SubType             *string    `parquet:"sub_type,optional"`
	SubUserID           *string    `parquet:"sub_user_id,optional"`
	SubIntegrationKeyID *string    `parquet:"sub_integration_key_id,optional"`
	SubHBMonitorID      *string    `parquet:"sub_hb_monitor_id,optional"`
	SubChannelID        *string    `parquet:"sub_channel_id,optional"`
	SubClassifier       string     `parquet:"sub_classifier"`
	Meta                []byte     `parquet:"meta,optional"`
}

// events are the values of enum_alert_log_event.
var events = []Type{
	TypeAcknowledged, "assignment_changed", TypeClosed, TypeCreated, TypeDuplicateSupressed, TypeEscalated,
	TypeEscalationRequest, TypeNoNotificationSent, TypeNotificationSent, TypePolicyUpdated, "reopened",
	_TypeResponseReceived, _TypeStatusChanged,
}

// dupWindow is how long after a duplicate was suppressed no other entry about
// one is added. Duplicates are indexed by time in steps of that length, so a
// new one is checked without reading all of them (F15).
const dupWindow = 5 * time.Second

func dupKey(alertID int64, step int64) string {
	return filestore.IntKey(alertID) + "/" + strconv.FormatInt(step, 10)
}

func dupStep(t time.Time) int64 { return t.Unix() / int64(dupWindow/time.Second) }

func alertEventKey(alertID int64, event Type) string {
	return filestore.IntKey(alertID) + "/" + string(event)
}

func userEventKey(userID string, event Type) string { return userID + "/" + string(event) }

// DeltaTables are the alert log's tables on the Delta backend. Other packages
// declare their foreign keys to Logs. Rows are only added, with Insert, and
// deleted by the engine's cleanup; like in Postgres, they stay when their
// alert is deleted, until then.
type DeltaTables struct {
	Logs *filestore.Table[DeltaLog]

	byAlertEvent, byUserEvent, byDuplicate, byClosed *filestore.Index[DeltaLog]
}

// DefineDeltaTables adds alert_logs, keyed by id (alert_logs_pkey), to the
// Delta backend's schema.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}
	// Every change of an alert adds a row, which rewrites the row's bucket
	// (F39).
	t.Logs = filestore.NewTable(s, "alert_logs", func(l DeltaLog) string { return filestore.IntKey(l.ID) }).Buckets(64)
	// This also serves idx_alert_logs_alert_id, one event after the other.
	t.byAlertEvent = t.Logs.Index("idx_alert_logs_alert_event", func(l DeltaLog) []string {
		if l.AlertID == nil {
			return nil
		}
		return []string{alertEventKey(*l.AlertID, Type(l.Event))}
	})
	t.byUserEvent = t.Logs.Index("idx_alert_logs_user_id", func(l DeltaLog) []string {
		if l.SubUserID == nil {
			return nil
		}
		return []string{userEventKey(*l.SubUserID, Type(l.Event))}
	})
	t.byDuplicate = t.Logs.Index("alert_logs_duplicate_suppressed", func(l DeltaLog) []string {
		if l.AlertID == nil || l.Timestamp == nil || Type(l.Event) != TypeDuplicateSupressed {
			return nil
		}
		return []string{dupKey(*l.AlertID, dupStep(*l.Timestamp))}
	})
	t.byClosed = t.Logs.Index("idx_closed_events", func(l DeltaLog) []string {
		if l.Timestamp == nil || Type(l.Event) != TypeClosed {
			return nil
		}
		return deltaquery.DayKeys(*l.Timestamp)
	})
	return t
}

// Insert adds a row, with the next ID and the transaction's time if it has
// none.
func (t *DeltaTables) Insert(tx *filestore.Tx, l DeltaLog) error {
	if !slices.Contains(events, Type(l.Event)) {
		return fmt.Errorf("invalid alert log event %q", l.Event)
	}
	if l.ID == 0 {
		var err error
		if l.ID, err = tx.NextID("alert_logs_id_seq"); err != nil {
			return err
		}
	}
	if l.Timestamp == nil {
		now := tx.Now()
		l.Timestamp = &now
	}
	return t.Logs.Insert(tx, l, "alert_logs_pkey")
}

// AscendEvent calls fn, in r, for an alert's entries of one type, oldest
// first, until fn returns false.
func (t *DeltaTables) AscendEvent(r filestore.Reader, alertID int64, event Type, fn func(DeltaLog) bool) {
	t.byAlertEvent.Ascend(r, alertEventKey(alertID, event), fn)
}

// AscendAlert calls fn, in r, for all of an alert's entries, one type after
// the other and oldest first within a type, until fn returns false.
func (t *DeltaTables) AscendAlert(r filestore.Reader, alertID int64, fn func(DeltaLog) bool) {
	more := true
	for _, event := range events {
		t.AscendEvent(r, alertID, event, func(l DeltaLog) bool {
			more = fn(l)
			return more
		})
		if !more {
			return
		}
	}
}

// AscendClosed calls fn, in r, for the entries about alerts that were closed
// on the days from from to to, earlier days first and then by id, until fn
// returns false. A zero time is no bound.
func (t *DeltaTables) AscendClosed(r filestore.Reader, from, to time.Time, fn func(DeltaLog) bool) {
	deltaquery.Days(t.byClosed, r, from, to, false, func(rows []DeltaLog) bool {
		for _, l := range rows {
			if !fn(l) {
				return false
			}
		}
		return true
	})
}

// AscendNotified calls fn, in r, for each time a user was notified of an
// alert, or would have been but for their rules, until fn returns false.
func (t *DeltaTables) AscendNotified(r filestore.Reader, userID string, fn func(alertID int64) bool) {
	more := true
	for _, event := range []Type{TypeNotificationSent, TypeNoNotificationSent} {
		t.byUserEvent.Ascend(r, userEventKey(userID, event), func(l DeltaLog) bool {
			if l.AlertID != nil {
				more = fn(*l.AlertID)
			}
			return more
		})
		if !more {
			return
		}
	}
}

// hasRecentDuplicate reports whether a suppressed duplicate was logged for
// the alert within dupWindow.
func (t *DeltaTables) hasRecentDuplicate(r filestore.Reader, alertID int64, now time.Time) bool {
	found := false
	since := now.Add(-dupWindow)
	for step := dupStep(since); step <= dupStep(now) && !found; step++ {
		t.byDuplicate.Ascend(r, dupKey(alertID, step), func(l DeltaLog) bool {
			found = l.Timestamp.After(since)
			return !found
		})
	}
	return found
}

// DeltaLookups are the reads of other packages' tables that the alert log
// needs. They run in its transactions, so they must do no I/O. A method that
// needs one that is absent returns deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// Names, for reading entries: the joins with users, integration_keys,
	// heartbeat_monitors and notification_channels.
	UserName             func(r filestore.Reader, id string) (name string, ok bool)
	IntegrationKeyName   func(r filestore.Reader, id string) (name string, ok bool)
	HeartbeatMonitorName func(r filestore.Reader, id string) (name string, ok bool)
	ChannelName          func(r filestore.Reader, id string) (name string, ok bool)

	// What describes the subject of a new entry. A destination that doesn't
	// exist is sql.ErrNoRows. MessageDest is that of an outgoing message: its
	// contact method's, or its channel's, or an empty one if it has neither.
	ChannelDest        func(r filestore.Reader, id string) (gadb.DestV1, error)
	ContactMethodDest  func(r filestore.Reader, id string) (gadb.DestV1, error)
	MessageDest        func(r filestore.Reader, messageID string) (gadb.DestV1, error)
	IntegrationKeyType func(r filestore.Reader, id string) (typ integrationkey.Type, ok bool)
	HeartbeatInterval  func(r filestore.Reader, monitorID string) (interval time.Duration, ok bool)

	// PolicyServices returns the IDs of the services that use an escalation
	// policy (services), and AlertIDs those of a service's alerts with one of
	// the statuses, or all of them without any (alert.DeltaTables.AlertIDs).
	PolicyServices func(r filestore.Reader, escalationPolicyID string) []string
	AlertIDs       func(r filestore.Reader, serviceID string, statuses ...string) []int64
}

// NewDeltaStore returns an alert log on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, reg *nfydest.Registry, lookups DeltaLookups) *Store {
	return &Store{reg: reg, delta: &deltaLogs{db: db, reg: reg, DeltaTables: tables, DeltaLookups: lookups}}
}

type deltaLogs struct {
	db  txn.DB
	reg *nfydest.Registry
	*DeltaTables
	DeltaLookups

	typeNames sync.Map // destination type -> its name
}

// typeName returns the name of a destination type. Looking one up can call the
// provider's API, so it is remembered (names never change), and in a
// transaction a lookup gets a context that is already canceled, which keeps a
// provider from doing any I/O (F3). Providers return the name regardless.
func (d *deltaLogs) typeName(ctx context.Context, destType string, inTx bool) (string, error) {
	if name, ok := d.typeNames.Load(destType); ok {
		return name.(string), nil
	}
	if inTx {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	info, err := d.reg.TypeInfo(ctx, destType)
	if err != nil {
		return "", err
	}
	d.typeNames.Store(destType, info.Name)
	return info.Name, nil
}

// prepare remembers the name of the destination type of ctx's source, before a
// transaction that adds entries on its behalf. What is missing now is missing
// then, too, and reported there.
func (d *deltaLogs) prepare(ctx context.Context) {
	src := permission.Source(ctx)
	if src == nil {
		return
	}
	var lookup func(subjects, uuid.UUID) (gadb.NullDestV1, error)
	switch src.Type {
	case permission.SourceTypeNotificationChannel:
		lookup = func(s subjects, id uuid.UUID) (gadb.NullDestV1, error) { return s.channelDest(ctx, id) }
	case permission.SourceTypeContactMethod:
		lookup = func(s subjects, id uuid.UUID) (gadb.NullDestV1, error) { return s.contactMethodDest(ctx, id) }
	case permission.SourceTypeNotificationCallback:
		lookup = func(s subjects, id uuid.UUID) (gadb.NullDestV1, error) { return s.messageDest(ctx, id) }
	default:
		return
	}
	id, err := uuid.Parse(src.ID)
	if err != nil {
		return
	}
	var dest gadb.NullDestV1
	_ = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) (err error) {
		dest, err = lookup(deltaSubjects{deltaLogs: d, r: s.Reader}, id)
		return err
	})
	if dest.Valid {
		_, _ = d.typeName(ctx, dest.DestV1.Type, false)
	}
}

// deltaSubjects looks up the subjects of new entries in a transaction.
type deltaSubjects struct {
	*deltaLogs
	r filestore.Reader
}

var _ subjects = deltaSubjects{}

func (d deltaSubjects) dest(lookup func(filestore.Reader, string) (gadb.DestV1, error), id uuid.UUID) (gadb.NullDestV1, error) {
	if lookup == nil {
		return gadb.NullDestV1{}, deltatxn.ErrNotPorted
	}
	dest, err := lookup(d.r, id.String())
	if err != nil {
		return gadb.NullDestV1{}, err
	}
	return gadb.NullDestV1{Valid: dest.Type != "", DestV1: dest}, nil
}

func (d deltaSubjects) channelDest(_ context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return d.dest(d.ChannelDest, id)
}

func (d deltaSubjects) contactMethodDest(_ context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return d.dest(d.ContactMethodDest, id)
}

func (d deltaSubjects) messageDest(_ context.Context, id uuid.UUID) (gadb.NullDestV1, error) {
	return d.dest(d.MessageDest, id)
}

// heartbeatMinutes rounds as EXTRACT(...)::int does.
func (d deltaSubjects) heartbeatMinutes(_ context.Context, id uuid.UUID) (int32, error) {
	if d.HeartbeatInterval == nil {
		return 0, deltatxn.ErrNotPorted
	}
	interval, ok := d.HeartbeatInterval(d.r, id.String())
	if !ok {
		return 0, sql.ErrNoRows
	}
	return int32(math.Round(interval.Minutes())), nil
}

func (d deltaSubjects) integrationKeyType(_ context.Context, id string) (integrationkey.Type, error) {
	if d.IntegrationKeyType == nil {
		return "", deltatxn.ErrNotPorted
	}
	u, err := uuid.Parse(id)
	if err != nil {
		return "", err // the Postgres query fails too
	}
	typ, ok := d.IntegrationKeyType(d.r, u.String())
	if !ok {
		return "", sql.ErrNoRows
	}
	return typ, nil
}

func (d deltaSubjects) typeName(ctx context.Context, destType string) (string, error) {
	return d.deltaLogs.typeName(ctx, destType, true)
}

func optText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func optID(id uuid.NullUUID) *string {
	if !id.Valid {
		return nil
	}
	return optText(id.UUID.String())
}

// row returns the new row for an entry.
func (e *Entry) row(alertID int64) DeltaLog {
	return DeltaLog{
		AlertID:             &alertID,
		Event:               string(e._type),
		Message:             e.message,
		SubType:             optText(string(e.subject._type)),
		SubUserID:           optID(e.subject.userID),
		SubIntegrationKeyID: optID(e.subject.integrationKeyID),
		SubHBMonitorID:      optID(e.subject.heartbeatMonitorID),
		SubChannelID:        optID(e.subject.channelID),
		SubClassifier:       e.subject.classifier,
		Meta:                e.meta,
	}
}

// log adds an entry for each of the alerts that ids returns. They get their
// IDs in that order.
func (d *deltaLogs) log(ctx context.Context, tx txn.Tx, _type Type, meta any, ids func(r filestore.Reader, event Type) ([]int64, error)) error {
	if tx == nil {
		d.prepare(ctx)
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		e, err := logEntry(ctx, deltaSubjects{deltaLogs: d, r: s.Reader}, _type, meta)
		if err != nil {
			return err
		}
		alertIDs, err := ids(s.Reader, e._type)
		if err != nil {
			return err
		}
		for _, id := range alertIDs {
			if err := d.Insert(s.Tx, e.row(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// logMany is AlertLog_InsertMany. Like the Postgres query, it doesn't check
// that the alerts exist.
func (d *deltaLogs) logMany(ctx context.Context, tx txn.Tx, alertIDs []int, _type Type, meta any) error {
	return d.log(ctx, tx, _type, meta, func(filestore.Reader, Type) ([]int64, error) {
		ids := make([]int64, len(alertIDs))
		for i, id := range alertIDs {
			ids[i] = int64(id)
		}
		return ids, nil
	})
}

// logEP is AlertLog_InsertEP: an entry for every open alert of the services
// that use the policy.
func (d *deltaLogs) logEP(ctx context.Context, tx txn.Tx, epID uuid.UUID, _type Type, meta any) error {
	if d.PolicyServices == nil || d.AlertIDs == nil {
		return deltatxn.ErrNotPorted
	}
	return d.log(ctx, tx, _type, meta, func(r filestore.Reader, _ Type) ([]int64, error) {
		var ids []int64
		for _, svcID := range d.PolicyServices(r, epID.String()) {
			ids = append(ids, d.AlertIDs(r, svcID, "triggered", "active")...)
		}
		slices.Sort(ids)
		return ids, nil
	})
}

// logService is AlertLog_InsertSvc: an entry for every alert of the service
// that the event is about.
func (d *deltaLogs) logService(ctx context.Context, tx txn.Tx, serviceID uuid.UUID, _type Type, meta any) error {
	if d.AlertIDs == nil {
		return deltatxn.ErrNotPorted
	}
	return d.log(ctx, tx, _type, meta, func(r filestore.Reader, event Type) ([]int64, error) {
		switch event {
		case TypeClosed:
			return d.AlertIDs(r, serviceID.String(), "triggered", "active"), nil
		case TypeAcknowledged, TypeNotificationSent:
			return d.AlertIDs(r, serviceID.String(), "triggered"), nil
		}
		return nil, nil
	})
}

// logDuplicate adds an entry for a suppressed duplicate, unless there was one
// in the last 5 seconds. It checks and adds in one transaction, so two pods
// that suppress one at once log it once (F4).
func (d *deltaLogs) logDuplicate(ctx context.Context, tx txn.Tx, alertID int, meta any) error {
	if tx == nil {
		d.prepare(ctx)
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		if d.hasRecentDuplicate(s.Reader, int64(alertID), s.Now) {
			return nil
		}
		e, err := logEntry(ctx, deltaSubjects{deltaLogs: d, r: s.Reader}, TypeDuplicateSupressed, meta)
		if err != nil {
			return err
		}
		return d.Insert(s.Tx, e.row(int64(alertID)))
	})
}

// subject sets the ID of one of an entry's subjects, and its name if the row
// still exists (a LEFT JOIN).
func subject(r filestore.Reader, lookup func(filestore.Reader, string) (string, bool), id *string, dst *uuid.NullUUID, name *sql.NullString) error {
	if id == nil {
		return nil
	}
	if lookup == nil {
		return deltatxn.ErrNotPorted
	}
	u, err := uuid.Parse(*id)
	if err != nil {
		return err
	}
	*dst = uuid.NullUUID{UUID: u, Valid: true}
	name.String, name.Valid = lookup(r, *id)
	return nil
}

// entry returns a row as the Postgres queries do.
func (d *deltaLogs) entry(r filestore.Reader, l DeltaLog) (*Entry, error) {
	e := &Entry{id: int(l.ID), _type: Type(l.Event), message: l.Message}
	if l.AlertID != nil {
		e.alertID = int(*l.AlertID)
	}
	if l.Timestamp != nil {
		e.timestamp = *l.Timestamp
	}
	if l.SubType != nil {
		e.subject._type = SubjectType(*l.SubType)
	}
	e.subject.classifier = l.SubClassifier
	e.meta = slices.Clone(l.Meta) // rows are shared

	for _, err := range []error{
		subject(r, d.UserName, l.SubUserID, &e.subject.userID, &e.subject.userName),
		subject(r, d.IntegrationKeyName, l.SubIntegrationKeyID, &e.subject.integrationKeyID, &e.subject.integrationKeyName),
		subject(r, d.HeartbeatMonitorName, l.SubHBMonitorID, &e.subject.heartbeatMonitorID, &e.subject.heartbeatMonitorName),
		subject(r, d.ChannelName, l.SubChannelID, &e.subject.channelID, &e.subject.channelName),
	} {
		if err != nil {
			return nil, errors.Wrap(err, "lookup subject of alert log entry")
		}
	}
	return e, nil
}

// findOne returns sql.ErrNoRows for a missing entry.
func (d *deltaLogs) findOne(ctx context.Context, logID int) (*Entry, error) {
	var e *Entry
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) (err error) {
		l, ok := d.Logs.Get(s.Reader, filestore.IntKey(int64(logID)))
		if !ok {
			return sql.ErrNoRows
		}
		e, err = d.entry(s.Reader, l)
		return err
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

// findLatestByType returns sql.ErrNoRows without an entry.
func (d *deltaLogs) findLatestByType(ctx context.Context, alertID int, event Type) (*Entry, error) {
	var e *Entry
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) (err error) {
		var latest DeltaLog
		d.AscendEvent(s.Reader, int64(alertID), event, func(l DeltaLog) bool {
			latest = l // in the order of their ids
			return true
		})
		if latest.ID == 0 {
			return sql.ErrNoRows
		}
		e, err = d.entry(s.Reader, latest)
		return err
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

// search is the search template. Without a filter it reads entries from the
// newest until it has enough.
func (d *deltaLogs) search(ctx context.Context, opts *renderData) ([]Entry, error) {
	var serviceID string
	if opts.FilterServiceID != nil {
		if d.AlertIDs == nil {
			return nil, deltatxn.ErrNotPorted
		}
		serviceID = opts.FilterServiceID.String()
	}
	var result []Entry
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil // as the Postgres store returns without rows
		match := func(l DeltaLog) bool {
			return (opts.After.ID == 0 || l.ID < int64(opts.After.ID)) &&
				(opts.Since == nil || (l.Timestamp != nil && !l.Timestamp.Before(*opts.Since)))
		}
		newest := deltaquery.NewTop(opts.Limit, func(a, b DeltaLog) int { return cmp.Compare(b.ID, a.ID) })

		ids := make([]int64, 0, len(opts.FilterAlertIDs))
		for _, id := range opts.FilterAlertIDs {
			ids = append(ids, int64(id))
		}
		filtered := len(ids) > 0 || serviceID != ""
		if serviceID != "" {
			ofService := d.AlertIDs(s.Reader, serviceID)
			if len(ids) == 0 {
				ids = ofService
			} else {
				ids = slices.DeleteFunc(ids, func(id int64) bool { return !slices.Contains(ofService, id) })
			}
		}
		if filtered {
			slices.Sort(ids)
			for _, id := range slices.Compact(ids) {
				d.AscendAlert(s.Reader, id, func(l DeltaLog) bool {
					if match(l) {
						newest.Add(l)
					}
					return true
				})
			}
		} else {
			n := 0
			d.Logs.Descend(s.Reader, func(l DeltaLog) bool {
				if match(l) {
					newest.Add(l)
					n++
				}
				return n < opts.Limit
			})
		}

		for _, l := range newest.Rows() {
			e, err := d.entry(s.Reader, l)
			if err != nil {
				return err
			}
			result = append(result, *e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
