package alert

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/target/goalert/alert/alertlog"
	"github.com/target/goalert/alert/alertmetrics"
	"github.com/target/goalert/alert/internal/deltaquery"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/log"
	"github.com/target/goalert/validation"
)

// DeltaAlert is an alerts row on the Delta backend.
type DeltaAlert struct {
	ID              int64      `parquet:"id"`
	Summary         string     `parquet:"summary"`
	Details         string     `parquet:"details"`
	ServiceID       *string    `parquet:"service_id,optional"`
	Source          string     `parquet:"source"`
	Status          string     `parquet:"status"`
	CreatedAt       time.Time  `parquet:"created_at,timestamp(microsecond:utc)"`
	DedupKey        *string    `parquet:"dedup_key,optional"`
	EscalationLevel int32      `parquet:"escalation_level"`
	LastEscalation  *time.Time `parquet:"last_escalation,optional,timestamp(microsecond:utc)"`
	LastProcessed   *time.Time `parquet:"last_processed,optional,timestamp(microsecond:utc)"`
}

func (a DeltaAlert) serviceID() string {
	if a.ServiceID == nil {
		return ""
	}
	return *a.ServiceID
}

func (a DeltaAlert) alert() (Alert, error) {
	res := Alert{
		ID:        int(a.ID),
		Status:    Status(a.Status),
		Summary:   a.Summary,
		Details:   a.Details,
		Source:    Source(a.Source),
		ServiceID: a.serviceID(),
		CreatedAt: a.CreatedAt,
	}
	if a.DedupKey != nil {
		var err error
		if res.Dedup, err = ParseDedupString(*a.DedupKey); err != nil {
			return Alert{}, errors.Wrapf(err, "dedup key of alert %d", a.ID)
		}
	}
	return res, nil
}

// DeltaSubscription is an alert_status_subscriptions row on the Delta backend.
type DeltaSubscription struct {
	ID              int64     `parquet:"id"`
	ChannelID       *string   `parquet:"channel_id,optional"`
	ContactMethodID *string   `parquet:"contact_method_id,optional"`
	AlertID         int64     `parquet:"alert_id"`
	LastAlertStatus string    `parquet:"last_alert_status"`
	UpdatedAt       time.Time `parquet:"updated_at,timestamp(microsecond:utc)"`
}

// destKey is the subscription's key in the index of destination and alert.
func (s DeltaSubscription) destKey() (string, bool) {
	switch {
	case s.ChannelID != nil:
		return "channel/" + *s.ChannelID + "/" + filestore.IntKey(s.AlertID), true
	case s.ContactMethodID != nil:
		return "contact_method/" + *s.ContactMethodID + "/" + filestore.IntKey(s.AlertID), true
	}
	return "", false
}

// dataRow is an alert_data row, feedbackRow an alert_feedback row.
type (
	dataRow struct {
		ID       int64  `parquet:"id"`
		AlertID  int64  `parquet:"alert_id"`
		Metadata []byte `parquet:"metadata,optional"`
	}
	feedbackRow struct {
		ID          int64  `parquet:"id"`
		AlertID     int64  `parquet:"alert_id"`
		NoiseReason string `parquet:"noise_reason"`
	}
)

// statusOrder is the order of enum_alert_status.
var statusOrder = map[Status]int{StatusTriggered: 0, StatusActive: 1, StatusClosed: 2}

// statuses are the statuses in that order.
var statuses = []Status{StatusTriggered, StatusActive, StatusClosed}

func serviceStatusKey(serviceID string, status Status) string {
	return serviceID + "/" + string(status)
}

func dedupIndexKey(serviceID, dedup string) string { return serviceID + "/" + dedup }

func dedupString(d *DedupID) string { return fmt.Sprintf("%s:%d:%s", d.Type, d.Version, d.Payload) }

// DeltaTables are the alert store's tables on the Delta backend. Other
// packages declare their foreign keys to Alerts, and read it and
// Subscriptions. Only the store writes Alerts (see DeltaInsert and
// DeltaSetStatus), since triggers come with a change; rows may be deleted.
type DeltaTables struct {
	Alerts        *filestore.Table[DeltaAlert]
	Subscriptions *filestore.Table[DeltaSubscription]

	// SubscriptionsByAlert finds the subscriptions to an alert, whose key is
	// filestore.IntKey of its ID.
	SubscriptionsByAlert *filestore.Index[DeltaSubscription]

	data     *filestore.Table[dataRow]
	feedback *filestore.Table[feedbackRow]

	byDedup         *filestore.Unique[DeltaAlert]
	byServiceStatus *filestore.Index[DeltaAlert]
	byOpenStatus    *filestore.Index[DeltaAlert]
	byCreated       *filestore.Index[DeltaAlert]
	byWord          *filestore.Index[DeltaAlert]
	subsByDest      *filestore.Unique[DeltaSubscription]
	subsByUpdated   *filestore.Index[DeltaSubscription]
}

// DefineDeltaTables adds the alert store's tables to the Delta backend's
// schema: alerts and alert_status_subscriptions, keyed by id, and alert_data
// and alert_feedback, keyed by alert_id, with the foreign keys between them.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{}

	t.Alerts = filestore.NewTable(s, "alerts", func(a DeltaAlert) string { return filestore.IntKey(a.ID) }).Buckets(8)
	t.byDedup = t.Alerts.Unique("idx_no_alert_duplicates", func(a DeltaAlert) (string, bool) {
		if a.ServiceID == nil || a.DedupKey == nil {
			return "", false
		}
		return dedupIndexKey(*a.ServiceID, *a.DedupKey), true
	})
	// This also serves idx_alert_service_id, one status after the other.
	t.byServiceStatus = t.Alerts.Index("idx_unacked_alert_service", func(a DeltaAlert) []string {
		if a.ServiceID == nil {
			return nil
		}
		return []string{serviceStatusKey(*a.ServiceID, Status(a.Status))}
	})
	t.byOpenStatus = t.Alerts.Index("alerts_open_status", func(a DeltaAlert) []string {
		if Status(a.Status) == StatusClosed {
			return nil
		}
		return []string{a.Status}
	})
	t.byCreated = t.Alerts.Index("alerts_created_at", func(a DeltaAlert) []string { return deltaquery.DayKeys(a.CreatedAt) })
	t.byWord = t.Alerts.Index("idx_search_alerts_summary_eng", func(a DeltaAlert) []string { return searchWords(a.Summary) })

	alertKey := func(id int64) (string, bool) { return filestore.IntKey(id), true }

	t.data = filestore.NewTable(s, "alert_data", func(d dataRow) string { return filestore.IntKey(d.AlertID) })
	t.data.Unique("alert_data_id_key", func(d dataRow) (string, bool) { return filestore.IntKey(d.ID), true })
	filestore.References(t.data, "alert_data_alert_id_fkey", t.Alerts,
		func(d dataRow) (string, bool) { return alertKey(d.AlertID) }, filestore.Cascade, nil)

	t.feedback = filestore.NewTable(s, "alert_feedback", func(f feedbackRow) string { return filestore.IntKey(f.AlertID) })
	t.feedback.Unique("alert_feedback_id_key", func(f feedbackRow) (string, bool) { return filestore.IntKey(f.ID), true })
	filestore.References(t.feedback, "alert_feedback_alert_id_fkey", t.Alerts,
		func(f feedbackRow) (string, bool) { return alertKey(f.AlertID) }, filestore.Cascade, nil)

	t.Subscriptions = filestore.NewTable(s, "alert_status_subscriptions", func(sub DeltaSubscription) string { return filestore.IntKey(sub.ID) })
	// In Postgres, one of the two destination columns is always NULL, so the
	// constraint never applies, and a destination that is subscribed twice
	// gets every status update twice (F3). Here it does apply.
	t.subsByDest = t.Subscriptions.Unique("alert_status_subscriptions_channel_id_contact_method_id_ale_key", DeltaSubscription.destKey)
	t.SubscriptionsByAlert = t.Subscriptions.Index("alert_status_subscriptions_alert_id", func(sub DeltaSubscription) []string {
		return []string{filestore.IntKey(sub.AlertID)}
	})
	t.subsByUpdated = t.Subscriptions.Index("alert_status_subscriptions_updated_at_idx", func(sub DeltaSubscription) []string {
		return deltaquery.DayKeys(sub.UpdatedAt)
	})
	filestore.References(t.Subscriptions, "alert_status_subscriptions_alert_id_fkey", t.Alerts,
		func(sub DeltaSubscription) (string, bool) { return alertKey(sub.AlertID) }, filestore.Cascade, nil)
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[S, NC, CM any](t *DeltaTables, services *filestore.Table[S], channels *filestore.Table[NC], contactMethods *filestore.Table[CM]) {
	filestore.References(t.Alerts, "alerts_services_id_fkey", services,
		func(a DeltaAlert) (string, bool) { return filestore.Ref(a.ServiceID) }, filestore.Cascade, nil)
	filestore.References(t.Subscriptions, "alert_status_subscriptions_channel_id_fkey", channels,
		func(sub DeltaSubscription) (string, bool) { return filestore.Ref(sub.ChannelID) }, filestore.Cascade, nil)
	filestore.References(t.Subscriptions, "alert_status_subscriptions_contact_method_id_fkey", contactMethods,
		func(sub DeltaSubscription) (string, bool) { return filestore.Ref(sub.ContactMethodID) }, filestore.Cascade, nil)
}

// AlertIDs returns, in r, the IDs of a service's alerts with one of the
// statuses, or of all of them without any, lowest first. It is the alert log's
// alertlog.DeltaLookups.AlertIDs.
func (t *DeltaTables) AlertIDs(r filestore.Reader, serviceID string, of ...string) []int64 {
	var ids []int64
	for _, status := range statuses {
		if len(of) > 0 && !slices.Contains(of, string(status)) {
			continue
		}
		t.byServiceStatus.Ascend(r, serviceStatusKey(serviceID, status), func(a DeltaAlert) bool {
			ids = append(ids, a.ID)
			return true
		})
	}
	slices.Sort(ids)
	return ids
}

// UnackedCount returns, in r, how many triggered alerts a service has.
func (t *DeltaTables) UnackedCount(r filestore.Reader, serviceID string) int {
	return t.byServiceStatus.Count(r, serviceStatusKey(serviceID, StatusTriggered))
}

// Status returns, in r, an alert's status.
func (t *DeltaTables) Status(r filestore.Reader, alertID int64) (status Status, ok bool) {
	a, ok := t.Alerts.Get(r, filestore.IntKey(alertID))
	return Status(a.Status), ok
}

// AscendOpen calls fn, in r, for the alerts with an open status, oldest first,
// until fn returns false.
func (t *DeltaTables) AscendOpen(r filestore.Reader, status Status, fn func(DeltaAlert) bool) {
	t.byOpenStatus.Ascend(r, string(status), fn)
}

// AscendCreated calls fn, in r, for the alerts created on the days from from
// to to, earlier days first and then by id, until fn returns false. A zero
// time is no bound.
func (t *DeltaTables) AscendCreated(r filestore.Reader, from, to time.Time, fn func(DeltaAlert) bool) {
	deltaquery.Days(t.byCreated, r, from, to, false, func(rows []DeltaAlert) bool { return each(rows, fn) })
}

// AscendSubscriptionsUpdated calls fn, in r, for the status subscriptions last
// updated on the days from from to to, earlier days first, until fn returns
// false. A zero time is no bound.
func (t *DeltaTables) AscendSubscriptionsUpdated(r filestore.Reader, from, to time.Time, fn func(DeltaSubscription) bool) {
	deltaquery.Days(t.subsByUpdated, r, from, to, false, func(rows []DeltaSubscription) bool { return each(rows, fn) })
}

func each[V any](rows []V, fn func(V) bool) bool {
	for _, v := range rows {
		if !fn(v) {
			return false
		}
	}
	return true
}

// Subscribe adds a subscription of a channel or a contact method to an alert's
// status, with the next ID and the current time, or does nothing if it has
// one.
func (t *DeltaTables) Subscribe(tx *filestore.Tx, sub DeltaSubscription) error {
	if (sub.ChannelID == nil) == (sub.ContactMethodID == nil) {
		return &filestore.CheckError{Table: "alert_status_subscriptions", Constraint: "alert_status_subscriptions_check"}
	}
	if key, _ := sub.destKey(); key != "" {
		if _, ok := t.subsByDest.Get(tx, key); ok {
			return nil
		}
	}
	var err error
	if sub.ID, err = tx.NextID("alert_status_subscriptions_id_seq"); err != nil {
		return err
	}
	sub.UpdatedAt = tx.Now()
	return t.Subscriptions.Insert(tx, sub, "alert_status_subscriptions_pkey")
}

// DeltaService is what the store needs to know about a service.
type DeltaService struct {
	Name               string
	EscalationPolicyID string
	InMaintenance      bool // maintenance_expires_at is set
}

// PendingMessage is a message that is waiting to be sent. An ID it doesn't
// have is empty.
type PendingMessage struct{ UserID, ContactMethodID, ChannelID string }

// DeltaLookups are the reads of other packages' tables, and the triggers that
// change them, that the store needs. They run in its transactions, so they
// must do no I/O. A method that needs one that is absent returns
// deltatxn.ErrNotPorted.
type DeltaLookups struct {
	// Service returns a row of services, and ServiceNames calls fn for every
	// one until it returns false.
	Service      func(r filestore.Reader, id string) (svc DeltaService, ok bool)
	ServiceNames func(r filestore.Reader, fn func(id, name string) bool)

	// MaxUnacked is the unacked_alerts_per_service limit (config_limits), or
	// -1 for none.
	MaxUnacked func(r filestore.Reader) int

	// PolicyHasSteps reports whether an escalation policy has steps
	// (escalation_policy_steps).
	PolicyHasSteps func(r filestore.Reader, escalationPolicyID string) bool

	// InsertEPState is fn_insert_ep_state_on_alert_insert: it adds the new
	// alert's escalation_policy_state, if its service's policy has steps.
	// ClearEPState is fn_clear_ep_state_on_alert_close: it removes it.
	InsertEPState func(tx *filestore.Tx, alertID int64, serviceID string) error
	ClearEPState  func(tx *filestore.Tx, alertID int64) error

	// RequestEscalation sets force_escalation of an alert that was not
	// escalated after asOf, and reports whether it did. Its trigger,
	// trg_30_trig_alert_on_force_escalation, calls DeltaSetStatus.
	RequestEscalation func(tx *filestore.Tx, alertID int64, asOf time.Time) (ok bool, err error)

	// EPState returns an alert's escalation_policy_state, and MultiAck whether
	// its current step has multi-ack enabled.
	EPState  func(r filestore.Reader, alertID int64) (state State, ok bool)
	MultiAck func(r filestore.Reader, alertID int64) bool

	// StatusChanged is fn_track_alert_status: it queues the status manager's
	// job for the alert, in the transaction. It can be called several times
	// for an alert in one. While the engine's jobs are not ported it may be
	// nil.
	StatusChanged func(tx *filestore.Tx, alertID int64) error

	// PendingMessages returns the messages with last_status pending, created
	// before before, that are about an alert or bundle (message_type
	// alert_notification_bundle) a service's alerts (outgoing_messages). The
	// rest is joined to them; a destination that doesn't exist is
	// sql.ErrNoRows.
	PendingMessages   func(r filestore.Reader, alertID int64, serviceID string, before time.Time) []PendingMessage
	UserName          func(r filestore.Reader, id string) (name string, ok bool)
	ChannelName       func(r filestore.Reader, id string) (name string, ok bool)
	ChannelDest       func(r filestore.Reader, id string) (gadb.DestV1, error)
	ContactMethodDest func(r filestore.Reader, id string) (gadb.DestV1, error)
}

// NewDeltaStore returns an alert store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables, logDB *alertlog.Store, logs *alertlog.DeltaTables, metrics *alertmetrics.DeltaTables, lookups DeltaLookups) *Store {
	return &Store{logDB: logDB, delta: &deltaAlerts{db: db, log: logDB, DeltaTables: tables, DeltaLookups: lookups, logs: logs, metrics: metrics}}
}

type deltaAlerts struct {
	db  txn.DB
	log *alertlog.Store
	*DeltaTables
	DeltaLookups
	logs    *alertlog.DeltaTables
	metrics *alertmetrics.DeltaTables
}

// run runs fn in tx, or in a transaction of its own. fn gets the transaction,
// to pass on, and its scope.
func (d *deltaAlerts) run(ctx context.Context, tx txn.Tx, fn func(tx txn.Tx, s deltatxn.Scope) error) error {
	if tx == nil {
		d.log.Prepare(ctx)
		return d.db.Run(ctx, nil, func(tx txn.Tx) error { return d.run(ctx, tx, fn) })
	}
	return deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error { return fn(tx, s) })
}

func (d *deltaAlerts) get(r filestore.Reader, id int) (DeltaAlert, bool) {
	return d.Alerts.Get(r, filestore.IntKey(int64(id)))
}

// canonicalID returns a validated UUID as the tables store it, in the form
// Postgres gives UUIDs.
func canonicalID(id string) string { return uuid.MustParse(id).String() }

// sortedIDs returns the IDs, each once and lowest first, like
// `id = any($1)` through the primary key.
func sortedIDs(ids []int) []int {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return slices.Compact(ids)
}

// DeltaInsert adds an alert as an INSERT into Postgres does, with its defaults,
// constraints and triggers, and returns the row. The store's own methods do
// so too.
func (s *Store) DeltaInsert(tx *filestore.Tx, a DeltaAlert) (DeltaAlert, error) {
	return s.delta.insert(tx, a)
}

// DeltaSetStatus sets the status of an alert, if it exists, as an UPDATE in
// Postgres does, with its constraints and triggers. Through it, a request for
// escalation triggers an alert that was acknowledged
// (fn_trig_alert_on_force_escalation).
func (s *Store) DeltaSetStatus(tx *filestore.Tx, alertID int64, status Status) error {
	a, ok := s.delta.Alerts.Get(tx, filestore.IntKey(alertID))
	if !ok {
		return nil
	}
	return s.delta.setStatus(tx, a, status)
}

func (d *deltaAlerts) insert(tx *filestore.Tx, a DeltaAlert) (DeltaAlert, error) {
	if d.InsertEPState == nil || d.MaxUnacked == nil {
		return DeltaAlert{}, deltatxn.ErrNotPorted
	}
	if a.ID == 0 {
		var err error
		if a.ID, err = tx.NextID("alerts_id_seq"); err != nil {
			return DeltaAlert{}, err
		}
	}
	if a.Source == "" {
		a.Source = string(SourceManual)
	}
	if a.Status == "" {
		a.Status = string(StatusTriggered)
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = tx.Now()
	}
	if a.LastEscalation == nil {
		now := tx.Now()
		a.LastEscalation = &now
	}
	if err := checkAlert(a); err != nil {
		return DeltaAlert{}, err
	}
	if err := d.Alerts.Insert(tx, a, "alerts_pkey"); err != nil {
		return DeltaAlert{}, err
	}
	// trg_10_insert_ep_state_on_alert_insert
	if Status(a.Status) != StatusClosed {
		if err := d.InsertEPState(tx, a.ID, a.serviceID()); err != nil {
			return DeltaAlert{}, err
		}
	}
	// trg_enforce_alert_limit, which counts the new alert too
	if limit := d.MaxUnacked(tx); limit != -1 && a.ServiceID != nil && d.UnackedCount(tx, *a.ServiceID) > limit {
		return DeltaAlert{}, &filestore.CheckError{
			Table: "alerts", Constraint: "unacked_alerts_per_service_limit",
			Message: "limit exceeded", HintText: fmt.Sprintf("max=%d", limit),
		}
	}
	return a, nil
}

func checkAlert(a DeltaAlert) error {
	if _, ok := statusOrder[Status(a.Status)]; !ok {
		return fmt.Errorf("invalid alert status %q", a.Status)
	}
	if (Status(a.Status) == StatusClosed) != (a.DedupKey == nil) {
		return &filestore.CheckError{Table: "alerts", Constraint: "dedup_key_only_for_open_alerts"}
	}
	return nil
}

// setStatus changes an alert's status, with everything that comes with it in
// the same transaction (F2).
//
// It leaves out trg_20_clear_next_esc_on_alert_ack. That trigger is meant to
// clear next_escalation when an alert is acknowledged, but runs when it stops
// being so, and an alert triggered again without a request for escalation
// would then never escalate (F2). Nothing needs it: next_escalation only
// matters while an alert is triggered, closing one removes its state, and
// escalating one sets it.
func (d *deltaAlerts) setStatus(tx *filestore.Tx, a DeltaAlert, status Status) error {
	if Status(a.Status) == StatusClosed { // trg_prevent_reopen
		return &filestore.CheckError{Table: "alerts", Constraint: "trg_prevent_reopen", Message: "cannot change status of closed alert"}
	}
	if Status(a.Status) == status {
		return nil
	}
	if status == StatusClosed && d.ClearEPState == nil {
		return deltatxn.ErrNotPorted
	}
	a.Status = string(status)
	if status == StatusClosed { // trg_clear_dedup_on_close
		a.DedupKey = nil
	}
	if err := checkAlert(a); err != nil {
		return err
	}
	if err := d.Alerts.Put(tx, a); err != nil {
		return err
	}
	if status == StatusClosed { // trg_10_clear_ep_state_on_alert_close
		if err := d.ClearEPState(tx, a.ID); err != nil {
			return err
		}
	}
	if d.StatusChanged != nil { // trg_track_alert_status_update
		return d.StatusChanged(tx, a.ID)
	}
	return nil
}

// createAlert adds a valid alert, and returns its row and the metadata of its
// log entry.
func (d *deltaAlerts) createAlert(s deltatxn.Scope, n *Alert, status Status) (DeltaAlert, *alertlog.CreatedMetaData, error) {
	if d.Service == nil || d.PolicyHasSteps == nil {
		return DeltaAlert{}, nil, deltatxn.ErrNotPorted
	}
	serviceID, dedup := canonicalID(n.ServiceID), dedupString(n.DedupKey())
	row, err := d.insert(s.Tx, DeltaAlert{
		Summary: n.Summary, Details: n.Details, ServiceID: &serviceID,
		Source: string(n.Source), Status: string(status), DedupKey: &dedup,
	})
	if err != nil {
		return DeltaAlert{}, nil, err
	}
	var meta alertlog.CreatedMetaData
	svc, ok := d.Service(s.Reader, serviceID)
	meta.EPNoSteps = !ok || !d.PolicyHasSteps(s.Reader, svc.EscalationPolicyID)
	return row, &meta, nil
}

// create adds a valid alert and its log entry.
func (d *deltaAlerts) create(ctx context.Context, tx txn.Tx, a Alert) (*Alert, error) {
	var res Alert
	err := d.run(ctx, tx, func(tx txn.Tx, s deltatxn.Scope) error {
		row, meta, err := d.createAlert(s, &a, a.Status)
		if err != nil {
			return err
		}
		res = a
		res.ID, res.CreatedAt = int(row.ID), row.CreatedAt
		d.log.MustLogTx(ctx, tx, res.ID, alertlog.TypeCreated, meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// createOrUpdate is CreateOrUpdateTx for a valid alert. The alert and its log
// entry are one commit (F2). Pods that get the same event at once create one
// alert: the one whose commit loses runs again and finds it (F4, F8).
func (d *deltaAlerts) createOrUpdate(ctx context.Context, tx txn.Tx, a Alert) (*Alert, bool, error) {
	var (
		res      *Alert
		inserted bool
	)
	err := d.run(ctx, tx, func(tx txn.Tx, s deltatxn.Scope) error {
		res, inserted = nil, false
		n := a
		row, ok := d.byDedup.Get(s.Reader, dedupIndexKey(canonicalID(n.ServiceID), dedupString(n.DedupKey())))
		// A row in that index is open, since only open alerts have a dedup key.
		var logType alertlog.Type
		var meta any
		switch n.Status {
		case StatusTriggered:
			m := &alertlog.CreatedMetaData{}
			logType = alertlog.TypeDuplicateSupressed
			if !ok {
				var err error
				if row, m, err = d.createAlert(s, &n, StatusTriggered); err != nil {
					return err
				}
				logType, inserted = alertlog.TypeCreated, true
			}
			n.Status, n.Source = Status(row.Status), Source(row.Source)
			meta = m
		case StatusActive, StatusClosed:
			if !ok {
				return nil // already closed, or never existed
			}
			old := Status(row.Status)
			if err := d.setStatus(s.Tx, row, n.Status); err != nil {
				return err
			}
			switch {
			case n.Status == StatusClosed:
				logType = alertlog.TypeClosed
			case old != StatusActive:
				logType = alertlog.TypeAcknowledged
			}
		}
		n.ID, n.Summary, n.Details, n.CreatedAt = int(row.ID), row.Summary, row.Details, row.CreatedAt
		if logType != "" {
			d.log.MustLogTx(ctx, tx, n.ID, logType, meta)
		}
		res = &n
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return res, inserted, nil
}

// canTouch is canTouchAlert, for an alert as the transaction sees it. (A
// separate read could miss an alert that another pod just created, F29.)
func (d *deltaAlerts) canTouch(ctx context.Context, r filestore.Reader, id int) error {
	checks := []permission.Checker{permission.System, permission.Admin, permission.User}
	if permission.Service(ctx) {
		a, ok := d.get(r, id)
		if !ok {
			return sql.ErrNoRows
		}
		checks = append(checks, permission.MatchService(a.serviceID()))
	}
	return permission.LimitCheckAny(ctx, checks...)
}

// updateStatus returns sql.ErrNoRows for a missing alert.
func (d *deltaAlerts) updateStatus(ctx context.Context, tx txn.Tx, id int, status Status) error {
	if d.MultiAck == nil {
		return deltatxn.ErrNotPorted
	}
	return d.run(ctx, tx, func(tx txn.Tx, s deltatxn.Scope) error {
		a, ok := d.get(s.Reader, id)
		if !ok || a.ServiceID == nil {
			return sql.ErrNoRows
		}
		if Status(a.Status) == StatusClosed {
			return logError{isAlreadyClosed: true, alertID: id, _type: alertlog.TypeClosed, logDB: d.log}
		}
		if Status(a.Status) == StatusActive && status == StatusActive {
			if !d.MultiAck(s.Reader, a.ID) {
				return logError{isAlreadyAcknowledged: true, alertID: id, _type: alertlog.TypeAcknowledged, logDB: d.log}
			}
			d.log.MustLogTx(ctx, tx, id, alertlog.TypeAcknowledged, nil)
			return nil
		}
		if err := d.setStatus(s.Tx, a, status); err != nil {
			return err
		}
		switch status {
		case StatusClosed:
			d.log.MustLogTx(ctx, tx, id, alertlog.TypeClosed, nil)
		case StatusActive:
			d.log.MustLogTx(ctx, tx, id, alertlog.TypeAcknowledged, nil)
		case StatusTriggered:
		default:
			log.Log(ctx, errors.Errorf("unknown/unhandled alert status update: %s", status))
		}
		return nil
	})
}

// below returns the statuses that an update to status changes
// (`$1 > status`).
func below(status Status) []string {
	var names []string
	for _, s := range statuses {
		if statusOrder[s] < statusOrder[status] {
			names = append(names, string(s))
		}
	}
	return names
}

// updateStatusByService also logs the acknowledgment of alerts that someone
// else acknowledged before, if their current step is multi-ack: until it is
// logged for them too, the caller keeps being notified (F4). The Postgres
// store does that only when one alert is updated (UpdateStatus).
func (d *deltaAlerts) updateStatusByService(ctx context.Context, serviceID string, status Status, t alertlog.Type) error {
	if d.MultiAck == nil {
		return deltatxn.ErrNotPorted
	}
	return d.run(ctx, nil, func(tx txn.Tx, s deltatxn.Scope) error {
		if err := d.log.LogServiceTx(ctx, tx, serviceID, t, nil); err != nil {
			return err
		}
		if status == StatusActive {
			var again []int
			for _, id := range d.AlertIDs(s.Reader, canonicalID(serviceID), string(StatusActive)) {
				if d.MultiAck(s.Reader, id) {
					again = append(again, int(id))
				}
			}
			if err := d.log.LogManyTx(ctx, tx, again, t, nil); err != nil {
				return err
			}
		}
		for _, id := range d.AlertIDs(s.Reader, canonicalID(serviceID), below(status)...) {
			a, _ := d.get(s.Reader, int(id))
			if err := d.setStatus(s.Tx, a, status); err != nil {
				return err
			}
		}
		return nil
	})
}

// updateMany returns the IDs of the alerts it changed, lowest first. Those
// include alerts that were acknowledged again on a multi-ack step, as in
// updateStatusByService.
func (d *deltaAlerts) updateMany(ctx context.Context, status Status, alertIDs []int, t alertlog.Type, logMeta any) ([]int, error) {
	if d.MultiAck == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var updated []int
	err := d.run(ctx, nil, func(tx txn.Tx, s deltatxn.Scope) error {
		updated = nil
		for _, id := range sortedIDs(alertIDs) {
			a, ok := d.get(s.Reader, id)
			switch {
			case !ok:
				continue
			case statusOrder[status] > statusOrder[Status(a.Status)]:
				if err := d.setStatus(s.Tx, a, status); err != nil {
					return err
				}
			case status != StatusActive || Status(a.Status) != StatusActive || !d.MultiAck(s.Reader, a.ID):
				continue
			}
			updated = append(updated, id)
		}
		return d.log.LogManyTx(ctx, tx, updated, t, logMeta)
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (d *deltaAlerts) escalateAsOf(ctx context.Context, id int, asOf time.Time) error {
	if d.Service == nil || d.RequestEscalation == nil || d.EPState == nil {
		return deltatxn.ErrNotPorted
	}
	return d.run(ctx, nil, func(tx txn.Tx, s deltatxn.Scope) error {
		a, ok := d.get(s.Reader, id)
		if !ok {
			return validation.NewGenericError("alert not found")
		}
		svc, ok := d.Service(s.Reader, a.serviceID())
		if !ok {
			return validation.NewGenericError("alert not found")
		}
		if Status(a.Status) == StatusClosed {
			return logError{isAlreadyClosed: true, alertID: id, _type: alertlog.TypeClosed, logDB: d.log}
		}
		if svc.InMaintenance {
			return validation.NewGenericError("service is in maintenance mode")
		}
		t := asOf
		if t.IsZero() {
			t = s.Now
		}
		ok, err := d.RequestEscalation(s.Tx, a.ID, t)
		if err != nil {
			return fmt.Errorf("request escalation: %w", err)
		}
		if !ok {
			if _, hasEP := d.EPState(s.Reader, a.ID); !hasEP {
				return validation.NewGenericError("alert escalation policy is empty")
			}
			return validation.NewGenericError("alert has already escalated")
		}
		if err := d.log.LogTx(ctx, tx, id, alertlog.TypeEscalationRequest, nil); err != nil {
			return fmt.Errorf("log escalation request: %w", err)
		}
		return nil
	})
}

func (d *deltaAlerts) findMany(ctx context.Context, alertIDs []int) ([]Alert, error) {
	var alerts []Alert
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		alerts = make([]Alert, 0, len(alertIDs))
		for _, id := range sortedIDs(alertIDs) {
			row, ok := d.get(s.Reader, id)
			if !ok {
				continue
			}
			a, err := row.alert()
			if err != nil {
				return err
			}
			alerts = append(alerts, a)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return alerts, nil
}

// serviceInfo returns sql.ErrNoRows for a missing service.
func (d *deltaAlerts) serviceInfo(ctx context.Context, serviceID string) (name string, count int, err error) {
	if d.Service == nil {
		return "", 0, deltatxn.ErrNotPorted
	}
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		svc, ok := d.Service(s.Reader, serviceID)
		if !ok {
			return sql.ErrNoRows
		}
		name, count = svc.Name, d.UnackedCount(s.Reader, serviceID)
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	return name, count, nil
}

// epID returns sql.ErrNoRows for a missing alert.
func (d *deltaAlerts) epID(ctx context.Context, alertID int) (id string, err error) {
	if d.Service == nil {
		return "", deltatxn.ErrNotPorted
	}
	err = deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		a, ok := d.get(s.Reader, alertID)
		if !ok {
			return sql.ErrNoRows
		}
		svc, ok := d.Service(s.Reader, a.serviceID())
		if !ok {
			return sql.ErrNoRows
		}
		id = svc.EscalationPolicyID
		return nil
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (d *deltaAlerts) state(ctx context.Context, alertIDs []int) ([]State, error) {
	if d.EPState == nil {
		return nil, deltatxn.ErrNotPorted
	}
	var list []State
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		list = make([]State, 0, len(alertIDs))
		for _, id := range sortedIDs(alertIDs) {
			if st, ok := d.EPState(s.Reader, int64(id)); ok {
				st.ID = id
				list = append(list, st)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *deltaAlerts) findFeedback(ctx context.Context, alertIDs []int) ([]Feedback, error) {
	var result []Feedback
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		result = nil // as the Postgres store returns without rows
		for _, id := range sortedIDs(alertIDs) {
			if f, ok := d.feedback.Get(s.Reader, filestore.IntKey(int64(id))); ok {
				result = append(result, Feedback{ID: id, NoiseReason: f.NoiseReason})
			}
		}
		return nil
	})
	return result, err
}

// setFeedback sets the noise reason of the alerts, and returns their IDs, each
// once and in the order given.
func (d *deltaAlerts) setFeedback(ctx context.Context, noiseReason string, alertIDs ...int) ([]int, error) {
	var updated []int
	err := deltatxn.Write(ctx, d.db, nil, func(s deltatxn.Scope) error {
		updated = make([]int, 0, len(alertIDs))
		for _, id := range alertIDs {
			if slices.Contains(updated, id) {
				continue
			}
			f, ok := d.feedback.Get(s.Reader, filestore.IntKey(int64(id)))
			if !ok {
				rowID, err := s.Tx.NextID("alert_feedback_id_seq")
				if err != nil {
					return err
				}
				f = feedbackRow{ID: rowID, AlertID: int64(id)}
			}
			f.NoiseReason = noiseReason
			if err := d.feedback.Put(s.Tx, f); err != nil {
				return err
			}
			updated = append(updated, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// findMetadata returns the rows of the alerts that have one, lowest ID first.
func (d *deltaAlerts) findMetadata(ctx context.Context, tx txn.Tx, alertIDs []int) ([]dataRow, error) {
	var rows []dataRow
	err := deltatxn.Read(ctx, d.db, tx, func(s deltatxn.Scope) error {
		rows = nil
		for _, id := range sortedIDs(alertIDs) {
			if row, ok := d.data.Get(s.Reader, filestore.IntKey(int64(id))); ok {
				rows = append(rows, row)
			}
		}
		return nil
	})
	return rows, err
}

// setMetadata sets an alert's metadata if it is open and, for a service, its
// own, and reports whether it did.
func (d *deltaAlerts) setMetadata(ctx context.Context, tx txn.Tx, alertID int, md json.RawMessage) (ok bool, err error) {
	svcID := permission.ServiceNullUUID(ctx)
	err = deltatxn.Write(ctx, d.db, tx, func(s deltatxn.Scope) error {
		ok = false
		a, found := d.get(s.Reader, alertID)
		if !found || Status(a.Status) == StatusClosed || (svcID.Valid && a.serviceID() != svcID.UUID.String()) {
			return nil
		}
		row, found := d.data.Get(s.Reader, filestore.IntKey(a.ID))
		if !found {
			id, err := s.Tx.NextID("alert_data_id_seq")
			if err != nil {
				return err
			}
			row = dataRow{ID: id, AlertID: a.ID}
		}
		row.Metadata = md
		if err := d.data.Put(s.Tx, row); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

func (d *deltaAlerts) serviceAlertCounts(ctx context.Context, serviceIDs []uuid.UUID) ([]gadb.ServiceAlertCountsRow, error) {
	var rows []gadb.ServiceAlertCountsRow
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		seen := make(map[uuid.UUID]bool, len(serviceIDs))
		for _, id := range serviceIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			for _, status := range statuses {
				if n := d.byServiceStatus.Count(s.Reader, serviceStatusKey(id.String(), status)); n > 0 {
					rows = append(rows, gadb.ServiceAlertCountsRow{
						Count: int64(n), Status: gadb.EnumAlertStatus(status), ServiceID: uuid.NullUUID{UUID: id, Valid: true},
					})
				}
			}
		}
		return nil
	})
	return rows, err
}

// binStart is date_bin(stride, t, origin), in microseconds.
func binStart(stride, t, origin int64) int64 {
	mod := (t - origin) % stride
	if mod < 0 {
		mod += stride
	}
	return t - mod
}

// avgSeconds is EXTRACT(EPOCH FROM AVG(...)) of intervals in microseconds, or 0
// without any.
func avgSeconds(sum, n int64) float64 {
	if n == 0 {
		return 0
	}
	return math.RoundToEven(float64(sum)/float64(n)) / 1e6
}

func (d *deltaAlerts) serviceAlertStats(ctx context.Context, p gadb.ServiceAlertStatsParams) ([]gadb.ServiceAlertStatsRow, error) {
	stride := int64(p.Stride.Days)*int64(24*time.Hour/time.Microsecond) + p.Stride.Microseconds
	start, end, origin := p.StartTime.UnixMicro(), p.EndTime.UnixMicro(), p.Origin.UnixMicro()

	serviceIDs := slices.Clone(p.ServiceIds)
	slices.SortFunc(serviceIDs, compareUUID)
	serviceIDs = slices.Compact(serviceIDs) // like `service_id = any($1)`

	type key struct {
		bucket  int64
		service uuid.UUID
	}
	type totals struct{ count, escalated, ackSum, ackN, closeSum, closeN int64 }
	var groups map[key]*totals
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		groups = make(map[key]*totals)
		var err error
		for _, id := range serviceIDs {
			d.metrics.AscendService(s.Reader, id.String(), func(m alertmetrics.DeltaMetric) bool {
				at := m.ClosedAt.UnixMicro()
				if at < start || at > end {
					return true
				}
				// As date_bin does, only once there is something to put in a bin.
				switch {
				case p.Stride.Months != 0:
					err = errors.New("timestamps cannot be binned into intervals containing months or years")
					return false
				case stride <= 0:
					err = errors.New("stride must be greater than zero")
					return false
				}
				k := key{bucket: binStart(stride, at, origin), service: id}
				g := groups[k]
				if g == nil {
					g = &totals{}
					groups[k] = g
				}
				g.count++
				if m.Escalated {
					g.escalated++
				}
				if m.TimeToAck != nil {
					g.ackSum += *m.TimeToAck
					g.ackN++
				}
				if m.TimeToClose != nil {
					g.closeSum += *m.TimeToClose
					g.closeN++
				}
				return true
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var rows []gadb.ServiceAlertStatsRow // nil without rows, like the Postgres query
	for k, g := range groups {
		rows = append(rows, gadb.ServiceAlertStatsRow{
			Bucket:                time.UnixMicro(k.bucket).UTC(),
			AvgTimeToAckSeconds:   avgSeconds(g.ackSum, g.ackN),
			AvgTimeToCloseSeconds: avgSeconds(g.closeSum, g.closeN),
			AlertCount:            g.count,
			EscalatedCount:        g.escalated,
			ServiceID:             k.service,
		})
	}
	slices.SortFunc(rows, func(a, b gadb.ServiceAlertStatsRow) int {
		if c := a.Bucket.Compare(b.Bucket); c != 0 {
			return c
		}
		return compareUUID(a.ServiceID, b.ServiceID)
	})
	return rows, nil
}

func compareUUID(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) }

// pendingMessageDests is AllPendingMsgDests.
func (d *deltaAlerts) pendingMessageDests(ctx context.Context, p gadb.AllPendingMsgDestsParams) ([]gadb.AllPendingMsgDestsRow, error) {
	if d.PendingMessages == nil || d.UserName == nil || d.ChannelName == nil || d.ChannelDest == nil || d.ContactMethodDest == nil {
		return nil, deltatxn.ErrNotPorted
	}
	name := func(r filestore.Reader, lookup func(filestore.Reader, string) (string, bool), id string) (n sql.NullString) {
		if id != "" {
			n.String, n.Valid = lookup(r, id)
		}
		return n
	}
	dest := func(r filestore.Reader, lookup func(filestore.Reader, string) (gadb.DestV1, error), id string) (gadb.NullDestV1, error) {
		if id == "" {
			return gadb.NullDestV1{}, nil
		}
		dest, err := lookup(r, id)
		if errors.Is(err, sql.ErrNoRows) {
			return gadb.NullDestV1{}, nil // like LEFT JOIN
		}
		return gadb.NullDestV1{Valid: err == nil, DestV1: dest}, err
	}
	var rows []gadb.AllPendingMsgDestsRow
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		rows = nil
		for _, m := range d.PendingMessages(s.Reader, p.AlertID, p.ServiceID.String(), s.Now.Add(-15*time.Second)) {
			row := gadb.AllPendingMsgDestsRow{
				UserName: name(s.Reader, d.UserName, m.UserID),
				NcName:   name(s.Reader, d.ChannelName, m.ChannelID),
			}
			var err error
			if row.CmDest, err = dest(s.Reader, d.ContactMethodDest, m.ContactMethodID); err != nil {
				return err
			}
			if row.NcDest, err = dest(s.Reader, d.ChannelDest, m.ChannelID); err != nil {
				return err
			}
			if !slices.ContainsFunc(rows, func(o gadb.AllPendingMsgDestsRow) bool { // DISTINCT
				return o.UserName == row.UserName && o.NcName == row.NcName &&
					o.CmDest.Valid == row.CmDest.Valid && o.CmDest.DestV1.Equal(row.CmDest.DestV1) &&
					o.NcDest.Valid == row.NcDest.Valid && o.NcDest.DestV1.Equal(row.NcDest.DestV1)
			}) {
				rows = append(rows, row)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}
