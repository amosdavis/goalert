package alertmetrics

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/alert/internal/deltaquery"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// DeltaMetric is an alert_metrics row on the Delta backend. The times to
// acknowledge and to close are intervals in Postgres; here they are in
// microseconds, which is an interval's precision.
type DeltaMetric struct {
	ID          int64     `parquet:"id"`
	AlertID     int64     `parquet:"alert_id"`
	ServiceID   string    `parquet:"service_id"`
	TimeToAck   *int64    `parquet:"time_to_ack,optional"`
	TimeToClose *int64    `parquet:"time_to_close,optional"`
	Escalated   bool      `parquet:"escalated"`
	ClosedAt    time.Time `parquet:"closed_at,timestamp(microsecond:utc)"`
}

// DeltaTables are the metrics store's tables on the Delta backend. The engine
// adds rows with Insert.
type DeltaTables struct {
	Metrics *filestore.Table[DeltaMetric]

	byService, byClosed *filestore.Index[DeltaMetric]
}

// DefineDeltaTables adds alert_metrics, keyed by alert_id
// (alert_metrics_pkey), to the Delta backend's schema.
func DefineDeltaTables(s *filestore.Schema) *DeltaTables {
	t := &DeltaTables{Metrics: filestore.NewTable(s, "alert_metrics", func(m DeltaMetric) string { return filestore.IntKey(m.AlertID) })}
	t.Metrics.Unique("alert_metrics_id_key", func(m DeltaMetric) (string, bool) { return filestore.IntKey(m.ID), true })
	t.byService = t.Metrics.Index("alert_metrics_service_id", func(m DeltaMetric) []string { return []string{m.ServiceID} })
	t.byClosed = t.Metrics.Index("alert_metrics_closed_date_idx", func(m DeltaMetric) []string { return deltaquery.DayKeys(m.ClosedAt) })
	return t
}

// DeclareDeltaForeignKeys declares this package's foreign keys to other
// packages' tables.
func DeclareDeltaForeignKeys[A any](t *DeltaTables, alerts *filestore.Table[A]) {
	filestore.References(t.Metrics, "alert_metrics_alert_id_fkey", alerts,
		func(m DeltaMetric) (string, bool) { return filestore.IntKey(m.AlertID), true }, filestore.Cascade, nil)
}

// Insert adds an alert's metrics, or does nothing if it has some (the
// engine's ON CONFLICT DO NOTHING). It sets the row's ID.
func (t *DeltaTables) Insert(s deltatxn.Scope, m DeltaMetric) error {
	if _, ok := t.Metrics.Get(s.Reader, filestore.IntKey(m.AlertID)); ok {
		return nil
	}
	svcID, err := uuid.Parse(m.ServiceID)
	if err != nil {
		return err
	}
	m.ServiceID = svcID.String()
	if m.ID, err = s.Tx.NextID("alert_metrics_id_seq"); err != nil {
		return err
	}
	return t.Metrics.Insert(s.Tx, m, "alert_metrics_pkey")
}

// ClosedAt returns, in r, when an alert was closed, if it has metrics.
func (t *DeltaTables) ClosedAt(r filestore.Reader, alertID int64) (time.Time, bool) {
	m, ok := t.Metrics.Get(r, filestore.IntKey(alertID))
	return m.ClosedAt, ok
}

// AscendService calls fn, in r, for the metrics of a service's alerts, oldest
// alert first, until fn returns false.
func (t *DeltaTables) AscendService(r filestore.Reader, serviceID string, fn func(DeltaMetric) bool) {
	t.byService.Ascend(r, serviceID, fn)
}

// AscendClosed calls fn, in r, for the metrics of the alerts closed on the
// days from from to to, earlier days first, until fn returns false. A zero
// time is no bound.
func (t *DeltaTables) AscendClosed(r filestore.Reader, from, to time.Time, fn func(DeltaMetric) bool) {
	deltaquery.Days(t.byClosed, r, from, to, false, func(rows []DeltaMetric) bool {
		for _, m := range rows {
			if !fn(m) {
				return false
			}
		}
		return true
	})
}

// NewDeltaStore returns a metrics store on the Delta backend.
func NewDeltaStore(db txn.DB, tables *DeltaTables) *Store {
	return &Store{delta: &deltaMetrics{db: db, DeltaTables: tables}}
}

type deltaMetrics struct {
	db txn.DB
	*DeltaTables
}

// metric fails for a time that is missing, as the Postgres store does: the
// engine adds a row only for a closed alert.
func (m DeltaMetric) metric() (Metric, error) {
	ack := m.TimeToAck
	if ack == nil {
		ack = m.TimeToClose // coalesce
	}
	if ack == nil || m.TimeToClose == nil {
		return Metric{}, fmt.Errorf("assigning metric: alert %d has no time to close", m.AlertID)
	}
	return Metric{
		ID:          int(m.AlertID),
		TimeToAck:   time.Duration(*ack) * time.Microsecond,
		TimeToClose: time.Duration(*m.TimeToClose) * time.Microsecond,
		ClosedAt:    m.ClosedAt,
		Escalated:   m.Escalated,
	}, nil
}

func (d *deltaMetrics) findMetrics(ctx context.Context, alertIDs []int) ([]Metric, error) {
	ids := slices.Clone(alertIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids) // like `alert_id = any($1)`

	var metrics []Metric
	err := deltatxn.Read(ctx, d.db, nil, func(s deltatxn.Scope) error {
		metrics = nil // as the Postgres store returns without rows
		for _, id := range ids {
			row, ok := d.Metrics.Get(s.Reader, filestore.IntKey(int64(id)))
			if !ok {
				continue
			}
			m, err := row.metric()
			if err != nil {
				return err
			}
			metrics = append(metrics, m)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return metrics, nil
}
