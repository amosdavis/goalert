package alertmetrics

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

var start = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

// parentRow stands in for an alert.
type parentRow struct {
	ID int64 `parquet:"id"`
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	alerts *filestore.Table[parentRow]
	clock  *filestore.FakeClock
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start)
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	e.alerts = filestore.NewTable(e.schema, "alerts", func(r parentRow) string { return filestore.IntKey(r.ID) })
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.alerts)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) (*Store, txn.DB) {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return NewDeltaStore(db, e.tables), db
}

func write(t *testing.T, db txn.DB, fn func(s deltatxn.Scope) error) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), db, nil, fn))
}

// read runs fn on a pod opened now, so it sees every commit (F29).
func (e *deltaEnv) read(t *testing.T, fn func(r filestore.Reader)) {
	t.Helper()
	_, db := e.pod(t, "reader")
	require.NoError(t, deltatxn.Read(context.Background(), db, nil, func(s deltatxn.Scope) error {
		fn(s.Reader)
		return nil
	}))
}

func (e *deltaEnv) addAlerts(t *testing.T, db txn.DB, ids ...int64) {
	t.Helper()
	write(t, db, func(s deltatxn.Scope) error {
		for _, id := range ids {
			if err := e.alerts.Put(s.Tx, parentRow{ID: id}); err != nil {
				return err
			}
		}
		return nil
	})
}

func micros(d time.Duration) *int64 {
	us := d.Microseconds()
	return &us
}

func TestDeltaFindMetrics(t *testing.T) {
	ctx := permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser)
	e := newDeltaEnv(t)
	a, db := e.pod(t, "a")
	e.addAlerts(t, db, 1, 2, 3)
	svcID := uuid.NewString()

	write(t, db, func(s deltatxn.Scope) error {
		require.NoError(t, e.tables.Insert(s, DeltaMetric{AlertID: 2, ServiceID: svcID, TimeToAck: micros(time.Minute), TimeToClose: micros(72 * time.Hour), Escalated: true, ClosedAt: start}))
		require.NoError(t, e.tables.Insert(s, DeltaMetric{AlertID: 1, ServiceID: svcID, TimeToClose: micros(time.Hour + time.Microsecond), ClosedAt: start.Add(-time.Hour)}))
		return nil
	})
	// A row that exists is kept, like the engine's ON CONFLICT DO NOTHING.
	write(t, db, func(s deltatxn.Scope) error {
		return e.tables.Insert(s, DeltaMetric{AlertID: 2, ServiceID: svcID, TimeToClose: micros(time.Second), ClosedAt: start.Add(time.Hour)})
	})

	want := []Metric{
		{ID: 1, TimeToAck: time.Hour + time.Microsecond, TimeToClose: time.Hour + time.Microsecond, ClosedAt: start.Add(-time.Hour)},
		{ID: 2, TimeToAck: time.Minute, TimeToClose: 72 * time.Hour, ClosedAt: start, Escalated: true},
	}
	b, _ := e.pod(t, "b")
	for _, s := range []*Store{a, b} {
		got, err := s.FindMetrics(ctx, []int{2, 1, 2, 3, 99})
		require.NoError(t, err)
		assert.Equal(t, want, got, "each alert once; closing without an ack counts as both")
	}

	got, err := a.FindMetrics(ctx, []int{99})
	require.NoError(t, err)
	assert.Nil(t, got, "nil without rows, as on Postgres")
	_, err = a.FindMetrics(context.Background(), []int{1})
	assert.True(t, permission.IsPermissionError(err))

	write(t, db, func(s deltatxn.Scope) error {
		return e.tables.Insert(s, DeltaMetric{AlertID: 3, ServiceID: svcID, TimeToAck: micros(time.Minute), ClosedAt: start})
	})
	_, err = a.FindMetrics(ctx, []int{1, 3})
	require.Error(t, err, "a time to close that is NULL is an error, as on Postgres")

	e.read(t, func(r filestore.Reader) {
		at, ok := e.tables.ClosedAt(r, 2)
		assert.True(t, ok)
		assert.Equal(t, start, at)
		_, ok = e.tables.ClosedAt(r, 99)
		assert.False(t, ok)

		var ids []int64
		e.tables.AscendService(r, svcID, func(m DeltaMetric) bool {
			ids = append(ids, m.AlertID)
			return true
		})
		assert.Equal(t, []int64{1, 2, 3}, ids)

		row, ok := e.tables.Metrics.Get(r, filestore.IntKey(2))
		require.True(t, ok)
		assert.EqualValues(t, 1, row.ID, "ids come from the sequence, in the order of the inserts")
	})
}

func TestDeltaMetricsConstraints(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	_, db := e.pod(t, "a")
	e.addAlerts(t, db, 1, 2)
	row := func(alertID int64) DeltaMetric {
		return DeltaMetric{AlertID: alertID, ServiceID: uuid.NewString(), TimeToClose: micros(time.Hour), ClosedAt: start}
	}
	put := func(fn func(tx *filestore.Tx) error) error {
		return deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error { return fn(s.Tx) })
	}

	err := deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error { return e.tables.Insert(s, row(99)) })
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr, "alert_metrics_alert_id_fkey")
	assert.Equal(t, "alert_metrics_alert_id_fkey", fkErr.ForeignKey)

	write(t, db, func(s deltatxn.Scope) error { return e.tables.Insert(s, row(1)) })
	err = put(func(tx *filestore.Tx) error { return e.tables.Metrics.Insert(tx, row(1), "alert_metrics_pkey") })
	var uErr *filestore.UniqueError
	require.ErrorAs(t, err, &uErr)
	assert.Equal(t, "alert_metrics_pkey", uErr.Index)

	dup := row(2)
	dup.ID = 1
	err = put(func(tx *filestore.Tx) error { return e.tables.Metrics.Put(tx, dup) })
	require.ErrorAs(t, err, &uErr)
	assert.Equal(t, "alert_metrics_id_key", uErr.Index)

	err = deltatxn.Write(ctx, db, nil, func(s deltatxn.Scope) error {
		m := row(2)
		m.ServiceID = "not a uuid"
		return e.tables.Insert(s, m)
	})
	require.Error(t, err, "service_id is a uuid")

	// ON DELETE CASCADE.
	write(t, db, func(s deltatxn.Scope) error {
		_, err := e.alerts.Delete(s.Tx, filestore.IntKey(1))
		return err
	})
	e.read(t, func(r filestore.Reader) { assert.Zero(t, e.tables.Metrics.Len(r)) })
}

func TestDeltaAscendClosed(t *testing.T) {
	e := newDeltaEnv(t)
	_, db := e.pod(t, "a")
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 23, 30, 0, 0, time.UTC) }
	closed := []time.Time{
		day(2026, 11, 24), day(2024, 2, 29), day(2025, 1, 1), day(2024, 12, 31), day(2026, 11, 24).Add(-time.Hour),
		time.Date(1677, 9, 22, 0, 0, 0, 0, time.UTC), time.Date(2262, 4, 1, 0, 0, 0, 0, time.UTC),
		// Near midnight, in a zone where it is already the next day.
		time.Date(2025, 6, 2, 1, 0, 0, 0, time.FixedZone("", 2*3600)),
	}
	ids := make([]int64, len(closed))
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	e.addAlerts(t, db, ids...)
	write(t, db, func(s deltatxn.Scope) error {
		for i, at := range closed {
			if err := e.tables.Insert(s, DeltaMetric{AlertID: ids[i], ServiceID: uuid.NewString(), TimeToClose: micros(1), ClosedAt: at}); err != nil {
				return err
			}
		}
		return nil
	})

	e.read(t, func(r filestore.Reader) {
		between := func(from, to time.Time, limit int) []int64 {
			var got []int64
			e.tables.AscendClosed(r, from, to, func(m DeltaMetric) bool {
				got = append(got, m.AlertID)
				return len(got) < limit
			})
			return got
		}
		var none time.Time
		assert.Equal(t, []int64{6, 2, 4, 3, 8, 1, 5, 7}, between(none, none, 100), "every row, by day")
		assert.Equal(t, []int64{6, 2}, between(none, none, 2), "until fn returns false")
		assert.Equal(t, []int64{4, 3}, between(day(2024, 12, 31), day(2025, 1, 1), 100), "both ends are included")
		assert.Equal(t, []int64{8}, between(day(2025, 6, 1), day(2025, 6, 1), 100), "days are in UTC")
		assert.Equal(t, []int64{1, 5, 7}, between(day(2025, 6, 2), none, 100))
		assert.Equal(t, []int64{6, 2}, between(none, day(2024, 12, 30), 100))
		assert.Empty(t, between(day(2025, 1, 2), day(2024, 1, 1), 100), "an empty range")
		assert.Empty(t, between(time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC), none, 100))
		assert.Empty(t, between(none, time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), 100))
	})
}
