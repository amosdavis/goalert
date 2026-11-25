package notice

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

// countRow stands in for the tables of other packages.
type countRow struct {
	ID    string `parquet:"id"`
	Count int64  `parquet:"count"`
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	fdb    *filestore.DeltaDB
	counts *filestore.Table[countRow]
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema()}
	e.counts = filestore.NewTable(e.schema, "counts", func(r countRow) string { return r.ID })
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	e.fdb = e.open(t)
	return e
}

func (e *deltaEnv) open(t *testing.T) *filestore.DeltaDB {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: "test", PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	return fdb
}

// set sets what a lookup returns for an id.
func (e *deltaEnv) set(t *testing.T, lookup, id string, n int) {
	t.Helper()
	_, err := e.fdb.Update(context.Background(), func(tx *filestore.Tx) error {
		return e.counts.Put(tx, countRow{ID: lookup + "/" + id, Count: int64(n)})
	})
	require.NoError(t, err)
}

func (e *deltaEnv) storeOn(fdb *filestore.DeltaDB) *Store {
	lookup := func(name string) func(r filestore.Reader, id string) int {
		return func(r filestore.Reader, id string) int {
			row, _ := e.counts.Get(r, name+"/"+id)
			return int(row.Count)
		}
	}
	return NewDeltaStore(deltatxn.DB{DB: fdb}, DeltaLookups{
		PolicyServices: lookup("services"),
		PolicyMinutes:  lookup("minutes"),
		UnackedAlerts:  lookup("alerts"),
		UnackedAlertLimit: func(r filestore.Reader) int {
			row, ok := e.counts.Get(r, "limit/unacked_alerts_per_service")
			if !ok {
				return -1
			}
			return int(row.Count)
		},
	})
}

func asUser(autoCloseDays int) context.Context {
	var cfg config.Config
	cfg.Maintenance.AlertAutoCloseDays = autoCloseDays
	return permission.UserContext(cfg.Context(context.Background()), uuid.NewString(), permission.RoleUser)
}

func TestDeltaPolicyNotices(t *testing.T) {
	e := newDeltaEnv(t)
	s := e.storeOn(e.fdb)
	id := uuid.NewString()
	messages := func(s *Store, days int) []string {
		t.Helper()
		notices, err := s.FindAllPolicyNotices(asUser(days), strings.ToUpper(id))
		require.NoError(t, err)
		var msgs []string
		for _, n := range notices {
			assert.Equal(t, TypeWarning, n.Type)
			msgs = append(msgs, n.Message)
		}
		return msgs
	}
	const unused, autoClose = "Not assigned to a service", "Auto-closure of unacknowledged alerts"

	assert.Equal(t, []string{unused}, messages(s, 1))
	e.set(t, "services", id, 2)
	assert.Nil(t, messages(s, 1))
	e.set(t, "minutes", id, 2*24*60-1)
	assert.Nil(t, messages(s, 2))
	assert.Equal(t, []string{autoClose}, messages(s, 1))
	e.set(t, "minutes", id, 2*24*60)
	assert.Equal(t, []string{autoClose}, messages(s, 2))
	assert.Nil(t, messages(s, 0), "alerts are not closed")
	e.set(t, "services", id, 0)
	assert.Equal(t, []string{unused, autoClose}, messages(e.storeOn(e.open(t)), 2), "on another pod")

	_, err := s.FindAllPolicyNotices(asUser(1), "bob")
	assert.True(t, validation.IsClientError(err), "still validated")
	_, err = s.FindAllPolicyNotices(context.Background(), id)
	assert.True(t, permission.IsPermissionError(err), "still checked")
}

func TestDeltaServiceNotices(t *testing.T) {
	e := newDeltaEnv(t)
	s := e.storeOn(e.fdb)
	id := uuid.NewString()
	find := func(s *Store) []Notice {
		t.Helper()
		notices, err := s.FindAllServiceNotices(asUser(0), strings.ToUpper(id))
		require.NoError(t, err)
		return notices
	}
	e.set(t, "alerts", id, 1000)
	assert.Nil(t, find(s), "no limit")

	e.set(t, "limit", "unacked_alerts_per_service", 100)
	for _, test := range []struct {
		alerts  int
		typ     Type
		message string
	}{
		{0, 0, ""},
		{75, 0, ""},
		{76, TypeWarning, "Near unacknowledged alert limit"},
		{99, TypeWarning, "Near unacknowledged alert limit"},
		{100, TypeError, "Unacknowledged alert limit reached"},
		{101, TypeError, "Unacknowledged alert limit reached"},
	} {
		e.set(t, "alerts", id, test.alerts)
		notices := find(s)
		if test.message == "" {
			assert.Nil(t, notices, "%d alerts", test.alerts)
			continue
		}
		require.Len(t, notices, 1, "%d alerts", test.alerts)
		assert.Equal(t, test.typ, notices[0].Type)
		assert.Equal(t, test.message, notices[0].Message)
	}
	assert.Len(t, find(e.storeOn(e.open(t))), 1, "on another pod")
	e.set(t, "limit", "unacked_alerts_per_service", -1)
	assert.Nil(t, find(s), "no limit")

	_, err := s.FindAllServiceNotices(asUser(0), "bob")
	assert.True(t, validation.IsClientError(err), "still validated")
	_, err = s.FindAllServiceNotices(context.Background(), id)
	assert.True(t, permission.IsPermissionError(err), "still checked")
}

func TestDeltaMissingLookups(t *testing.T) {
	e := newDeltaEnv(t)
	s := NewDeltaStore(deltatxn.DB{DB: e.fdb}, DeltaLookups{})
	_, err := s.FindAllPolicyNotices(asUser(0), uuid.NewString())
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)
	_, err = s.FindAllServiceNotices(asUser(0), uuid.NewString())
	require.ErrorIs(t, err, deltatxn.ErrNotPorted)
}
