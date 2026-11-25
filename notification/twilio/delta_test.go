package twilio

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// Stand-ins for the tables of other packages.
type (
	deltaAlert struct {
		ID     int64  `parquet:"id"`
		Status string `parquet:"status"`
	}
	deltaService struct {
		ID   string `parquet:"id"`
		Name string `parquet:"name"`
	}
)

var deltaStart = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

const (
	phone1 = "+17635550100"
	phone2 = "+17635550101"
)

type deltaEnv struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	alerts   *filestore.Table[deltaAlert]
	services *filestore.Table[deltaService]
	clock    *filestore.FakeClock
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(deltaStart)}
	e.clock.SetSpeed(0)
	e.clock.Set(deltaStart)
	e.alerts = filestore.NewTable(e.schema, "alerts", func(a deltaAlert) string { return filestore.IntKey(a.ID) })
	e.services = filestore.NewTable(e.schema, "services", func(s deltaService) string { return s.ID })
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.alerts, e.services)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	return e
}

type deltaPod struct {
	*dbSMS
	e  *deltaEnv
	db txn.DB
	f  *filestore.DeltaDB
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) *deltaPod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	alertOpen := func(r filestore.Reader, id int64) bool {
		a, ok := e.alerts.Get(r, filestore.IntKey(id))
		return ok && a.Status != "closed"
	}
	serviceName := func(r filestore.Reader, id string) (string, bool) {
		s, ok := e.services.Get(r, id)
		return s.Name, ok
	}
	s := NewDeltaSMS(db, e.tables, &Config{}, alertOpen, serviceName)
	return &deltaPod{dbSMS: s.b, e: e, db: db, f: fdb}
}

func (p *deltaPod) write(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	require.NoError(t, deltatxn.Write(context.Background(), p.db, nil, func(s deltatxn.Scope) error { return fn(s.Tx) }))
}

func (p *deltaPod) setAlert(t *testing.T, id int64, status string) {
	t.Helper()
	p.write(t, func(tx *filestore.Tx) error { return p.e.alerts.Put(tx, deltaAlert{ID: id, Status: status}) })
}

func (p *deltaPod) addService(t *testing.T, name string) string {
	t.Helper()
	id := uuid.NewString()
	p.write(t, func(tx *filestore.Tx) error { return p.e.services.Put(tx, deltaService{ID: id, Name: name}) })
	return id
}

func (p *deltaPod) insert(t *testing.T, phone, callbackID string, alertID int, serviceID string) int {
	t.Helper()
	code, err := p.insertDB(context.Background(), phone, callbackID, alertID, serviceID)
	require.NoError(t, err)
	return code
}

func (e *deltaEnv) rows(t *testing.T) []smsCallbackRow {
	t.Helper()
	var rows []smsCallbackRow
	e.tables.callbacks.Ascend(e.pod(t, "reader").f.View(), func(r smsCallbackRow) bool {
		rows = append(rows, r)
		return true
	})
	return rows
}

func TestDeltaSMSCodes(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	for id := range int64(4) {
		p.setAlert(t, id+1, "triggered")
	}
	svc, svc2 := p.addService(t, "svc"), p.addService(t, "svc2")

	assert.Equal(t, 1, p.insert(t, phone1, uuid.NewString(), 1, ""), "alert codes start at 1")
	assert.Equal(t, 2, p.insert(t, phone1, uuid.NewString(), 2, ""))
	assert.Equal(t, 1, p.insert(t, phone2, uuid.NewString(), 2, ""), "each number has its own codes")
	assert.Equal(t, 100, p.insert(t, phone1, uuid.NewString(), 0, svc), "service codes start at 100")
	assert.Equal(t, 101, p.insert(t, phone1, uuid.NewString(), 0, svc2))

	assert.Equal(t, 1, p.insert(t, phone1, uuid.NewString(), 1, ""), "an open alert keeps its code")
	assert.Equal(t, 100, p.insert(t, phone1, uuid.NewString(), 0, svc), "and so does a service")
	assert.Len(t, e.rows(t), 5)

	// A closed alert's code is free again, and the row is replaced.
	p.setAlert(t, 1, "closed")
	assert.Equal(t, 1, p.insert(t, phone1, uuid.NewString(), 3, ""))
	assert.Equal(t, 3, p.insert(t, phone1, uuid.NewString(), 4, ""))
	assert.Len(t, e.rows(t), 6)

	// A message about a closed alert gets a code too.
	assert.Equal(t, 4, p.insert(t, phone1, uuid.NewString(), 1, ""))
}

func TestDeltaSMSCodesErrors(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	p.setAlert(t, 1, "triggered")

	_, err := p.insertDB(ctx, phone1, "not a uuid", 1, "")
	require.ErrorContains(t, err, "callback_id")
	_, err = p.insertDB(ctx, phone1, uuid.NewString(), 0, "not a uuid")
	require.ErrorContains(t, err, "service_id")

	var fkErr *filestore.ForeignKeyError
	_, err = p.insertDB(ctx, phone1, uuid.NewString(), 2, "")
	require.ErrorAs(t, err, &fkErr, "the alert must exist (F7)")
	assert.Equal(t, "twilio_sms_callbacks_alert_id_fkey", fkErr.ConstraintName())
	assert.NotContains(t, err.Error(), "5550100", "errors don't show phone numbers (F20)")
	_, err = p.insertDB(ctx, phone1, uuid.NewString(), 0, uuid.NewString())
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "twilio_sms_callbacks_service_id_fkey", fkErr.ConstraintName())
	assert.Empty(t, e.rows(t))

	// The unique index is the last line of defense.
	put := func(row smsCallbackRow) error {
		return deltatxn.Write(ctx, p.db, nil, func(s deltatxn.Scope) error {
			row.CallbackID, row.SentAt = uuid.NewString(), s.Now
			return e.tables.callbacks.Insert(s.Tx, row, "twilio_sms_callbacks_uniq_id")
		})
	}
	require.NoError(t, put(smsCallbackRow{ID: 1, PhoneNumber: phone1, Code: 7}))
	var uErr *filestore.UniqueError
	err = put(smsCallbackRow{ID: 2, PhoneNumber: phone1, Code: 7})
	require.ErrorAs(t, err, &uErr)
	assert.Equal(t, "idx_twilio_sms_codes", uErr.ConstraintName())
	assert.NotContains(t, err.Error(), "5550100")
	require.ErrorAs(t, put(smsCallbackRow{ID: 1, PhoneNumber: phone2, Code: 7}), &uErr)
	assert.Equal(t, "twilio_sms_callbacks_uniq_id", uErr.ConstraintName())
	require.NoError(t, put(smsCallbackRow{ID: 3, PhoneNumber: phone2, Code: 7}))
}

// Two pods send to one number at once: every alert gets a code of its own
// (F8).
func TestDeltaSMSCodesRace(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	setup := e.pod(t, "setup")
	const n = 12
	for id := range int64(n) {
		setup.setAlert(t, id+1, "triggered")
	}
	svc := setup.addService(t, "svc")
	pods := []*deltaPod{e.pod(t, "a"), e.pod(t, "b")}

	codes := make([]int, n)
	svcCodes := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			var err error
			codes[i], err = pods[i%2].insertDB(ctx, phone1, uuid.NewString(), i+1, "")
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			var err error
			svcCodes[i], err = pods[i%2].insertDB(ctx, phone1, uuid.NewString(), 0, svc)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	want := make([]int, n)
	for i := range want {
		want[i] = i + 1
	}
	assert.ElementsMatch(t, want, codes, "no two alerts share a code, and none is skipped")
	for _, c := range svcCodes {
		assert.Equal(t, 100, c, "a service has one code, whichever pod sends")
	}
	rows := e.rows(t)
	assert.Len(t, rows, n+1)
	reader := e.pod(t, "check")
	for i, c := range codes {
		info, err := reader.LookupByCode(ctx, phone1, c)
		require.NoError(t, err)
		assert.Equal(t, i+1, info.AlertID, "a reply with a code reaches the alert it was sent for")
	}
}

// A code is freed and taken by two pods at once.
func TestDeltaSMSReuseRace(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	for id := range int64(3) {
		a.setAlert(t, id+1, "triggered")
	}
	require.Equal(t, 1, a.insert(t, phone1, uuid.NewString(), 1, ""))
	a.setAlert(t, 1, "closed")

	codes := make([]int, 2)
	var wg sync.WaitGroup
	for i, p := range []*deltaPod{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			codes[i], err = p.insertDB(ctx, phone1, uuid.NewString(), i+2, "")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.ElementsMatch(t, []int{1, 2}, codes)
}

func TestDeltaSMSLookup(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	for id := range int64(3) {
		p.setAlert(t, id+1, "triggered")
	}
	svc := p.addService(t, "My Service")
	cb := make([]string, 5)
	for i := range cb {
		cb[i] = uuid.NewString()
	}

	_, err := p.LookupByCode(ctx, phone1, 0)
	require.ErrorIs(t, err, sql.ErrNoRows)
	info, err := p.LookupByCode(ctx, phone1, 1)
	require.ErrorIs(t, err, sql.ErrNoRows, "callers check for sql.ErrNoRows, as with Postgres")
	assert.NotNil(t, info, "callers use the result along with an error")

	p.insert(t, phone1, cb[0], 1, "")
	e.clock.Advance(time.Minute)
	p.insert(t, phone1, cb[1], 2, "")
	e.clock.Advance(time.Minute)
	p.insert(t, phone1, cb[2], 0, svc)

	fresh := e.pod(t, "b")
	info, err = fresh.LookupByCode(ctx, phone1, 1)
	require.NoError(t, err)
	assert.Equal(t, codeInfo{AlertID: 1, CallbackID: cb[0]}, *info)
	info, err = fresh.LookupByCode(ctx, phone1, 0)
	require.NoError(t, err)
	assert.Equal(t, codeInfo{AlertID: 2, CallbackID: cb[1]}, *info, "without a code, the latest alert")
	info, err = fresh.LookupByAlertID(ctx, phone1, 1)
	require.NoError(t, err)
	assert.Equal(t, codeInfo{AlertID: 1, CallbackID: cb[0]}, *info)
	info, err = fresh.LookupSvcByCode(ctx, phone1, 100)
	require.NoError(t, err)
	assert.Equal(t, codeInfo{ServiceName: "My Service", CallbackID: cb[2]}, *info)

	for name, lookup := range map[string]func() (*codeInfo, error){
		"another number's code":              func() (*codeInfo, error) { return fresh.LookupByCode(ctx, phone2, 1) },
		"another number's latest":            func() (*codeInfo, error) { return fresh.LookupByCode(ctx, phone2, 0) },
		"another number's alert":             func() (*codeInfo, error) { return fresh.LookupByAlertID(ctx, phone2, 1) },
		"an alert without a message":         func() (*codeInfo, error) { return fresh.LookupByAlertID(ctx, phone1, 3) },
		"a service code, as an alert's":      func() (*codeInfo, error) { return fresh.LookupByCode(ctx, phone1, 100) },
		"an alert code, as a service's":      func() (*codeInfo, error) { return fresh.LookupSvcByCode(ctx, phone1, 1) },
		"another number's service code":      func() (*codeInfo, error) { return fresh.LookupSvcByCode(ctx, phone2, 100) },
		"a service code that was never sent": func() (*codeInfo, error) { return fresh.LookupSvcByCode(ctx, phone1, 101) },
	} {
		_, err := lookup()
		assert.ErrorIs(t, err, sql.ErrNoRows, name)
	}

	// Alert 1 and the service are sent again, with the codes they have: a
	// reply is to these messages.
	e.clock.Advance(time.Minute)
	assert.Equal(t, 1, p.insert(t, phone1, cb[3], 1, ""))
	assert.Equal(t, 100, p.insert(t, phone1, cb[4], 0, svc))
	for name, lookup := range map[string]func() (*codeInfo, error){
		"latest":   func() (*codeInfo, error) { return p.LookupByCode(ctx, phone1, 0) },
		"by code":  func() (*codeInfo, error) { return p.LookupByCode(ctx, phone1, 1) },
		"by alert": func() (*codeInfo, error) { return p.LookupByAlertID(ctx, phone1, 1) },
	} {
		info, err = lookup()
		require.NoError(t, err, name)
		assert.Equal(t, codeInfo{AlertID: 1, CallbackID: cb[3]}, *info, name)
	}
	info, err = p.LookupSvcByCode(ctx, phone1, 100)
	require.NoError(t, err)
	assert.Equal(t, cb[4], info.CallbackID)
}

func TestDeltaSMSCascade(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	p.setAlert(t, 1, "triggered")
	p.setAlert(t, 2, "triggered")
	svc := p.addService(t, "svc")
	p.insert(t, phone1, uuid.NewString(), 1, "")
	p.insert(t, phone1, uuid.NewString(), 2, "")
	p.insert(t, phone1, uuid.NewString(), 0, svc)

	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.alerts.Delete(tx, filestore.IntKey(1))
		return err
	})
	rows := e.rows(t)
	require.Len(t, rows, 2, "deleting an alert deletes its codes (ON DELETE CASCADE)")
	assert.EqualValues(t, 2, *rows[0].AlertID)

	p.write(t, func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, svc)
		return err
	})
	require.Len(t, e.rows(t), 1, "and so does deleting a service")
	_, err := p.LookupSvcByCode(ctx, phone1, 100)
	require.ErrorIs(t, err, sql.ErrNoRows)
	p.setAlert(t, 3, "triggered")
	assert.Equal(t, 1, p.insert(t, phone1, uuid.NewString(), 3, ""), "the deleted alert's code is free")
}

// Many codes for one number: the search for a free one must not slow down
// with the numbers that have none to do with it.
func TestDeltaSMSManyNumbers(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	p.setAlert(t, 1, "triggered")
	p.write(t, func(tx *filestore.Tx) error {
		for i := range int64(200) {
			alertID := int64(1)
			row := smsCallbackRow{ID: i + 1000, PhoneNumber: fmt.Sprintf("+1763555%04d", i+1000), Code: 1, CallbackID: uuid.NewString(), AlertID: &alertID, SentAt: tx.Now()}
			if err := e.tables.callbacks.Put(tx, row); err != nil {
				return err
			}
		}
		return nil
	})
	assert.Equal(t, 1, p.insert(t, phone1, uuid.NewString(), 1, ""))
	seen := 0
	e.tables.byPhone.Ascend(p.f.View(), phoneKey(phone1), func(smsCallbackRow) bool { seen++; return true })
	assert.Equal(t, 1, seen)
}
