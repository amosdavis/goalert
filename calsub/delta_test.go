package calsub

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/auth/authdelta"
	"github.com/target/goalert/auth/authtoken"
	"github.com/target/goalert/config"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/keyring"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/oncall"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

type testKeys struct{ keyring.Keyring }

func (testKeys) Sign([]byte) ([]byte, error) { return []byte("signed"), nil }

// nameRow stands in for users and schedules, limitRow for config_limits.
type nameRow struct {
	ID   string `parquet:"id"`
	Name string `parquet:"name"`
}

type limitRow struct {
	ID  string `parquet:"id"`
	Max int64  `parquet:"max"`
}

type env struct {
	store            objstore.Store
	schema           *filestore.Schema
	tables           *DeltaTables
	users, schedules *filestore.Table[nameRow]
	limits           *filestore.Table[limitRow]
	clock            *filestore.FakeClock
	start            time.Time
}

const (
	alice = "00000000-0000-0000-0000-00000000000a"
	bob   = "00000000-0000-0000-0000-00000000000b"
	sched = "00000000-0000-0000-0000-000000000001"
	other = "00000000-0000-0000-0000-000000000002"
)

func newEnv(t *testing.T) *env {
	t.Helper()
	start := time.Date(2026, 11, 25, 12, 0, 0, 123456000, time.UTC)
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: filestore.NewFakeClock(start), start: start}
	e.clock.SetSpeed(0)
	e.clock.Set(start)
	e.tables = DefineDeltaTables(e.schema)
	key := func(r nameRow) string { return r.ID }
	e.users = filestore.NewTable(e.schema, "users", key)
	e.schedules = filestore.NewTable(e.schema, "schedules", key)
	e.limits = filestore.NewTable(e.schema, "config_limits", func(r limitRow) string { return r.ID })
	DeclareDeltaForeignKeys(e.tables, e.users, e.schedules)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: e.clock}))
	e.update(t, e.pod(t, "setup"), func(tx *filestore.Tx) error {
		for _, u := range []nameRow{{alice, "Alice"}, {bob, "Bob"}} {
			if err := e.users.Put(tx, u); err != nil {
				return err
			}
		}
		for _, s := range []nameRow{{sched, "Sched"}, {other, "Other"}} {
			if err := e.schedules.Put(tx, s); err != nil {
				return err
			}
		}
		return nil
	})
	return e
}

func (e *env) update(t *testing.T, p *pod, fn func(tx *filestore.Tx) error) {
	t.Helper()
	_, err := p.db.Update(context.Background(), fn)
	require.NoError(t, err)
}

type pod struct {
	*Store
	db  *filestore.DeltaDB
	txn txn.DB
}

func (e *env) pod(t *testing.T, name string) *pod {
	t.Helper()
	db, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	p := &pod{db: db, txn: deltatxn.DB{DB: db}}
	p.Store = NewDeltaStore(p.txn, e.tables, testKeys{}, nil, DeltaLookups{
		ScheduleName: func(r filestore.Reader, id string) (string, bool) { s, ok := e.schedules.Get(r, id); return s.Name, ok },
		UserName:     func(r filestore.Reader, id string) (string, bool) { u, ok := e.users.Get(r, id); return u.Name, ok },
		MaxPerUser: func(r filestore.Reader) int {
			l, ok := e.limits.Get(r, "calendar_subscriptions_per_user")
			if !ok {
				return -1
			}
			return int(l.Max)
		},
	})
	return p
}

func (e *env) setLimit(t *testing.T, n int64) {
	t.Helper()
	e.update(t, e.pod(t, "limits-"+uuid.NewString()), func(tx *filestore.Tx) error {
		return e.limits.Put(tx, limitRow{"calendar_subscriptions_per_user", n})
	})
}

func (e *env) row(t *testing.T, id string) (subRow, bool) {
	t.Helper()
	return e.tables.Subscriptions.Get(e.pod(t, "reader-"+uuid.NewString()).db.View(), id)
}

func userCtx(id string) context.Context {
	return config.Config{}.Context(permission.UserContext(context.Background(), id, permission.RoleUser))
}

func sub(userID, scheduleID, name string) *Subscription {
	return &Subscription{Name: name, UserID: userID, ScheduleID: scheduleID, Config: SubscriptionConfig{ReminderMinutes: []int{5, 10}}}
}

// create returns a new subscription and its token.
func (p *pod) create(t *testing.T, cs *Subscription) (*Subscription, authtoken.Token) {
	t.Helper()
	cs, err := p.CreateTx(userCtx(cs.UserID), nil, cs)
	require.NoError(t, err)
	tok, _, err := authtoken.Parse(cs.Token(), nil)
	require.NoError(t, err)
	return cs, *tok
}

func find(t *testing.T, p *pod, id string) *Subscription {
	t.Helper()
	cs, err := p.FindOne(userCtx(alice), id)
	require.NoError(t, err)
	return cs
}

func TestDeltaSubscriptions(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	none, err := p.FindAllByUser(userCtx(alice), alice)
	require.NoError(t, err)
	assert.Equal(t, []Subscription{}, none, "an empty list, not nil")

	cs, tok := p.create(t, sub(alice, sched, "first"))
	assert.Equal(t, cs.ID, tok.ID.String())
	assert.Equal(t, authtoken.TypeCalSub, tok.Type)
	assert.Equal(t, e.start.Truncate(time.Second), tok.CreatedAt.UTC())
	e.clock.Advance(time.Minute)
	cs2, _ := p.create(t, sub(alice, other, "second"))
	p.create(t, sub(bob, sched, "first"))

	want := []Subscription{
		{ID: cs.ID, Name: "first", UserID: alice, ScheduleID: sched, Config: cs.Config},
		{ID: cs2.ID, Name: "second", UserID: alice, ScheduleID: other, Config: cs.Config},
	}
	all, err := e.pod(t, "b").FindAllByUser(userCtx(alice), alice)
	require.NoError(t, err)
	assert.Equal(t, want, all, "oldest first, read by a fresh pod")
	assert.Equal(t, &want[0], find(t, p, cs.ID))

	_, err = p.FindOne(userCtx(alice), uuid.NewString())
	assert.True(t, validation.IsClientError(err))
	assert.ErrorContains(t, err, "not found")

	// Update, like the GraphQL mutation does.
	require.NoError(t, p.txn.Run(context.Background(), nil, func(tx txn.Tx) error {
		cs, err := p.FindOneForUpdate(userCtx(alice), tx, cs.ID)
		if err != nil {
			return err
		}
		cs.Name, cs.Disabled, cs.Config.FullSchedule = "renamed", true, true
		return p.UpdateTx(userCtx(alice), tx, cs)
	}))
	want[0].Name, want[0].Disabled, want[0].Config.FullSchedule = "renamed", true, true
	assert.Equal(t, &want[0], find(t, e.pod(t, "c"), cs.ID))
	row, _ := e.row(t, cs.ID)
	assert.Equal(t, e.start, row.CreatedAt)
	assert.Equal(t, e.start.Add(time.Minute), row.LastUpdate)

	stolen := want[0]
	stolen.UserID, stolen.Name = bob, "stolen"
	require.NoError(t, p.UpdateTx(userCtx(bob), nil, &stolen))
	assert.Equal(t, &want[0], find(t, p, cs.ID), "only the owner's are changed")
	missing := want[0]
	missing.ID = uuid.NewString()
	require.NoError(t, p.UpdateTx(userCtx(alice), nil, &missing), "an UPDATE of no rows")

	require.NoError(t, p.DeleteTx(userCtx(bob), nil, bob, cs.ID, cs2.ID))
	assert.Equal(t, &want[0], find(t, p, cs.ID), "only the owner's are deleted")
	require.NoError(t, p.DeleteTx(userCtx(alice), nil, alice))
	require.NoError(t, p.DeleteTx(userCtx(alice), nil, alice, cs.ID, uuid.NewString()))
	all, err = e.pod(t, "d").FindAllByUser(userCtx(alice), alice)
	require.NoError(t, err)
	assert.Equal(t, want[1:], all)
}

func TestDeltaConstraints(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	cs, _ := p.create(t, sub(alice, sched, "name"))
	cs2, _ := p.create(t, sub(alice, sched, "name2"))
	p.create(t, sub(alice, other, "name"))
	p.create(t, sub(bob, sched, "name"))

	check := func(err error, constraint, message string) {
		t.Helper()
		var c interface{ ConstraintName() string }
		require.ErrorAs(t, err, &c)
		assert.Equal(t, constraint, c.ConstraintName())
		err = errutil.MapDBError(err)
		assert.True(t, validation.IsClientError(err))
		assert.ErrorContains(t, err, message)
	}
	const unique = "user_calendar_subscriptions_name_schedule_id_user_id_key"
	_, err := p.CreateTx(userCtx(alice), nil, sub(alice, sched, "name"))
	check(err, unique, "already in use")
	cs2.Name = "name"
	check(p.UpdateTx(userCtx(alice), nil, cs2), unique, "already in use")
	_, err = p.CreateTx(userCtx(alice), nil, sub(alice, uuid.NewString(), "name"))
	check(err, "user_calendar_subscriptions_schedule_id_fkey", "schedule does not exist")
	carol := uuid.NewString()
	_, err = p.CreateTx(userCtx(carol), nil, sub(carol, sched, "name"))
	check(err, "user_calendar_subscriptions_user_id_fkey", "user does not exist")

	_, err = p.CreateTx(userCtx(alice), nil, cs)
	var uerr *filestore.UniqueError
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "user_calendar_subscriptions_pkey", uerr.ConstraintName())

	_, err = p.CreateTx(userCtx(alice), nil, sub(alice, "not a uuid", "name"))
	assert.True(t, validation.IsClientError(err))
	_, err = p.CreateTx(userCtx(bob), nil, sub(alice, sched, "name3"))
	assert.True(t, permission.IsPermissionError(err))
	off := config.Config{}
	off.General.DisableCalendarSubscriptions = true
	_, err = p.CreateTx(off.Context(userCtx(alice)), nil, sub(alice, sched, "name3"))
	assert.ErrorContains(t, err, "disabled by administrator")
}

func TestDeltaCascades(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	a, _ := p.create(t, sub(alice, sched, "name"))
	b, _ := p.create(t, sub(bob, other, "name"))
	c, _ := p.create(t, sub(bob, sched, "name"))
	e.update(t, p, func(tx *filestore.Tx) error { _, err := e.users.Delete(tx, alice); return err })
	_, ok := e.row(t, a.ID)
	assert.False(t, ok, "the user was deleted")
	e.update(t, p, func(tx *filestore.Tx) error { _, err := e.schedules.Delete(tx, other); return err })
	_, ok = e.row(t, b.ID)
	assert.False(t, ok, "the schedule was deleted")
	_, ok = e.row(t, c.ID)
	assert.True(t, ok)
}

func TestDeltaLimit(t *testing.T) {
	e := newEnv(t)
	e.setLimit(t, 2)
	p := e.pod(t, "a")
	p.create(t, sub(alice, sched, "one"))
	disabled := sub(alice, sched, "two")
	disabled.Disabled = true
	p.create(t, disabled)
	_, err := p.CreateTx(userCtx(alice), nil, sub(alice, other, "three"))
	require.Error(t, err)
	err = errutil.MapDBError(err)
	require.True(t, errutil.IsLimitError(err), "disabled ones count too: %v", err)
	var limit errutil.LimitError
	require.ErrorAs(t, err, &limit)
	assert.Equal(t, 2, limit.Max())
	assert.EqualValues(t, "calendar_subscriptions_per_user", limit.ID())
	all, err := p.FindAllByUser(userCtx(alice), alice)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	p.create(t, sub(bob, sched, "one"))

	for _, cs := range all {
		require.NoError(t, p.UpdateTx(userCtx(alice), nil, &cs), "updates aren't limited")
	}
	e.setLimit(t, 0)
	_, err = e.pod(t, "b").CreateTx(userCtx(bob), nil, sub(bob, sched, "two"))
	assert.True(t, errutil.IsLimitError(errutil.MapDBError(err)))
	e.setLimit(t, -1)
	p = e.pod(t, "c")
	p.create(t, sub(alice, other, "three"))
}

func TestDeltaLimitRace(t *testing.T) {
	e := newEnv(t)
	e.setLimit(t, 3)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].CreateTx(userCtx(alice), nil, sub(alice, sched, "name"+string(rune('a'+i))))
			if err == nil {
				created.Add(1)
				return
			}
			assert.True(t, errutil.IsLimitError(errutil.MapDBError(err)), "%v", err)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 3, created.Load(), "pods racing can't exceed the limit (F8)")
	all, err := e.pod(t, "c").FindAllByUser(userCtx(alice), alice)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestDeltaNameRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].CreateTx(userCtx(alice), nil, sub(alice, sched, "name"))
			if err == nil {
				created.Add(1)
				return
			}
			var uerr *filestore.UniqueError
			assert.ErrorAs(t, err, &uerr)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "one subscription gets the name (F8)")
}

func requireUnauthorized(t *testing.T, p *pod, tok authtoken.Token) {
	t.Helper()
	_, err := p.Authorize(context.Background(), tok)
	require.Error(t, err)
	require.True(t, permission.IsUnauthorized(err), "%v", err)
}

func TestDeltaAuthorize(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	_, warm := a.create(t, sub(bob, sched, "warm"))
	_, err := b.Authorize(context.Background(), warm)
	require.NoError(t, err)

	cs, tok := a.create(t, sub(alice, sched, "name"))
	for _, p := range []*pod{a, b} {
		ctx, err := p.Authorize(context.Background(), tok)
		require.NoError(t, err, "also on a pod that hasn't followed the log yet (F29)")
		assert.Equal(t, alice, permission.UserID(ctx))
		assert.False(t, permission.Admin(ctx))
		assert.Equal(t, &permission.SourceInfo{Type: permission.SourceTypeCalendarSubscription, ID: cs.ID}, permission.Source(ctx))
	}

	wrong := tok
	wrong.CreatedAt = tok.CreatedAt.Add(time.Second)
	requireUnauthorized(t, a, wrong)
	wrong = tok
	wrong.ID = uuid.New()
	requireUnauthorized(t, a, wrong)
	wrong = tok
	wrong.Type = authtoken.TypeSession
	requireUnauthorized(t, a, wrong)

	b.delta.check = authdelta.NewChecker(b.txn, 0)
	cs.Disabled = true
	require.NoError(t, a.UpdateTx(userCtx(alice), nil, cs))
	requireUnauthorized(t, a, tok)
	requireUnauthorized(t, b, tok)
	cs.Disabled = false
	require.NoError(t, a.UpdateTx(userCtx(alice), nil, cs))
	_, err = b.Authorize(context.Background(), tok)
	require.NoError(t, err)

	require.NoError(t, a.DeleteTx(userCtx(alice), nil, alice, cs.ID))
	requireUnauthorized(t, a, tok)
	requireUnauthorized(t, b, tok)
	requireUnauthorized(t, e.pod(t, "c"), tok)
}

func (p *pod) flush(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, p.Shutdown(context.Background()), "shutting down writes what isn't written yet")
	_, ok := p.delta.access.Pending(id)
	require.False(t, ok)
}

func TestDeltaLastAccess(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	cs, tok := p.create(t, sub(alice, sched, "name"))
	assert.Zero(t, find(t, p, cs.ID).LastAccess)

	access := func() {
		t.Helper()
		_, err := p.Authorize(context.Background(), tok)
		require.NoError(t, err)
		p.flush(t, cs.ID)
	}
	e.clock.Set(e.start.Add(time.Hour))
	access()
	assert.Equal(t, e.start.Add(time.Hour), find(t, e.pod(t, "b"), cs.ID).LastAccess)

	version := p.db.View().Index()
	e.clock.Advance(59 * time.Second)
	access()
	assert.Equal(t, version, p.db.View().Index(), "an access within a minute of the recorded one commits nothing (F37)")
	e.clock.Advance(time.Second)
	access()
	row, _ := e.row(t, cs.ID)
	assert.Equal(t, e.start.Add(time.Hour+time.Minute), *row.LastAccess)
	assert.Equal(t, e.start, row.LastUpdate, "an access is not an update")

	// Accesses pods write late.
	late := e.start.Add(30 * time.Minute)
	require.NoError(t, e.pod(t, "c").delta.writeAccess(context.Background(), map[string]time.Time{cs.ID: late}))
	row, _ = e.row(t, cs.ID)
	assert.Equal(t, e.start.Add(time.Hour+time.Minute), *row.LastAccess, "an earlier one changes nothing")

	q := e.pod(t, "d")
	require.NoError(t, p.DeleteTx(userCtx(alice), nil, alice, cs.ID))
	require.NoError(t, q.delta.writeAccess(context.Background(), map[string]time.Time{cs.ID: e.start.Add(2 * time.Hour)}))
	_, ok := e.row(t, cs.ID)
	assert.False(t, ok, "a subscription deleted on another pod stays deleted (F29)")
}

func TestDeltaLastAccessKeepsUpdates(t *testing.T) {
	e := newEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	cs, tok := a.create(t, sub(alice, sched, "name"))
	_, err := b.Authorize(context.Background(), tok)
	require.NoError(t, err)
	cs.Name = "renamed"
	require.NoError(t, a.UpdateTx(userCtx(alice), nil, cs))
	require.NoError(t, b.delta.writeAccess(context.Background(), map[string]time.Time{cs.ID: e.start.Add(time.Minute)}))
	got := find(t, e.pod(t, "c"), cs.ID)
	assert.Equal(t, "renamed", got.Name, "a pod that hasn't seen the update keeps it (F4)")
	assert.Equal(t, e.start.Add(time.Minute), got.LastAccess)
}

func TestDeltaDisableUnused(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	used, _ := p.create(t, sub(alice, sched, "used"))
	unused, _ := p.create(t, sub(alice, sched, "unused"))
	unused2, _ := p.create(t, sub(alice, sched, "unused too"))
	off := sub(alice, sched, "off")
	off.Disabled = true
	p.create(t, off)
	e.clock.Advance(48 * time.Hour)
	fresh, _ := p.create(t, sub(alice, sched, "fresh"))
	require.NoError(t, p.delta.writeAccess(context.Background(), map[string]time.Time{
		used.ID: e.start.Add(47 * time.Hour), unused.ID: e.start.Add(time.Hour),
	}))

	disable := func(limit int) (n int) {
		t.Helper()
		e.update(t, p, func(tx *filestore.Tx) error {
			var err error
			n, err = e.tables.DisableUnused(tx, e.start.Add(24*time.Hour), limit)
			return err
		})
		return n
	}
	assert.Equal(t, 1, disable(1))
	assert.Equal(t, 1, disable(100))
	assert.Equal(t, 0, disable(100))
	q := e.pod(t, "b")
	for id, disabled := range map[string]bool{used.ID: false, unused.ID: true, unused2.ID: true, fresh.ID: false} {
		assert.Equal(t, disabled, find(t, q, id).Disabled)
	}
	row, _ := e.row(t, unused.ID)
	assert.Equal(t, e.start, row.LastUpdate)
}

func TestDeltaRender(t *testing.T) {
	e := newEnv(t)
	p := e.pod(t, "a")
	cs, _ := p.create(t, sub(alice, sched, "name"))
	e.clock.Advance(time.Hour)
	info, err := p.renderInfo(context.Background(), uuid.MustParse(cs.ID))
	require.NoError(t, err)
	assert.JSONEq(t, `{"ReminderMinutes":[5,10],"FullSchedule":false}`, string(info.Config))
	info.Config = nil
	assert.Equal(t, renderInfo{
		Now: e.start.Add(time.Hour), ScheduleID: uuid.MustParse(sched), ScheduleName: "Sched", UserID: uuid.MustParse(alice),
	}, info)
	_, err = p.renderInfo(context.Background(), uuid.New())
	require.ErrorIs(t, err, sql.ErrNoRows)

	carol := uuid.NewString()
	names, err := p.userNameMap(context.Background(), []oncall.Shift{{UserID: alice}, {UserID: carol}, {UserID: alice}})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{alice: "Alice", carol: "Unknown User"}, names)
}
