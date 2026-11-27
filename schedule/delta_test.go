package schedule

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata" // like cmd/goalert; not every machine has zone data

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/util/timeutil"
)

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	favs   map[string]map[string]bool
	users  map[string]bool
	byUser map[string][]string
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	e := &deltaEnv{
		store:  objstore.NewMem(),
		schema: filestore.NewSchema(),
		favs:   make(map[string]map[string]bool),
		users:  make(map[string]bool),
		byUser: make(map[string][]string),
	}
	e.tables = DefineDeltaTables(e.schema)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

// pod opens the shared in-memory table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) (*Store, txn.DB) {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	lookups := DeltaLookups{
		IsFavorite: func(_ filestore.Reader, userID, scheduleID string) bool {
			return e.favs[userID][scheduleID]
		},
		UserExists: func(_ filestore.Reader, userID string) bool {
			return e.users[userID]
		},
		SchedulesOfUser: func(_ filestore.Reader, userID string) []string {
			return e.byUser[userID]
		},
	}
	return NewDeltaStore(db, e.tables, lookups), db
}

func createDeltaSchedule(t *testing.T, s *Store, name string) *Schedule {
	t.Helper()
	sched, err := s.Create(permission.SystemContext(context.Background(), "test"), &Schedule{
		Name: name, Description: "description for " + name, TimeZone: time.UTC,
	})
	require.NoError(t, err)
	return sched
}

func requireConstraint(t *testing.T, err error, name string) {
	t.Helper()
	var ce interface{ ConstraintName() string }
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, name, ce.ConstraintName())
}

func scheduleIDs(schedules []Schedule) []string {
	ids := make([]string, len(schedules))
	for i, sched := range schedules {
		ids[i] = sched.ID
	}
	return ids
}

func TestDeltaScheduleCRUD(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	alice := uuid.NewString()
	asAlice := permission.UserContext(context.Background(), alice, permission.RoleUser)

	created := createDeltaSchedule(t, s, "Alpha")
	assert.NotEqual(t, uuid.Nil, uuid.MustParse(created.ID))
	assert.Equal(t, "description for Alpha", created.Description)
	assert.Equal(t, "UTC", created.TimeZone.String())
	other := createDeltaSchedule(t, s, "Beta")

	_, err := s.Create(ctx, &Schedule{Name: "Alpha", Description: "duplicate", TimeZone: time.UTC})
	requireConstraint(t, err, "schedules_name_key")
	_, err = s.Create(ctx, &Schedule{Name: "alpha", Description: "duplicate", TimeZone: time.UTC})
	requireConstraint(t, err, "schedules_name")

	_, err = s.FindOne(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	e.favs[alice] = map[string]bool{created.ID: true}
	one, err := s.FindOne(asAlice, created.ID)
	require.NoError(t, err)
	assert.True(t, one.IsUserFavorite())
	one, err = s.FindOne(ctx, created.ID)
	require.NoError(t, err)
	assert.False(t, one.IsUserFavorite(), "favorites belong to the context user")

	many, err := s.FindMany(asAlice, []string{other.ID, created.ID, created.ID, uuid.NewString()})
	require.NoError(t, err)
	require.Len(t, many, 2, "duplicates and missing ids behave like id = any(...)")
	assert.Equal(t, other.ID, many[0].ID)
	assert.Equal(t, created.ID, many[1].ID)
	assert.False(t, many[0].IsUserFavorite())
	assert.True(t, many[1].IsUserFavorite())
	empty, err := s.FindMany(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.NotNil(t, empty, "FindMany returns an empty, non-nil slice")
	assert.Empty(t, empty)

	all, err := s.FindAll(asAlice)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{created.ID, other.ID}, scheduleIDs(all))
	for _, sched := range all {
		assert.False(t, sched.IsUserFavorite(), "FindAll does not join favorites")
		assert.NotNil(t, sched.TimeZone)
	}

	renamed := *other
	renamed.Name = "ALPHA"
	err = s.Update(ctx, &renamed)
	requireConstraint(t, err, "schedules_name")
	assert.ErrorContains(t, errutil.MapDBError(err), "already in use", "users get the same message as on Postgres")
	requireConstraint(t, s.delta.create(ctx, nil, uuid.MustParse(created.ID), &Schedule{Name: "Same ID", TimeZone: time.UTC}), "schedules_pkey")

	updated := *created
	updated.Name = "Alpha Updated"
	updated.Description = "new description"
	updated.TimeZone, err = time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	require.NoError(t, s.Update(ctx, &updated))
	one, err = s.FindOne(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, updated.Name, one.Name)
	assert.Equal(t, updated.Description, one.Description)
	assert.Equal(t, updated.TimeZone.String(), one.TimeZone.String())

	missing := &Schedule{ID: uuid.NewString(), Name: "Missing", Description: "none", TimeZone: time.UTC}
	assert.NoError(t, s.Update(ctx, missing), "an UPDATE of no rows is not an error")

	var txID string
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		made, err := s.CreateScheduleTx(ctx, tx, &Schedule{Name: "Transaction", Description: "tx", TimeZone: time.UTC})
		require.NoError(t, err)
		txID = made.ID
		many, err := s.FindManyTx(ctx, tx, []string{txID})
		require.NoError(t, err)
		require.Len(t, many, 1, "a transaction sees its own insert")

		made.Name = "Transaction Updated"
		require.NoError(t, s.UpdateTx(ctx, tx, made))
		locked, err := s.FindOneForUpdate(asAlice, tx, txID)
		require.NoError(t, err)
		assert.Equal(t, made.Name, locked.Name)
		assert.False(t, locked.IsUserFavorite(), "FOR UPDATE does not join favorites")
		return s.DeleteTx(ctx, tx, txID)
	}))
	_, err = s.FindOne(ctx, txID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	assert.NoError(t, s.DeleteManyTx(ctx, nil, nil))
	assert.NoError(t, s.DeleteManyTx(ctx, nil, []string{uuid.NewString()}))
}

func TestDeltaScheduleFindManyByUser(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	a, b := createDeltaSchedule(t, s, "Schedule A"), createDeltaSchedule(t, s, "Schedule B")
	userID := uuid.New()
	e.byUser[userID.String()] = []string{b.ID, a.ID, a.ID, uuid.NewString()}

	got, err := s.FindManyByUserID(ctx, nil, uuid.NullUUID{UUID: userID, Valid: true})
	require.NoError(t, err)
	expected := []string{a.ID, b.ID}
	slices.Sort(expected)
	assert.Equal(t, expected, scheduleIDs(got))
	for _, sched := range got {
		assert.Nil(t, sched.TimeZone, "like the Postgres store, this method leaves out the time zone")
	}

	got, err = s.FindManyByUserID(ctx, nil, uuid.NullUUID{})
	require.NoError(t, err)
	assert.Nil(t, got, "a NULL user id matches no schedules")

	bare := NewDeltaStore(db, e.tables, DeltaLookups{})
	_, err = bare.FindManyByUserID(ctx, nil, uuid.NullUUID{UUID: userID, Valid: true})
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted)
	got, err = bare.FindManyByUserID(ctx, nil, uuid.NullUUID{})
	require.NoError(t, err, "a null user needs no cross-package lookup")
	assert.Nil(t, got)
	_, err = bare.TemporarySchedules(ctx, nil, uuid.MustParse(a.ID))
	assert.ErrorIs(t, err, deltatxn.ErrNotPorted)
	_, err = bare.Search(ctx, nil)
	assert.NoError(t, err, "favorites are optional and default to false")
}

func TestDeltaScheduleSearch(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, _ := e.pod(t, "a")
	alpha := createDeltaSchedule(t, s, "Alpha")
	beta := createDeltaSchedule(t, s, "Beta")
	delta := createDeltaSchedule(t, s, "Delta")
	dev := createDeltaSchedule(t, s, "Dev US-East")
	favUser := uuid.NewString()
	e.favs[favUser] = map[string]bool{alpha.ID: true, dev.ID: true}

	got, err := s.Search(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{alpha.ID, beta.ID, delta.ID, dev.ID}, scheduleIDs(got), "case-insensitive name order")

	got, err = s.Search(ctx, &SearchOptions{Search: "dev us"})
	require.NoError(t, err)
	assert.Equal(t, []string{dev.ID}, scheduleIDs(got), "ordered word prefixes match with '-' as a separator")
	got, err = s.Search(ctx, &SearchOptions{Search: "SCRIPTION FOR DEL"})
	require.NoError(t, err)
	assert.Equal(t, []string{delta.ID}, scheduleIDs(got), "description matching is case-insensitive")
	got, err = s.Search(ctx, &SearchOptions{Search: "no such schedule"})
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = s.Search(ctx, &SearchOptions{
		Only: []string{strings.ToUpper(delta.ID), alpha.ID, alpha.ID}, Omit: []string{strings.ToUpper(alpha.ID)},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{delta.ID}, scheduleIDs(got), "Only and Omit accept UUIDs in Postgres's canonical forms")

	opts := &SearchOptions{FavoritesUserID: favUser, FavoritesFirst: true, Limit: 2}
	got, err = s.Search(ctx, opts)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{alpha.ID, dev.ID}, scheduleIDs(got))
	assert.True(t, got[0].IsUserFavorite())
	assert.True(t, got[1].IsUserFavorite())

	got, err = s.Search(ctx, &SearchOptions{
		FavoritesUserID: favUser, FavoritesFirst: true, Limit: 1,
		After: SearchCursor{Name: dev.Name, IsFavorite: true},
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, beta.ID, got[0].ID, "all non-favorites follow the last favorite, regardless of name")
	assert.False(t, got[0].IsUserFavorite())

	got, err = s.Search(ctx, &SearchOptions{
		FavoritesUserID: favUser, FavoritesFirst: true,
		After: SearchCursor{Name: beta.Name, IsFavorite: false},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{delta.ID}, scheduleIDs(got), "a non-favorite cursor stays in the non-favorite partition")

	got, err = s.Search(ctx, &SearchOptions{FavoritesUserID: favUser, FavoritesOnly: true})
	require.NoError(t, err)
	assert.Equal(t, []string{alpha.ID, dev.ID}, scheduleIDs(got))
}

func getDataRow(t *testing.T, db txn.DB, tables *DeltaTables, scheduleID uuid.UUID) (dataRow, bool) {
	t.Helper()
	var row dataRow
	var ok bool
	require.NoError(t, db.Run(context.Background(), &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, r := deltatxn.Unwrap(tx)
		row, ok = tables.Data.Get(r, scheduleID.String())
		return nil
	}))
	return row, ok
}

func TestDeltaScheduleDataMergeAndConstraints(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	a, b := createDeltaSchedule(t, s, "Schedule A"), createDeltaSchedule(t, s, "Schedule B")

	data, err := s.scheduleData(ctx, nil, uuid.New())
	require.NoError(t, err)
	assert.Equal(t, &Data{}, data, "a schedule without a data row has empty data")
	err = s.updateScheduleData(ctx, nil, uuid.New(), func(*Data) error { return nil })
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "schedule_data_schedule_id_fkey", fkErr.ForeignKey)
	assert.ErrorContains(t, errutil.MapDBError(err), "schedule does not exist")

	for _, sched := range []*Schedule{a, b} {
		require.NoError(t, s.updateScheduleData(ctx, nil, uuid.MustParse(sched.ID), func(*Data) error { return nil }))
	}
	rowA, ok := getDataRow(t, db, e.tables, uuid.MustParse(a.ID))
	require.True(t, ok)
	rowB, ok := getDataRow(t, db, e.tables, uuid.MustParse(b.ID))
	require.True(t, ok)
	assert.EqualValues(t, 1, rowA.ID)
	assert.EqualValues(t, 2, rowB.ID, "schedule_data uses its own sequence")
	requireConstraint(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		dup := rowB
		dup.ID = rowA.ID
		return e.tables.Data.Put(ftx, dup)
	}), "schedule_data_id_key")
	requireConstraint(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		return e.tables.Data.Insert(ftx, rowB, "schedule_data_pkey")
	}), "schedule_data_pkey")

	cleanupAt := time.Date(2026, time.November, 25, 1, 2, 3, 456000, time.UTC)
	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		ftx, _ := deltatxn.Unwrap(tx)
		rowA.Data = []byte(`{"V1":{"Future":{"keep":true}},"FutureTop":"preserved"}`)
		rowA.LastCleanupAt = &cleanupAt
		return e.tables.Data.Put(ftx, rowA)
	}))
	rules := []OnCallNotificationRule{{ChannelID: uuid.New()}}
	require.NoError(t, s.SetOnCallNotificationRules(ctx, nil, uuid.MustParse(a.ID), rules))
	rowA, ok = getDataRow(t, db, e.tables, uuid.MustParse(a.ID))
	require.True(t, ok)
	assert.EqualValues(t, 1, rowA.ID, "updating JSON does not replace the row")
	if assert.NotNil(t, rowA.LastCleanupAt) {
		assert.True(t, cleanupAt.Equal(*rowA.LastCleanupAt), "unrelated columns are preserved")
	}
	var doc map[string]any
	require.NoError(t, json.Unmarshal(rowA.Data, &doc))
	assert.Equal(t, "preserved", doc["FutureTop"])
	v1 := doc["V1"].(map[string]any)
	assert.Equal(t, true, v1["Future"].(map[string]any)["keep"], "unknown nested JSON fields survive an update")
	assert.Contains(t, v1, "OnCallNotificationRules")

	require.NoError(t, s.Delete(ctx, a.ID))
	_, ok = getDataRow(t, db, e.tables, uuid.MustParse(a.ID))
	assert.False(t, ok, "deleting a schedule cascades to schedule_data")
}

func TestDeltaOnCallNotificationRules(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, db := e.pod(t, "a")
	sched := createDeltaSchedule(t, s, "Notifications")
	scheduleID := uuid.MustParse(sched.ID)

	got, err := s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.Nil(t, got)

	clock := timeutil.NewClock(9, 30)
	weekdays := timeutil.EveryDay()
	rules := []OnCallNotificationRule{
		{ChannelID: uuid.New()},
		{ChannelID: uuid.New(), Time: &clock, WeekdayFilter: &weekdays},
	}
	require.NoError(t, s.SetOnCallNotificationRules(ctx, nil, scheduleID, rules))
	got, err = s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, sched.ID+":0", got[0].ID.String())
	assert.Equal(t, sched.ID+":1", got[1].ID.String())
	assert.Equal(t, rules[0].ChannelID, got[0].ChannelID)
	assert.Equal(t, clock.String(), got[1].Time.String())

	kept := got[1]
	require.NoError(t, s.SetOnCallNotificationRules(ctx, nil, scheduleID, []OnCallNotificationRule{kept, {ChannelID: uuid.New()}}))
	got, err = s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, kept.ID.String(), got[0].ID.String(), "existing rule IDs stay stable")
	assert.Equal(t, sched.ID+":0", got[1].ID.String(), "a new rule takes the first free ID")

	duplicate := got[0].ChannelID
	err = s.SetOnCallNotificationRules(ctx, nil, scheduleID, []OnCallNotificationRule{{ChannelID: duplicate}, {ChannelID: duplicate}})
	assert.Error(t, err)
	unchanged, readErr := s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, readErr)
	assert.Equal(t, got, unchanged, "a rejected update changes nothing")

	require.NoError(t, db.Run(ctx, nil, func(tx txn.Tx) error {
		require.NoError(t, s.SetOnCallNotificationRules(ctx, tx, scheduleID, []OnCallNotificationRule{{ChannelID: uuid.New()}}))
		inTx, err := s.OnCallNotificationRules(ctx, tx, scheduleID)
		require.NoError(t, err)
		assert.Len(t, inTx, 1, "a transaction sees its own schedule-data update")
		return nil
	}))

	require.NoError(t, s.SetOnCallNotificationRules(ctx, nil, scheduleID, []OnCallNotificationRule{}))
	got, err = s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
	require.NoError(t, s.SetOnCallNotificationRules(ctx, nil, scheduleID, nil))
	got, err = s.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDeltaTemporarySchedules(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	s, _ := e.pod(t, "a")
	sched := createDeltaSchedule(t, s, "Temporary")
	scheduleID := uuid.MustParse(sched.ID)
	alice, bob := uuid.NewString(), uuid.NewString()
	e.users[alice], e.users[bob] = true, true
	start := time.Now().Truncate(time.Minute).Add(time.Hour)

	tempA := TemporarySchedule{Start: start, End: start.Add(2 * time.Hour), Shifts: []FixedShift{
		{Start: start, End: start.Add(2 * time.Hour), UserID: alice},
		{Start: start, End: start.Add(2 * time.Hour), UserID: bob},
	}}
	require.NoError(t, s.SetTemporarySchedule(ctx, nil, scheduleID, tempA))
	tempB := TemporarySchedule{Start: start.Add(3 * time.Hour), End: start.Add(4 * time.Hour), Shifts: []FixedShift{
		{Start: start.Add(3 * time.Hour), End: start.Add(4 * time.Hour), UserID: bob},
	}}
	require.NoError(t, s.SetTemporarySchedule(ctx, nil, scheduleID, tempB))
	got, err := s.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Len(t, got[0].Shifts, 2)
	assert.Len(t, got[1].Shifts, 1)

	delete(e.users, bob)
	got, err = s.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Len(t, got[0].Shifts, 1)
	assert.Equal(t, alice, got[0].Shifts[0].UserID)
	assert.Empty(t, got[1].Shifts, "shifts for deleted users are omitted")
	e.users[bob] = true

	tempC := TemporarySchedule{Start: start.Add(6 * time.Hour), End: start.Add(7 * time.Hour), Shifts: []FixedShift{
		{Start: start.Add(6 * time.Hour), End: start.Add(7 * time.Hour), UserID: alice},
	}}
	require.NoError(t, s.SetClearTemporarySchedule(ctx, nil, scheduleID, tempC, start.Add(30*time.Minute), start.Add(60*time.Minute)))
	got, err = s.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.Len(t, got, 4, "SetClear splits the old range and adds the new one")

	require.NoError(t, s.ClearTemporarySchedules(ctx, nil, scheduleID, start, start.Add(8*time.Hour)))
	got, err = s.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestDeltaScheduleCreateNameRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	b, _ := e.pod(t, "b")
	pods := []*Store{a, b}

	const attempts = 8
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].Create(ctx, &Schedule{Name: "One Name", Description: fmt.Sprintf("writer %d", i), TimeZone: time.UTC})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		requireConstraint(t, err, "schedules_name_key")
	}
	assert.Equal(t, 1, successes, "two pods cannot create duplicate names")
	fresh, _ := e.pod(t, "reader")
	all, err := fresh.FindAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 1)
}

func TestDeltaTemporaryScheduleWritersRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	sched := createDeltaSchedule(t, a, "Shared")
	b, _ := e.pod(t, "b")
	pods := []*Store{a, b}
	userID := uuid.NewString()
	e.users[userID] = true
	start := time.Now().Truncate(time.Minute).Add(24 * time.Hour)

	const each = 4
	errs := make(chan error, 2*each)
	var wg sync.WaitGroup
	for i := range 2 * each {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shiftStart := start.Add(time.Duration(3*i) * time.Hour)
			err := pods[i%2].SetTemporarySchedule(ctx, nil, uuid.MustParse(sched.ID), TemporarySchedule{
				Start: shiftStart, End: shiftStart.Add(time.Hour),
				Shifts: []FixedShift{{Start: shiftStart, End: shiftStart.Add(time.Hour), UserID: userID}},
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	fresh, _ := e.pod(t, "reader")
	got, err := fresh.TemporarySchedules(ctx, nil, uuid.MustParse(sched.ID))
	require.NoError(t, err)
	assert.Len(t, got, 2*each, "no temporary schedule update is lost when two pods race")
	for i := 1; i < len(got); i++ {
		assert.True(t, got[i-1].Start.Before(got[i].Start), "merged schedules remain sorted and separate")
	}
}

// Different kinds of writers change the same document, and the first of them
// adds the row: Postgres has a row lock and a primary key for that.
func TestDeltaScheduleDataMixedWritersRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	sched := createDeltaSchedule(t, a, "Shared")
	b, _ := e.pod(t, "b")
	scheduleID := uuid.MustParse(sched.ID)
	userID, channel := uuid.NewString(), uuid.New()
	e.users[userID] = true
	start := time.Now().Truncate(time.Minute).Add(24 * time.Hour)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, a.SetOnCallNotificationRules(ctx, nil, scheduleID, []OnCallNotificationRule{{ChannelID: channel}}))
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, b.SetTemporarySchedule(ctx, nil, scheduleID, TemporarySchedule{
			Start: start, End: start.Add(time.Hour),
			Shifts: []FixedShift{{Start: start, End: start.Add(time.Hour), UserID: userID}},
		}))
	}()
	wg.Wait()

	fresh, db := e.pod(t, "reader")
	rules, err := fresh.OnCallNotificationRules(ctx, nil, scheduleID)
	require.NoError(t, err)
	if assert.Len(t, rules, 1, "the notification rules are kept (F4)") {
		assert.Equal(t, channel, rules[0].ChannelID)
	}
	temps, err := fresh.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	assert.Len(t, temps, 1, "the temporary schedule is kept (F4)")
	row, ok := getDataRow(t, db, e.tables, scheduleID)
	require.True(t, ok)
	assert.EqualValues(t, 1, row.ID, "only one of the writers added the row")
}

// A transaction that loses to another pod's commit runs again, on the other
// pod's changes, and not on what its first run left behind.
func TestDeltaTemporaryScheduleRerun(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, dbA := e.pod(t, "a")
	sched := createDeltaSchedule(t, a, "Shared")
	scheduleID := uuid.MustParse(sched.ID)
	alice, bob := uuid.NewString(), uuid.NewString()
	e.users[alice], e.users[bob] = true, true
	start := time.Now().Truncate(time.Minute).Add(24 * time.Hour)
	at := func(h int) time.Time { return start.Add(time.Duration(h) * time.Hour) }

	// A schedule right after the new one, which the first run merges it with.
	require.NoError(t, a.SetTemporarySchedule(ctx, nil, scheduleID, TemporarySchedule{
		Start: at(2), End: at(4), Shifts: []FixedShift{{Start: at(2), End: at(4), UserID: alice}},
	}))
	b, _ := e.pod(t, "b")

	// Alice's two shifts merge into one, and there is room to merge in place.
	shifts := append(make([]FixedShift, 0, 10),
		FixedShift{Start: at(0), End: at(1), UserID: alice},
		FixedShift{Start: at(1), End: at(2), UserID: alice},
		FixedShift{Start: at(0), End: at(2), UserID: bob},
	)
	given := slices.Clone(shifts)
	runs := 0
	require.NoError(t, dbA.Run(ctx, nil, func(tx txn.Tx) error {
		runs++
		if runs == 1 { // another pod removes the neighbor, and commits first
			if err := b.ClearTemporarySchedules(ctx, nil, scheduleID, at(2), at(4)); err != nil {
				return err
			}
		}
		return a.SetTemporarySchedule(ctx, tx, scheduleID, TemporarySchedule{Start: at(0), End: at(2), Shifts: shifts})
	}))
	assert.Equal(t, 2, runs, "the transaction ran again")
	assert.Equal(t, given, shifts, "the caller's shifts are left alone")

	fresh, _ := e.pod(t, "reader")
	got, err := fresh.TemporarySchedules(ctx, nil, scheduleID)
	require.NoError(t, err)
	require.Len(t, got, 1, "the neighbor is gone")
	assert.True(t, got[0].Start.Equal(at(0)))
	assert.True(t, got[0].End.Equal(at(2)))
	require.Len(t, got[0].Shifts, 2)
	for _, shift := range got[0].Shifts {
		assert.True(t, shift.Start.Equal(at(0)), "%+v", shift)
		assert.True(t, shift.End.Equal(at(2)), "%+v", shift)
	}
}

// A pod adding data to a schedule that another pod just deleted is refused.
func TestDeltaScheduleDeleteRace(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	sched := createDeltaSchedule(t, a, "Shared")
	b, _ := e.pod(t, "b") // sees the schedule, and doesn't follow the log
	require.NoError(t, a.Delete(ctx, sched.ID))

	err := b.SetOnCallNotificationRules(ctx, nil, uuid.MustParse(sched.ID), []OnCallNotificationRule{{ChannelID: uuid.New()}})
	requireConstraint(t, err, "schedule_data_schedule_id_fkey")
	assert.NoError(t, b.Update(ctx, sched), "an UPDATE of no rows")

	fresh, db := e.pod(t, "reader")
	_, ok := getDataRow(t, db, e.tables, uuid.MustParse(sched.ID))
	assert.False(t, ok)
	_, err = fresh.FindOne(ctx, sched.ID)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

// A row decodes to what it was when it was written (F25), time zone and
// unset columns included.
func TestDeltaScheduleFreshPod(t *testing.T) {
	ctx := permission.SystemContext(context.Background(), "test")
	e := newDeltaEnv(t)
	a, _ := e.pod(t, "a")
	loc, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	made, err := a.Create(ctx, &Schedule{Name: "Fresh", Description: "desc", TimeZone: loc})
	require.NoError(t, err)
	require.NoError(t, a.SetOnCallNotificationRules(ctx, nil, uuid.MustParse(made.ID), []OnCallNotificationRule{{ChannelID: uuid.New()}}))
	want, err := a.OnCallNotificationRules(ctx, nil, uuid.MustParse(made.ID))
	require.NoError(t, err)

	fresh, _ := e.pod(t, "reader")
	got, err := fresh.FindOne(ctx, made.ID)
	require.NoError(t, err)
	assert.Equal(t, "Fresh", got.Name)
	assert.Equal(t, "America/Chicago", got.TimeZone.String())
	rules, err := fresh.OnCallNotificationRules(ctx, nil, uuid.MustParse(made.ID))
	require.NoError(t, err)
	assert.Equal(t, want, rules)
}

func TestDeltaSearchMatch(t *testing.T) {
	for _, c := range []struct {
		search, name string
		want         bool
	}{
		{"", "Anything", true},
		{"pri on", "Primary On-Call", true},
		{"on pri", "Primary On-Call", false}, // in order only
		{"call", "Primary On-Call", true},    // "-" separates words
		{"team b", "Team_Blue", true},        // and so does "_"
		{"rim", "Primary", true},             // contained
		{"rim on", "Primary On-Call", false}, // neither a prefix nor contained
		{"on on", "On-Call", false},          // a word is used once
		{"o'b", "O'Brien", true},
		{"--", "Primary", true}, // no words: `\m` matches any name
	} {
		got := newSearchMatch(c.search).schedule(scheduleRow{Name: c.name, Description: "zzz"})
		assert.Equal(t, c.want, got, "%q in %q", c.search, c.name)
	}
	assert.True(t, newSearchMatch("50%_OFF").schedule(scheduleRow{Name: "x1", Description: "a 50%_off day"}), "no wildcards")
	assert.False(t, newSearchMatch("a%y").schedule(scheduleRow{Name: "x1", Description: "any"}), "no wildcards")
}
