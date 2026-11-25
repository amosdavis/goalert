package rotation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "time/tzdata" // time zones wherever the tests run, as in the app

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/limit"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util"
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// Stand-ins for other packages' tables.
type userRow struct {
	ID string `parquet:"id"`
}

type favRow struct {
	Key string `parquet:"key"`
}

type jobRow struct {
	RotationID string `parquet:"rotation_id"`
	Count      int64  `parquet:"count"`
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables
	users  *filestore.Table[userRow]
	favs   *filestore.Table[favRow]
	jobs   *filestore.Table[jobRow]
	clock  *filestore.FakeClock
	max    atomic.Int64 // participants_per_rotation
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock}
	e.max.Store(-1)
	e.tables = DefineDeltaTables(e.schema)
	e.users = filestore.NewTable(e.schema, "users", func(u userRow) string { return u.ID })
	e.favs = filestore.NewTable(e.schema, "favs", func(f favRow) string { return f.Key })
	e.jobs = filestore.NewTable(e.schema, "jobs", func(j jobRow) string { return j.RotationID })
	DeclareDeltaForeignKeys(e.tables, e.users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

// pod is one GoAlert pod on the table.
type pod struct {
	*Store
	e   *deltaEnv
	fdb *filestore.DeltaDB
	db  txn.DB
}

func (e *deltaEnv) pod(t *testing.T, name string) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	isFav := func(r filestore.Reader, userID, rotationID string) bool {
		_, ok := e.favs.Get(r, userID+"/"+rotationID)
		return ok
	}
	limit := func(filestore.Reader) int { return int(e.max.Load()) }
	changed := func(tx *filestore.Tx, rotationID string) error {
		j, _ := e.jobs.Get(tx, rotationID)
		return e.jobs.Put(tx, jobRow{RotationID: rotationID, Count: j.Count + 1})
	}
	return &pod{Store: NewDeltaStore(db, e.tables, isFav, limit, changed), e: e, fdb: fdb, db: db}
}

func (p *pod) update(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	_, err := p.fdb.Update(context.Background(), fn)
	require.NoError(t, err)
}

func (p *pod) addUsers(t *testing.T, n int) []string {
	t.Helper()
	ids := make([]string, n)
	p.update(t, func(tx *filestore.Tx) error {
		for i := range ids {
			ids[i] = uuid.NewString()
			if err := p.e.users.Put(tx, userRow{ID: ids[i]}); err != nil {
				return err
			}
		}
		return nil
	})
	return ids
}

func (p *pod) addFavorite(t *testing.T, userID, rotationID string) {
	t.Helper()
	p.update(t, func(tx *filestore.Tx) error { return p.e.favs.Put(tx, favRow{Key: userID + "/" + rotationID}) })
}

// jobCount is how often the rotation was reported as changed.
func (p *pod) jobCount(rotationID string) int {
	j, _ := p.e.jobs.Get(p.fdb.View(), rotationID)
	return int(j.Count)
}

func testCtx() context.Context {
	return permission.UserContext(context.Background(), "a1b2c3d4-0000-4000-8000-000000000001", permission.RoleAdmin)
}

func (p *pod) newRotation(t *testing.T, name string, userIDs ...string) string {
	t.Helper()
	rot, err := p.CreateRotationTx(testCtx(), nil, &Rotation{Name: name, Type: TypeDaily, Start: start})
	require.NoError(t, err)
	if len(userIDs) > 0 {
		require.NoError(t, p.AddRotationUsersTx(testCtx(), nil, rot.ID, userIDs))
	}
	return rot.ID
}

// userIDs returns the rotation's users in order, and checks that the rows
// are consistent with each other.
func (p *pod) userIDs(t *testing.T, rotationID string) []string {
	t.Helper()
	p.checkRotation(t, rotationID)
	parts, err := p.FindAllParticipants(testCtx(), rotationID)
	require.NoError(t, err)
	var ids []string
	for _, part := range parts {
		ids = append(ids, part.Target.TargetID())
	}
	return ids
}

// activeUser returns the user whose shift it is, or "".
func (p *pod) activeUser(t *testing.T, rotationID string) string {
	t.Helper()
	st, err := p.State(testCtx(), rotationID)
	if errors.Is(err, ErrNoState) {
		return ""
	}
	require.NoError(t, err)
	part, err := p.FindParticipant(testCtx(), st.ParticipantID)
	require.NoError(t, err)
	return part.Target.TargetID()
}

// checkRotation checks what the triggers keep true: positions without gaps
// from 0, the participant count, and a state that agrees with the active
// participant.
func (p *pod) checkRotation(t *testing.T, rotationID string) {
	t.Helper()
	r := p.fdb.View()
	parts := p.e.tables.OrderedParticipants(r, rotationID)
	for i, part := range parts {
		require.EqualValues(t, i, part.Position, "no gap in positions")
	}
	rot, ok := p.e.tables.Rotations.Get(r, rotationID)
	require.True(t, ok)
	require.EqualValues(t, len(parts), rot.ParticipantCount)
	count, err := p.FindParticipantCount(testCtx(), rotationID)
	require.NoError(t, err)
	require.Equal(t, len(parts), count)
	st, ok := p.e.tables.State.Get(r, rotationID)
	if !ok {
		return
	}
	active, ok := p.e.tables.Participants.Get(r, st.ParticipantID)
	require.True(t, ok, "the state refers to a participant")
	require.Equal(t, rotationID, active.RotationID)
	require.Equal(t, active.Position, st.Position)
}

func clientError(t *testing.T, err error, want string) {
	t.Helper()
	got := errutil.MapDBError(err)
	require.Error(t, got)
	assert.True(t, validation.IsClientError(got), "a message for the user, not %v", got)
	assert.ErrorContains(t, got, want)
}

func TestDeltaRotations(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	chicago, err := util.LoadLocation("America/Chicago")
	require.NoError(t, err)

	rot, err := p.CreateRotationTx(ctx, nil, &Rotation{
		Name: "Primary", Description: "The first", Type: TypeWeekly, ShiftLength: 2, Start: start.In(chicago).Add(30 * time.Second),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, p.jobCount(rot.ID), "a new rotation is reported to the rotation manager (trg_track_rotation_updates)")

	got, err := p.FindRotation(ctx, rot.ID)
	require.NoError(t, err)
	assert.Equal(t, "Primary", got.Name)
	assert.Equal(t, "The first", got.Description)
	assert.Equal(t, TypeWeekly, got.Type)
	assert.Equal(t, 2, got.ShiftLength)
	assert.True(t, start.Equal(got.Start))
	assert.Equal(t, "America/Chicago", got.Start.Location().String(), "the time zone is kept by name")
	assert.False(t, got.IsUserFavorite())

	got, err = p.FindRotation(ctx, strings.ToUpper(rot.ID))
	require.NoError(t, err, "a uuid may be written in capitals, as on Postgres")
	assert.Equal(t, rot.ID, got.ID)

	_, err = p.FindRotation(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindRotationForUpdateTx(ctx, nil, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	count, err := p.FindParticipantCount(ctx, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)
	assert.Equal(t, -1, count)

	second := p.newRotation(t, "Secondary")
	p.addFavorite(t, permission.UserID(ctx), second)
	many, err := p.FindMany(ctx, []string{second, rot.ID, strings.ToUpper(second), uuid.NewString()})
	require.NoError(t, err)
	require.Len(t, many, 2, "each rotation once, like `where id = any($1)`")
	assert.Equal(t, second, many[0].ID)
	assert.True(t, many[0].IsUserFavorite())
	assert.False(t, many[1].IsUserFavorite())
	none, err := p.FindMany(ctx, []string{uuid.NewString()})
	require.NoError(t, err)
	assert.NotNil(t, none, "an empty list, not nil, as on Postgres")
	assert.Empty(t, none)

	got, err = p.FindRotation(ctx, second)
	require.NoError(t, err)
	assert.True(t, got.IsUserFavorite())
	got, err = p.FindRotationForUpdateTx(ctx, nil, second)
	require.NoError(t, err)
	assert.False(t, got.IsUserFavorite(), "not when locking for update, as on Postgres")
	got, err = p.FindRotation(permission.SystemContext(context.Background(), "test"), second)
	require.NoError(t, err)
	assert.False(t, got.IsUserFavorite(), "no favorites without a user")

	got, err = p.FindRotation(ctx, rot.ID)
	require.NoError(t, err)
	got.Name, got.Description, got.Type, got.ShiftLength = "Main", "", TypeDaily, 3
	got.Start = start.Add(time.Hour).In(time.UTC)
	require.NoError(t, p.UpdateRotationTx(ctx, nil, got))
	got, err = p.FindRotation(ctx, rot.ID)
	require.NoError(t, err)
	assert.Equal(t, "Main", got.Name)
	assert.Empty(t, got.Description)
	assert.Equal(t, TypeDaily, got.Type)
	assert.Equal(t, 3, got.ShiftLength)
	assert.True(t, start.Add(time.Hour).Equal(got.Start))
	assert.Equal(t, "Etc/UTC", got.Start.Location().String(), "the canonical name, as on Postgres")
	assert.Equal(t, 2, p.jobCount(rot.ID))

	got.ID = uuid.NewString()
	require.NoError(t, p.UpdateRotationTx(ctx, nil, got), "an UPDATE of no rows")
	_, err = p.FindRotation(ctx, got.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)

	require.NoError(t, p.DeleteManyTx(ctx, nil, []string{rot.ID, uuid.NewString()}))
	_, err = p.FindRotation(ctx, rot.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	assert.Equal(t, 2, p.jobCount(rot.ID), "nothing to report for a rotation without participants")

	fresh := e.pod(t, "b")
	got, err = fresh.FindRotation(ctx, second)
	require.NoError(t, err)
	assert.Equal(t, "Secondary", got.Name, "a new pod reads the same rotations")
}

func TestDeltaRotationConstraints(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	first := p.newRotation(t, "Primary")
	second := p.newRotation(t, "Secondary")

	for _, name := range []string{"Primary", "PRIMARY"} {
		_, err := p.CreateRotationTx(ctx, nil, &Rotation{Name: name, Type: TypeDaily, Start: start})
		clientError(t, err, "already in use")
		rot, err := p.FindRotation(ctx, second)
		require.NoError(t, err)
		rot.Name = name
		clientError(t, p.UpdateRotationTx(ctx, nil, rot), "already in use")
	}
	var uerr *filestore.UniqueError
	_, err := p.CreateRotationTx(ctx, nil, &Rotation{Name: "Primary", Type: TypeDaily, Start: start})
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "rotations_name_unique", uerr.Index)
	_, err = p.CreateRotationTx(ctx, nil, &Rotation{Name: "pRIMARY", Type: TypeDaily, Start: start})
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "rotations_name", uerr.Index)

	rot, err := p.FindRotation(ctx, first)
	require.NoError(t, err)
	rot.Name = "primary"
	require.NoError(t, p.UpdateRotationTx(ctx, nil, rot), "a rotation's own name is free to it")

	err = p.delta.create(ctx, nil, &Rotation{ID: first, Name: "Third", Type: TypeDaily, ShiftLength: 1, Start: start})
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "rotations_pkey", uerr.Index)

	// The store's validation stops these before they get here.
	var cerr *filestore.CheckError
	err = p.delta.create(ctx, nil, &Rotation{ID: uuid.NewString(), Name: "Third", Type: TypeDaily, Start: start})
	require.ErrorAs(t, err, &cerr)
	assert.Equal(t, "rotations_shift_length_check", cerr.Constraint)
	err = p.delta.update(ctx, nil, &Rotation{ID: first, Name: "Third", Type: TypeDaily, ShiftLength: -1, Start: start})
	require.ErrorAs(t, err, &cerr)
	assert.Equal(t, "rotations_shift_length_check", cerr.Constraint)

	err = p.UpdateRotationTx(ctx, nil, &Rotation{ID: first, Name: "x", Type: TypeDaily, Start: start})
	assert.True(t, validation.IsValidationError(err), "the store still validates")
	_, err = p.CreateRotationTx(context.Background(), nil, &Rotation{Name: "Third", Type: TypeDaily, Start: start})
	assert.True(t, permission.IsPermissionError(err), "and checks permissions")
}

func TestDeltaParticipants(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	u := p.addUsers(t, 3)
	rot := p.newRotation(t, "Primary")

	parts, err := p.FindAllParticipants(ctx, rot)
	require.NoError(t, err)
	assert.Nil(t, parts, "nil, as on Postgres")
	_, err = p.State(ctx, rot)
	require.ErrorIs(t, err, ErrNoState)

	e.clock.Advance(time.Hour)
	require.NoError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{u[0], strings.ToUpper(u[1])}))
	st, err := p.State(ctx, rot)
	require.NoError(t, err)
	assert.Equal(t, rot, st.RotationID)
	assert.Equal(t, 0, st.Position, "the first participant starts the rotation (trg_start_rotation_on_first_part_add)")
	assert.True(t, st.ShiftStart.Equal(start.Add(time.Hour)), "now")
	row, ok := e.tables.State.Get(p.fdb.View(), rot)
	require.True(t, ok)
	assert.EqualValues(t, 2, row.Version, "the column's default")

	e.clock.Advance(time.Hour)
	require.NoError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{u[2], u[0]}), "a user can take part more than once")
	assert.Equal(t, []string{u[0], u[1], u[2], u[0]}, p.userIDs(t, rot), "added at the end (trg_inc_rot_part_position_on_insert)")
	st2, err := p.State(ctx, rot)
	require.NoError(t, err)
	assert.Equal(t, st, st2, "adding more does not restart it")

	parts, err = p.FindAllParticipants(ctx, rot)
	require.NoError(t, err)
	part, err := p.FindParticipant(ctx, parts[2].ID)
	require.NoError(t, err)
	assert.Equal(t, parts[2], *part)
	assert.Equal(t, rot, part.RotationID)
	assert.Equal(t, 2, part.Position)
	missing := uuid.NewString()
	part, err = p.FindParticipant(ctx, missing)
	require.ErrorIs(t, err, sql.ErrNoRows)
	assert.Equal(t, &Participant{ID: missing}, part, "as the Postgres store returns")

	jobs := p.jobCount(rot)
	require.NoError(t, p.UpdateParticipantUserIDTx(ctx, nil, parts[3].ID, u[1]))
	assert.Equal(t, []string{u[0], u[1], u[2], u[1]}, p.userIDs(t, rot))
	assert.Equal(t, jobs+1, p.jobCount(rot), "trg_track_rotation_part_updates")
	require.NoError(t, p.UpdateParticipantUserIDTx(ctx, nil, missing, u[1]), "an UPDATE of no rows")

	// Foreign keys.
	clientError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{uuid.NewString()}), "user does not exist")
	clientError(t, p.UpdateParticipantUserIDTx(ctx, nil, parts[3].ID, uuid.NewString()), "user does not exist")
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, p.AddRotationUsersTx(ctx, nil, missing, []string{u[0]}), &fkErr)
	assert.Equal(t, "rotation_participants_rotation_id_fkey", fkErr.ForeignKey)
	assert.Equal(t, []string{u[0], u[1], u[2], u[1]}, p.userIDs(t, rot), "nothing changed")

	// In a transaction, the changes are seen before they are committed.
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.AddRotationUsersTx(ctx, tx, rot, []string{u[2]}); err != nil {
			return err
		}
		parts, err := p.FindAllParticipantsTx(ctx, tx, rot)
		assert.NoError(t, err)
		assert.Len(t, parts, 5)
		r, err := p.FindRotationForUpdateTx(ctx, tx, rot)
		assert.NoError(t, err)
		assert.Equal(t, "Primary", r.Name)
		assert.NoError(t, p.DeleteRotationParticipantsTx(ctx, tx, []string{parts[0].ID}))
		st, err := p.StateTx(ctx, tx, rot)
		if assert.NoError(t, err) {
			assert.Equal(t, parts[1].ID, st.ParticipantID)
		}
		return errors.New("rolled back")
	})
	require.ErrorContains(t, err, "rolled back")
	assert.Equal(t, []string{u[0], u[1], u[2], u[1]}, p.userIDs(t, rot), "a failed transaction changes nothing")
	require.ErrorIs(t, p.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		return p.AddRotationUsersTx(ctx, tx, rot, u[:1])
	}), deltatxn.ErrReadOnly)

	fresh := e.pod(t, "b")
	assert.Equal(t, []string{u[0], u[1], u[2], u[1]}, fresh.userIDs(t, rot), "a new pod reads the same participants")
	assert.Equal(t, u[0], fresh.activeUser(t, rot))
}

func TestDeltaParticipantLimit(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	e.max.Store(2)
	p := e.pod(t, "a")
	u := p.addUsers(t, 3)
	rot := p.newRotation(t, "Primary", u[0], u[1])
	other := p.newRotation(t, "Secondary", u[0])

	err := p.AddRotationUsersTx(ctx, nil, rot, []string{u[2]})
	var le errutil.LimitError
	require.ErrorAs(t, errutil.MapDBError(err), &le, "trg_enforce_rotation_participant_limit")
	assert.Equal(t, limit.ParticipantsPerRotation, le.ID())
	assert.Equal(t, 2, le.Max())
	require.Error(t, p.AddRotationUsersTx(ctx, nil, other, []string{u[1], u[2]}), "all or nothing")
	assert.Equal(t, []string{u[0]}, p.userIDs(t, other))
	assert.Equal(t, []string{u[0], u[1]}, p.userIDs(t, rot))

	e.max.Store(1)
	parts, err := p.FindAllParticipants(ctx, rot)
	require.NoError(t, err)
	require.NoError(t, p.UpdateParticipantUserIDTx(ctx, nil, parts[0].ID, u[2]), "the limit is for new participants")
	e.max.Store(-1)
	require.NoError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{u[0]}), "no limit")
}

// TestDeltaParticipantLimitRace: on Postgres, adding participants locks the
// table.
func TestDeltaParticipantLimitRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	e.max.Store(5)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	u := a.addUsers(t, 1)
	rot := a.newRotation(t, "Primary")

	var added atomic.Int32
	var wg sync.WaitGroup
	for i := range 12 {
		p := []*pod{a, b}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.AddRotationUsersTx(ctx, nil, rot, u)
			var le errutil.LimitError
			if err == nil {
				added.Add(1)
			} else {
				assert.ErrorAs(t, errutil.MapDBError(err), &le)
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 5, added.Load(), "two pods never exceed the limit together (F8)")
	fresh := e.pod(t, "c")
	assert.Len(t, fresh.userIDs(t, rot), 5)
	assert.Equal(t, 1, e.tables.State.Len(fresh.fdb.View()), "and the rotation starts once")
}

// rotationCase describes a rotation by its users' names, with * at the
// active one.
type rotationCase struct{ before, after string }

// setup makes a rotation as described and returns its participants' ids, in
// order.
func (p *pod) setup(t *testing.T, name string, users map[string]string, desc string) (rot string, partIDs []string) {
	t.Helper()
	var ids []string
	active := -1
	for i, n := range strings.Fields(desc) {
		if strings.HasSuffix(n, "*") {
			active = i
			n = strings.TrimSuffix(n, "*")
		}
		ids = append(ids, users[n])
	}
	rot = p.newRotation(t, name, ids...)
	if active >= 0 {
		require.NoError(t, p.SetActiveIndexTx(testCtx(), nil, rot, active))
	} else {
		require.NoError(t, p.DeleteStateTx(testCtx(), nil, rot))
	}
	parts, err := p.FindAllParticipants(testCtx(), rot)
	require.NoError(t, err)
	for _, part := range parts {
		partIDs = append(partIDs, part.ID)
	}
	return rot, partIDs
}

// describe describes a rotation as setup takes it.
func (p *pod) describe(t *testing.T, users map[string]string, rot string) string {
	t.Helper()
	names := make(map[string]string, len(users))
	for n, id := range users {
		names[id] = n
	}
	active := -1
	if st, err := p.State(testCtx(), rot); err == nil {
		active = st.Position
	}
	var out []string
	for i, id := range p.userIDs(t, rot) {
		n := names[id]
		if i == active {
			n += "*"
		}
		out = append(out, n)
	}
	return strings.Join(out, " ")
}

func (p *pod) namedUsers(t *testing.T) map[string]string {
	t.Helper()
	users := make(map[string]string)
	ids := p.addUsers(t, 6)
	for i, n := range []string{"U", "A", "B", "C", "D", "E"} {
		users[n] = ids[i]
	}
	return users
}

func TestDeltaDeleteParticipants(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)

	for i, c := range []struct {
		before string
		delete []int
		after  string
	}{
		// One at a time, as the triggers describe it.
		{"A* B C", []int{2}, "A* B"},
		{"A* B C", []int{1}, "A* C"},
		{"A B C*", []int{0}, "B C*"}, // the state follows the new position
		{"A B* C", []int{0}, "B* C"},
		{"A* B C", []int{0}, "B* C"}, // the next one takes over
		{"A B* C", []int{1}, "A C*"},
		{"A B C*", []int{2}, "A* B"}, // after the last, the first
		{"A*", []int{0}, ""},         // the last participant ends the rotation
		{"A B C", []int{1}, "A C"},   // without a state, there is none after
		// The last ones, as the GraphQL layer does; any order gives the same
		// result on Postgres.
		{"A B* C D E", []int{2, 3, 4}, "A B*"},
		{"A B C D* E", []int{2, 3, 4}, "A* B"},
		{"A B C D E*", []int{4, 2, 3}, "A* B"},
		{"A B C* D", []int{0, 1, 2, 3}, ""},
		// Any others: the next remaining one takes over.
		{"A B C* D E", []int{1, 2, 3}, "A E*"},
		{"A* B C", []int{0, 1}, "C*"},
		{"A B C* D E", []int{0, 2, 4}, "B D*"},
		{"A B C D E*", []int{0, 4}, "B* C D"},
	} {
		t.Run(fmt.Sprintf("%s/%v", c.before, c.delete), func(t *testing.T) {
			rot, partIDs := p.setup(t, fmt.Sprintf("Rotation %d", i), users, c.before)
			st, stErr := p.State(ctx, rot)
			jobs := p.jobCount(rot)
			var ids []string
			for _, j := range c.delete {
				ids = append(ids, partIDs[j])
			}
			require.NoError(t, p.DeleteRotationParticipantsTx(ctx, nil, append(ids, uuid.NewString())))
			assert.Equal(t, c.after, p.describe(t, users, rot))
			assert.Equal(t, jobs+1, p.jobCount(rot), "reported to the rotation manager")
			if st2, err := p.State(ctx, rot); err == nil && stErr == nil {
				assert.Equal(t, st.ShiftStart, st2.ShiftStart, "the shift's start stays, as on Postgres")
			}
		})
	}

	fresh := e.pod(t, "b")
	assert.Equal(t, p.e.tables.Participants.Len(p.fdb.View()), p.e.tables.Participants.Len(fresh.fdb.View()))
}

func TestDeltaSetActiveIndex(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)
	rot, partIDs := p.setup(t, "Primary", users, "A* B C")
	jobs := p.jobCount(rot)

	e.clock.Advance(time.Hour)
	require.NoError(t, p.SetActiveIndexTx(ctx, nil, rot, 2))
	assert.Equal(t, "A B C*", p.describe(t, users, rot))
	st, err := p.State(ctx, rot)
	require.NoError(t, err)
	assert.Equal(t, partIDs[2], st.ParticipantID)
	assert.True(t, st.ShiftStart.Equal(start), "the shift's start stays, as on Postgres")
	assert.Equal(t, jobs+1, p.jobCount(rot), "trg_track_rotation_state_updates")

	for _, pos := range []int{3, -1, 1 << 40} {
		err := p.SetActiveIndexTx(ctx, nil, rot, pos)
		assert.True(t, validation.IsValidationError(err))
		assert.ErrorContains(t, err, "invalid index for rotation", "the NOT NULL that Postgres reports")
	}
	assert.Equal(t, "A B C*", p.describe(t, users, rot), "nothing changed")

	// Without a state there is nothing to update, valid index or not.
	require.NoError(t, p.DeleteStateTx(ctx, nil, rot))
	require.NoError(t, p.SetActiveIndexTx(ctx, nil, rot, 1))
	require.NoError(t, p.SetActiveIndexTx(ctx, nil, rot, 7))
	_, err = p.State(ctx, rot)
	require.ErrorIs(t, err, ErrNoState)
	require.NoError(t, p.DeleteStateTx(ctx, nil, rot), "a DELETE of no rows")
	require.NoError(t, p.SetActiveIndexTx(ctx, nil, uuid.NewString(), 0))

	// Any new participant starts it again, from the first one.
	require.NoError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{users["D"]}))
	assert.Equal(t, "A* B C D", p.describe(t, users, rot))
	st, err = p.State(ctx, rot)
	require.NoError(t, err)
	assert.True(t, st.ShiftStart.Equal(start.Add(time.Hour)))

	// Updating the rotation starts its shift anew.
	e.clock.Advance(time.Hour)
	r, err := p.FindRotation(ctx, rot)
	require.NoError(t, err)
	require.NoError(t, p.UpdateRotationTx(ctx, nil, r))
	st, err = p.State(ctx, rot)
	require.NoError(t, err)
	assert.True(t, st.ShiftStart.Equal(start.Add(2*time.Hour)))
	assert.Equal(t, "A* B C D", p.describe(t, users, rot))
}

// TestDeltaRemoveUser: whoever is active stays active when another user is
// deleted, and the next participant takes over when they are (F5).
func TestDeltaRemoveUser(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)

	var rots []string
	cases := []rotationCase{
		{"U B U A* C", "B A* C"},
		{"U* B", "B*"},
		{"B U*", "B*"},
		{"B U U*", "B*"},
		{"B U* U", "B*"},
		{"U*", ""},
		{"U U* U", ""},
		{"A U* B", "A B*"},
		{"A U* U B", "A B*"},
		{"U* U B C", "B* C"},
		{"A U B C* U D", "A B C* D"},
		{"U U U A* U", "A*"},
		{"A B* U", "A B*"},
		{"A* U B U C U", "A* B C"},
		{"A B*", "A B*"}, // not in it
		{"A U B", "A B"}, // no state: it stays without one
	}
	for i, c := range cases {
		rot, _ := p.setup(t, fmt.Sprintf("Rotation %d", i), users, c.before)
		rots = append(rots, rot)
	}
	untouched := p.jobCount(rots[14])

	// In one transaction with the user's deletion, as the user store does.
	p.update(t, func(tx *filestore.Tx) error {
		if err := p.RemoveUserDelta(tx, strings.ToUpper(users["U"])); err != nil {
			return err
		}
		_, err := e.users.Delete(tx, users["U"])
		return err
	})
	for i, c := range cases {
		assert.Equal(t, c.after, p.describe(t, users, rots[i]), "from %s", c.before)
		if st, err := p.State(ctx, rots[i]); err == nil {
			assert.True(t, st.ShiftStart.Equal(start), "the shift's start stays, as on Postgres")
		}
	}
	assert.Equal(t, untouched, p.jobCount(rots[14]), "a rotation without the user is not touched")
	assert.Empty(t, e.tables.UserRotationIDs(p.fdb.View(), users["U"]))

	p.update(t, func(tx *filestore.Tx) error { return p.RemoveUserDelta(tx, uuid.NewString()) })

	fresh := e.pod(t, "b")
	for i, c := range cases {
		assert.Equal(t, c.after, fresh.describe(t, users, rots[i]), "a new pod: from %s", c.before)
	}
}

func TestDeltaUserForeignKey(t *testing.T) {
	ctx := context.Background()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)
	rot, _ := p.setup(t, "Primary", users, "A* B C")

	// Deleting a user without taking them out first would leave a gap, since
	// a cascade runs no triggers: it is refused instead (F5, F7).
	for _, n := range []string{"A", "B"} {
		_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error {
			_, err := e.users.Delete(tx, users[n])
			return err
		})
		var fkErr *filestore.ForeignKeyError
		require.ErrorAs(t, err, &fkErr)
		assert.Equal(t, "rotation_participants_user_id_fkey", fkErr.ForeignKey)
	}
	assert.Equal(t, "A* B C", p.describe(t, users, rot))
}

func TestDeltaDeleteRotation(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)
	gone, partIDs := p.setup(t, "Primary", users, "A B* C")
	noState, _ := p.setup(t, "Secondary", users, "A B")
	kept, _ := p.setup(t, "Third", users, "B A*")
	jobs := p.jobCount(gone)

	require.NoError(t, p.DeleteManyTx(ctx, nil, []string{gone, strings.ToUpper(noState)}))
	v := p.fdb.View()
	assert.Equal(t, 1, e.tables.Rotations.Len(v))
	assert.Equal(t, 1, e.tables.State.Len(v), "ON DELETE CASCADE")
	assert.Equal(t, 2, e.tables.Participants.Len(v), "ON DELETE CASCADE")
	assert.Equal(t, "B A*", p.describe(t, users, kept))
	assert.Equal(t, jobs+1, p.jobCount(gone), "its participants' trg_track_rotation_part_updates")
	_, err := p.FindParticipant(ctx, partIDs[0])
	require.ErrorIs(t, err, sql.ErrNoRows)

	// A referenced participant can't go away under a state (NO ACTION).
	row, ok := e.tables.State.Get(v, kept)
	require.True(t, ok)
	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		_, err := e.tables.Participants.Delete(tx, row.ParticipantID)
		return err
	})
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "rotation_state_rotation_participant_id_fkey", fkErr.ForeignKey)

	fresh := e.pod(t, "b")
	assert.Equal(t, 2, e.tables.Participants.Len(fresh.fdb.View()))
}

func TestDeltaPositionRules(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)
	rot, partIDs := p.setup(t, "Primary", users, "A* B C")
	other, _ := p.setup(t, "Secondary", users, "A* B")

	var cerr *filestore.CheckError
	_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		part, _ := e.tables.Participants.Get(tx, partIDs[1])
		part.RotationID = other
		part.Position = 2
		return p.delta.updateParticipant(tx, part)
	})
	require.ErrorAs(t, err, &cerr)
	assert.ErrorContains(t, err, "must not change rotation_id of existing participant")

	var uerr *filestore.UniqueError
	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		part, _ := e.tables.Participants.Get(tx, partIDs[1])
		part.Position = 2
		return p.delta.updateParticipant(tx, part)
	})
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "rotation_participants_rotation_id_position_key", uerr.Index)

	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		part, _ := e.tables.Participants.Get(tx, partIDs[2])
		part.Position = 5
		if err := p.delta.updateParticipant(tx, part); err != nil {
			return err
		}
		return p.delta.checkNoGaps(tx, rot)
	})
	require.ErrorAs(t, err, &cerr)
	assert.ErrorContains(t, err, "must not have gap in participant positions")

	// trg_set_rot_state_pos_on_part_reorder: the state follows the active
	// participant's position.
	p.update(t, func(tx *filestore.Tx) error {
		for _, m := range []struct {
			part int
			pos  int32
		}{{0, 3}, {1, 0}, {2, 1}, {0, 2}} {
			part, _ := e.tables.Participants.Get(tx, partIDs[m.part])
			part.Position = m.pos
			if err := p.delta.updateParticipant(tx, part); err != nil {
				return err
			}
		}
		return p.delta.checkNoGaps(tx, rot)
	})
	assert.Equal(t, "B C A*", p.describe(t, users, rot))

	// A state id is used once (rotation_state_uniq_id).
	st, ok := e.tables.State.Get(p.fdb.View(), rot)
	require.True(t, ok)
	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		st2, _ := e.tables.State.Get(tx, other)
		st2.ID = st.ID
		return e.tables.State.Put(tx, st2)
	})
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "rotation_state_uniq_id", uerr.Index)

	// A gap that some other code left is found, not built on.
	p.update(t, func(tx *filestore.Tx) error {
		_, err := e.tables.Participants.Delete(tx, partIDs[1]) // position 0
		return err
	})
	err = p.UpdateParticipantUserIDTx(ctx, nil, partIDs[0], users["D"])
	require.ErrorAs(t, err, &cerr)
	assert.Equal(t, "trg_enforce_rot_part_position_no_gaps", cerr.Constraint)
}

// TestDeltaRotationOps runs random operations and checks after each that
// positions, count and state agree, and that whoever is active stays active
// unless the operation says otherwise (F5).
func TestDeltaRotationOps(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.addUsers(t, 5)
	rot := p.newRotation(t, "Primary")
	rnd := rand.New(rand.NewPCG(1, 2))
	for i := range 300 {
		parts, err := p.FindAllParticipants(ctx, rot)
		require.NoError(t, err)
		before, stErr := p.State(ctx, rot)
		switch op := rnd.IntN(10); {
		case op < 5 || len(parts) == 0:
			require.NoError(t, p.AddRotationUsersTx(ctx, nil, rot, []string{users[rnd.IntN(len(users))]}))
		case op < 8:
			var ids []string
			deleted := make(map[string]bool)
			for range 1 + rnd.IntN(3) {
				id := parts[rnd.IntN(len(parts))].ID
				ids = append(ids, id)
				deleted[id] = true
			}
			require.NoError(t, p.DeleteRotationParticipantsTx(ctx, nil, ids))
			if stErr != nil {
				break
			}
			want := ""
			for j := range parts { // from the active one on, the first that stays
				part := parts[(before.Position+j)%len(parts)]
				if !deleted[part.ID] {
					want = part.ID
					break
				}
			}
			st, err := p.State(ctx, rot)
			if want == "" {
				require.ErrorIs(t, err, ErrNoState)
				break
			}
			require.NoError(t, err)
			require.Equal(t, want, st.ParticipantID, "step %d", i)
		case op < 9:
			require.NoError(t, p.SetActiveIndexTx(ctx, nil, rot, rnd.IntN(len(parts))))
		default:
			p.update(t, func(tx *filestore.Tx) error { return p.RemoveUserDelta(tx, users[rnd.IntN(len(users))]) })
		}
		ids := p.userIDs(t, rot) // checks the rows
		_, err = p.State(ctx, rot)
		require.Equal(t, len(ids) > 0, err == nil, "a rotation with participants has a state, step %d", i)
	}

	fresh := e.pod(t, "b")
	assert.Equal(t, p.userIDs(t, rot), fresh.userIDs(t, rot))
	assert.Equal(t, p.activeUser(t, rot), fresh.activeUser(t, rot))
}

// TestDeltaParticipantRace: on Postgres every change to participants locks
// the table, so positions never collide.
func TestDeltaParticipantRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	users := a.namedUsers(t)
	rot, partIDs := a.setup(t, "Primary", users, "U A* B U C D")

	var wg sync.WaitGroup
	run := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, fn())
		}()
	}
	for i := range 4 {
		p := []*pod{a, b}[i%2]
		run(func() error { return p.AddRotationUsersTx(ctx, nil, rot, []string{users["E"]}) })
	}
	run(func() error { return b.DeleteRotationParticipantsTx(ctx, nil, []string{partIDs[4]}) })
	run(func() error { return a.UpdateParticipantUserIDTx(ctx, nil, partIDs[5], users["E"]) })
	run(func() error {
		_, err := b.fdb.Update(ctx, func(tx *filestore.Tx) error {
			if err := b.RemoveUserDelta(tx, users["U"]); err != nil {
				return err
			}
			_, err := e.users.Delete(tx, users["U"])
			return err
		})
		return err
	})
	wg.Wait()
	fresh := e.pod(t, "c")
	assert.Equal(t, "A* B E E E E E", fresh.describe(t, users, rot), "no change is lost, and the same person stays on call (F4, F5)")
}

// TestDeltaDeleteUserRace: one pod deletes a user while another adds them to
// a rotation.
func TestDeltaDeleteUserRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	for i := range 10 {
		u := a.addUsers(t, 1)[0]
		rot := a.newRotation(t, fmt.Sprintf("Rotation %d", i))
		var delErr, addErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, delErr = a.fdb.Update(ctx, func(tx *filestore.Tx) error {
				if err := a.RemoveUserDelta(tx, u); err != nil {
					return err
				}
				_, err := e.users.Delete(tx, u)
				return err
			})
		}()
		go func() {
			defer wg.Done()
			addErr = b.AddRotationUsersTx(ctx, nil, rot, []string{u})
		}()
		wg.Wait()
		require.NoError(t, delErr, "the user goes either way")
		if addErr != nil {
			clientError(t, addErr, "user does not exist")
		}
		fresh := e.pod(t, "c")
		assert.Empty(t, fresh.userIDs(t, rot), "no dangling participant (F7)")
		assert.Empty(t, fresh.activeUser(t, rot))
	}
}

// TestDeltaRotationNameRace: the unique index on the name.
func TestDeltaRotationNameRace(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	pods := []*pod{e.pod(t, "a"), e.pod(t, "b")}
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := []string{"Primary", "primary"}[i/2%2]
			_, err := pods[i%2].CreateRotationTx(ctx, nil, &Rotation{Name: name, Type: TypeDaily, Start: start})
			if err == nil {
				created.Add(1)
				return
			}
			assert.ErrorContains(t, errutil.MapDBError(err), "already in use")
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, created.Load(), "one name, one rotation (F8)")
	assert.Equal(t, 1, e.tables.Rotations.Len(e.pod(t, "c").fdb.View()))
}

// TestDeltaRotationTimeZone checks that a rotation read back hands off at the
// same times over a change to and from daylight saving time.
func TestDeltaRotationTimeZone(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	for i, zone := range []string{"America/Chicago", "Australia/Lord_Howe", "Asia/Kolkata", "UTC"} {
		loc, err := util.LoadLocation(zone)
		require.NoError(t, err)
		for j, typ := range []Type{TypeHourly, TypeDaily, TypeWeekly, TypeMonthly} {
			orig := &Rotation{Name: fmt.Sprintf("Rotation %d-%d", i, j), Type: typ, ShiftLength: 2, Start: time.Date(2026, 2, 28, 9, 30, 0, 0, loc)}
			created, err := p.CreateRotationTx(ctx, nil, orig)
			require.NoError(t, err)
			got, err := p.FindRotation(ctx, created.ID)
			require.NoError(t, err)
			require.Equal(t, loc.String(), got.Start.Location().String())
			at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
			for range 250 {
				require.True(t, orig.StartTime(at).Equal(got.StartTime(at)))
				require.True(t, orig.EndTime(at).Equal(got.EndTime(at)))
				at = at.Add(31 * time.Hour)
			}
		}
	}

	// A zone that cannot be loaded is an error, as on Postgres.
	rot := p.newRotation(t, "Broken")
	p.update(t, func(tx *filestore.Tx) error {
		row, _ := e.tables.Rotations.Get(tx, rot)
		row.TimeZone = "Nowhere/Land"
		return e.tables.Rotations.Put(tx, row)
	})
	_, err := p.FindRotation(ctx, rot)
	require.Error(t, err)
	_, err = p.FindMany(ctx, []string{rot})
	require.Error(t, err)
}

func TestDeltaRotationSearch(t *testing.T) {
	ctx := testCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	me := permission.UserID(ctx)
	ids := make(map[string]string)
	for _, r := range []Rotation{
		{Name: "Web Primary", Description: "Looks after the 100% site"},
		{Name: "web-secondary"},
		{Name: "API_team on-call", Description: "Back end"},
		{Name: "Database"},
		{Name: "O'Brien team"},
	} {
		r.Type, r.Start = TypeDaily, start
		created, err := p.CreateRotationTx(ctx, nil, &r)
		require.NoError(t, err)
		ids[r.Name] = created.ID
	}
	p.addFavorite(t, me, ids["Web Primary"])
	p.addFavorite(t, me, ids["O'Brien team"])
	p.addFavorite(t, uuid.NewString(), ids["Database"])

	search := func(opts SearchOptions) []string {
		t.Helper()
		rots, err := p.Search(ctx, &opts)
		require.NoError(t, err)
		var names []string
		for _, r := range rots {
			names = append(names, r.Name)
		}
		return names
	}

	assert.Equal(t, []string{"API_team on-call", "Database", "O'Brien team", "Web Primary", "web-secondary"}, search(SearchOptions{}))
	assert.Equal(t, []string{"O'Brien team", "Web Primary", "API_team on-call", "Database", "web-secondary"},
		search(SearchOptions{FavoritesFirst: true, FavoritesUserID: strings.ToUpper(me)}))
	assert.Equal(t, []string{"O'Brien team", "Web Primary"}, search(SearchOptions{FavoritesOnly: true, FavoritesUserID: me}))
	assert.Equal(t, []string{"API_team on-call", "Database"}, search(SearchOptions{Limit: 2}))
	assert.Equal(t, []string{"API_team on-call", "O'Brien team", "web-secondary"},
		search(SearchOptions{Omit: []string{ids["Database"], strings.ToUpper(ids["Web Primary"])}}))
	assert.Equal(t, []string{"Database", "web-secondary"},
		search(SearchOptions{Only: []string{ids["web-secondary"], ids["Database"], ids["Database"], uuid.NewString()}}))

	for query, want := range map[string][]string{
		"web":        {"Web Primary", "web-secondary"},
		"WEB SEC":    {"web-secondary"},
		"we pri":     {"Web Primary"}, // words that begin with these, in order
		"pri we":     nil,             // not in this order
		"rimary":     {"Web Primary"}, // anywhere in the name
		"team":       {"API_team on-call", "O'Brien team"},
		"ap te":      {"API_team on-call"}, // "_" and "-" end words
		"on ca":      {"API_team on-call"},
		"brien":      {"O'Brien team"},
		"o'b":        {"O'Brien team"},
		"o b te":     {"O'Brien team"},
		"back END":   {"API_team on-call"}, // in the description
		"100%":       {"Web Primary"},      // not a pattern
		"1%s":        nil,
		"b_c":        nil,
		"a.*b":       nil, // neither is this: the words a, b
		"nothing":    nil,
		"-":          {"API_team on-call", "Database", "O'Brien team", "Web Primary", "web-secondary"}, // no words: any name, as on Postgres
		"database x": nil,
	} {
		assert.Equal(t, want, search(SearchOptions{Search: query}), "search %q", query)
	}

	assert.Equal(t, []string{"Web Primary", "web-secondary"}, search(SearchOptions{After: SearchCursor{Name: "o'brien TEAM"}}))
	all := SearchOptions{FavoritesFirst: true, FavoritesUserID: me, Limit: 1}
	var paged []string
	for {
		rots, err := p.Search(ctx, &all)
		require.NoError(t, err)
		if len(rots) == 0 {
			break
		}
		require.Len(t, rots, 1)
		paged = append(paged, rots[0].Name)
		all.After = SearchCursor{Name: rots[0].Name, IsFavorite: rots[0].IsUserFavorite()}
	}
	assert.Equal(t, []string{"O'Brien team", "Web Primary", "API_team on-call", "Database", "web-secondary"}, paged, "page by page")

	rots, err := p.Search(ctx, &SearchOptions{Search: "nothing"})
	require.NoError(t, err)
	assert.Nil(t, rots, "nil, as on Postgres")
	_, err = p.Search(context.Background(), nil)
	assert.True(t, permission.IsPermissionError(err))

	// The destination provider works through the same methods.
	label, err := p.FieldLabel(ctx, FieldRotationID, ids["Database"])
	require.NoError(t, err)
	assert.Equal(t, "Database", label)
}

func TestDeltaRotationLookups(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	users := p.namedUsers(t)
	first, _ := p.setup(t, "Primary", users, "A B* A")
	second, _ := p.setup(t, "Secondary", users, "A*")
	stopped, _ := p.setup(t, "Third", users, "C")

	v := p.fdb.View()
	assert.ElementsMatch(t, []string{first, second}, e.tables.UserRotationIDs(v, users["A"]), "each once")
	assert.Empty(t, e.tables.UserRotationIDs(v, users["E"]))
	active, ok := e.tables.ActiveUserID(v, first)
	assert.True(t, ok)
	assert.Equal(t, users["B"], active)
	_, ok = e.tables.ActiveUserID(v, stopped)
	assert.False(t, ok)
	_, ok = e.tables.ActiveUserID(v, uuid.NewString())
	assert.False(t, ok)
}
