package notificationchannel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification/nfydest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// provider is a destination type whose lookups count as calling an API.
type provider struct {
	info nfydest.TypeInfo
	env  *deltaEnv
}

func (p provider) ID() string { return p.info.Type }
func (p provider) TypeInfo(context.Context) (*nfydest.TypeInfo, error) {
	info := p.info
	return &info, nil
}

func (p provider) ValidateField(_ context.Context, _, value string) error {
	p.env.calls.Add(1)
	if value == "" {
		return validation.NewGenericError("required")
	}
	return nil
}

func (p provider) DisplayInfo(_ context.Context, args map[string]string) (*nfydest.DisplayInfo, error) {
	p.env.calls.Add(1)
	if err := p.env.displayErr; err != nil {
		return nil, err
	}
	return &nfydest.DisplayInfo{Text: p.env.name}, nil
}

// childRow stands in for a row of another package that refers to a channel.
type childRow struct {
	ID        string `parquet:"id"`
	ChannelID string `parquet:"channel_id"`
}

type deltaEnv struct {
	store    objstore.Store
	schema   *filestore.Schema
	tables   *DeltaTables
	children *filestore.Table[childRow]
	clock    *filestore.FakeClock
	reg      *nfydest.Registry

	calls      atomic.Int32 // to providers
	name       string       // what they call a destination
	displayErr error
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	ctx := context.Background()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock, reg: nfydest.NewRegistry(), name: "#general"}
	e.tables = DefineDeltaTables(e.schema)
	e.children = filestore.NewTable(e.schema, "children", func(c childRow) string { return c.ID })
	filestore.References(e.children, "children_channel_id_fkey", e.tables.Channels,
		func(c childRow) (string, bool) { return c.ChannelID, true }, filestore.Cascade, nil)
	require.NoError(t, filestore.CreateDelta(ctx, e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))

	add := func(info nfydest.TypeInfo, fields ...string) {
		for _, f := range fields {
			info.RequiredFields = append(info.RequiredFields, nfydest.FieldConfig{FieldID: f})
		}
		e.reg.RegisterProvider(ctx, provider{info: info, env: e})
	}
	add(nfydest.TypeInfo{Type: "builtin-slack-channel", Enabled: true}, "slack_channel_id")
	add(nfydest.TypeInfo{Type: "builtin-slack-usergroup", Enabled: true}, "slack_usergroup_id", "slack_channel_id")
	add(nfydest.TypeInfo{Type: "builtin-webhook", Enabled: true}, "webhook_url")
	add(nfydest.TypeInfo{Type: "plugin", Enabled: true}, "a")
	add(nfydest.TypeInfo{Type: "disabled"}, "a")
	return e
}

type pod struct {
	*Store
	fdb *filestore.DeltaDB
	db  txn.DB
}

// pod opens the table as one GoAlert pod.
func (e *deltaEnv) pod(t *testing.T, name string) pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		WriterID: name, Clock: e.clock, PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return pod{Store: NewDeltaStore(db, e.tables, e.reg), fdb: fdb, db: db}
}

func (p pod) update(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	_, err := p.fdb.Update(context.Background(), fn)
	require.NoError(t, err)
}

// row returns a channel's row as a newly opened pod reads it.
func (e *deltaEnv) row(t *testing.T, id uuid.UUID) Row {
	t.Helper()
	row, ok := e.tables.Channels.Get(e.pod(t, "reader").fdb.View(), id.String())
	require.True(t, ok)
	return row
}

func userCtx() context.Context {
	return permission.UserContext(context.Background(), "bcefacc0-4764-012d-7bfb-002500d5decb", permission.RoleUser)
}

func slack(id string) gadb.DestV1 {
	return gadb.NewDestV1("builtin-slack-channel", "slack_channel_id", id)
}

// constraint returns the name of the constraint that err broke.
func constraint(err error) string {
	var c interface{ ConstraintName() string }
	if errors.As(err, &c) {
		return c.ConstraintName()
	}
	return ""
}

func TestDeltaChannels(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")

	id, err := p.MapDestToID(ctx, nil, slack("C1"))
	require.NoError(t, err)
	row := e.row(t, id)
	assert.Equal(t, "#general", row.Name)
	assert.True(t, row.CreatedAt.Equal(start), "created_at DEFAULT now()")
	assert.Equal(t, "{}", string(row.Meta))

	e.clock.Set(start.Add(time.Hour))
	e.name = "#renamed"
	again, err := p.MapDestToID(ctx, nil, slack("C1"))
	require.NoError(t, err)
	assert.Equal(t, id, again, "one channel per destination")
	row = e.row(t, id)
	assert.Equal(t, "#renamed", row.Name, "the name follows")
	assert.True(t, row.CreatedAt.Equal(start), "nothing else changes")

	id2, err := p.MapDestToID(ctx, nil, slack("C2"))
	require.NoError(t, err)
	assert.NotEqual(t, id, id2)

	c, err := p.FindOne(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, &Channel{ID: id, Name: "#renamed", Dest: slack("C1")}, c)
	dest, err := p.FindDestByID(ctx, nil, id2)
	require.NoError(t, err)
	assert.Equal(t, slack("C2"), dest)
	found, err := p.LookupDestID(ctx, nil, slack("C2"))
	require.NoError(t, err)
	assert.Equal(t, id2, found)

	// A fresh pod reads the same channels.
	p2 := e.pod(t, "b")
	many, err := p2.FindMany(ctx, []string{id2.String(), uuid.NewString(), id.String(), id2.String()})
	require.NoError(t, err)
	assert.Equal(t, []Channel{{ID: id2, Name: "#renamed", Dest: slack("C2")}, {ID: id, Name: "#renamed", Dest: slack("C1")}}, many,
		"each once, like `where id = any($1)`")
	many, err = p2.FindMany(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []Channel{}, many, "an empty list, not nil, as on Postgres")

	_, err = p.FindOne(ctx, uuid.New())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindDestByID(ctx, nil, uuid.New())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.LookupDestID(ctx, nil, slack("C3"))
	require.ErrorIs(t, err, sql.ErrNoRows)

	// Deleting a channel deletes the rows that refer to it (F7).
	p.update(t, func(tx *filestore.Tx) error {
		for i, id := range []uuid.UUID{id, id2} {
			if err := e.children.Put(tx, childRow{ID: fmt.Sprint(i), ChannelID: id.String()}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, p.DeleteManyTx(ctx, nil, []string{id.String(), uuid.NewString()}))
	require.NoError(t, p.DeleteManyTx(ctx, nil, nil))
	_, err = p.FindOne(ctx, id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	view := e.pod(t, "c").fdb.View()
	assert.Equal(t, 1, e.tables.Channels.Len(view))
	assert.Equal(t, 1, e.children.Len(view))
	_, ok := e.children.Get(view, "1")
	assert.True(t, ok)

	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		return e.children.Put(tx, childRow{ID: "2", ChannelID: id.String()})
	})
	assert.Equal(t, "children_channel_id_fkey", constraint(err))

	again, err = p.MapDestToID(ctx, nil, slack("C1"))
	require.NoError(t, err)
	assert.NotEqual(t, id, again, "a new channel once the old one is deleted")
}

func TestDeltaChannelsInvalid(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")

	for _, ctx := range []context.Context{context.Background(), permission.ServiceContext(context.Background(), uuid.NewString())} {
		_, err := p.MapDestToID(ctx, nil, slack("C1"))
		assert.True(t, permission.IsPermissionError(err))
		_, err = p.ResolveDests(ctx, slack("C1"))
		assert.True(t, permission.IsPermissionError(err))
		_, err = p.LookupDestID(ctx, nil, slack("C1"))
		assert.True(t, permission.IsPermissionError(err))
		_, err = p.FindDestByID(ctx, nil, uuid.New())
		assert.True(t, permission.IsPermissionError(err))
		_, err = p.FindOne(ctx, uuid.New())
		assert.True(t, permission.IsPermissionError(err))
		_, err = p.FindMany(ctx, nil)
		assert.True(t, permission.IsPermissionError(err))
		assert.True(t, permission.IsPermissionError(p.DeleteManyTx(ctx, nil, nil)))
	}
	assert.Zero(t, e.calls.Load(), "no lookups for someone without permission")

	_, err := p.FindMany(ctx, []string{"x"})
	assert.True(t, validation.IsValidationError(err))
	assert.True(t, validation.IsValidationError(p.DeleteManyTx(ctx, nil, []string{"x"})))

	mapBoth := func(d gadb.DestV1) error {
		_, err := p.ResolveDests(ctx, d)
		require.Error(t, err)
		_, err2 := p.MapDestToID(ctx, nil, d)
		assert.EqualError(t, err2, err.Error())
		return err
	}
	require.ErrorIs(t, mapBoth(gadb.NewDestV1("nope")), nfydest.ErrUnknownType)
	require.ErrorIs(t, mapBoth(gadb.NewDestV1("disabled", "a", "1")), nfydest.ErrNotEnabled)
	var argErr *nfydest.DestArgError
	require.ErrorAs(t, mapBoth(slack("")), &argErr)
	e.displayErr = errors.New("the API is down")
	require.ErrorIs(t, mapBoth(slack("C1")), e.displayErr)
	assert.Zero(t, e.tables.Channels.Len(p.fdb.View()))
}

func TestDeltaChannelsTypeValue(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")

	// From the dest, as trg_10_nc_compat_set_type_val_on_insert does.
	for _, tc := range []struct {
		dest       gadb.DestV1
		typ, value string
	}{
		{slack("C1"), "SLACK", "C1"},
		{gadb.NewDestV1("builtin-slack-usergroup", "slack_usergroup_id", "S1", "slack_channel_id", "C1"), "SLACK_USER_GROUP", "S1:C1"},
		{gadb.NewDestV1("builtin-webhook", "webhook_url", "https://example.com"), "WEBHOOK", "https://example.com"},
	} {
		id, err := p.MapDestToID(ctx, nil, tc.dest)
		require.NoError(t, err)
		row := e.row(t, id)
		assert.Equal(t, tc.typ, row.Type)
		assert.Equal(t, tc.value, row.Value)
	}
	id, err := p.MapDestToID(ctx, nil, gadb.NewDestV1("plugin", "a", "1"))
	require.NoError(t, err)
	row := e.row(t, id)
	assert.Equal(t, "DEST", row.Type)
	_, err = uuid.Parse(row.Value)
	require.NoError(t, err, "only unique")

	insert := func(row Row) error {
		_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error { return e.tables.Insert(tx, row) })
		return err
	}

	// To the dest, as trg_10_nc_set_dest_on_insert does.
	for i, tc := range []struct {
		typ, value string
		dest       gadb.DestV1
	}{
		{"SLACK", "C2", slack("C2")},
		{"SLACK_USER_GROUP", "S2:C2", gadb.NewDestV1("builtin-slack-usergroup", "slack_usergroup_id", "S2", "slack_channel_id", "C2")},
		{"SLACK_USER_GROUP", "S3", gadb.NewDestV1("builtin-slack-usergroup", "slack_usergroup_id", "S3", "slack_channel_id", "")},
		{"WEBHOOK", "https://example.com/2", gadb.NewDestV1("builtin-webhook", "webhook_url", "https://example.com/2")},
	} {
		id := fmt.Sprint("channel", i)
		require.NoError(t, insert(Row{ID: id, Name: "x", Type: tc.typ, Value: tc.value}))
		dest, err := e.tables.Dest(p.fdb.View(), id)
		require.NoError(t, err)
		assert.Equal(t, tc.dest, dest)
		row, ok := e.tables.FindByDest(p.fdb.View(), tc.dest)
		require.True(t, ok)
		assert.Equal(t, tc.typ, row.Type)
		assert.Equal(t, tc.value, row.Value)
	}

	require.ErrorContains(t, insert(Row{ID: "x", Type: "DEST", Value: "x"}), "unknown notification channel type: DEST")
	require.ErrorContains(t, insert(Row{ID: "x", Dest: destJSON(gadb.NewDestV1("builtin-webhook"))}), "value must not be null")
	require.ErrorContains(t, insert(Row{ID: "x", Dest: destJSON(gadb.NewDestV1("builtin-slack-usergroup", "slack_usergroup_id", "S9"))}), "value must not be null")
	require.Error(t, insert(Row{ID: "x", Dest: []byte(`{`)}))

	// The dest wins over a type and value given with it, and is stored in
	// one form. A given time and meta are kept.
	require.NoError(t, insert(Row{
		ID: "x", Type: "SLACK", Value: "C9", Dest: []byte(`{"Type": "builtin-webhook", "Args": {"webhook_url": "u"}}`),
		Meta: []byte(`{"a":1}`), CreatedAt: start.Add(-time.Hour),
	}))
	row, ok := e.tables.Channels.Get(p.fdb.View(), "x")
	require.True(t, ok)
	assert.Equal(t, "WEBHOOK", row.Type)
	assert.Equal(t, "u", row.Value)
	assert.Equal(t, `{"Args":{"webhook_url":"u"},"Type":"builtin-webhook"}`, string(row.Dest))
	assert.Equal(t, `{"a":1}`, string(row.Meta))
	assert.True(t, row.CreatedAt.Equal(start.Add(-time.Hour)))
}

func TestDeltaChannelsUnique(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	id, err := p.MapDestToID(ctx, nil, slack("C1"))
	require.NoError(t, err)
	plugin := gadb.NewDestV1("plugin", "a", "1")
	_, err = p.MapDestToID(ctx, nil, plugin)
	require.NoError(t, err)

	insert := func(row Row) string {
		_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error { return e.tables.Insert(tx, row) })
		require.Error(t, err)
		return constraint(err)
	}
	assert.Equal(t, "notification_channels_pkey", insert(Row{ID: id.String(), Dest: destJSON(slack("C2"))}))
	assert.Equal(t, "nc_unique_type_value", insert(Row{ID: "x", Dest: destJSON(slack("C1"))}))
	assert.Equal(t, "nc_unique_type_value", insert(Row{ID: "x", Type: "SLACK", Value: "C1"}))
	assert.Equal(t, "notification_channels_dest_key", insert(Row{ID: "x", Dest: destJSON(plugin)}))

	// The type and value are unique even if the dests differ, which ON
	// CONFLICT (dest) doesn't cover.
	assert.Equal(t, "nc_unique_type_value", insert(Row{
		ID: "x", Dest: destJSON(gadb.NewDestV1("builtin-slack-channel", "slack_channel_id", "C1", "more", "x")),
	}))
}

// TestDeltaChannelsNoIO checks that nothing in a transaction calls a provider
// (F3).
func TestDeltaChannelsNoIO(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	var id uuid.UUID
	mapInTx := func(ctx context.Context, d gadb.DestV1) error {
		e.calls.Store(0)
		err := p.db.Run(ctx, nil, func(tx txn.Tx) (err error) {
			id, err = p.MapDestToID(ctx, tx, d)
			if err != nil {
				return err
			}
			found, err := p.LookupDestID(ctx, tx, d)
			assert.Equal(t, id, found, "the transaction sees its own channel")
			return err
		})
		assert.Zero(t, e.calls.Load(), "a provider was called in a transaction")
		return err
	}

	require.ErrorIs(t, mapInTx(ctx, slack("C1")), ErrNotResolved)

	rctx, err := p.ResolveDests(ctx, slack("C1"), slack("C2"))
	require.NoError(t, err)
	assert.EqualValues(t, 4, e.calls.Load(), "a validation and a name each")
	require.NoError(t, mapInTx(rctx, slack("C1")))
	assert.Equal(t, "#general", e.row(t, id).Name)
	require.NoError(t, mapInTx(rctx, slack("C2")))
	require.ErrorIs(t, mapInTx(rctx, slack("C3")), ErrNotResolved, "another destination than the resolved ones")
	require.ErrorIs(t, mapInTx(ctx, slack("C1")), ErrNotResolved, "an existing channel too, so a caller that doesn't resolve is found out")

	// Resolving more keeps what was resolved, and leaves the old context
	// alone.
	e.calls.Store(0)
	rctx2, err := p.ResolveDests(rctx, slack("C1"), slack("C3"))
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.calls.Load(), "only C3 is looked up")
	require.NoError(t, mapInTx(rctx2, slack("C1")))
	require.NoError(t, mapInTx(rctx2, slack("C3")))
	require.ErrorIs(t, mapInTx(rctx, slack("C3")), ErrNotResolved)

	// Out of a transaction, MapDestToID looks up before its own.
	e.calls.Store(0)
	_, err = p.MapDestToID(ctx, nil, slack("C4"))
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.calls.Load())
}

func TestDeltaChannelsTx(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	rctx, err := p.ResolveDests(ctx, slack("C1"))
	require.NoError(t, err)

	errUndo := errors.New("undo")
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if _, err := p.MapDestToID(rctx, tx, slack("C1")); err != nil {
			return err
		}
		return errUndo
	})
	require.ErrorIs(t, err, errUndo)
	assert.Zero(t, e.tables.Channels.Len(p.fdb.View()), "undone with the caller's transaction")

	id, err := p.MapDestToID(ctx, nil, slack("C1"))
	require.NoError(t, err)
	err = p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.DeleteManyTx(ctx, tx, []string{id.String()}); err != nil {
			return err
		}
		_, err := p.FindDestByID(ctx, tx, id)
		assert.ErrorIs(t, err, sql.ErrNoRows)
		return errUndo
	})
	require.ErrorIs(t, err, errUndo)
	_, err = p.FindOne(ctx, id)
	require.NoError(t, err)

	err = p.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, err := p.MapDestToID(rctx, tx, slack("C1"))
		return err
	})
	require.ErrorIs(t, err, deltatxn.ErrReadOnly)
}

// TestDeltaChannelsRace has two pods map new destinations at once. Every
// caller gets the same channel for a destination (F4), as the upsert on
// Postgres gives.
func TestDeltaChannelsRace(t *testing.T) {
	ctx := userCtx()
	e := newDeltaEnv(t)
	pods := []pod{e.pod(t, "a"), e.pod(t, "b")}

	const dests, callers = 4, 4
	var ids [dests][callers]uuid.UUID
	var wg sync.WaitGroup
	for i := range dests {
		dest := slack(fmt.Sprint("C", i))
		if i%2 == 1 {
			dest = gadb.NewDestV1("plugin", "a", fmt.Sprint(i))
		}
		for j := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, err := pods[j%2].MapDestToID(ctx, nil, dest)
				assert.NoError(t, err)
				ids[i][j] = id
			}()
		}
	}
	wg.Wait()

	view := e.pod(t, "c").fdb.View()
	assert.Equal(t, dests, e.tables.Channels.Len(view))
	for i := range dests {
		for j := range callers {
			assert.Equal(t, ids[i][0], ids[i][j])
		}
		_, ok := e.tables.Channels.Get(view, ids[i][0].String())
		assert.True(t, ok)
	}
}

func TestDeltaChannelDuplicates(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	insert := func(oldID string, id int64) error {
		_, err := p.fdb.Update(context.Background(), func(tx *filestore.Tx) error {
			return e.tables.Duplicates.Insert(tx, DuplicateRow{ID: id, OldID: oldID, NewID: "new", OldCreatedAt: tx.Now()}, "notification_channel_duplicates_pkey")
		})
		return err
	}
	require.NoError(t, insert("a", 1))
	require.NoError(t, insert("b", 2))
	assert.Equal(t, "notification_channel_duplicates_pkey", constraint(insert("a", 3)))
	assert.Equal(t, "notification_channel_duplicates_id_key", constraint(insert("c", 2)))
	assert.Equal(t, 2, e.tables.Duplicates.Len(e.pod(t, "b").fdb.View()))
}
