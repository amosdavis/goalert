package contactmethod

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// provider is a destination type whose validation counts as calling an API.
type provider struct {
	info  nfydest.TypeInfo
	calls *atomic.Int32
}

func (p provider) ID() string { return p.info.Type }
func (p provider) TypeInfo(context.Context) (*nfydest.TypeInfo, error) {
	info := p.info
	return &info, nil
}

func (p provider) ValidateField(_ context.Context, _, value string) error {
	p.calls.Add(1)
	if value == "" {
		return validation.NewGenericError("required")
	}
	return nil
}

func (p provider) DisplayInfo(context.Context, map[string]string) (*nfydest.DisplayInfo, error) {
	p.calls.Add(1)
	return &nfydest.DisplayInfo{}, nil
}

// userRow stands in for the users table.
type userRow struct {
	ID string `parquet:"id"`
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	users  *filestore.Table[userRow]
	tables *DeltaTables
	clock  *filestore.FakeClock
	reg    *nfydest.Registry
	calls  atomic.Int32 // to providers
	max    int          // contact_methods_per_user
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	ctx := context.Background()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock, reg: nfydest.NewRegistry(), max: -1}
	e.users = filestore.NewTable(e.schema, "users", func(u userRow) string { return u.ID })
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.users)
	require.NoError(t, filestore.CreateDelta(ctx, e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))

	add := func(info nfydest.TypeInfo, fields ...string) {
		info.SupportsAlertNotifications = true
		info.SupportsUserVerification = true
		for _, f := range fields {
			info.RequiredFields = append(info.RequiredFields, nfydest.FieldConfig{FieldID: f})
		}
		e.reg.RegisterProvider(ctx, provider{info: info, calls: &e.calls})
	}
	add(nfydest.TypeInfo{Type: "builtin-twilio-sms", Enabled: true, SupportsStatusUpdates: true}, "phone_number")
	add(nfydest.TypeInfo{Type: "builtin-twilio-voice", Enabled: true}, "phone_number")
	add(nfydest.TypeInfo{Type: "builtin-smtp-email", Enabled: true}, "email_address")
	add(nfydest.TypeInfo{Type: "builtin-webhook", Enabled: true}, "webhook_url")
	add(nfydest.TypeInfo{Type: "builtin-slack-dm", Enabled: true, SupportsStatusUpdates: true, StatusUpdatesRequired: true}, "slack_user_id")
	add(nfydest.TypeInfo{Type: "plugin", Enabled: true}, "a", "b")
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
	return pod{Store: NewDeltaStore(db, e.tables, e.reg, func(filestore.Reader) int { return e.max }), fdb: fdb, db: db}
}

func (p pod) update(t *testing.T, fn func(tx *filestore.Tx) error) {
	t.Helper()
	_, err := p.fdb.Update(context.Background(), fn)
	require.NoError(t, err)
}

// addUser adds a user and returns their id and a context as them.
func (e *deltaEnv) addUser(t *testing.T, p pod) (string, context.Context) {
	t.Helper()
	id := uuid.NewString()
	p.update(t, func(tx *filestore.Tx) error { return e.users.Put(tx, userRow{ID: id}) })
	return id, permission.UserContext(context.Background(), id, permission.RoleUser)
}

// row returns a contact method's row as a newly opened pod reads it.
func (e *deltaEnv) row(t *testing.T, id uuid.UUID) Row {
	t.Helper()
	row, ok := e.tables.Methods.Get(e.pod(t, "reader").fdb.View(), id.String())
	require.True(t, ok)
	return row
}

func systemCtx() context.Context { return permission.SystemContext(context.Background(), "test") }

func adminCtx() context.Context {
	return permission.UserContext(context.Background(), "bcefacc0-4764-012d-7bfb-002500d5decb", permission.RoleAdmin)
}

func sms(number string) gadb.DestV1 {
	return gadb.NewDestV1("builtin-twilio-sms", "phone_number", number)
}

// constraint returns the name of the constraint that err broke.
func constraint(err error) string {
	var c interface{ ConstraintName() string }
	if errors.As(err, &c) {
		return c.ConstraintName()
	}
	return ""
}

func TestDeltaContactMethods(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)

	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+15550001"), Disabled: true, StatusUpdates: true})
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, c.ID, "a new id is made")
	id, err := uuid.Parse("00000000-0000-0000-0000-00000000000A")
	require.NoError(t, err)
	c2, err := p.Create(ctx, nil, &ContactMethod{ID: id, Name: "desk", UserID: userID, Dest: gadb.NewDestV1("builtin-twilio-voice", "phone_number", "+15550001"), Private: true, StatusUpdates: true})
	require.NoError(t, err)
	assert.Equal(t, id, c2.ID, "a given id is kept")
	assert.False(t, c2.StatusUpdates, "not supported by this type")

	got, err := p.FindOne(ctx, nil, c.ID)
	require.NoError(t, err)
	assert.Equal(t, &ContactMethod{
		ID: c.ID, Name: "cell", UserID: userID, Dest: sms("+15550001"),
		Disabled: true, StatusUpdates: true, Pending: true,
	}, got)
	dest, err := p.FindDestByID(ctx, nil, c2.ID)
	require.NoError(t, err)
	assert.Equal(t, "builtin-twilio-voice", dest.Type)

	// A fresh pod reads the same rows.
	p2 := e.pod(t, "b")
	got2, err := p2.FindOne(ctx, nil, c.ID)
	require.NoError(t, err)
	assert.Equal(t, got, got2)

	many, err := p2.FindMany(ctx, nil, []string{c2.ID.String(), uuid.NewString(), c.ID.String(), c2.ID.String()})
	require.NoError(t, err)
	assert.Len(t, many, 2, "each once, like `where id = any($1)`")
	many, err = p2.FindMany(ctx, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, many, "an empty list, not nil, as on Postgres")
	assert.Empty(t, many)

	_, err = p.FindOne(ctx, nil, uuid.New())
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = p.FindDestByID(ctx, nil, uuid.New())
	require.ErrorIs(t, err, sql.ErrNoRows)

	require.NoError(t, p.Delete(ctx, nil, c.ID.String(), uuid.NewString()))
	_, err = p.FindOne(ctx, nil, c.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = e.pod(t, "c").FindOne(ctx, nil, c.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.NoError(t, p.Delete(ctx, nil))

	// The destination can be used again once it is deleted.
	_, err = p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+15550001")})
	require.NoError(t, err)
}

func TestDeltaCreateInvalid(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	_, otherCtx := e.addUser(t, p)

	create := func(ctx context.Context, c ContactMethod) error {
		_, err := p.Create(ctx, nil, &c)
		require.Error(t, err)
		return err
	}
	assert.True(t, permission.IsPermissionError(create(otherCtx, ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})), "only for oneself")
	assert.True(t, validation.IsValidationError(create(ctx, ContactMethod{Name: "", UserID: userID, Dest: sms("+1")})))
	require.ErrorIs(t, create(ctx, ContactMethod{Name: "cell", UserID: userID, Dest: gadb.NewDestV1("nope")}), nfydest.ErrUnknownType)
	require.ErrorIs(t, create(ctx, ContactMethod{Name: "cell", UserID: userID, Dest: gadb.NewDestV1("disabled", "a", "1")}), nfydest.ErrNotEnabled)
	var argErr *nfydest.DestArgError
	require.ErrorAs(t, create(ctx, ContactMethod{Name: "cell", UserID: userID, Dest: sms("")}), &argErr)
	assert.True(t, validation.IsValidationError(create(systemCtx(), ContactMethod{Name: "cell", UserID: "x", Dest: sms("+1")})), "an invalid user id is an error")

	assert.Zero(t, e.tables.Methods.Len(p.fdb.View()))
}

func TestDeltaTypeValue(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)

	for _, tc := range []struct{ typ, destType, arg string }{
		{"SMS", "builtin-twilio-sms", "phone_number"},
		{"VOICE", "builtin-twilio-voice", "phone_number"},
		{"EMAIL", "builtin-smtp-email", "email_address"},
		{"WEBHOOK", "builtin-webhook", "webhook_url"},
		{"SLACK_DM", "builtin-slack-dm", "slack_user_id"},
	} {
		// From the dest, as trg_10_compat_set_type_val_on_insert does.
		c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: gadb.NewDestV1(tc.destType, tc.arg, "value 1")})
		require.NoError(t, err, tc.typ)
		row := e.row(t, c.ID)
		assert.Equal(t, tc.typ, row.Type)
		assert.Equal(t, "value 1", row.Value)
		assert.Equal(t, tc.typ == "SLACK_DM", row.EnableStatusUpdates, "required by Slack DMs only")

		// To the dest, as trg_10_cm_set_dest_on_insert does.
		p.update(t, func(tx *filestore.Tx) error {
			return e.tables.Insert(tx, Row{UserID: userID, Name: "cell", Type: tc.typ, Value: "value 2"}, -1)
		})
		row, ok := e.tables.FindByTypeValue(p.fdb.View(), tc.typ, "value 2")
		require.True(t, ok, tc.typ)
		_, err = uuid.Parse(row.ID)
		require.NoError(t, err, "a new id is made")
		dest, err := row.DestV1()
		require.NoError(t, err)
		assert.Equal(t, gadb.NewDestV1(tc.destType, tc.arg, "value 2"), dest)
	}

	// Other destinations get DEST and a value that is only unique.
	var values []string
	for i := range 2 {
		c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: gadb.NewDestV1("plugin", "a", "1", "b", fmt.Sprint(i))})
		require.NoError(t, err)
		row := e.row(t, c.ID)
		assert.Equal(t, "DEST", row.Type)
		_, err = uuid.Parse(row.Value)
		require.NoError(t, err)
		values = append(values, row.Value)
	}
	assert.NotEqual(t, values[0], values[1])

	insert := func(row Row) error {
		row.UserID, row.Name = userID, "cell"
		_, err := p.fdb.Update(ctx, func(tx *filestore.Tx) error { return e.tables.Insert(tx, row, -1) })
		return err
	}
	require.ErrorContains(t, insert(Row{Type: "PUSH", Value: "x"}), "unknown contact method type: PUSH")
	require.ErrorContains(t, insert(Row{Type: "DEST", Value: "x"}), "unknown contact method type: DEST")
	require.ErrorContains(t, insert(Row{Dest: []byte(`{"Type":"builtin-twilio-sms","Args":{}}`)}), "value must not be null")
	require.Error(t, insert(Row{Dest: []byte(`{`)}))

	// The dest wins over a type and value given with it, and is stored in
	// one form.
	require.NoError(t, insert(Row{ID: "cm", Type: "EMAIL", Value: "x", Dest: []byte(`{"Type": "builtin-twilio-sms", "Args": {"phone_number": "+1"}}`)}))
	row, ok := e.tables.Methods.Get(p.fdb.View(), "cm")
	require.True(t, ok)
	assert.Equal(t, "SMS", row.Type)
	assert.Equal(t, "+1", row.Value)
	assert.Equal(t, `{"Args":{"phone_number":"+1"},"Type":"builtin-twilio-sms"}`, string(row.Dest))
	require.NoError(t, insert(Row{ID: "cm2", Dest: []byte(`{"Type":"plugin"}`)}))
	row, ok = e.tables.FindByDest(p.fdb.View(), gadb.DestV1{Type: "plugin"})
	require.True(t, ok)
	assert.Equal(t, `{"Args":{},"Type":"plugin"}`, string(row.Dest), "no args are an empty object")
}

func TestDeltaUnique(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)

	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)
	_, err = p.Create(ctx, nil, &ContactMethod{ID: c.ID, Name: "cell", UserID: userID, Dest: sms("+2")})
	assert.Equal(t, "user_contact_methods_pkey", constraint(err))

	// A destination belongs to one contact method of one user.
	_, err = p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: sms("+1")})
	assert.Equal(t, "user_contact_methods_type_value_key", constraint(err), "the constraint Postgres reports")
	var fieldErr validation.FieldError
	require.ErrorAs(t, errutil.MapDBError(err), &fieldErr)
	assert.Equal(t, "Value", fieldErr.Field())
	assert.NotContains(t, err.Error(), "+1", "no phone number in the error (F20)")

	plugin := gadb.NewDestV1("plugin", "a", "1", "b", "2")
	_, err = p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: plugin})
	require.NoError(t, err)
	_, err = p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: plugin})
	assert.Equal(t, "user_contact_methods_dest_key", constraint(err))

	// The type and value are unique even if the dests differ.
	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		return e.tables.Insert(tx, Row{UserID: otherID, Name: "cell", Dest: destJSON(gadb.NewDestV1("builtin-twilio-sms", "phone_number", "+1", "more", "x"))}, -1)
	})
	assert.Equal(t, "user_contact_methods_type_value_key", constraint(err))

	// The same value with another type is another destination.
	_, err = p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: gadb.NewDestV1("builtin-twilio-voice", "phone_number", "+1")})
	require.NoError(t, err)
}

func TestDeltaLimit(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	create := func(ctx context.Context, userID, number string) error {
		_, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms(number)})
		return err
	}

	e.max = 2
	require.NoError(t, create(ctx, userID, "+1"))
	require.NoError(t, create(ctx, userID, "+2"))
	err := create(ctx, userID, "+3")
	assert.Equal(t, "contact_methods_per_user_limit", constraint(err))
	mapped := errutil.MapDBError(err)
	require.True(t, errutil.IsLimitError(mapped), "%v", mapped)
	assert.EqualError(t, mapped, "too many contact methods")
	assert.Equal(t, "user_contact_methods_type_value_key", constraint(create(ctx, userID, "+1")), "a duplicate is reported first, as on Postgres")
	require.NoError(t, create(otherCtx, otherID, "+3"), "the limit is per user")

	e.max = 0
	assert.Equal(t, "contact_methods_per_user_limit", constraint(create(otherCtx, otherID, "+4")))
	e.max = -1
	require.NoError(t, create(ctx, userID, "+4"), "no limit")

	// A row over the limit is not left behind, even by a caller that goes on.
	p.update(t, func(tx *filestore.Tx) error {
		err := e.tables.Insert(tx, Row{UserID: userID, Name: "cell", Type: "SMS", Value: "+5"}, 1)
		assert.Equal(t, "contact_methods_per_user_limit", constraint(err))
		return nil
	})
	_, ok := e.tables.FindByTypeValue(p.fdb.View(), "SMS", "+5")
	assert.False(t, ok)
}

func TestDeltaForeignKey(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)
	c2, err := p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: sms("+2")})
	require.NoError(t, err)

	p.update(t, func(tx *filestore.Tx) error {
		_, err := e.users.Delete(tx, userID)
		return err
	})
	p2 := e.pod(t, "b")
	_, err = p2.FindOne(ctx, nil, c.ID)
	require.ErrorIs(t, err, sql.ErrNoRows, "deleting a user deletes their contact methods (F7)")
	_, err = p2.FindOne(ctx, nil, c2.ID)
	require.NoError(t, err)

	_, err = p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	var fkErr *filestore.ForeignKeyError
	require.ErrorAs(t, err, &fkErr)
	assert.Equal(t, "user_contact_methods_user_id_fkey", fkErr.ForeignKey)
}

func TestDeltaUpdate(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1"), Disabled: true})
	require.NoError(t, err)

	up := *c
	up.Name, up.Private, up.StatusUpdates = "mobile", true, true
	require.NoError(t, p.Update(ctx, nil, &up))
	row := e.row(t, c.ID)
	assert.Equal(t, "mobile", row.Name)
	assert.True(t, row.Private)
	assert.True(t, row.EnableStatusUpdates)
	assert.True(t, row.Disabled)
	assert.True(t, row.Pending, "still disabled, so still pending")

	// Enabling one verifies it (trg_cm_set_not_pending_on_verify).
	up.Disabled = false
	require.NoError(t, p.Update(adminCtx(), nil, &up))
	row = e.row(t, c.ID)
	assert.False(t, row.Disabled)
	assert.False(t, row.Pending)
	up.Disabled = true
	require.NoError(t, p.Update(ctx, nil, &up))
	assert.False(t, e.row(t, c.ID).Pending, "disabling one leaves it verified")

	bad := up
	bad.Dest = sms("+2")
	require.ErrorContains(t, p.Update(ctx, nil, &bad), "cannot update destination")
	bad = up
	bad.UserID = otherID
	require.ErrorContains(t, p.Update(ctx, nil, &bad), "cannot update owner")
	bad = up
	bad.Name = ""
	assert.True(t, validation.IsValidationError(p.Update(ctx, nil, &bad)))
	bad = up
	bad.ID = uuid.New()
	require.ErrorIs(t, p.Update(ctx, nil, &bad), sql.ErrNoRows)
	bad = up
	bad.Name = "stolen"
	assert.True(t, permission.IsPermissionError(p.Update(otherCtx, nil, &bad)))
	assert.Equal(t, "mobile", e.row(t, c.ID).Name)
}

func TestDeltaUpdateNotEnabled(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	dest := gadb.NewDestV1("disabled", "a", "1")
	id := uuid.New()
	p.update(t, func(tx *filestore.Tx) error {
		return e.tables.Insert(tx, Row{ID: id.String(), UserID: userID, Name: "cell", Dest: destJSON(dest)}, -1)
	})
	err := p.Update(ctx, nil, &ContactMethod{ID: id, UserID: userID, Name: "desk", Dest: dest})
	require.ErrorIs(t, err, nfydest.ErrNotEnabled, "as on Postgres")
}

func TestDeltaEnableDisable(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1"), Disabled: true})
	require.NoError(t, err)

	assert.True(t, permission.IsPermissionError(p.EnableByDest(ctx, nil, sms("+1"))), "system only")
	require.NoError(t, p.EnableByDest(systemCtx(), nil, sms("+1")))
	row := e.row(t, c.ID)
	assert.False(t, row.Disabled)
	assert.False(t, row.Pending, "enabling one verifies it")
	require.NoError(t, p.DisableByDest(systemCtx(), nil, sms("+1")))
	row = e.row(t, c.ID)
	assert.True(t, row.Disabled)
	assert.False(t, row.Pending)
	require.ErrorIs(t, p.EnableByDest(systemCtx(), nil, sms("+2")), sql.ErrNoRows)
	require.ErrorIs(t, p.DisableByDest(systemCtx(), nil, sms("+2")), sql.ErrNoRows)
	assert.Equal(t, "cell", row.Name, "nothing else changes")

	// What the notification store's verification uses.
	p.update(t, func(tx *filestore.Tx) error {
		found, err := e.tables.SetDisabled(tx, c.ID.String(), false)
		assert.True(t, found)
		if err != nil {
			return err
		}
		found, err = e.tables.SetDisabled(tx, uuid.NewString(), false)
		assert.False(t, found)
		return err
	})
	assert.False(t, e.row(t, c.ID).Disabled)
}

func TestDeltaLastTestVerify(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)

	set := func(id uuid.UUID) (ok bool) {
		p.update(t, func(tx *filestore.Tx) (err error) {
			ok, err = e.tables.SetLastTestVerify(tx, id.String(), time.Minute)
			return err
		})
		return ok
	}
	assert.False(t, set(uuid.New()))
	assert.True(t, set(c.ID), "never sent before")
	assert.False(t, set(c.ID))
	got, err := p.FindOne(ctx, nil, c.ID)
	require.NoError(t, err)
	assert.True(t, got.LastTestVerifyAt().Equal(start), "%s", got.LastTestVerifyAt())
	assert.True(t, got.Pending, "sending a message doesn't verify")

	e.clock.Set(start.Add(time.Minute))
	assert.False(t, set(c.ID), "not more than a minute ago")
	e.clock.Set(start.Add(time.Minute + time.Second))
	assert.True(t, set(c.ID))
	assert.True(t, e.row(t, c.ID).LastTestVerifyAt.Equal(start.Add(time.Minute+time.Second)))
}

func TestDeltaMetadata(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)

	_, err = p.MetadataByDest(ctx, nil, sms("+1"))
	assert.True(t, permission.IsPermissionError(err), "admin only")
	m, err := p.MetadataByDest(adminCtx(), nil, sms("+1"))
	require.NoError(t, err)
	assert.True(t, m.FetchedAt.Equal(start))
	assert.True(t, m.CarrierV1.UpdatedAt.IsZero(), "no carrier yet")
	_, err = p.MetadataByDest(adminCtx(), nil, sms("+2"))
	require.ErrorIs(t, err, sql.ErrNoRows)

	e.clock.Set(start.Add(time.Hour))
	m = &Metadata{}
	m.CarrierV1.Name, m.CarrierV1.Type = "Example", "mobile"
	m.CarrierV1.MobileCountryCode, m.CarrierV1.MobileNetworkCode = "310", "260"
	require.NoError(t, p.SetCarrierV1MetadataByDest(adminCtx(), nil, sms("+1"), m))
	require.NoError(t, p.SetCarrierV1MetadataByDest(adminCtx(), nil, sms("+2"), m), "an UPDATE of nothing")
	assert.True(t, permission.IsPermissionError(p.SetCarrierV1MetadataByDest(ctx, nil, sms("+1"), m)))

	e.clock.Set(start.Add(2 * time.Hour))
	got, err := e.pod(t, "b").MetadataByDest(adminCtx(), nil, sms("+1"))
	require.NoError(t, err)
	assert.Equal(t, "Example", got.CarrierV1.Name)
	assert.Equal(t, "mobile", got.CarrierV1.Type)
	assert.Equal(t, "310", got.CarrierV1.MobileCountryCode)
	assert.Equal(t, "260", got.CarrierV1.MobileNetworkCode)
	assert.True(t, got.CarrierV1.UpdatedAt.Equal(start.Add(time.Hour)), "stamped when it was set")

	// Other metadata is kept, as jsonb_set does.
	p.update(t, func(tx *filestore.Tx) error {
		row, _ := e.tables.Methods.Get(tx, c.ID.String())
		row.Metadata = []byte(`{"Other":{"x":1},"CarrierV1":{"Name":"old"}}`)
		return e.tables.Methods.Put(tx, row)
	})
	require.NoError(t, p.SetCarrierV1MetadataByDest(adminCtx(), nil, sms("+1"), m))
	var meta map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(e.row(t, c.ID).Metadata, &meta))
	assert.JSONEq(t, `{"x":1}`, string(meta["Other"]))
	assert.Contains(t, string(meta["CarrierV1"]), "Example")
}

func TestDeltaDeletePermission(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	mine, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)
	theirs, err := p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: sms("+2")})
	require.NoError(t, err)

	assert.True(t, permission.IsPermissionError(p.Delete(ctx, nil, theirs.ID.String())))
	assert.True(t, permission.IsPermissionError(p.Delete(ctx, nil, mine.ID.String(), theirs.ID.String())),
		"every one of them must be the user's")
	assert.True(t, permission.IsPermissionError(p.Delete(context.Background(), nil, mine.ID.String())))
	for _, id := range []uuid.UUID{mine.ID, theirs.ID} {
		_, err = p.FindOne(ctx, nil, id)
		require.NoError(t, err, "nothing was deleted")
	}

	require.NoError(t, p.Delete(adminCtx(), nil, mine.ID.String(), theirs.ID.String()))
	assert.Zero(t, e.tables.Methods.Len(p.fdb.View()))
}

func TestDeltaFindAll(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	var want []uuid.UUID
	for i := range 3 {
		c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms(fmt.Sprint("+", i)), Private: i == 0})
		require.NoError(t, err)
		want = append(want, c.ID)
	}
	c, err := p.Create(otherCtx, nil, &ContactMethod{Name: "cell", UserID: otherID, Dest: sms("+9")})
	require.NoError(t, err)

	ids := func(cms []ContactMethod) (ids []uuid.UUID) {
		for _, c := range cms {
			ids = append(ids, c.ID)
		}
		return ids
	}
	cms, omitted, err := p.FindAll(ctx, nil, userID)
	require.NoError(t, err)
	assert.ElementsMatch(t, want, ids(cms), "their own private ones too")
	assert.Zero(t, omitted)
	cms, omitted, err = p.FindAll(otherCtx, nil, userID)
	require.NoError(t, err)
	assert.ElementsMatch(t, want[1:], ids(cms))
	assert.Equal(t, 1, omitted, "another user's private ones are left out and counted")
	cms, omitted, err = p.FindAll(systemCtx(), nil, userID)
	require.NoError(t, err)
	assert.Len(t, cms, 2)
	assert.Equal(t, 1, omitted)
	cms, omitted, err = p.FindAll(ctx, nil, uuid.NewString())
	require.NoError(t, err)
	assert.NotNil(t, cms)
	assert.Empty(t, cms)
	assert.Zero(t, omitted)

	cms, err = p.FindAllByUserIDs(otherCtx, nil, []string{userID, otherID, userID, uuid.NewString()})
	require.NoError(t, err)
	assert.ElementsMatch(t, append([]uuid.UUID{c.ID}, want...), ids(cms), "private ones too, each once")
	cms, err = p.FindAllByUserIDs(otherCtx, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, cms)
	assert.Empty(t, cms)
}

func TestDeltaLookups(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	c, err := p.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms("+1")})
	require.NoError(t, err)
	c2, err := p.Create(ctx, nil, &ContactMethod{Name: "desk", UserID: userID, Dest: gadb.NewDestV1("builtin-twilio-voice", "phone_number", "+1")})
	require.NoError(t, err)
	view := p.fdb.View()

	owner, ok := e.tables.Owner(view, c.ID.String())
	assert.True(t, ok)
	assert.Equal(t, userID, owner)
	_, ok = e.tables.Owner(view, uuid.NewString())
	assert.False(t, ok)

	dest, err := e.tables.Dest(view, c.ID.String())
	require.NoError(t, err)
	assert.Equal(t, sms("+1"), dest)
	_, err = e.tables.Dest(view, uuid.NewString())
	require.ErrorIs(t, err, sql.ErrNoRows)

	row, ok := e.tables.FindByDest(view, sms("+1"))
	assert.True(t, ok)
	assert.Equal(t, c.ID.String(), row.ID)
	_, ok = e.tables.FindByDest(view, sms("+2"))
	assert.False(t, ok)

	args := map[string]string{"phone_number": "+1"}
	rows, err := e.tables.FindByDestArgs(view, "", args)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "of any type")
	rows, err = e.tables.FindByDestArgs(view, "builtin-twilio-voice", args)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, c2.ID.String(), rows[0].ID)
	rows, err = e.tables.FindByDestArgs(view, "", map[string]string{"phone_number": "+1", "more": ""})
	require.NoError(t, err)
	assert.Empty(t, rows, "exactly these arguments")
}

// TestDeltaNoIO checks that nothing in a transaction calls a provider (F3).
func TestDeltaNoIO(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	run := func(fn func(tx txn.Tx) error) error {
		e.calls.Store(0)
		err := p.db.Run(ctx, nil, fn)
		assert.Zero(t, e.calls.Load(), "a provider was called in a transaction")
		return err
	}

	c := &ContactMethod{Name: "dm", UserID: userID, Dest: gadb.NewDestV1("builtin-slack-dm", "slack_user_id", "U1")}
	err := run(func(tx txn.Tx) error {
		_, err := p.Create(ctx, tx, c)
		return err
	})
	require.ErrorIs(t, err, ErrNotNormalized)

	c, err = c.Normalize(ctx, e.reg)
	require.NoError(t, err)
	assert.Positive(t, e.calls.Load(), "Normalize calls the provider")
	changed := *c
	changed.Dest = gadb.NewDestV1("builtin-slack-dm", "slack_user_id", "U2")
	err = run(func(tx txn.Tx) error {
		_, err := p.Create(ctx, tx, &changed)
		return err
	})
	require.ErrorIs(t, err, ErrNotNormalized, "another destination than the validated one")

	var created *ContactMethod
	require.NoError(t, run(func(tx txn.Tx) error {
		created, err = p.Create(ctx, tx, c)
		return err
	}))
	assert.Equal(t, c.ID, created.ID)

	// UpdateUserContactMethod finds and updates in one transaction.
	require.NoError(t, run(func(tx txn.Tx) error {
		c, err := p.FindOne(ctx, tx, created.ID)
		if err != nil {
			return err
		}
		c.Name = "slack"
		return p.Update(ctx, tx, c)
	}))
	assert.Equal(t, "slack", e.row(t, created.ID).Name)

	// Out of a transaction, Create normalizes before its own.
	e.calls.Store(0)
	_, err = p.Create(ctx, nil, &ContactMethod{Name: "dm", UserID: userID, Dest: gadb.NewDestV1("builtin-slack-dm", "slack_user_id", "U3")})
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.calls.Load())
}

// TestDeltaCreateRace has two pods give two users the same destination at
// once: the unique index lets one have it (F8).
func TestDeltaCreateRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []pod{e.pod(t, "a"), e.pod(t, "b")}
	users := make([]string, 8)
	for i := range users {
		users[i], _ = e.addUser(t, pods[0])
	}

	for _, dest := range []gadb.DestV1{sms("+1"), gadb.NewDestV1("plugin", "a", "1", "b", "2")} {
		var created atomic.Int32
		var wg sync.WaitGroup
		for i, userID := range users {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := pods[i%2].Create(systemCtx(), nil, &ContactMethod{Name: "cell", UserID: userID, Dest: dest})
				if err == nil {
					created.Add(1)
					return
				}
				assert.Contains(t, []string{"user_contact_methods_type_value_key", "user_contact_methods_dest_key"}, constraint(err), "%v", err)
			}()
		}
		wg.Wait()
		assert.EqualValues(t, 1, created.Load())
	}
	assert.Equal(t, 2, e.tables.Methods.Len(e.pod(t, "c").fdb.View()))
}

// TestDeltaLimitRace has two pods add contact methods for one user at once:
// no more than the limit are added (F8).
func TestDeltaLimitRace(t *testing.T) {
	e := newDeltaEnv(t)
	e.max = 3
	pods := []pod{e.pod(t, "a"), e.pod(t, "b")}
	userID, _ := e.addUser(t, pods[0])

	var created atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pods[i%2].Create(systemCtx(), nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms(fmt.Sprint("+", i))})
			if err == nil {
				created.Add(1)
				return
			}
			assert.Equal(t, "contact_methods_per_user_limit", constraint(err), "%v", err)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 3, created.Load())
	assert.Equal(t, 3, e.tables.ByUser.Count(e.pod(t, "c").fdb.View(), userID))
}

// TestDeltaUpdateRace has one pod rename a contact method, reading it first
// as the GraphQL mutation does, while another disables it. Both changes are
// kept (F4), as FOR UPDATE does on Postgres.
func TestDeltaUpdateRace(t *testing.T) {
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	userID, ctx := e.addUser(t, a)
	for i := range 5 {
		dest := sms(fmt.Sprint("+", i))
		c, err := a.Create(ctx, nil, &ContactMethod{Name: "cell", UserID: userID, Dest: dest})
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			assert.NoError(t, a.db.Run(ctx, nil, func(tx txn.Tx) error {
				c, err := a.FindOne(ctx, tx, c.ID)
				if err != nil {
					return err
				}
				c.Name = "renamed"
				return a.Update(ctx, tx, c)
			}))
		}()
		go func() {
			defer wg.Done()
			assert.NoError(t, b.DisableByDest(systemCtx(), nil, dest))
		}()
		wg.Wait()

		row := e.row(t, c.ID)
		assert.Equal(t, "renamed", row.Name)
		assert.True(t, row.Disabled)
	}
}

// TestDeltaUserDeleteRace has one pod delete a user while another adds a
// contact method for them: no contact method is left without a user (F7).
func TestDeltaUserDeleteRace(t *testing.T) {
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	for i := range 5 {
		userID, _ := e.addUser(t, a)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := a.fdb.Update(context.Background(), func(tx *filestore.Tx) error {
				_, err := e.users.Delete(tx, userID)
				return err
			})
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := b.Create(systemCtx(), nil, &ContactMethod{Name: "cell", UserID: userID, Dest: sms(fmt.Sprint("+", i))})
			if err != nil {
				assert.Equal(t, "user_contact_methods_user_id_fkey", constraint(err), "%v", err)
			}
		}()
		wg.Wait()
	}
	assert.Zero(t, e.tables.Methods.Len(e.pod(t, "c").fdb.View()))
}
