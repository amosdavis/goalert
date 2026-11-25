package notificationrule

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/target/goalert/util/errutil"
	"github.com/target/goalert/validation"
)

var start = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

// userRow and cmRow stand in for the users and user_contact_methods tables.
type userRow struct {
	ID string `parquet:"id"`
}

type cmRow struct {
	ID     string `parquet:"id"`
	UserID string `parquet:"user_id"`
}

type deltaEnv struct {
	store  objstore.Store
	schema *filestore.Schema
	users  *filestore.Table[userRow]
	cms    *filestore.Table[cmRow]
	tables *DeltaTables
	clock  *filestore.FakeClock
	max    int // notification_rules_per_user
}

func newDeltaEnv(t *testing.T) *deltaEnv {
	t.Helper()
	clock := filestore.NewFakeClock(start)
	clock.SetSpeed(0)
	clock.Set(start) // it ran until it was stopped
	e := &deltaEnv{store: objstore.NewMem(), schema: filestore.NewSchema(), clock: clock, max: -1}
	e.users = filestore.NewTable(e.schema, "users", func(u userRow) string { return u.ID })
	e.cms = filestore.NewTable(e.schema, "user_contact_methods", func(c cmRow) string { return c.ID })
	filestore.References(e.cms, "user_contact_methods_user_id_fkey", e.users,
		func(c cmRow) (string, bool) { return c.UserID, true }, filestore.Cascade, nil)
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.cms, e.users)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin", Clock: clock}))
	return e
}

func (e *deltaEnv) owner(r filestore.Reader, id string) (string, bool) {
	c, ok := e.cms.Get(r, id)
	return c.UserID, ok
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
	return pod{Store: NewDeltaStore(db, e.tables, e.owner, func(filestore.Reader) int { return e.max }), fdb: fdb, db: db}
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

func (e *deltaEnv) addCM(t *testing.T, p pod, userID string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	p.update(t, func(tx *filestore.Tx) error { return e.cms.Put(tx, cmRow{ID: id.String(), UserID: userID}) })
	return id
}

func systemCtx() context.Context { return permission.SystemContext(context.Background(), "test") }

func adminCtx() context.Context {
	return permission.UserContext(context.Background(), "bcefacc0-4764-012d-7bfb-002500d5decb", permission.RoleAdmin)
}

// constraint returns the name of the constraint that err broke.
func constraint(err error) string {
	var c interface{ ConstraintName() string }
	if errors.As(err, &c) {
		return c.ConstraintName()
	}
	return ""
}

func TestDeltaRules(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm := e.addCM(t, p, userID)
	otherID, otherCtx := e.addUser(t, p)
	otherCM := e.addCM(t, p, otherID)

	var want []NotificationRule
	for _, delay := range []int{5, 0, 9000} {
		in := &NotificationRule{ID: "ignored", UserID: userID, DelayMinutes: delay, ContactMethodID: cm}
		n, err := p.Insert(ctx, in)
		require.NoError(t, err)
		_, err = uuid.Parse(n.ID)
		require.NoError(t, err, "a new id is always made")
		want = append(want, *n)
		e.clock.Advance(time.Second)
	}
	other, err := p.Insert(otherCtx, &NotificationRule{UserID: otherID, ContactMethodID: otherCM})
	require.NoError(t, err)

	got, err := p.FindAll(otherCtx, userID)
	require.NoError(t, err)
	assert.Equal(t, want, got, "oldest first")
	// A fresh pod reads the same rules.
	got, err = e.pod(t, "b").FindAll(ctx, strings.ToUpper(userID))
	require.NoError(t, err)
	assert.Equal(t, want, got)
	got, err = p.FindAll(ctx, uuid.NewString())
	require.NoError(t, err)
	assert.Equal(t, []NotificationRule{}, got, "an empty list, not nil, as on Postgres")

	got, err = p.FindAllByUserIDs(ctx, []string{userID, otherID, userID, uuid.NewString()})
	require.NoError(t, err)
	assert.ElementsMatch(t, append([]NotificationRule{*other}, want...), got, "each once")
	got, err = p.FindAllByUserIDs(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []NotificationRule{}, got)

	row, ok := e.tables.Rules.Get(p.fdb.View(), want[1].ID)
	require.True(t, ok)
	require.NotNil(t, row.CreatedAt)
	assert.True(t, row.CreatedAt.Equal(start.Add(time.Second)), "created_at DEFAULT now()")

	require.NoError(t, p.DeleteTx(ctx, nil, strings.ToUpper(want[0].ID), uuid.NewString()))
	require.NoError(t, p.DeleteTx(ctx, nil))
	got, err = e.pod(t, "c").FindAll(ctx, userID)
	require.NoError(t, err)
	assert.Equal(t, want[1:], got)
}

func TestDeltaRulesInvalid(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm := e.addCM(t, p, userID)
	_, otherCtx := e.addUser(t, p)

	_, err := p.Insert(otherCtx, &NotificationRule{UserID: userID, ContactMethodID: cm})
	assert.True(t, permission.IsPermissionError(err), "only for oneself")
	_, err = p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: 9001})
	assert.True(t, validation.IsValidationError(err))
	_, err = p.Insert(systemCtx(), &NotificationRule{UserID: "x", ContactMethodID: cm})
	assert.True(t, validation.IsValidationError(err))
	_, err = p.FindAll(ctx, "x")
	assert.True(t, validation.IsValidationError(err))
	_, err = p.FindAllByUserIDs(ctx, []string{"x"})
	assert.True(t, validation.IsValidationError(err))
	assert.True(t, validation.IsValidationError(p.DeleteTx(ctx, nil, "x")))
	assert.Zero(t, e.tables.Rules.Len(p.fdb.View()))
}

func TestDeltaRulesSameUser(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, _ := e.addUser(t, p)
	otherCM := e.addCM(t, p, otherID)

	_, err := p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: otherCM})
	assert.Equal(t, "notification_rule_user_id_match", constraint(err))
	var fieldErr validation.FieldError
	require.ErrorAs(t, errutil.MapDBError(err), &fieldErr)
	assert.Equal(t, "UserID", fieldErr.Field())
	assert.Equal(t, "contact method is for wrong user", fieldErr.Reason())
	assert.Zero(t, e.tables.Rules.Len(p.fdb.View()))
}

func TestDeltaRulesUnique(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm, cm2 := e.addCM(t, p, userID), e.addCM(t, p, userID)
	n, err := p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: 5})
	require.NoError(t, err)

	_, err = p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: 5})
	assert.Equal(t, "user_notification_rules_contact_method_id_delay_minutes_key", constraint(err))
	var fieldErr validation.FieldError
	require.ErrorAs(t, errutil.MapDBError(err), &fieldErr)
	assert.Equal(t, "DelayMinutes", fieldErr.Field())

	_, err = p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm2, DelayMinutes: 5})
	require.NoError(t, err)
	_, err = p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: 6})
	require.NoError(t, err)

	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		return e.tables.Insert(tx, Row{ID: n.ID, UserID: userID, ContactMethodID: cm.String(), DelayMinutes: 7}, e.owner, -1)
	})
	assert.Equal(t, "user_notification_rules_pkey", constraint(err))
}

func TestDeltaRulesLimit(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm := e.addCM(t, p, userID)
	otherID, otherCtx := e.addUser(t, p)
	otherCM := e.addCM(t, p, otherID)
	insert := func(ctx context.Context, userID string, cm uuid.UUID, delay int) error {
		_, err := p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: delay})
		return err
	}

	e.max = 2
	require.NoError(t, insert(ctx, userID, cm, 1))
	require.NoError(t, insert(ctx, userID, cm, 2))
	err := insert(ctx, userID, cm, 3)
	assert.Equal(t, "notification_rules_per_user_limit", constraint(err))
	mapped := errutil.MapDBError(err)
	require.True(t, errutil.IsLimitError(mapped), "%v", mapped)
	assert.EqualError(t, mapped, "too many notification rules")
	assert.Equal(t, "user_notification_rules_contact_method_id_delay_minutes_key", constraint(insert(ctx, userID, cm, 1)), "a duplicate is reported first, as on Postgres")
	require.NoError(t, insert(otherCtx, otherID, otherCM, 1), "the limit is per user")

	e.max = 0
	assert.Equal(t, "notification_rules_per_user_limit", constraint(insert(otherCtx, otherID, otherCM, 2)))
	e.max = -1
	require.NoError(t, insert(ctx, userID, cm, 3), "no limit")

	// A row over the limit is not left behind, even by a caller that goes on.
	p.update(t, func(tx *filestore.Tx) error {
		err := e.tables.Insert(tx, Row{UserID: userID, ContactMethodID: cm.String(), DelayMinutes: 4}, e.owner, 1)
		assert.Equal(t, "notification_rules_per_user_limit", constraint(err))
		return nil
	})
	assert.Equal(t, 3, e.tables.ByUser.Count(p.fdb.View(), userID))
}

func TestDeltaRulesForeignKeys(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm, cm2 := e.addCM(t, p, userID), e.addCM(t, p, userID)
	otherID, otherCtx := e.addUser(t, p)
	otherCM := e.addCM(t, p, otherID)
	for _, c := range []uuid.UUID{cm, cm2} {
		_, err := p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: c})
		require.NoError(t, err)
	}
	_, err := p.Insert(otherCtx, &NotificationRule{UserID: otherID, ContactMethodID: otherCM})
	require.NoError(t, err)

	_, err = p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: uuid.New()})
	assert.Equal(t, "user_notification_rules_contact_method_id_fkey", constraint(err))

	// The user's own foreign key, with the same-user rule out of the way.
	_, err = p.fdb.Update(ctx, func(tx *filestore.Tx) error {
		noOwner := func(filestore.Reader, string) (string, bool) { return "", false }
		return e.tables.Insert(tx, Row{UserID: uuid.NewString(), ContactMethodID: cm.String(), DelayMinutes: 9}, noOwner, -1)
	})
	assert.Equal(t, "user_notification_rules_user_id_fkey", constraint(err))

	p.update(t, func(tx *filestore.Tx) error {
		_, err := e.cms.Delete(tx, cm.String())
		return err
	})
	rules, err := e.pod(t, "b").FindAll(ctx, userID)
	require.NoError(t, err)
	require.Len(t, rules, 1, "deleting a contact method deletes its rules (F7)")
	assert.Equal(t, cm2, rules[0].ContactMethodID)

	p.update(t, func(tx *filestore.Tx) error {
		_, err := e.users.Delete(tx, userID)
		return err
	})
	rules, err = e.pod(t, "c").FindAllByUserIDs(ctx, []string{userID, otherID})
	require.NoError(t, err)
	require.Len(t, rules, 1, "deleting a user deletes their rules (F7)")
	assert.Equal(t, otherID, rules[0].UserID)
}

func TestDeltaRulesDeletePermission(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	otherID, otherCtx := e.addUser(t, p)
	mine, err := p.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: e.addCM(t, p, userID)})
	require.NoError(t, err)
	theirs, err := p.Insert(otherCtx, &NotificationRule{UserID: otherID, ContactMethodID: e.addCM(t, p, otherID)})
	require.NoError(t, err)

	assert.True(t, permission.IsPermissionError(p.DeleteTx(ctx, nil, theirs.ID)))
	assert.True(t, permission.IsPermissionError(p.DeleteTx(ctx, nil, mine.ID, theirs.ID)), "every one of them must be the user's")
	assert.True(t, permission.IsPermissionError(p.DeleteTx(context.Background(), nil, mine.ID)))
	assert.Equal(t, 2, e.tables.Rules.Len(p.fdb.View()), "nothing was deleted")
	require.NoError(t, p.DeleteTx(adminCtx(), nil, mine.ID, theirs.ID))
	assert.Zero(t, e.tables.Rules.Len(p.fdb.View()))
}

func TestDeltaRulesTx(t *testing.T) {
	e := newDeltaEnv(t)
	p := e.pod(t, "a")
	userID, ctx := e.addUser(t, p)
	cm := e.addCM(t, p, userID)

	// In a caller's transaction, which is undone as a whole.
	errUndo := errors.New("undo")
	err := p.db.Run(ctx, nil, func(tx txn.Tx) error {
		n, err := p.CreateTx(ctx, tx, &NotificationRule{UserID: userID, ContactMethodID: cm})
		if err != nil {
			return err
		}
		if err := p.DeleteTx(ctx, tx, n.ID); err != nil {
			return err
		}
		if _, err := p.CreateTx(ctx, tx, &NotificationRule{UserID: userID, ContactMethodID: cm}); err != nil {
			return err
		}
		return errUndo
	})
	require.ErrorIs(t, err, errUndo)
	assert.Zero(t, e.tables.Rules.Len(p.fdb.View()))

	err = p.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		_, err := p.CreateTx(ctx, tx, &NotificationRule{UserID: userID, ContactMethodID: cm})
		return err
	})
	require.ErrorIs(t, err, deltatxn.ErrReadOnly)
}

// TestDeltaRulesRace has two pods add rules for one user at once: the unique
// index lets one have each delay, and no more than the limit are added (F8).
func TestDeltaRulesRace(t *testing.T) {
	e := newDeltaEnv(t)
	pods := []pod{e.pod(t, "a"), e.pod(t, "b")}
	userID, _ := e.addUser(t, pods[0])
	cm := e.addCM(t, pods[0], userID)

	race := func(delay func(i int) int, wantConstraint string) int {
		var created atomic.Int32
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := pods[i%2].Insert(systemCtx(), &NotificationRule{UserID: userID, ContactMethodID: cm, DelayMinutes: delay(i)})
				if err == nil {
					created.Add(1)
					return
				}
				assert.Equal(t, wantConstraint, constraint(err), "%v", err)
			}()
		}
		wg.Wait()
		return int(created.Load())
	}

	assert.Equal(t, 1, race(func(int) int { return 100 }, "user_notification_rules_contact_method_id_delay_minutes_key"))
	e.max = 3
	assert.Equal(t, 2, race(func(i int) int { return i }, "notification_rules_per_user_limit"))
	assert.Equal(t, 3, e.tables.ByUser.Count(e.pod(t, "c").fdb.View(), userID))
}

// TestDeltaRulesDeleteRace has one pod delete a contact method while another
// adds a rule for it: no rule is left without a contact method (F7).
func TestDeltaRulesDeleteRace(t *testing.T) {
	e := newDeltaEnv(t)
	a, b := e.pod(t, "a"), e.pod(t, "b")
	userID, ctx := e.addUser(t, a)
	for range 5 {
		cm := e.addCM(t, a, userID)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := a.fdb.Update(ctx, func(tx *filestore.Tx) error {
				_, err := e.cms.Delete(tx, cm.String())
				return err
			})
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := b.Insert(ctx, &NotificationRule{UserID: userID, ContactMethodID: cm})
			if err != nil {
				assert.Equal(t, "user_notification_rules_contact_method_id_fkey", constraint(err), "%v", err)
			}
		}()
		wg.Wait()
	}
	assert.Zero(t, e.tables.Rules.Len(e.pod(t, "c").fdb.View()))
}
