package label

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/assignment"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
	"github.com/target/goalert/util/sqlutil"
	"github.com/target/goalert/validation"
)

// parent stands in for the tables that labels refer to.
type parent struct {
	ID string `parquet:"id"`
}

type env struct {
	store  objstore.Store
	schema *filestore.Schema
	tables *DeltaTables

	services, schedules, rotations, policies *filestore.Table[parent]
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{store: objstore.NewMem(), schema: filestore.NewSchema()}
	newParent := func(name string) *filestore.Table[parent] {
		return filestore.NewTable(e.schema, name, func(p parent) string { return p.ID })
	}
	e.services, e.schedules = newParent("services"), newParent("schedules")
	e.rotations, e.policies = newParent("rotations"), newParent("escalation_policies")
	e.tables = DefineDeltaTables(e.schema)
	DeclareDeltaForeignKeys(e.tables, e.services, e.schedules, e.rotations, e.policies)
	require.NoError(t, filestore.CreateDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{WriterID: "admin"}))
	return e
}

type pod struct {
	*Store
	db  deltatxn.DB
	env *env
}

// open opens the table as one GoAlert pod.
func (e *env) open(t *testing.T) *pod {
	t.Helper()
	fdb, err := filestore.OpenDelta(context.Background(), e.store, e.schema, filestore.DeltaOptions{
		PollInterval: -1, Logf: func(string, ...any) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fdb.Close() })
	db := deltatxn.DB{DB: fdb}
	return &pod{Store: NewDeltaStore(db, e.tables), db: db, env: e}
}

// update runs fn on the filestore tables.
func (p *pod) update(fn func(tx *filestore.Tx) error) error {
	_, err := p.db.DB.Update(context.Background(), fn)
	return err
}

// add adds a row to a parent table.
func (p *pod) add(t *testing.T, table *filestore.Table[parent]) string {
	t.Helper()
	id := uuid.NewString()
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return table.Put(tx, parent{ID: id}) }))
	return id
}

func testCtx() context.Context {
	return permission.UserContext(context.Background(), uuid.NewString(), permission.RoleUser)
}

func (p *pod) set(t *testing.T, tgt assignment.Target, key, value string) {
	t.Helper()
	require.NoError(t, p.SetTx(testCtx(), nil, &Label{Target: tgt, Key: key, Value: value}))
}

func (p *pod) labels(t *testing.T, tgt assignment.Target) []string {
	t.Helper()
	labels, err := p.FindAllByTarget(testCtx(), nil, tgt)
	require.NoError(t, err)
	require.NotNil(t, labels, "an empty list, not nil, as on Postgres")
	res := []string{}
	for _, l := range labels {
		assert.Equal(t, tgt, l.Target)
		res = append(res, l.Key+"="+l.Value)
	}
	return res
}

func requireConstraint(t *testing.T, err error, code, constraint string) {
	t.Helper()
	require.Error(t, err)
	dbErr := sqlutil.MapError(err)
	require.NotNil(t, dbErr, "%v", err)
	assert.Equal(t, code, dbErr.Code)
	assert.Equal(t, constraint, dbErr.ConstraintName)
}

func TestDeltaLabels(t *testing.T) {
	e := newEnv(t)
	p := e.open(t)
	targets := []assignment.Target{
		assignment.ServiceTarget(p.add(t, e.services)),
		assignment.ScheduleTarget(p.add(t, e.schedules)),
		assignment.RotationTarget(p.add(t, e.rotations)),
		assignment.EscalationPolicyTarget(p.add(t, e.policies)),
	}
	for _, tgt := range targets {
		assert.Empty(t, p.labels(t, tgt))
		p.set(t, tgt, "foo/bar", "value")
		p.set(t, tgt, "foo/baz", "other")
	}
	for _, tgt := range targets {
		assert.Equal(t, []string{"foo/bar=value", "foo/baz=other"}, p.labels(t, tgt))
		p.set(t, tgt, "foo/bar", "")
		assert.Equal(t, []string{"foo/baz=other"}, p.labels(t, tgt), "an empty value deletes the label")
		p.set(t, tgt, "foo/missing", "")
	}
	assert.Empty(t, p.labels(t, assignment.ServiceTarget(uuid.NewString())))
	assert.Empty(t, p.labels(t, assignment.UserTarget(uuid.NewString())), "not a target of labels")

	upper := assignment.ServiceTarget(strings.ToUpper(targets[0].TargetID()))
	p.set(t, upper, "upp/per", "case")
	assert.Equal(t, []string{"foo/baz=other", "upp/per=case"}, p.labels(t, targets[0]), "a UUID in another form is the same UUID")

	fresh := e.open(t)
	assert.Equal(t, []string{"foo/baz=other", "upp/per=case"}, fresh.labels(t, targets[0]), "another pod reads the same")

	// Checks shared with Postgres.
	err := p.SetTx(testCtx(), nil, &Label{Target: targets[0], Key: "invalid", Value: "value"})
	assert.True(t, validation.IsValidationError(err), "%v", err)
	err = p.SetTx(testCtx(), nil, &Label{Target: assignment.UserTarget(uuid.NewString()), Key: "foo/bar", Value: "value"})
	assert.True(t, validation.IsValidationError(err), "%v", err)
	err = p.SetTx(context.Background(), nil, &Label{Target: targets[0], Key: "foo/bar", Value: "value"})
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	_, err = p.FindAllByTarget(context.Background(), nil, targets[0])
	assert.True(t, permission.IsPermissionError(err), "%v", err)
}

func TestDeltaLabelsSetAgain(t *testing.T) {
	e := newEnv(t)
	p := e.open(t)
	tgt := assignment.ServiceTarget(p.add(t, e.services))
	p.set(t, tgt, "foo/bar", "one")
	p.set(t, tgt, "foo/bar", "two")
	assert.Equal(t, []string{"foo/bar=two"}, p.labels(t, tgt), "a target has one label with a key")
	p.set(t, tgt, "foo/baz", "two")
	p.set(t, tgt, "foo/bar", "two")
	assert.Equal(t, []string{"foo/bar=two", "foo/baz=two"}, p.labels(t, tgt))
	p.set(t, tgt, "foo/bar", "")
	assert.Equal(t, []string{"foo/baz=two"}, p.labels(t, tgt))
}

func TestDeltaLabelsUniqueIndex(t *testing.T) {
	e := newEnv(t)
	p := e.open(t)
	svc, sched := p.add(t, e.services), p.add(t, e.schedules)
	row := func(id int64) labelRow {
		return labelRow{ID: id, Key: "foo/bar", Value: "value", TgtServiceID: &svc}
	}
	require.NoError(t, p.update(func(tx *filestore.Tx) error {
		return e.tables.labels.Put(tx, labelRow{ID: 9, Key: "foo/bar", Value: "value", TgtScheduleID: &sched})
	}))
	require.NoError(t, p.update(func(tx *filestore.Tx) error { return e.tables.labels.Put(tx, row(1)) }))
	err := p.update(func(tx *filestore.Tx) error { return e.tables.labels.Put(tx, row(2)) })
	requireConstraint(t, err, "23505", "idx_labels_resource_id")
	err = p.update(func(tx *filestore.Tx) error { return e.tables.labels.Insert(tx, row(1), "labels_pkey") })
	requireConstraint(t, err, "23505", "labels_pkey")
}

func TestDeltaLabelsForeignKeys(t *testing.T) {
	e := newEnv(t)
	p := e.open(t)
	tests := []struct {
		constraint string
		parent     *filestore.Table[parent]
		target     func(id string) assignment.Target
	}{
		{"labels_tgt_service_id_fkey", e.services, func(id string) assignment.Target { return assignment.ServiceTarget(id) }},
		{"labels_tgt_schedule_id_fkey", e.schedules, func(id string) assignment.Target { return assignment.ScheduleTarget(id) }},
		{"labels_tgt_rotation_id_fkey", e.rotations, func(id string) assignment.Target { return assignment.RotationTarget(id) }},
		{"labels_tgt_ep_id_fkey", e.policies, func(id string) assignment.Target { return assignment.EscalationPolicyTarget(id) }},
	}
	for _, tt := range tests {
		t.Run(tt.constraint, func(t *testing.T) {
			err := p.SetTx(testCtx(), nil, &Label{Target: tt.target(uuid.NewString()), Key: "foo/bar", Value: "value"})
			requireConstraint(t, err, "23503", tt.constraint)

			gone, kept := p.add(t, tt.parent), p.add(t, tt.parent)
			p.set(t, tt.target(gone), "foo/bar", "value")
			p.set(t, tt.target(kept), "foo/bar", "value")
			require.NoError(t, p.update(func(tx *filestore.Tx) error {
				_, err := tt.parent.Delete(tx, gone)
				return err
			}))
			assert.Empty(t, p.labels(t, tt.target(gone)), "labels go with their target")
			assert.Equal(t, []string{"foo/bar=value"}, p.labels(t, tt.target(kept)))
		})
	}
}

func TestDeltaLabelsTx(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	tgt := assignment.ServiceTarget(p.add(t, e.services))

	boom := errors.New("boom")
	err := p.db.Run(ctx, nil, func(tx txn.Tx) error {
		if err := p.SetTx(ctx, tx, &Label{Target: tgt, Key: "foo/bar", Value: "value"}); err != nil {
			return err
		}
		keys, err := p.UniqueKeysTx(ctx, tx)
		if err != nil {
			return err
		}
		assert.Equal(t, []string{"foo/bar"}, keys, "a transaction sees its own changes")
		labels, err := p.FindAllByTarget(ctx, tx, tgt)
		if err != nil {
			return err
		}
		assert.Len(t, labels, 1)
		return boom
	})
	require.ErrorIs(t, err, boom)
	assert.Empty(t, p.labels(t, tgt), "a failed transaction changes nothing")

	keys, err := p.UniqueKeysTx(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, keys, "nil without labels, as on Postgres")

	err = p.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		return p.SetTx(ctx, tx, &Label{Target: tgt, Key: "foo/bar", Value: "value"})
	})
	require.ErrorIs(t, err, deltatxn.ErrReadOnly)
}

func TestDeltaLabelsSearch(t *testing.T) {
	ctx := testCtx()
	e := newEnv(t)
	p := e.open(t)
	a, b := assignment.ServiceTarget(p.add(t, e.services)), assignment.ServiceTarget(p.add(t, e.services))

	keys, err := p.SearchKeys(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, keys, "nil without results, as on Postgres")

	p.set(t, a, "foo/Zed", "100%")
	p.set(t, a, "foo/abc", "red")
	p.set(t, a, "foo/ABC", "Red")
	p.set(t, b, "foo/abc", "blue")
	p.set(t, b, "bar/one", "RED")
	p.set(t, b, "bar/one_x", "red")

	searchKeys := func(opts KeySearchOptions) []string {
		t.Helper()
		keys, err := p.SearchKeys(ctx, &opts)
		require.NoError(t, err)
		return keys
	}
	all := []string{"bar/one", "bar/one_x", "foo/ABC", "foo/abc", "foo/Zed"}
	assert.Equal(t, all, searchKeys(KeySearchOptions{}), "each once, by lower(key), then key")
	assert.Equal(t, []string{"foo/ABC", "foo/abc"}, searchKeys(KeySearchOptions{Search: "Abc"}), "in any case")
	assert.Equal(t, []string{"bar/one_x"}, searchKeys(KeySearchOptions{Search: "_"}), "_ is no wildcard")
	assert.Nil(t, searchKeys(KeySearchOptions{Search: "%"}), "% is no wildcard")
	assert.Equal(t, []string{"bar/one", "foo/ABC", "foo/Zed"}, searchKeys(KeySearchOptions{Omit: []string{"foo/abc", "bar/one_x"}}), "omitted exactly")
	assert.Equal(t, all[:2], searchKeys(KeySearchOptions{Limit: 2}))
	assert.Equal(t, all[2:4], searchKeys(KeySearchOptions{Limit: 2, After: "bar/one_x"}))
	assert.Equal(t, all[3:], searchKeys(KeySearchOptions{After: "foo/ABC"}))
	assert.Equal(t, all[4:], searchKeys(KeySearchOptions{After: "foo/abc"}))
	assert.Equal(t, []string{"foo/abc"}, searchKeys(KeySearchOptions{Search: "nothing", Omit: []string{"foo/abc"}, After: "foo/ABC"}),
		"the query's precedence: a key that differs from After only in case is always included")

	searchValues := func(opts ValueSearchOptions) []string {
		t.Helper()
		values, err := p.SearchValues(ctx, &opts)
		require.NoError(t, err)
		return values
	}
	assert.Equal(t, []string{"blue", "red"}, searchValues(ValueSearchOptions{Key: "foo/abc"}))
	assert.Equal(t, []string{"RED"}, searchValues(ValueSearchOptions{Key: "bar/one"}))
	assert.Equal(t, []string{"100%"}, searchValues(ValueSearchOptions{Key: "foo/Zed", KeySearchOptions: KeySearchOptions{Search: "0%"}}))
	assert.Equal(t, []string{"red"}, searchValues(ValueSearchOptions{Key: "foo/abc", KeySearchOptions: KeySearchOptions{After: "blue"}}))
	assert.Equal(t, []string{"blue"}, searchValues(ValueSearchOptions{Key: "foo/abc", KeySearchOptions: KeySearchOptions{Limit: 1}}))
	assert.Nil(t, searchValues(ValueSearchOptions{Key: "bar/one", KeySearchOptions: KeySearchOptions{After: "RED"}}), "not other keys' values")
	assert.Nil(t, searchValues(ValueSearchOptions{Key: "foo/none"}))
	assert.Nil(t, searchValues(ValueSearchOptions{}))

	_, err = p.SearchKeys(context.Background(), nil)
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	_, err = p.SearchValues(context.Background(), nil)
	assert.True(t, permission.IsPermissionError(err), "%v", err)
	_, err = p.UniqueKeysTx(context.Background(), nil)
	assert.True(t, permission.IsPermissionError(err), "%v", err)

	// The service store's lookup.
	view := p.db.DB.View()
	assert.ElementsMatch(t, []string{a.TargetID(), b.TargetID()}, e.tables.ServicesWithLabel(view, "foo/abc", ""))
	assert.Equal(t, []string{b.TargetID()}, e.tables.ServicesWithLabel(view, "foo/abc", "blue"))
	assert.Empty(t, e.tables.ServicesWithLabel(view, "foo/abc", "BLUE"))
	p.set(t, a, "foo/abc", "red")
	assert.Equal(t, []string{a.TargetID()}, e.tables.ServicesWithLabel(p.db.DB.View(), "foo/abc", "red"), "each service once")
}

func TestDeltaLabelsRace(t *testing.T) {
	e := newEnv(t)
	pods := []*pod{e.open(t), e.open(t)}
	tgt := assignment.ServiceTarget(pods[0].add(t, e.services))

	const each = 5
	errs := make(chan error, len(pods)*each)
	var wg sync.WaitGroup
	for i, p := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range each {
				errs <- p.SetTx(testCtx(), nil, &Label{Target: tgt, Key: fmt.Sprintf("pod-%d/num%d", i, n), Value: "value"})
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Len(t, e.open(t).labels(t, tgt), len(pods)*each, "pods get different ids, and no label is lost (F4)")

	// Both set the same label: the one that commits last changes the other's.
	for i, p := range pods {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, p.SetTx(testCtx(), nil, &Label{Target: tgt, Key: "same/key", Value: fmt.Sprintf("pod-%d", i)}))
		}()
	}
	wg.Wait()
	assert.Len(t, e.open(t).labels(t, tgt), len(pods)*each+1, "a target has one label with a key (F8)")

	// A pod that doesn't know yet that a target is gone can't label it.
	behind := e.open(t)
	require.NoError(t, pods[0].update(func(tx *filestore.Tx) error {
		_, err := e.services.Delete(tx, tgt.TargetID())
		return err
	}))
	err := behind.SetTx(testCtx(), nil, &Label{Target: tgt, Key: "foo/bar", Value: "value"})
	requireConstraint(t, err, "23503", "labels_tgt_service_id_fkey")
	assert.Empty(t, e.open(t).labels(t, tgt))
}
