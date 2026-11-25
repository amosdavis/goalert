package managedlog_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/managedlog"
	"github.com/target/goalert/managedlog/managedlogtest"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

// register decides by creating an entry for each version, if there is none.
type register struct {
	mu        sync.Mutex
	decisions map[string]string
}

func (r *register) key(storeID string, version int64) string {
	return fmt.Sprintf("%s/%d", storeID, version)
}

type registerArbiter struct{ r *register }

func (a registerArbiter) Manager() string { return "register" }

func (a registerArbiter) Prepare(ctx context.Context, p managedlog.Proposal) (managedlog.Ask, error) {
	if p.Version > 1 {
		d, err := a.Decision(ctx, p.Info.GoAlertStoreID, p.Version-1)
		if err != nil {
			return nil, err
		}
		if d.CommitID != p.Base {
			return nil, fmt.Errorf("%w: base is %s, not %s", tablelog.ErrForeign, d.CommitID, p.Base)
		}
	}
	return func(ctx context.Context) error {
		a.r.mu.Lock()
		defer a.r.mu.Unlock()
		k := a.r.key(p.Info.GoAlertStoreID, p.Version)
		if _, ok := a.r.decisions[k]; ok {
			return tablelog.ErrConflict
		}
		a.r.decisions[k] = p.Info.GoAlertCommitID
		return nil
	}, nil
}

func (a registerArbiter) Decision(ctx context.Context, storeID string, version int64) (managedlog.Decision, error) {
	if storeID == "" {
		return managedlog.Decision{}, tablelog.ErrNoTable
	}
	a.r.mu.Lock()
	defer a.r.mu.Unlock()
	return managedlog.Decision{CommitID: a.r.decisions[a.r.key(storeID, version)]}, nil
}

func (a registerArbiter) Check(ctx context.Context, s *tablelog.Snapshot) error {
	if s.Version == 0 {
		return nil
	}
	d, err := a.Decision(ctx, s.MetaData.ID, s.Version)
	if err == nil && d.CommitID != s.CommitID {
		return tablelog.ErrForeign
	}
	return err
}

// pointer decides by moving on one pointer to the newest commit, and creates
// tables.
type pointer struct {
	mu      sync.Mutex
	storeID string
	version int64
	id      string
}

type pointerArbiter struct{ p *pointer }

func (a pointerArbiter) Manager() string { return "pointer" }

func (a pointerArbiter) Create(ctx context.Context, meta tablelog.MetaData, info tablelog.CommitInfo) error {
	a.p.mu.Lock()
	defer a.p.mu.Unlock()
	if a.p.storeID != "" {
		return tablelog.ErrTableExists
	}
	a.p.storeID, a.p.version, a.p.id = meta.ID, 0, info.GoAlertCommitID
	return nil
}

func (a pointerArbiter) Prepare(ctx context.Context, p managedlog.Proposal) (managedlog.Ask, error) {
	a.p.mu.Lock()
	version, id := a.p.version, a.p.id
	a.p.mu.Unlock()
	switch {
	case version >= p.Version:
		return nil, tablelog.ErrConflict
	case version < p.Version-1:
		return nil, errors.New("too far")
	case id != p.Base:
		return nil, tablelog.ErrForeign
	}
	return func(ctx context.Context) error {
		a.p.mu.Lock()
		defer a.p.mu.Unlock()
		if a.p.version != version || a.p.id != id {
			return tablelog.ErrConflict
		}
		a.p.version, a.p.id = p.Version, p.Info.GoAlertCommitID
		return nil
	}, nil
}

func (a pointerArbiter) Decision(ctx context.Context, storeID string, version int64) (managedlog.Decision, error) {
	a.p.mu.Lock()
	defer a.p.mu.Unlock()
	switch {
	case a.p.storeID == "":
		return managedlog.Decision{}, tablelog.ErrNoTable
	case a.p.version == version:
		return managedlog.Decision{CommitID: a.p.id}, nil
	}
	return managedlog.Decision{Later: a.p.version > version}, nil
}

func (a pointerArbiter) Check(ctx context.Context, s *tablelog.Snapshot) error {
	a.p.mu.Lock()
	defer a.p.mu.Unlock()
	if s.Version > a.p.version || s.Version == a.p.version && s.CommitID != a.p.id {
		return tablelog.ErrForeign
	}
	return nil
}

// TestArbiters runs the suite on the two kinds of arbiters there are.
func TestArbiters(t *testing.T) {
	t.Run("Register", func(t *testing.T) {
		managedlogtest.Run(t, func(t *testing.T) managedlogtest.Env {
			r := &register{decisions: map[string]string{}}
			return managedlogtest.Env{Store: objstore.NewMem(), Connect: func(*testing.T, objstore.Store) managedlog.Arbiter { return registerArbiter{r} }}
		})
	})
	t.Run("Pointer", func(t *testing.T) {
		managedlogtest.Run(t, func(t *testing.T) managedlogtest.Env {
			p := &pointer{}
			return managedlogtest.Env{Store: objstore.NewMem(), Connect: func(*testing.T, objstore.Store) managedlog.Arbiter { return pointerArbiter{p} }}
		})
	})
}

func TestNewChecksConfig(t *testing.T) {
	a := registerArbiter{&register{}}
	for _, c := range []managedlog.Config{{}, {Arbiter: a}, {Store: objstore.NewMem()}, {Arbiter: unnamed{a}, Store: objstore.NewMem()}} {
		_, err := managedlog.New(c, "w")
		assert.Error(t, err)
	}
}

type unnamed struct{ managedlog.Arbiter }

func (unnamed) Manager() string { return "" }

type tested struct {
	registerArbiter
	calls *int
	err   *error
}

func (t tested) SelfTest(ctx context.Context) error { *t.calls++; return *t.err }

// TestSelfTest checks that a pod that can't rely on its arbiter does not start
// (F33).
func TestSelfTest(t *testing.T) {
	ctx := context.Background()
	calls, failure := 0, errors.New("a used ID was accepted")
	l, err := managedlog.New(managedlog.Config{
		Arbiter: tested{registerArbiter{&register{decisions: map[string]string{}}}, &calls, &failure}, Store: objstore.NewMem(),
	}, "a")
	require.NoError(t, err)
	require.NoError(t, l.Create(ctx, tablelog.MetaData{ID: "t", SchemaString: "{}"}, time.Now()))
	_, err = l.Load(ctx)
	require.ErrorIs(t, err, failure)
	failure = nil
	for range 3 {
		_, err = l.Load(ctx)
		require.NoError(t, err)
	}
	assert.Equal(t, 2, calls, "until it succeeds, and once then")
}

// TestNotKept checks that a version the arbiter remembers, and the Delta log
// does not any more, is reported as missing, not as damage (F16).
func TestNotKept(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	r := &register{decisions: map[string]string{}}
	open := func() *managedlog.Log {
		l, err := managedlog.New(managedlog.Config{Arbiter: registerArbiter{r}, Store: store}, "a")
		require.NoError(t, err)
		return l
	}
	l := open()
	require.NoError(t, l.Create(ctx, tablelog.MetaData{ID: "t", SchemaString: "{}"}, time.Now()))
	slow := open()
	_, err := slow.Load(ctx)
	require.NoError(t, err)
	at := time.Now()
	for v := int64(1); v <= 4; v++ {
		_, err := l.Commit(ctx, v, "t", "WRITE", at.Add(time.Duration(v)*time.Second), nil)
		require.NoError(t, err)
	}
	s, err := l.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, l.WriteCheckpoint(ctx, s, at.Add(time.Minute), time.Hour))
	n, err := l.Cleanup(ctx, at.Add(3*time.Hour), time.Hour)
	require.NoError(t, err)
	assert.Positive(t, n)
	_, err = store.Get(ctx, deltalog.CommitName(1))
	require.ErrorIs(t, err, objstore.ErrNotFound)
	for _, l := range []*managedlog.Log{slow, open()} {
		_, _, err = l.Read(ctx, 1)
		require.ErrorIs(t, err, objstore.ErrNotFound)
		require.NotErrorIs(t, err, tablelog.ErrCorrupt)
	}
}
