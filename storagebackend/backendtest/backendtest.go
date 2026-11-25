// Package backendtest checks a whole backend: several pods that use one table
// at once, as a deployment does. Every backend must pass it, besides the suites
// of its parts (docs/filestore-failure-modes.md, F14).
package backendtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	"github.com/target/goalert/tablelog"
)

type user struct {
	ID    string     `parquet:"id"`
	Name  string     `parquet:"name"`
	Count int64      `parquet:"count"`
	Seen  *time.Time `parquet:"seen,optional,timestamp(microsecond:utc)"`
}

type event struct {
	ID   string `parquet:"id"`
	User string `parquet:"user"`
	N    int64  `parquet:"n"`
}

type schema struct {
	s      *filestore.Schema
	users  *filestore.Table[user]
	events *filestore.Table[event]
}

func newSchema() schema {
	s := filestore.NewSchema()
	return schema{
		s:      s,
		users:  filestore.NewTable(s, "users", func(u user) string { return u.ID }).Buckets(4),
		events: filestore.NewTable(s, "events", func(e event) string { return e.ID }).Buckets(4),
	}
}

func options(t *testing.T, writer string) filestore.DeltaOptions {
	return filestore.DeltaOptions{
		WriterID: writer, PollInterval: -1, CheckpointEvery: 7,
		CommitBackoff: time.Millisecond, MaxConflicts: 100, Logf: t.Logf,
	}
}

type pod struct {
	schema
	db *filestore.DeltaDB
}

func openPod(t *testing.T, b *storagebackend.Backend, writer string) pod {
	t.Helper()
	s := newSchema()
	db, err := b.Open(context.Background(), s.s, options(t, writer))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return pod{schema: s, db: db}
}

// dump is a pod's whole state.
func (p pod) dump() (users []user, events []event) {
	v := p.db.View()
	p.users.Ascend(v, func(u user) bool { users = append(users, u); return true })
	p.events.Ascend(v, func(e event) bool { events = append(events, e); return true })
	return users, events
}

func same(t *testing.T, want, got pod, msg string) {
	t.Helper()
	wu, we := want.dump()
	gu, ge := got.dump()
	assert.Equal(t, wu, gu, msg)
	assert.Equal(t, we, ge, msg)
}

// Run runs the suite. newBackend returns new, empty storage each time it is
// called, as a function that connects to it, which a test calls for every pod.
func Run(t *testing.T, newBackend func(t *testing.T) (connect func(writerID string) *storagebackend.Backend)) {
	ctx := context.Background()

	create := func(t *testing.T, connect func(string) *storagebackend.Backend) {
		t.Helper()
		require.NoError(t, connect("admin").Create(ctx, newSchema().s, options(t, "admin")))
	}

	t.Run("CreateOnce", func(t *testing.T) {
		// Opening creates nothing, and of several creates one does (F27).
		connect := newBackend(t)
		_, err := connect("a").Open(ctx, newSchema().s, options(t, "a"))
		require.ErrorIs(t, err, tablelog.ErrNoTable)
		errs := make([]error, 4)
		var wg sync.WaitGroup
		for i := range errs {
			b := connect(fmt.Sprint(i))
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = b.Create(ctx, newSchema().s, options(t, fmt.Sprint(i)))
			}()
		}
		wg.Wait()
		created := 0
		for _, err := range errs {
			if err == nil {
				created++
				continue
			}
			require.ErrorIs(t, err, tablelog.ErrTableExists)
		}
		assert.Equal(t, 1, created)
		p := openPod(t, connect("b"), "b")
		assert.Zero(t, p.users.Len(p.db.View()))
	})

	t.Run("Pods", func(t *testing.T) {
		// Pods change the same rows and different ones at once. No change is
		// lost, a transaction that spans tables is whole, every pod ends with
		// the same rows, and so does one that starts later (F2, F4, F25).
		connect := newBackend(t)
		create(t, connect)
		const pods, workers, writes = 3, 2, 6
		ps := make([]pod, pods)
		for i := range ps {
			ps[i] = openPod(t, connect(fmt.Sprintf("pod-%d", i)), fmt.Sprintf("pod-%d", i))
		}
		seen := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
		var latest atomic.Int64
		var wg sync.WaitGroup
		for i, p := range ps {
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := range writes {
						id := fmt.Sprintf("%d-%d-%d", i, w, n)
						v, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
							c, _ := p.users.Get(tx, "counter")
							c.ID, c.Seen, c.Count = "counter", &seen, c.Count+1
							if err := p.users.Put(tx, c); err != nil {
								return err
							}
							return p.events.Put(tx, event{ID: id, User: "counter", N: c.Count})
						})
						if !assert.NoError(t, err) {
							return
						}
						for old := latest.Load(); v > old && !latest.CompareAndSwap(old, v); old = latest.Load() {
						}
						// This pod sees its own write at once, and any other pod
						// does once it waited for its version (F29).
						if _, ok := p.events.Get(p.db.View(), id); !assert.True(t, ok) {
							return
						}
						other := ps[(i+1)%pods]
						if !assert.NoError(t, other.db.WaitFor(ctx, v)) {
							return
						}
						if _, ok := other.events.Get(other.db.View(), id); !assert.True(t, ok) {
							return
						}
					}
				}()
			}
		}
		wg.Wait()
		const total = pods * workers * writes
		var conflicts, commits, txns int64
		for _, p := range ps {
			require.NoError(t, p.db.WaitFor(ctx, latest.Load()))
			same(t, ps[0], p, "every pod has the same rows")
			c, n := p.db.Batches()
			commits, txns, conflicts = commits+c, txns+n, conflicts+p.db.Conflicts()
		}
		c, _ := ps[0].users.Get(ps[0].db.View(), "counter")
		assert.EqualValues(t, total, c.Count, "no change is lost")
		counts := map[int64]bool{}
		ps[0].events.Ascend(ps[0].db.View(), func(e event) bool { counts[e.N] = true; return true })
		assert.Len(t, counts, total, "each transaction ran on top of the one before")
		same(t, ps[0], openPod(t, connect("late"), "late"), "a pod that starts later has them too")
		t.Logf("%d transactions in %d commits, %d conflicts", txns, commits, conflicts)
	})

	t.Run("Maintenance", func(t *testing.T) {
		// Cleaning up while pods write breaks nothing (F6, F16, F42).
		connect := newBackend(t)
		create(t, connect)
		writer, cleaner := openPod(t, connect("writer"), "writer"), openPod(t, connect("cleaner"), "cleaner")
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				_, err := cleaner.db.Vacuum(ctx)
				assert.NoError(t, err)
				_, err = cleaner.db.CleanLog(ctx)
				assert.NoError(t, err)
			}
			close(done)
		}()
		n := 0
		for finished := false; !finished && n < 200; n++ {
			select {
			case <-done:
				finished = true
			default:
			}
			_, err := writer.db.Update(ctx, func(tx *filestore.Tx) error {
				if n > 5 {
					if _, err := writer.users.Delete(tx, fmt.Sprintf("u%d", n-5)); err != nil {
						return err
					}
				}
				return writer.users.Put(tx, user{ID: fmt.Sprintf("u%d", n), Count: int64(n)})
			})
			require.NoError(t, err)
		}
		wg.Wait()
		require.NoError(t, cleaner.db.WaitFor(ctx, int64(writer.db.View().Index())))
		same(t, writer, cleaner, "the pods agree")
		same(t, writer, openPod(t, connect("late"), "late"), "and the table can be opened")
	})

	t.Run("FailedTransactions", func(t *testing.T) {
		// A transaction that fails, or whose caller gives up, changes nothing,
		// and others in its batch are committed.
		connect := newBackend(t)
		create(t, connect)
		p := openPod(t, connect("a"), "a")
		boom := errors.New("boom")
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
					if err := p.users.Put(tx, user{ID: fmt.Sprintf("u%d", i)}); err != nil {
						return err
					}
					if i%2 == 1 {
						return boom
					}
					return nil
				})
				if i%2 == 1 {
					assert.ErrorIs(t, err, boom)
				} else {
					assert.NoError(t, err)
				}
			}()
		}
		wg.Wait()
		gone, cancel := context.WithCancel(ctx)
		cancel()
		_, err := p.db.Update(gone, func(tx *filestore.Tx) error { return p.users.Put(tx, user{ID: "gone"}) })
		require.Error(t, err)
		if !errors.Is(err, objstore.ErrUnknown) {
			_, ok := p.users.Get(openPod(t, connect("b"), "b").db.View(), "gone")
			assert.False(t, ok)
		}
		users, _ := openPod(t, connect("c"), "c").dump()
		var ids []string
		for _, u := range users {
			if u.ID != "gone" {
				ids = append(ids, u.ID)
			}
		}
		assert.Equal(t, []string{"u0", "u2", "u4", "u6"}, ids)
	})
}
