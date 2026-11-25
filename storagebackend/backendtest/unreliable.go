package backendtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
)

// RunUnreliable runs pods at once on storage that fails now and then, as a
// real one does. reliable connects to the storage, and unreliable connects
// through something that makes it fail, such as Faults, a new one for each pod.
//
// Every transaction that was acknowledged is in the table, and every one that
// failed is not. One whose outcome is unknown may be, but no transaction is
// applied twice, each ran on top of the one before, and all pods end with the
// same rows (F1, F3, F4, F25, F32).
func RunUnreliable(t *testing.T, reliable, unreliable func(writerID string) *storagebackend.Backend) {
	ctx := context.Background()
	opts := func(writer string) filestore.DeltaOptions {
		o := options(t, writer)
		o.PollInterval, o.MaxPollInterval = 20*time.Millisecond, 200*time.Millisecond
		return o
	}
	require.NoError(t, reliable("admin").Create(ctx, newSchema().s, opts("admin")))

	// Opening meets faults too, and must work in the end.
	open := func(writer string) pod {
		t.Helper()
		var err error
		for range 5 {
			s := newSchema()
			var db *filestore.DeltaDB
			if db, err = unreliable(writer).Open(ctx, s.s, opts(writer)); err == nil {
				t.Cleanup(func() { _ = db.Close() })
				return pod{schema: s, db: db}
			}
			t.Logf("open %s: %v", writer, err)
		}
		require.NoError(t, err, "open %s", writer)
		return pod{}
	}

	const pods, workers, writes = 3, 3, 12
	ps := make([]pod, pods)
	for i := range ps {
		ps[i] = open(fmt.Sprintf("pod-%d", i))
	}

	type result struct {
		err     error
		unknown bool
	}
	var mu sync.Mutex
	results := map[string]result{}
	var wg sync.WaitGroup
	for i, p := range ps {
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := range writes {
					id := fmt.Sprintf("%d-%d-%d", i, w, n)
					_, err := p.db.Update(ctx, func(tx *filestore.Tx) error {
						c, _ := p.users.Get(tx, "counter")
						c.ID, c.Count = "counter", c.Count+1
						if err := p.users.Put(tx, c); err != nil {
							return err
						}
						return p.events.Put(tx, event{ID: id, User: "counter", N: c.Count})
					})
					mu.Lock()
					results[id] = result{err, errors.Is(err, objstore.ErrUnknown)}
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()

	// What is in the table, read without faults.
	check := openPod(t, reliable("check"), "check")
	users, events := check.dump()
	var count int64
	for _, u := range users {
		if u.ID == "counter" {
			count = u.Count
		}
	}
	assert.EqualValues(t, len(events), count, "each transaction that was applied, was applied once (F3, F4)")
	byID := map[string]event{}
	ns := map[int64]bool{}
	for _, e := range events {
		byID[e.ID] = e
		ns[e.N] = true
		assert.True(t, e.N >= 1 && e.N <= count, "event %s counted %d of %d", e.ID, e.N, count)
	}
	assert.Len(t, ns, len(events), "each transaction ran on top of the one before (F4)")

	var acked, failed, unknown int
	for id, r := range results {
		_, applied := byID[id]
		switch {
		case r.err == nil:
			acked++
			assert.True(t, applied, "transaction %s was acknowledged, but is not in the table (F1)", id)
		case r.unknown:
			unknown++
		default:
			failed++
			assert.False(t, applied, "transaction %s failed, but is in the table: %v", id, r.err)
		}
		delete(byID, id)
	}
	assert.Empty(t, byID, "the table has only the transactions that were run")
	assert.Greater(t, acked, len(results)/2, "most transactions get through")

	// Every pod catches up with the table, faults and all, and has the same rows
	// (F25).
	latest := int64(check.db.View().Index())
	var conflicts int64
	for i, p := range ps {
		var err error
		for range 10 {
			if err = p.db.WaitFor(ctx, latest); err == nil {
				break
			}
		}
		require.NoError(t, err, "pod %d catches up", i)
		same(t, check, p, fmt.Sprintf("pod %d has the same rows", i))
		conflicts += p.db.Conflicts()
	}
	t.Logf("%d acknowledged, %d failed, %d unknown, in %d versions; %d conflicts", acked, failed, unknown, latest, conflicts)
	for id, r := range results {
		if r.err != nil {
			t.Logf("%s: %v", id, r.err)
		}
	}
}
