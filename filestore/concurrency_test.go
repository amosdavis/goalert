package filestore

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/deltatable"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/objstore/chunkstore"
)

func TestConcurrencyOptions(t *testing.T) {
	o := (DeltaOptions{}).withDefaults()
	assert.Equal(t, 256, o.MaxBatch)
	assert.Equal(t, time.Minute, o.BatchTimeout)
	assert.Equal(t, 5*time.Millisecond, o.ConflictBackoff)
	assert.Equal(t, time.Second, o.MaxConflictBackoff)
	assert.Equal(t, 8, o.CommitAttempts)
	assert.Equal(t, 100*time.Millisecond, o.CommitBackoff)
	assert.Equal(t, deltalog.DefaultTombstoneRetention, o.ChunkGracePeriod)

	assert.Equal(t, 8, o.UploadConcurrency)
	assert.EqualValues(t, chunkstore.MinGracePeriod, minChunkGrace)
	o = (DeltaOptions{ChunkGracePeriod: time.Second}).withDefaults()
	assert.Equal(t, chunkstore.MinGracePeriod, o.ChunkGracePeriod, "a shorter one could delete an upload in progress (F42)")

	o = (DeltaOptions{ConflictBackoff: 2 * time.Second}).withDefaults()
	assert.Equal(t, 2*time.Second, o.MaxConflictBackoff, "the cap never cuts off the first delay")

	store := objstore.NewMem()
	log := deltalog.New(store, "admin")
	opts := dOpts("admin")
	opts.CommitAttempts, opts.CommitBackoff = 3, time.Millisecond
	require.NoError(t, CreateBackend(context.Background(), store, log, newDSchema().s, opts))
	assert.Equal(t, 3, log.MaxAttempts)
	assert.Equal(t, time.Millisecond, log.Backoff)
}

type chunkGCStore struct {
	*objstore.Mem
	calls []time.Duration
}

func (s *chunkGCStore) CollectGarbage(_ context.Context, grace time.Duration) (int, error) {
	s.calls = append(s.calls, grace)
	return 0, nil
}

func TestCleanupCollectsChunks(t *testing.T) {
	ctx := context.Background()
	store := &chunkGCStore{Mem: objstore.NewMem()}
	sch := newDSchema()
	require.NoError(t, CreateDelta(ctx, store, sch.s, dOpts("admin")))
	db := openPod(t, store, sch, dOpts("a"))

	_, err := db.CleanLog(ctx)
	require.NoError(t, err)
	_, err = db.Vacuum(ctx)
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{deltalog.DefaultTombstoneRetention, deltalog.DefaultTombstoneRetention}, store.calls)
}

// dataStore counts uploads of data files, and can slow them down or hook them.
type dataStore struct {
	*objstore.Mem
	uploads, running, peak atomic.Int64
	delay                  time.Duration
	before                 func(name string) error
}

func (s *dataStore) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if s.before != nil {
		if err := s.before(name); err != nil {
			return err
		}
	}
	if !strings.HasPrefix(name, "tbl=") {
		return s.Mem.PutIfAbsent(ctx, name, data)
	}
	s.uploads.Add(1)
	n := s.running.Add(1)
	defer s.running.Add(-1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.Mem.PutIfAbsent(ctx, name, data)
}

// TestDeltaParallelUploads checks that a commit uploads several data files at
// once, but no more than it may (F15, F17), and that its actions come in the
// same order however the uploads finish (F25).
func TestDeltaParallelUploads(t *testing.T) {
	ctx := context.Background()
	store := &dataStore{Mem: objstore.NewMem(), delay: 20 * time.Millisecond}
	sch := newDSchema()
	require.NoError(t, CreateDelta(ctx, store, sch.s, dOpts("admin")))
	opts := dOpts("a")
	opts.UploadConcurrency = 3
	db := openPod(t, store, sch, opts)
	fill := func(tx *Tx) error {
		for i := range 200 {
			if err := sch.users.Put(tx, dUser{ID: fmt.Sprintf("u%d", i)}); err != nil {
				return err
			}
		}
		return nil
	}
	v, err := db.Update(ctx, fill)
	require.NoError(t, err)
	assert.Greater(t, store.peak.Load(), int64(1))
	assert.LessOrEqual(t, store.peak.Load(), int64(3))
	assert.Greater(t, store.uploads.Load(), int64(3))

	_, actions, err := db.log.Read(ctx, v)
	require.NoError(t, err)
	var buckets []string
	for _, a := range actions {
		if a.Add != nil {
			buckets = append(buckets, fmt.Sprintf("%s/%02s", a.Add.PartitionValues["tbl"], a.Add.PartitionValues["bucket"]))
		}
	}
	assert.IsIncreasing(t, buckets)

	// One failed upload fails the whole commit, and nothing changes.
	boom := errors.New("no space")
	var n atomic.Int64
	store.before = func(name string) error {
		if strings.HasPrefix(name, "tbl=") && n.Add(1) == 4 {
			return boom
		}
		return nil
	}
	_, err = db.Update(ctx, func(tx *Tx) error {
		for i := range 200 {
			if err := sch.users.Put(tx, dUser{ID: fmt.Sprintf("u%d", i), Name: "x"}); err != nil {
				return err
			}
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
	store.before = nil
	u, ok := sch.users.Get(db.View(), "u7")
	assert.True(t, ok)
	assert.Empty(t, u.Name)
	assert.Equal(t, dumpUsers(sch, openPod(t, store, sch, dOpts("b")).View()), dumpUsers(sch, db.View()))
}

// TestDeltaConflictReusesUploads checks that a pod that loses a race to a
// change of other rows does not upload its data files again (F37, F40).
func TestDeltaConflictReusesUploads(t *testing.T) {
	ctx := context.Background()
	store := &dataStore{Mem: objstore.NewMem()}
	sch := newDSchema()
	require.NoError(t, CreateDelta(ctx, store, sch.s, dOpts("admin")))
	a, b := openPod(t, store, sch, dOpts("a")), openPod(t, store, sch, dOpts("b"))

	// Users in different buckets.
	bucket := func(id string) int { return deltatable.Bucket(id, sch.users.deltaBuckets()) }
	mine, other := "mine", "other"
	for i := 0; bucket(other) == bucket(mine); i++ {
		other = fmt.Sprintf("other-%d", i)
	}
	var lost, aUploads atomic.Int64
	race := func(otherID string) {
		lost.Store(0)
		store.before = func(name string) error {
			if strings.HasPrefix(name, "tbl=") && lost.Load() != 1 {
				aUploads.Add(1)
			}
			if isCommitFile(name) && lost.CompareAndSwap(0, 1) {
				// Right before a commits, b does.
				_, err := b.Update(ctx, putUser(sch, otherID))
				assert.NoError(t, err)
				lost.Store(2)
			}
			return nil
		}
	}

	race(other)
	before := a.Conflicts()
	_, err := a.Update(ctx, putUser(sch, mine))
	require.NoError(t, err)
	assert.Equal(t, before+1, a.Conflicts())
	assert.EqualValues(t, 1, aUploads.Load(), "its data file was the same the second time")

	// A race within one bucket changes the file.
	aUploads.Store(0)
	same := "same"
	for i := 0; bucket(same) != bucket(mine); i++ {
		same = fmt.Sprintf("same-%d", i)
	}
	race(same)
	_, err = a.Update(ctx, func(tx *Tx) error { return sch.users.Put(tx, dUser{ID: mine, Name: "changed"}) })
	require.NoError(t, err)
	assert.EqualValues(t, 2, aUploads.Load(), "the file must hold the other pod's row")

	store.before = nil
	require.NoError(t, a.WaitFor(ctx, int64(a.View().Index())))
	fresh := openPod(t, store, sch, dOpts("c"))
	ids := []string{}
	for _, u := range dumpUsers(sch, fresh.View()) {
		ids = append(ids, u.ID)
	}
	assert.ElementsMatch(t, []string{mine, other, same}, ids)
	assert.Equal(t, dumpUsers(sch, fresh.View()), dumpUsers(sch, a.View()))
}

// TestDeltaFatalLogErrors checks that a log that says the table is not this
// pod's to write stops its writes, whichever call finds out (F11).
func TestDeltaFatalLogErrors(t *testing.T) {
	ctx := context.Background()
	for _, fatal := range []error{deltalog.ErrForeign, deltalog.ErrCorrupt, deltalog.ErrUnsupported, deltalog.ErrManaged} {
		for _, op := range []string{"read", "commit"} {
			store := newDeltaStore(t)
			sch := newDSchema()
			log := &failingLog{Log: deltalog.New(store, "a")}
			db, err := OpenBackend(ctx, store, log, sch.s, dOpts("a"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			log.op, log.err = op, fmt.Errorf("%w: injected", fatal)
			_, err = db.Update(ctx, putUser(sch, "u"))
			require.ErrorIs(t, err, fatal)
			require.ErrorIs(t, db.Health(), fatal, "%s: health goes red", op)
			log.err = nil
			_, err = db.Update(ctx, putUser(sch, "u"))
			require.ErrorIs(t, err, fatal, "%s: and stays red", op)
		}
	}
}

type failingLog struct {
	*deltalog.Log
	op  string
	err error
}

func (l *failingLog) Read(ctx context.Context, version int64) (deltalog.CommitInfo, []deltalog.Action, error) {
	if l.err != nil && l.op == "read" {
		return deltalog.CommitInfo{}, nil, l.err
	}
	return l.Log.Read(ctx, version)
}

func (l *failingLog) Commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []deltalog.Action) (deltalog.CommitInfo, error) {
	if l.err != nil && l.op == "commit" {
		return deltalog.CommitInfo{}, l.err
	}
	return l.Log.Commit(ctx, version, storeID, operation, at, actions)
}

// TestDeltaConcurrentIndependentWrites checks that many writers making
// independent changes converge without losing any, and that every pod catches
// up before a subsequent read (F4, F25, F29).
func TestDeltaConcurrentIndependentWrites(t *testing.T) {
	ctx := context.Background()
	store := newDeltaStore(t)
	sch := newDSchema()
	pods := []*DeltaDB{openPod(t, store, sch, dOpts("a")), openPod(t, store, sch, dOpts("b")), openPod(t, store, sch, dOpts("c"))}
	const workers, writes = 4, 10

	start := time.Now()
	var latest atomic.Int64
	var wg sync.WaitGroup
	for p, pod := range pods {
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := range writes {
					id := fmt.Sprintf("pod-%d-worker-%d-write-%d", p, w, n)
					v, err := pod.Update(ctx, putUser(sch, id))
					if !assert.NoError(t, err) {
						return
					}
					for {
						old := latest.Load()
						if v <= old || latest.CompareAndSwap(old, v) {
							break
						}
					}
				}
			}()
		}
	}
	wg.Wait()

	for _, pod := range pods {
		require.NoError(t, pod.WaitFor(ctx, latest.Load()))
		assert.Len(t, dumpUsers(sch, pod.View()), len(pods)*workers*writes)
		assert.Equal(t, dumpUsers(sch, pods[0].View()), dumpUsers(sch, pod.View()))
	}
	var commits, txns, conflicts int64
	for _, pod := range pods {
		c, n := pod.Batches()
		commits, txns = commits+c, txns+n
		conflicts += pod.Conflicts()
	}
	t.Logf("%d transactions in %d commits, %d conflicts, %v", txns, commits, conflicts, time.Since(start))
}

// BenchmarkDeltaPods measures the two important contention shapes: writers
// updating one row, and writers changing independent rows. The global commit
// sequence is serializable in both; independent changes must not skip replay.
func BenchmarkDeltaPods(b *testing.B) {
	for _, sameRow := range []bool{false, true} {
		name := "independent"
		if sameRow {
			name = "same-row"
		}
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			store := objstore.NewMem()
			sch := newDSchema()
			require.NoError(b, CreateDelta(ctx, store, sch.s, dOpts("admin")))
			pods := []*DeltaDB{openPod(b, store, sch, dOpts("a")), openPod(b, store, sch, dOpts("b")), openPod(b, store, sch, dOpts("c"))}

			var next atomic.Int64
			var latest atomic.Int64
			var wg sync.WaitGroup
			b.ResetTimer()
			start := time.Now()
			for p, pod := range pods {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1)) - 1
						if i >= b.N {
							return
						}
						v, err := pod.Update(ctx, func(tx *Tx) error {
							if !sameRow {
								return sch.users.Put(tx, dUser{ID: fmt.Sprintf("pod-%d-write-%d", p, i)})
							}
							u, _ := sch.users.Get(tx, "counter")
							u.ID = "counter"
							u.Count++
							return sch.users.Put(tx, u)
						})
						if err != nil {
							b.Error(err)
							return
						}
						for {
							old := latest.Load()
							if v <= old || latest.CompareAndSwap(old, v) {
								break
							}
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			for _, pod := range pods {
				require.NoError(b, pod.WaitFor(ctx, latest.Load()))
				if sameRow {
					u, _ := sch.users.Get(pod.View(), "counter")
					assert.EqualValues(b, b.N, u.Count)
				} else {
					assert.Len(b, dumpUsers(sch, pod.View()), b.N)
				}
			}
			var commits, conflicts int64
			for _, pod := range pods {
				c, _ := pod.Batches()
				commits += c
				conflicts += pod.Conflicts()
			}
			b.ReportMetric(float64(commits)/float64(b.N), "commits/txn")
			b.ReportMetric(float64(conflicts)/float64(b.N), "conflicts/txn")
			b.ReportMetric(float64(time.Since(start).Microseconds())/float64(b.N), "us/txn")
		})
	}
}

// TestDeltaUpdateEach checks that a group of transactions is one commit, in
// which each sees those before it, and none is hurt by another's failure.
func TestDeltaUpdateEach(t *testing.T) {
	ctx := t.Context()
	store := newDeltaStore(t)
	sch := newDSchema()
	db := openPod(t, store, sch, dOpts("a"))
	boom := errors.New("boom")
	before := db.View()

	var seen []int
	errs := db.UpdateEach(ctx,
		putUser(sch, "1"),
		func(tx *Tx) error {
			seen = append(seen[:0], sch.users.Len(tx))
			if err := putUser(sch, "2")(tx); err != nil {
				return err
			}
			return boom
		},
		func(tx *Tx) error { panic("oops") },
		func(tx *Tx) error {
			seen = append(seen, sch.users.Len(tx))
			return putUser(sch, "3")(tx)
		},
		func(tx *Tx) error { runtime.Goexit(); return nil },
		putUser(sch, "4"),
	)
	require.NoError(t, errs[0])
	require.ErrorIs(t, errs[1], boom)
	var pe *TxPanic
	require.ErrorAs(t, errs[2], &pe)
	require.Equal(t, "oops", pe.Value)
	require.NoError(t, errs[3])
	require.Error(t, errs[4])
	require.NoError(t, errs[5])
	require.Equal(t, []int{1, 1}, seen)
	require.Equal(t, before.Index()+1, db.View().Index(), "one commit")
	require.Equal(t, 3, sch.users.Len(db.View()))

	require.Empty(t, db.UpdateEach(ctx))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, err := range db.UpdateEach(canceled, putUser(sch, "5"), putUser(sch, "6")) {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, 3, sch.users.Len(db.View()))

	// More than fit in a commit still run in order.
	opts := dOpts("b")
	opts.MaxBatch = 2
	b := openPod(t, store, newDSchema(), opts)
	var fns []func(tx *Tx) error
	var order []int
	for i := range 5 {
		fns = append(fns, func(tx *Tx) error {
			if i == len(order) {
				order = append(order, i)
			}
			return nil
		})
	}
	for _, err := range b.UpdateEach(ctx, fns...) {
		require.NoError(t, err)
	}
	require.Equal(t, []int{0, 1, 2, 3, 4}, order)

	require.NoError(t, b.Close())
	for _, err := range b.UpdateEach(ctx, fns...) {
		require.ErrorIs(t, err, ErrClosed)
	}
}

// TestDeltaUpdateEachRace checks that groups of several pods lose no update.
func TestDeltaUpdateEachRace(t *testing.T) {
	ctx := t.Context()
	store := newDeltaStore(t)
	const pods, rounds = 3, 8
	var wg sync.WaitGroup
	var last *DeltaDB
	for p := range pods {
		db := openPod(t, store, newDSchema(), dOpts(fmt.Sprint(p)))
		last = db
		wg.Go(func() {
			for range rounds {
				count := func(tx *Tx) error {
					_, err := tx.NextID("count")
					return err
				}
				for _, err := range db.UpdateEach(ctx, count, count) {
					assert.NoError(t, err)
				}
			}
		})
	}
	wg.Wait()
	var got int64
	_, err := last.Update(ctx, func(tx *Tx) (err error) {
		got, err = tx.NextID("count")
		return err
	})
	require.NoError(t, err)
	require.EqualValues(t, 2*pods*rounds+1, got)
}
