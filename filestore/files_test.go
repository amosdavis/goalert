package filestore

import (
	"context"
	"fmt"
	"maps"
	"math/bits"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
)

// countingStore counts the data files read and written, and their bytes.
type countingStore struct {
	*objstore.Mem
	reads, writes, written atomic.Int64
}

func (s *countingStore) Get(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "tbl=") {
		s.reads.Add(1)
	}
	return s.Mem.Get(ctx, name)
}

func (s *countingStore) PutIfAbsent(ctx context.Context, name string, data []byte) error {
	if strings.HasPrefix(name, "tbl=") {
		s.writes.Add(1)
		s.written.Add(int64(len(data)))
	}
	return s.Mem.PutIfAbsent(ctx, name, data)
}

// oneBucket is a schema whose users are all in one bucket.
func oneBucket() dSchema {
	s := NewSchema()
	users := NewTable(s, "users", func(u dUser) string { return u.ID }).Buckets(1)
	email := users.Unique("email", func(u dUser) (string, bool) { return u.Email, u.Email != "" })
	return dSchema{s: s, users: users, email: email}
}

// checkFiles checks that a pod's view of the data files is what the log has,
// and that they hold its rows.
func checkFiles(t *testing.T, db *DeltaDB, sch dSchema) {
	t.Helper()
	require.ElementsMatch(t, db.snap.FilePaths(), slices.Collect(maps.Keys(db.files)))
	n := 0
	var keys []string
	for id, files := range db.buckets {
		require.NotEmpty(t, files)
		for _, f := range files {
			n++
			require.Same(t, f, db.files[f.path])
			require.Equal(t, id, f.id)
			require.NotEmpty(t, f.keys)
			require.True(t, slices.IsSorted(f.keys))
			if id.table == "users" {
				keys = append(keys, f.keys...)
			}
		}
	}
	require.Equal(t, len(db.files), n)
	slices.Sort(keys)
	var want []string
	sch.users.Ascend(db.View(), func(u dUser) bool { want = append(want, u.ID); return true })
	require.Equal(t, want, keys)
}

// TestDeltaFileSizes checks what one change costs as a table grows: adding a
// row rewrites few others, however many there are, changing one rewrites its
// file only, and a bucket keeps few files (F39).
func TestDeltaFileSizes(t *testing.T) {
	ctx := context.Background()
	store := &countingStore{Mem: objstore.NewMem()}
	sch := oneBucket()
	opts := dOpts("a")
	opts.MaxFileRows = 64
	opts.CheckpointEvery = -1
	require.NoError(t, CreateDelta(ctx, store, sch.s, opts))
	db := openPod(t, store, sch, opts)
	other := openPod(t, store, oneBucket(), dOpts("b"))

	written := 0
	commit := func(fn func(tx *Tx) error) (rows, files int) {
		t.Helper()
		v, err := db.Update(ctx, fn)
		require.NoError(t, err)
		_, actions, err := db.log.Read(ctx, v)
		require.NoError(t, err)
		for _, a := range actions {
			if a.Add == nil {
				continue
			}
			f := db.files[a.Add.Path]
			require.LessOrEqual(t, len(f.keys), opts.MaxFileRows)
			rows, files = rows+len(f.keys), files+1
		}
		written += rows
		return rows, files
	}
	const n = 1000
	perRow := bits.Len(uint(opts.MaxFileRows))
	for i := range n {
		_, files := commit(putUser(sch, fmt.Sprintf("%06d", i)))
		require.Equal(t, 1, files)
		require.LessOrEqual(t, len(db.files), (i+1)/opts.MaxFileRows+perRow)
	}
	require.LessOrEqual(t, written, n*perRow, "each row is rewritten as often as files double")
	checkFiles(t, db, sch)

	rows, files := commit(func(tx *Tx) error { return sch.users.Put(tx, dUser{ID: "000003", Name: "changed"}) })
	require.Equal(t, 1, files)
	require.LessOrEqual(t, rows, opts.MaxFileRows)
	require.NoError(t, other.WaitFor(ctx, int64(db.View().Index())))
	before := store.reads.Load()
	rows, _ = commit(func(tx *Tx) error {
		_, err := sch.users.Delete(tx, "000500")
		return err
	})
	require.LessOrEqual(t, rows, opts.MaxFileRows)
	require.NoError(t, other.WaitFor(ctx, int64(db.View().Index())))
	require.EqualValues(t, 1, store.reads.Load()-before, "another pod reads the new file only")
	require.Equal(t, dumpUsers(sch, db.View()), dumpUsers(sch, openPod(t, store, oneBucket(), dOpts("c")).View()))

	// A lot at once is several files.
	rows, files = commit(func(tx *Tx) error {
		for i := range 200 {
			if err := putUser(sch, fmt.Sprintf("bulk%06d", i))(tx); err != nil {
				return err
			}
		}
		return nil
	})
	require.GreaterOrEqual(t, rows, 200)
	require.LessOrEqual(t, files, 200/opts.MaxFileRows+1)

	// Nothing is left of an empty table, and nothing is written for a change
	// that comes to nothing.
	before = store.writes.Load()
	v, err := db.Update(ctx, func(tx *Tx) error {
		if err := putUser(sch, "gone")(tx); err != nil {
			return err
		}
		_, err := sch.users.Delete(tx, "gone")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, int64(db.View().Index()), v)
	commit(func(tx *Tx) error {
		for _, u := range dumpUsers(sch, tx) {
			if _, err := sch.users.Delete(tx, u.ID); err != nil {
				return err
			}
		}
		return nil
	})
	require.Equal(t, before, store.writes.Load())
	require.Empty(t, db.files)
	require.Empty(t, db.buckets)
	checkFiles(t, db, sch)
}

// TestDeltaFilesRandom changes a table in random ways on several pods, which do
// not agree on how large a file may be, and checks that they all have what a
// simple map has, and so does a pod that starts later.
func TestDeltaFilesRandom(t *testing.T) {
	ctx := context.Background()
	seed := rand.Uint64()
	t.Logf("seed %d", seed)
	rnd := rand.New(rand.NewPCG(seed, 1))
	store := objstore.NewMem()
	require.NoError(t, CreateDelta(ctx, store, newDSchema().s, dOpts("admin")))
	type pod struct {
		db  *DeltaDB
		sch dSchema
	}
	var pods []pod
	for i, size := range []int{8, 64, 1024} {
		opts := dOpts(fmt.Sprint(i))
		opts.MaxFileRows = size
		opts.CheckpointEvery = 7
		sch := newDSchema()
		pods = append(pods, pod{openPod(t, store, sch, opts), sch})
	}
	model := map[string]dUser{}
	for step := range 250 {
		p := pods[rnd.IntN(len(pods))]
		next := maps.Clone(model)
		type change struct {
			id  string
			row *dUser
		}
		var changes []change
		for range 1 + rnd.IntN(1+rnd.IntN(60)) {
			id := fmt.Sprint(rnd.IntN(400))
			old, exists := next[id]
			switch {
			case exists && rnd.IntN(3) == 0:
				delete(next, id)
				changes = append(changes, change{id: id})
			default:
				row := dUser{ID: id, Name: fmt.Sprint(step), Count: old.Count + 1}
				next[id] = row
				changes = append(changes, change{id, &row})
			}
		}
		// Two rows that exist exchange a unique value.
		if ids := slices.Sorted(maps.Keys(next)); len(ids) > 2 {
			a, b := next[ids[rnd.IntN(len(ids))]], next[ids[rnd.IntN(len(ids))]]
			if a.ID != b.ID {
				ea, eb := a.Email, b.Email
				if ea == "" {
					ea = fmt.Sprintf("e%d", step)
				}
				a.Email, b.Email = "", ea
				clear, take := a, b
				changes = append(changes, change{a.ID, &clear}, change{b.ID, &take})
				a.Email = eb
				changes = append(changes, change{a.ID, &a})
				next[a.ID], next[b.ID] = a, b
			}
		}
		_, err := p.db.Update(ctx, func(tx *Tx) error {
			for _, c := range changes {
				if c.row == nil {
					if _, err := p.sch.users.Delete(tx, c.id); err != nil {
						return err
					}
				} else if err := p.sch.users.Put(tx, *c.row); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
		model = next
		want := slices.SortedFunc(maps.Values(model), func(a, b dUser) int { return strings.Compare(a.ID, b.ID) })
		if step%10 == 0 {
			for _, p := range pods {
				require.NoError(t, p.db.WaitFor(ctx, deltaVersion(t, store)))
				require.Equal(t, want, dumpUsers(p.sch, p.db.View()))
				checkFiles(t, p.db, p.sch)
			}
		}
		if step%50 == 0 {
			sch := newDSchema()
			require.Equal(t, want, dumpUsers(sch, openPod(t, store, sch, dOpts("fresh")).View()))
		}
	}
}

// deltaVersion is the newest version in the log.
func deltaVersion(t *testing.T, store objstore.Store) int64 {
	t.Helper()
	snap, err := deltalog.New(store, "reader").Load(context.Background())
	require.NoError(t, err)
	return snap.Version
}

// BenchmarkDeltaInsert measures what adding a row costs in tables of different
// sizes, in time and in bytes of data files (F39).
func BenchmarkDeltaInsert(b *testing.B) {
	for _, rows := range []int{0, 2000, 50000} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			ctx := context.Background()
			store := &countingStore{Mem: objstore.NewMem()}
			sch := newDSchema()
			opts := dOpts("a")
			opts.CheckpointEvery = -1
			if err := CreateDelta(ctx, store, sch.s, opts); err != nil {
				b.Fatal(err)
			}
			db := openPod(b, store, sch, opts)
			for start := 0; start < rows; start += 5000 {
				if _, err := db.Update(ctx, func(tx *Tx) error {
					for i := start; i < min(start+5000, rows); i++ {
						if err := putUser(sch, fmt.Sprintf("old%08d", i))(tx); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			before := store.written.Load()
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.Update(ctx, putUser(sch, fmt.Sprintf("new%08d", i))); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(store.written.Load()-before)/float64(b.N), "uploaded-B/op")
		})
	}
}
