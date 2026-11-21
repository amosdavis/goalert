package filestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const memDir = "/data"

type testRow struct {
	ID    string
	Group string    `json:",omitempty"`
	Email string    `json:",omitempty"`
	N     int       `json:",omitempty"`
	At    time.Time `json:",omitzero"`
}

type testSchema struct {
	*Schema
	rows    *Table[testRow]
	pairs   *Table[testRow]
	byGroup *Index[testRow]
	byEmail *Unique[testRow]
}

func newTestSchema() *testSchema {
	s := &testSchema{Schema: NewSchema()}
	s.rows = NewTable(s.Schema, "rows", func(r testRow) string { return r.ID })
	s.pairs = NewTable(s.Schema, "pairs", func(r testRow) string { return r.ID })
	s.byGroup = s.rows.Index("group", func(r testRow) []string {
		if r.Group == "" {
			return nil
		}
		return []string{r.Group}
	})
	s.byEmail = s.rows.Unique("email", func(r testRow) (string, bool) { return r.Email, r.Email != "" })
	return s
}

func memOpts(t *testing.T, m *memFS) Options {
	return Options{fs: m, skipPlatform: true, Logf: t.Logf, SnapshotEvery: -1, SnapshotBytes: -1}
}

func openMem(t *testing.T, m *memFS, s *testSchema, opts Options) *DB {
	t.Helper()
	db, err := Open(memDir, s.Schema, opts)
	require.NoError(t, err)
	return db
}

func put(t *testing.T, db *DB, tbl *Table[testRow], rows ...testRow) {
	t.Helper()
	err := db.Update(context.Background(), func(tx *Tx) error {
		for _, r := range rows {
			if err := tbl.Put(tx, r); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// dump returns every row of every table as JSON, for comparing states.
func dump(t *testing.T, v *View) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	for name, d := range v.tables {
		rows := map[string]string{}
		require.NoError(t, d.encodeRows(func(key string, raw []byte) error {
			rows[key] = string(raw)
			return nil
		}))
		out[name] = rows
	}
	return out
}

func ids(v *View, tbl *Table[testRow]) []string {
	var out []string
	tbl.Ascend(v, func(r testRow) bool { out = append(out, r.ID); return true })
	return out
}

func TestCRUD(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	put(t, db, s.rows, testRow{ID: "b", N: 2}, testRow{ID: "a", N: 1}, testRow{ID: "c", N: 3})
	assert.Equal(t, []string{"a", "b", "c"}, ids(db.View(), s.rows))
	assert.Equal(t, 3, s.rows.Len(db.View()))

	put(t, db, s.rows, testRow{ID: "b", N: 20})
	r, ok := s.rows.Get(db.View(), "b")
	require.True(t, ok)
	assert.Equal(t, 20, r.N)

	err := db.Update(context.Background(), func(tx *Tx) error {
		ok, err := s.rows.Delete(tx, "a")
		assert.True(t, ok)
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "c"}, ids(db.View(), s.rows))

	var desc []string
	s.rows.Descend(db.View(), func(r testRow) bool { desc = append(desc, r.ID); return true })
	assert.Equal(t, []string{"c", "b"}, desc)
}

func TestIndexes(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	put(t, db, s.rows,
		testRow{ID: "1", Group: "x", Email: "a@x"},
		testRow{ID: "2", Group: "x"},
		testRow{ID: "3", Group: "y", Email: "b@x"},
	)
	group := func(g string) []string {
		var out []string
		s.byGroup.Ascend(db.View(), g, func(r testRow) bool { out = append(out, r.ID); return true })
		return out
	}
	assert.Equal(t, []string{"1", "2"}, group("x"))
	assert.Equal(t, []string{"3"}, group("y"))
	assert.Equal(t, 2, s.byGroup.Count(db.View(), "x"))

	r, ok := s.byEmail.Get(db.View(), "b@x")
	require.True(t, ok)
	assert.Equal(t, "3", r.ID)

	// A duplicate unique key is refused and changes nothing (F8).
	err := db.Update(context.Background(), func(tx *Tx) error {
		return s.rows.Put(tx, testRow{ID: "4", Email: "a@x"})
	})
	var uerr *UniqueError
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, "email", uerr.Index)
	assert.Equal(t, 3, s.rows.Len(db.View()))

	// Moving a row to another group or email updates the indexes.
	put(t, db, s.rows, testRow{ID: "1", Group: "y", Email: "c@x"})
	assert.Equal(t, []string{"2"}, group("x"))
	assert.Equal(t, []string{"1", "3"}, group("y"))
	_, ok = s.byEmail.Get(db.View(), "a@x")
	assert.False(t, ok, "old unique key released")
	put(t, db, s.rows, testRow{ID: "4", Email: "a@x"}) // now allowed

	// Deleting a row releases its keys.
	require.NoError(t, db.Update(context.Background(), func(tx *Tx) error {
		_, err := s.rows.Delete(tx, "3")
		return err
	}))
	assert.Equal(t, []string{"1"}, group("y"))
	_, ok = s.byEmail.Get(db.View(), "b@x")
	assert.False(t, ok)
}

func TestFailedTransactionChangesNothing(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	put(t, db, s.rows, testRow{ID: "keep"})
	before := db.View()
	boom := errors.New("boom")
	err := db.Update(context.Background(), func(tx *Tx) error {
		require.NoError(t, s.rows.Put(tx, testRow{ID: "new"}))
		_, err := s.rows.Delete(tx, "keep")
		require.NoError(t, err)
		_, err = tx.NextID("ids")
		require.NoError(t, err)
		return boom
	})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, before.Index(), db.View().Index())
	assert.Equal(t, []string{"keep"}, ids(db.View(), s.rows))

	var id int64
	require.NoError(t, db.Update(context.Background(), func(tx *Tx) (err error) {
		id, err = tx.NextID("ids")
		return err
	}))
	assert.EqualValues(t, 1, id, "a failed transaction does not use up a sequence value")
}

func TestReadersSeeASnapshot(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	put(t, db, s.rows, testRow{ID: "a", N: 1})
	old := db.View()
	put(t, db, s.rows, testRow{ID: "a", N: 2}, testRow{ID: "b"})

	r, _ := s.rows.Get(old, "a")
	assert.Equal(t, 1, r.N, "an old view never changes (F10)")
	assert.Equal(t, []string{"a"}, ids(old, s.rows))
	r, _ = s.rows.Get(db.View(), "a")
	assert.Equal(t, 2, r.N)
}

func TestTxSeesOwnWrites(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	require.NoError(t, db.Update(context.Background(), func(tx *Tx) error {
		require.NoError(t, s.rows.Put(tx, testRow{ID: "a", Group: "g"}))
		_, ok := s.rows.Get(tx, "a")
		assert.True(t, ok)
		assert.Equal(t, 1, s.byGroup.Count(tx, "g"))
		_, ok = s.rows.Get(db.View(), "a")
		assert.False(t, ok, "readers don't see uncommitted changes")
		return nil
	}))
}

func TestTxNowAndUseAfterUpdate(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	clock := NewFakeClock(time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("x", 3600)))
	clock.SetSpeed(0)
	opts := memOpts(t, m)
	opts.Clock = clock
	db := openMem(t, m, s, opts)
	defer db.Close()

	var saved *Tx
	require.NoError(t, db.Update(context.Background(), func(tx *Tx) error {
		saved = tx
		assert.Equal(t, time.Date(2026, 1, 2, 2, 4, 5, 123456000, time.UTC), tx.Now())
		assert.Equal(t, time.UTC, tx.Now().Location())
		return nil
	}))
	assert.ErrorIs(t, s.rows.Put(saved, testRow{ID: "late"}), ErrTxDone)
}

func TestUpdateHonorsContext(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	defer db.Close()

	inside, release := make(chan struct{}), make(chan struct{})
	done := make(chan error)
	go func() {
		done <- db.Update(context.Background(), func(tx *Tx) error {
			close(inside)
			<-release
			return nil
		})
	}()
	<-inside
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, db.Update(ctx, func(*Tx) error { return nil }), context.DeadlineExceeded)
	close(release)
	require.NoError(t, <-done)
}

func TestReopenReplaysTheLog(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	opts := memOpts(t, m)
	opts.SegmentSize = 512 // several segments
	db := openMem(t, m, s, opts)
	for i := range 50 {
		put(t, db, s.rows, testRow{ID: fmt.Sprint(i), Group: fmt.Sprint(i % 3), N: i})
	}
	require.NoError(t, db.Update(context.Background(), func(tx *Tx) error {
		_, err := s.rows.Delete(tx, "7")
		return err
	}))
	want := dump(t, db.View())
	wantIndex := db.View().Index()
	require.NoError(t, db.Close())

	segs := 0
	names, _ := m.ReadDir(memDir)
	for _, n := range names {
		if strings.HasSuffix(n, walExt) {
			segs++
		}
	}
	assert.Greater(t, segs, 1, "the test should cover several segments")

	db = openMem(t, m, s, opts)
	defer db.Close()
	assert.Equal(t, want, dump(t, db.View()))
	assert.Equal(t, wantIndex, db.View().Index())
	assert.Equal(t, 17, s.byGroup.Count(db.View(), "0"), "indexes are rebuilt")
	put(t, db, s.rows, testRow{ID: "after"})
	assert.Equal(t, wantIndex+1, db.View().Index())
}

func TestSequencesPersist(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	next := func() int64 {
		var id int64
		require.NoError(t, db.Update(context.Background(), func(tx *Tx) (err error) {
			id, err = tx.NextID("alerts")
			return err
		}))
		return id
	}
	assert.EqualValues(t, 1, next())
	assert.EqualValues(t, 2, next())
	require.NoError(t, db.Close())
	db = openMem(t, m, s, memOpts(t, m))
	defer db.Close()
	assert.EqualValues(t, 3, next())
}

// TestReplayIsDeterministic runs the same changes against a store that
// snapshots often and one that never does, and checks that the live states,
// the states rebuilt from disk, and each other all match (F25).
func TestReplayIsDeterministic(t *testing.T) {
	run := func(snapEvery int) (live, reopened map[string]map[string]string, m *memFS) {
		s := newTestSchema()
		m = newMemFS()
		opts := memOpts(t, m)
		opts.SnapshotEvery = snapEvery
		opts.SegmentSize = 1024
		clock := NewFakeClock(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
		clock.SetSpeed(0)
		opts.Clock = clock
		db := openMem(t, m, s, opts)
		for i := range 300 {
			clock.Advance(time.Second)
			require.NoError(t, db.Update(context.Background(), func(tx *Tx) error {
				id, err := tx.NextID("n")
				if err != nil {
					return err
				}
				r := testRow{ID: fmt.Sprint(i % 40), Group: fmt.Sprint(i % 7), N: int(id), At: tx.Now()}
				if i%5 == 0 {
					r.Email = fmt.Sprintf("u%d@x", i)
				}
				if err := s.rows.Put(tx, r); err != nil {
					return err
				}
				if i%11 == 0 {
					if _, err := s.rows.Delete(tx, fmt.Sprint((i+3)%40)); err != nil {
						return err
					}
				}
				return s.pairs.Put(tx, testRow{ID: fmt.Sprint(i % 13), N: i})
			}))
		}
		live = dump(t, db.View())
		require.NoError(t, db.Close())
		db = openMem(t, m, s, opts)
		reopened = dump(t, db.View())
		require.NoError(t, db.Close())
		return live, reopened, m
	}

	liveA, reopenA, mA := run(-1)
	liveB, reopenB, mB := run(17)
	assert.Equal(t, liveA, reopenA, "log replay rebuilds the live state")
	assert.Equal(t, liveB, reopenB, "snapshot + log rebuilds the live state")
	assert.Equal(t, liveA, liveB, "snapshots don't change the state")

	snaps, err := listSnapshots(mB, memDir)
	require.NoError(t, err)
	assert.Len(t, snaps, 2, "only the two newest snapshots are kept")
	segsA, _ := mA.ReadDir(memDir)
	segsB, _ := mB.ReadDir(memDir)
	assert.Less(t, len(segsB), len(segsA), "compaction removed old log segments")
}

// appendToLastSegment appends raw bytes to the newest log segment.
func appendToLastSegment(t *testing.T, m *memFS, b []byte) string {
	t.Helper()
	names, _ := m.ReadDir(memDir)
	var last string
	for _, n := range names {
		if strings.HasSuffix(n, walExt) {
			last = n
		}
	}
	require.NotEmpty(t, last)
	path := filepath.Join(memDir, last)
	data, err := m.ReadFile(path)
	require.NoError(t, err)
	m.put(path, append(data, b...))
	return path
}

func TestUnfinishedWriteIsDropped(t *testing.T) {
	cases := map[string]func(rec []byte) []byte{
		"partial header":  func(rec []byte) []byte { return rec[:5] },
		"partial payload": func(rec []byte) []byte { return rec[:len(rec)-3] },
		"bad checksum":    func(rec []byte) []byte { r := bytes.Clone(rec); r[len(r)-1] ^= 0xff; return r },
		"zeros":           func([]byte) []byte { return make([]byte, 300) },
		"garbage length":  func(rec []byte) []byte { r := bytes.Clone(rec); r[0], r[3] = 0xff, 0x7f; return r },
	}
	for name, tear := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTestSchema()
			m := newMemFS()
			db := openMem(t, m, s, memOpts(t, m))
			put(t, db, s.rows, testRow{ID: "a"}, testRow{ID: "b"})
			want := dump(t, db.View())
			index := db.View().Index()
			require.NoError(t, db.Close())

			rec := encodeRecord(encodePayload(index+1, time.Now(), []mutation{{op: opPut, table: "rows", key: "c", val: []byte(`{"ID":"c"}`)}}))
			path := appendToLastSegment(t, m, tear(rec))
			sizeBefore := len(must(m.ReadFile(path)))

			db = openMem(t, m, s, memOpts(t, m))
			defer db.Close()
			assert.Equal(t, want, dump(t, db.View()))
			assert.Less(t, len(must(m.ReadFile(path))), sizeBefore, "the unfinished write is truncated")
			put(t, db, s.rows, testRow{ID: "d"}) // the log is usable again
			require.NoError(t, db.Close())
			db = openMem(t, m, s, memOpts(t, m))
			assert.Equal(t, []string{"a", "b", "d"}, ids(db.View(), s.rows))
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestDamageBeforeValidRecordsStopsStartup(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	for i := range 5 {
		put(t, db, s.rows, testRow{ID: fmt.Sprint(i)})
	}
	require.NoError(t, db.Close())

	names, _ := m.ReadDir(memDir)
	var seg string
	for _, n := range names {
		if strings.HasSuffix(n, walExt) {
			seg = filepath.Join(memDir, n)
		}
	}
	data := must(m.ReadFile(seg))
	data[walHeaderSize+recordHeaderSize+2] ^= 0x01 // damage the first record
	m.put(seg, data)

	_, err := Open(memDir, s.Schema, memOpts(t, m))
	require.ErrorIs(t, err, ErrCorrupt, "acknowledged data after the damage must not be dropped (F6)")
}

func TestDamagedSnapshotFallsBack(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	for i := range 30 {
		put(t, db, s.rows, testRow{ID: fmt.Sprint(i)})
		if i == 9 || i == 19 {
			require.NoError(t, db.Snapshot())
		}
	}
	want := dump(t, db.View())
	require.NoError(t, db.Close())

	snaps, err := listSnapshots(m, memDir)
	require.NoError(t, err)
	require.Len(t, snaps, 2)
	newest := filepath.Join(memDir, snapshotName(snaps[1]))
	data := must(m.ReadFile(newest))
	data[len(data)/2] ^= 0xff
	m.put(newest, data)

	db = openMem(t, m, s, memOpts(t, m))
	defer db.Close()
	assert.Equal(t, want, dump(t, db.View()), "the older snapshot plus the log rebuild everything")
	left, err := listSnapshots(m, memDir)
	require.NoError(t, err)
	assert.Equal(t, snaps[:1], left, "the damaged snapshot no longer counts")
	_, err = m.ReadFile(newest + damagedExt)
	assert.NoError(t, err, "the damaged snapshot is kept for inspection")
}

func TestUnknownTableStopsStartup(t *testing.T) {
	m := newMemFS()
	s := newTestSchema()
	db := openMem(t, m, s, memOpts(t, m))
	put(t, db, s.pairs, testRow{ID: "p"})
	require.NoError(t, db.Close())

	older := NewSchema()
	NewTable(older, "rows", func(r testRow) string { return r.ID })
	_, err := Open(memDir, older, memOpts(t, m))
	require.ErrorIs(t, err, ErrVersion, "a table this version does not know must stop startup (F19)")
}

func TestUnknownFieldStopsStartup(t *testing.T) {
	m := newMemFS()
	s := newTestSchema()
	db := openMem(t, m, s, memOpts(t, m))
	put(t, db, s.rows, testRow{ID: "a", Email: "a@x"})
	require.NoError(t, db.Close())

	type olderRow struct{ ID string }
	older := NewSchema()
	NewTable(older, "rows", func(r olderRow) string { return r.ID })
	NewTable(older, "pairs", func(r olderRow) string { return r.ID })
	_, err := Open(memDir, older, memOpts(t, m))
	require.ErrorIs(t, err, ErrVersion, "dropping a field written by a newer version would lose data (F19)")
}

func TestRefusesNonStoreDirectory(t *testing.T) {
	m := newMemFS()
	m.put(filepath.Join(memDir, "notes.txt"), []byte("hello"))
	_, err := Open(memDir, newTestSchema().Schema, memOpts(t, m))
	require.ErrorIs(t, err, ErrNotStore)
}

func TestRefusesForeignSegment(t *testing.T) {
	m := newMemFS()
	s := newTestSchema()
	db := openMem(t, m, s, memOpts(t, m))
	put(t, db, s.rows, testRow{ID: "a"})
	require.NoError(t, db.Close())

	var other [16]byte
	other[0] = 1
	m.put(filepath.Join(memDir, segmentName(3)), encodeWALHeader(other, 3))
	_, err := Open(memDir, s.Schema, memOpts(t, m))
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestWriteErrorStopsWrites(t *testing.T) {
	for _, mode := range []string{"write", "sync"} {
		t.Run(mode, func(t *testing.T) {
			s := newTestSchema()
			m := newMemFS()
			db := openMem(t, m, s, memOpts(t, m))
			put(t, db, s.rows, testRow{ID: "a"})

			diskFull := errors.New("no space left on device")
			m.mu.Lock()
			if mode == "write" {
				m.failWrite, m.failWritePartial = diskFull, 7
			} else {
				m.failSync = diskFull
			}
			m.mu.Unlock()

			err := db.Update(context.Background(), func(tx *Tx) error { return s.rows.Put(tx, testRow{ID: "b"}) })
			require.ErrorIs(t, err, diskFull)
			assert.Equal(t, []string{"a"}, ids(db.View(), s.rows), "a failed write is never visible")
			require.ErrorIs(t, db.Health(), diskFull, "health goes red (F21)")
			err = db.Update(context.Background(), func(tx *Tx) error { return s.rows.Put(tx, testRow{ID: "c"}) })
			require.Error(t, err, "later writes are refused")
			_ = db.Close()

			db = openMem(t, m, s, memOpts(t, m))
			defer db.Close()
			require.NoError(t, db.Health())
			got := ids(db.View(), s.rows)
			assert.Contains(t, got, "a")
			assert.NotContains(t, got, "c")
			if mode == "write" {
				assert.NotContains(t, got, "b", "a partial write is removed")
			}
		})
	}
}

func TestBackupAndRestore(t *testing.T) {
	s := newTestSchema()
	m := newMemFS()
	db := openMem(t, m, s, memOpts(t, m))
	for i := range 20 {
		put(t, db, s.rows, testRow{ID: fmt.Sprint(i), Group: "g"})
	}
	var buf bytes.Buffer
	require.NoError(t, db.Backup(&buf))
	want := dump(t, db.View())
	index := db.View().Index()
	require.NoError(t, db.Close())

	m2 := newMemFS()
	require.NoError(t, restore(memDir, s.Schema, bytes.NewReader(buf.Bytes()), m2, true))
	db = openMem(t, m2, s, memOpts(t, m2))
	defer db.Close()
	assert.Equal(t, want, dump(t, db.View()))
	assert.Equal(t, index, db.View().Index())
	assert.Equal(t, 20, s.byGroup.Count(db.View(), "g"))
	put(t, db, s.rows, testRow{ID: "new"})
	assert.Equal(t, index+1, db.View().Index())

	err := restore(memDir, s.Schema, bytes.NewReader(buf.Bytes()), m2, true)
	require.ErrorIs(t, err, ErrNotStore, "restore never overwrites a store")

	damaged := bytes.Clone(buf.Bytes())
	damaged[40] ^= 0xff
	require.ErrorIs(t, restore("/other", s.Schema, bytes.NewReader(damaged), newMemFS(), true), ErrCorrupt)
}

func TestPayloadRoundTrip(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 8000, time.UTC)
	muts := []mutation{
		{op: opPut, table: "rows", key: "a", val: []byte(`{"ID":"a"}`)},
		{op: opDelete, table: "pairs", key: "b"},
	}
	index, gotAt, got, err := decodePayload(encodePayload(42, at, muts))
	require.NoError(t, err)
	assert.EqualValues(t, 42, index)
	assert.Equal(t, at, gotAt)
	assert.Equal(t, muts, got)

	bad := encodePayload(42, at, muts)
	binary.LittleEndian.PutUint32(bad[16:], 1<<30)
	_, _, _, err = decodePayload(bad)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewFakeClock(start)
	c.SetSpeed(0)
	frozen := c.Now()
	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, frozen, c.Now(), "speed 0 freezes time")
	c.Advance(time.Hour)
	assert.Equal(t, frozen.Add(time.Hour), c.Now())
	c.Set(start)
	assert.Equal(t, start, c.Now())
	c.SetSpeed(1000)
	time.Sleep(5 * time.Millisecond)
	assert.Greater(t, c.Now().Sub(start), 4*time.Second, "speed 1000 runs 1000x")
}
