package filestore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func txKey(i int) string { return fmt.Sprintf("%08d", i) }

// commitPair writes transaction i: one row in each of two tables, so a
// half-applied transaction would show up as a row in one table only.
func commitPair(db *DB, s *testSchema, i int) error {
	return db.Update(context.Background(), func(tx *Tx) error {
		if err := s.rows.Put(tx, testRow{ID: txKey(i), N: i, Group: strconv.Itoa(i % 5)}); err != nil {
			return err
		}
		return s.pairs.Put(tx, testRow{ID: txKey(i), N: i})
	})
}

// checkCommits verifies what survived: transactions 1..n are all present in
// both tables, where acked <= n <= acked+1 (the one write in flight at the
// crash may or may not have reached the disk). It returns n.
func checkCommits(t *testing.T, db *DB, s *testSchema, acked int, context string) int {
	t.Helper()
	v := db.View()
	got := func(tbl *Table[testRow]) []string {
		var out []string
		tbl.Ascend(v, func(r testRow) bool { out = append(out, r.ID); return true })
		return out
	}
	rows, pairs := got(s.rows), got(s.pairs)
	require.Equal(t, rows, pairs, "%s: a transaction was half applied", context)
	n := len(rows)
	for i, id := range rows {
		require.Equal(t, txKey(i+1), id, "%s: transactions are not a contiguous prefix", context)
	}
	require.GreaterOrEqual(t, n, acked, "%s: an acknowledged transaction was lost (F1)", context)
	require.LessOrEqual(t, n, acked+1, "%s: a transaction appeared that was never attempted", context)
	return n
}

// TestCrashAtAnyPoint crashes the machine at a random filesystem operation
// while transactions, log rotation, snapshots and compaction are running.
// It reboots with a random part of the unsynced data surviving, and checks
// that nothing acknowledged was lost and no transaction was half applied
// (F1, F6). Each store is crashed several times in a row.
func TestCrashAtAnyPoint(t *testing.T) {
	seed := time.Now().UnixNano()
	if s := os.Getenv("FILESTORE_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("seed %d (set FILESTORE_SEED to repeat)", seed)
	rng := rand.New(rand.NewSource(seed))

	trials := 400
	if testing.Short() {
		trials = 50
	}
	for trial := range trials {
		m := newMemFS()
		s := newTestSchema()
		opts := memOpts(t, m)
		opts.Logf = func(string, ...any) {}
		opts.SegmentSize = int64(200 + rng.Intn(2000))
		opts.SnapshotEvery = 1 + rng.Intn(15)
		if rng.Intn(4) == 0 {
			opts.SnapshotEvery = -1
		}

		acked := 0
		for round := range 4 {
			ctx := fmt.Sprintf("trial %d round %d", trial, round)
			if rng.Intn(4) == 0 {
				m.crashAfter(1 + rng.Intn(6)) // crash while opening and recovering
			}
			db, err := Open(memDir, s.Schema, opts)
			if errors.Is(err, errCrashed) {
				m.reboot(rng)
				db, err = Open(memDir, s.Schema, opts)
			}
			require.NoError(t, err, "%s: open after crash", ctx)
			acked = checkCommits(t, db, s, acked, ctx)

			m.crashAfter(1 + rng.Intn(80))
			for i := acked + 1; ; i++ {
				if err := commitPair(db, s, i); err != nil {
					require.True(t, m.isCrashed(), "%s: unexpected error: %v", ctx, err)
					break
				}
				acked = i
			}
			_ = db.Close()
			m.reboot(rng)
		}

		db, err := Open(memDir, s.Schema, opts)
		require.NoError(t, err, "trial %d: final open", trial)
		checkCommits(t, db, s, acked, fmt.Sprintf("trial %d final", trial))
		require.NoError(t, db.Close())
	}
}

const childDirEnv = "FILESTORE_KILL_CHILD_DIR"

// TestKillProcess kills a real process, on the real filesystem, while it
// commits as fast as it can, then checks what survived (F1, F14). It runs
// on both Windows and Linux.
func TestKillProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("starts child processes")
	}
	dir := t.TempDir()
	s := newTestSchema()
	acked := 0
	for round := range 8 {
		ctx := fmt.Sprintf("round %d", round)
		cmd := exec.Command(os.Args[0], "-test.run=^TestKillProcessChild$")
		cmd.Env = append(os.Environ(), childDirEnv+"="+dir)
		out, err := cmd.StdoutPipe()
		require.NoError(t, err)
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Start())

		var last atomic.Int64
		first := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				if n, ok := strings.CutPrefix(sc.Text(), "ack "); ok {
					i, _ := strconv.Atoi(n)
					if last.Swap(int64(i)) == 0 {
						close(first)
					}
				}
			}
		}()
		select {
		case <-first:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("%s: child never committed", ctx)
		}
		time.Sleep(time.Duration(50+rand.Intn(250)) * time.Millisecond)
		require.NoError(t, cmd.Process.Kill())
		_ = cmd.Wait()
		acked = max(acked, int(last.Load()))

		// Windows releases a dead process's file locks asynchronously.
		var db *DB
		require.Eventually(t, func() bool {
			db, err = Open(dir, s.Schema, Options{Logf: t.Logf})
			return !errors.Is(err, ErrLocked)
		}, 10*time.Second, 50*time.Millisecond)
		require.NoError(t, err, "%s: open after kill", ctx)
		acked = checkCommits(t, db, s, acked, ctx)
		t.Logf("%s: %d transactions survived", ctx, acked)
		require.NoError(t, db.Close())
	}
}

// TestKillProcessChild is the child process for TestKillProcess. It commits
// transactions forever and prints each one after Update returns.
func TestKillProcessChild(t *testing.T) {
	dir := os.Getenv(childDirEnv)
	if dir == "" {
		t.Skip("helper process for TestKillProcess")
	}
	s := newTestSchema()
	db, err := Open(dir, s.Schema, Options{SegmentSize: 8 << 10, SnapshotEvery: 40, Logf: func(string, ...any) {}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "child: open:", err)
		os.Exit(1)
	}
	for i := s.rows.Len(db.View()) + 1; ; i++ {
		if err := commitPair(db, s, i); err != nil {
			fmt.Fprintln(os.Stderr, "child: commit:", err)
			os.Exit(1)
		}
		if _, err := fmt.Fprintf(os.Stdout, "ack %d\n", i); err != nil {
			os.Exit(1)
		}
	}
}

func TestLockPreventsSecondOpen(t *testing.T) {
	dir := t.TempDir()
	s := newTestSchema()
	db, err := Open(dir, s.Schema, Options{Logf: t.Logf})
	require.NoError(t, err)
	_, err = Open(dir, newTestSchema().Schema, Options{Logf: t.Logf})
	require.ErrorIs(t, err, ErrLocked, "a second process must not open the same data (F11)")
	require.NoError(t, db.Close())

	db, err = Open(dir, s.Schema, Options{Logf: t.Logf})
	require.NoError(t, err, "the lock is released on close")
	require.NoError(t, db.Close())
}
