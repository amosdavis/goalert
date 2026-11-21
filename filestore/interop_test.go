package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
)

type interopUser struct {
	ID      string     `parquet:"id"`
	Name    string     `parquet:"name"`
	Created time.Time  `parquet:"created,timestamp(microsecond:utc)"`
	Deleted *time.Time `parquet:"deleted,optional,timestamp(microsecond:utc)"`
	Tags    []string   `parquet:"tags,list"`
	Count   int64      `parquet:"count"`
}

type interopTeam struct {
	ID      string   `parquet:"id"`
	Members []string `parquet:"members,list"`
}

// TestWriteDeltaInteropTable writes a table through the store, the way
// GoAlert does, for another Delta reader to check (F35): bucket rewrites,
// tombstones, the store's own sequences table, and a checkpoint that the
// reader has to use. CI runs it with GOALERT_DELTA_INTEROP_DIR set and reads
// the table with delta-rs (deltatable/testdata/check_interop.py).
func TestWriteDeltaInteropTable(t *testing.T) {
	root := os.Getenv("GOALERT_DELTA_INTEROP_DIR")
	if root == "" {
		t.Skip("set GOALERT_DELTA_INTEROP_DIR to write the interop table")
	}
	dir := filepath.Join(root, "filestore")
	ctx := context.Background()
	store, err := objstore.NewDir(filepath.Join(dir, "table"))
	require.NoError(t, err)

	s := NewSchema()
	users := NewTable(s, "users", func(u interopUser) string { return u.ID }).Buckets(4)
	teams := NewTable(s, "teams", func(tm interopTeam) string { return tm.ID }).Buckets(2)
	opts := DeltaOptions{WriterID: "interop", Clock: frozenClock(dStart), PollInterval: -1, CheckpointEvery: 3, Logf: t.Logf}
	require.NoError(t, CreateDelta(ctx, store, s, opts))
	db, err := OpenDelta(ctx, store, s, opts)
	require.NoError(t, err)
	update := func(fn func(tx *Tx) error) {
		t.Helper()
		_, err := db.Update(ctx, fn)
		require.NoError(t, err)
	}

	update(func(tx *Tx) error {
		for i := range 6 {
			u := interopUser{
				ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User %d", i), Count: int64(i), Tags: []string{"oncall"},
				Created: dStart.Add(time.Duration(i)*time.Minute + 123456*time.Microsecond),
			}
			if err := users.Put(tx, u); err != nil {
				return err
			}
		}
		if _, err := tx.NextID("alerts"); err != nil {
			return err
		}
		return teams.Put(tx, interopTeam{ID: "t1", Members: []string{"u1", "u2"}})
	})
	deleted := dStart.Add(2 * time.Hour)
	update(func(tx *Tx) error {
		u, _ := users.Get(tx, "u2")
		u.Name, u.Deleted, u.Tags = "Renamed", &deleted, nil
		return users.Put(tx, u)
	})
	update(func(tx *Tx) error {
		_, err := users.Delete(tx, "u3")
		return err
	})
	update(func(tx *Tx) error {
		if _, err := tx.NextID("alerts"); err != nil {
			return err
		}
		return teams.Put(tx, interopTeam{ID: "t2"})
	})
	written := dumpInterop(s, users, teams, db.View())
	require.NoError(t, db.Close(), "Close waits for the checkpoint of version 3")

	// Delete the commits the checkpoint covers, as log cleanup will.
	for v := int64(0); v <= 3; v++ {
		require.NoError(t, store.Delete(ctx, deltalog.CommitName(v)))
	}
	fresh, err := OpenDelta(ctx, store, s, opts)
	require.NoError(t, err)
	defer func() { _ = fresh.Close() }()
	view := fresh.View()
	require.Equal(t, written, dumpInterop(s, users, teams, view), "GoAlert reads back what it wrote (F25)")

	expected := map[string]any{
		"version": view.Index(),
		"files":   fresh.snap.FilePaths(),
		"tables":  written,
	}
	data, err := json.MarshalIndent(expected, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "expected.json"), data, 0o644))
}

// dumpInterop gives every table's rows as check_interop.py compares them.
func dumpInterop(s *Schema, users *Table[interopUser], teams *Table[interopTeam], r Reader) map[string][]map[string]any {
	ts := func(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000+00:00") }
	list := func(l []string) []string {
		if l == nil {
			return []string{}
		}
		return l
	}
	out := map[string][]map[string]any{"users": {}, "teams": {}, "_seq": {}}
	users.Ascend(r, func(u interopUser) bool {
		m := map[string]any{"id": u.ID, "name": u.Name, "created": ts(u.Created), "deleted": nil, "tags": list(u.Tags), "count": u.Count}
		if u.Deleted != nil {
			m["deleted"] = ts(*u.Deleted)
		}
		out["users"] = append(out["users"], m)
		return true
	})
	teams.Ascend(r, func(tm interopTeam) bool {
		out["teams"] = append(out["teams"], map[string]any{"id": tm.ID, "members": list(tm.Members)})
		return true
	})
	s.seq.Ascend(r, func(row seqRow) bool {
		out["_seq"] = append(out["_seq"], map[string]any{"name": row.Name, "value": row.Value})
		return true
	})
	return out
}
