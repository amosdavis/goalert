package iceberglog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/iceberglog"
	"github.com/target/goalert/iceberglog/icebergtest"
	"github.com/target/goalert/objstore"
)

type interopUser struct {
	ID      string     `parquet:"id"`
	Name    string     `parquet:"name"`
	Created time.Time  `parquet:"created,timestamp(microsecond:utc)"`
	Deleted *time.Time `parquet:"deleted,optional,timestamp(microsecond:utc)"`
	Tags    []string   `parquet:"tags,list"`
	Count   int64      `parquet:"count"`
	Active  bool       `parquet:"active"`
}

type interopTeam struct {
	ID    string  `parquet:"id"`
	Boss  *string `parquet:"boss,optional"`
	Score float64 `parquet:"score"`
}

// TestWriteIcebergInteropTable writes a table through an Iceberg catalog for
// other readers to check (F35): pyiceberg reads it as Iceberg
// (testdata/check_interop.py), and delta-rs reads the same directory as Delta
// (deltatable/testdata/check_interop.py). CI runs it with
// GOALERT_DELTA_INTEROP_DIR set.
func TestWriteIcebergInteropTable(t *testing.T) {
	root := os.Getenv("GOALERT_DELTA_INTEROP_DIR")
	if root == "" {
		t.Skip("set GOALERT_DELTA_INTEROP_DIR to write the interop table")
	}
	ctx := context.Background()
	dir, err := filepath.Abs(filepath.Join(root, "iceberg"))
	require.NoError(t, err)
	store, err := objstore.NewDir(filepath.Join(dir, "table"))
	require.NoError(t, err)
	location := "file://" + filepath.ToSlash(filepath.Join(dir, "table"))
	cat := &icebergtest.Catalog{}
	open := func() *iceberglog.Log {
		l, err := iceberglog.New(iceberglog.Config{
			Catalog: cat, Ident: table.Identifier{"ns", "goalert"}, Store: store, Location: location,
			Snapshots: 2, Warn: func(err error) { t.Error(err) },
		}, "interop")
		require.NoError(t, err)
		return l
	}
	s := filestore.NewSchema()
	users := filestore.NewTable(s, "users", func(u interopUser) string { return u.ID }).Buckets(4)
	teams := filestore.NewTable(s, "teams", func(tm interopTeam) string { return tm.ID }).Buckets(2)
	opts := filestore.DeltaOptions{WriterID: "interop", PollInterval: -1, CheckpointEvery: 3, Logf: t.Logf}
	require.NoError(t, filestore.CreateBackend(ctx, store, open(), s, opts))
	db, err := filestore.OpenBackend(ctx, store, open(), s, opts)
	require.NoError(t, err)
	update := func(fn func(tx *filestore.Tx) error) {
		t.Helper()
		_, err := db.Update(ctx, fn)
		require.NoError(t, err)
	}
	update(func(tx *filestore.Tx) error {
		for i := range 8 {
			u := interopUser{
				ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User %d \u00e9", i), Count: int64(i), Tags: []string{"oncall", "sre"}[:i%3],
				Created: start.Add(time.Duration(i)*time.Minute + 123456*time.Microsecond), Active: i%2 == 0,
			}
			if err := users.Put(tx, u); err != nil {
				return err
			}
		}
		return teams.Put(tx, interopTeam{ID: "t1", Score: 1.5})
	})
	deleted, boss := start.Add(2*time.Hour), "u1"
	update(func(tx *filestore.Tx) error {
		u, _ := users.Get(tx, "u2")
		u.Name, u.Deleted, u.Tags = "Renamed", &deleted, nil
		return users.Put(tx, u)
	})
	update(func(tx *filestore.Tx) error {
		_, err := users.Delete(tx, "u3")
		return err
	})
	update(func(tx *filestore.Tx) error {
		_, err := teams.Delete(tx, "t1")
		if err != nil {
			return err
		}
		return teams.Put(tx, interopTeam{ID: "t2", Boss: &boss, Score: -0.25})
	})
	view := db.View()
	require.NoError(t, db.Close())

	rows := map[string][]any{"users": {}, "teams": {}}
	users.Ascend(view, func(u interopUser) bool { rows["users"] = append(rows["users"], u); return true })
	teams.Ascend(view, func(tm interopTeam) bool { rows["teams"] = append(rows["teams"], tm); return true })
	for name, rs := range rows {
		for i, r := range rs {
			rows[name][i] = interopJSON(t, r)
		}
	}
	snap, err := open().Load(ctx)
	require.NoError(t, err)
	write := func(name string, v any) {
		data, err := json.MarshalIndent(v, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))
	}
	write("expected.json", map[string]any{"version": snap.Version, "files": snap.FilePaths(), "tables": rows})
	write("iceberg.metadata.json", cat.Metadata())
	_, err = store.Get(ctx, deltalog.CommitName(snap.Version))
	require.NoError(t, err, "it is a Delta table too")
}

// interopJSON is a row in the form check_interop.py turns rows into.
func interopJSON(t *testing.T, row any) map[string]any {
	t.Helper()
	const layout = "2006-01-02T15:04:05.000000+00:00"
	switch r := row.(type) {
	case interopUser:
		m := map[string]any{"id": r.ID, "name": r.Name, "created": r.Created.UTC().Format(layout), "deleted": nil, "tags": append([]string{}, r.Tags...), "count": r.Count, "active": r.Active}
		if r.Deleted != nil {
			m["deleted"] = r.Deleted.UTC().Format(layout)
		}
		return m
	case interopTeam:
		m := map[string]any{"id": r.ID, "boss": nil, "score": r.Score}
		if r.Boss != nil {
			m["boss"] = *r.Boss
		}
		return m
	}
	t.Fatalf("unexpected row %T", row)
	return nil
}
