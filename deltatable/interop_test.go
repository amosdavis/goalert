package deltatable

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/objstore"
)

type interopAlert struct {
	ID       int64       `parquet:"id"`
	Summary  string      `parquet:"summary"`
	Active   bool        `parquet:"active"`
	Level    int32       `parquet:"level"`
	Score    float64     `parquet:"score"`
	Blob     []byte      `parquet:"blob"`
	Created  time.Time   `parquet:"created,timestamp(microsecond:utc)"`
	Closed   *time.Time  `parquet:"closed,optional,timestamp(microsecond:utc)"`
	Owner    *string     `parquet:"owner,optional"`
	Tags     []string    `parquet:"tags,list"`
	Detail   testDetail  `parquet:"detail"`
	Previous *testDetail `parquet:"previous,optional"`
}

type interopUser struct {
	ID   string `parquet:"id"`
	Name string `parquet:"name"`
}

// TestWriteInteropTable writes a small Delta table for another Delta reader
// to check (F35). CI runs it with GOALERT_DELTA_INTEROP_DIR set, then reads
// the table with delta-rs (testdata/check_interop.py) and compares the rows
// with expected.json.
func TestWriteInteropTable(t *testing.T) {
	dir := os.Getenv("GOALERT_DELTA_INTEROP_DIR")
	if dir == "" {
		t.Skip("set GOALERT_DELTA_INTEROP_DIR to write the interop table")
	}
	dir = filepath.Join(dir, "deltatable")
	ctx := context.Background()
	store, err := objstore.NewDir(filepath.Join(dir, "table"))
	require.NoError(t, err)

	alerts, err := NewRowType[interopAlert]("alerts")
	require.NoError(t, err)
	users, err := NewRowType[interopUser]("users")
	require.NoError(t, err)
	schema, err := SchemaString(alerts, users)
	require.NoError(t, err)

	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	const tableID = "a1b2c3d4-0000-4000-8000-00000000f00d"
	log := deltalog.New(store, "interop-test")
	require.NoError(t, log.Create(ctx, deltalog.MetaData{
		ID: tableID, Name: "goalert", SchemaString: schema, PartitionColumns: PartitionColumns(),
	}, at))

	closed := at.Add(time.Hour)
	owner := "bob"
	alertRows := []interopAlert{
		{
			ID: 1, Summary: "disk full", Active: true, Level: 2, Score: 0.5, Blob: []byte{0xde, 0xad},
			Created: at.Add(123456 * time.Microsecond), Closed: &closed, Owner: &owner,
			Tags: []string{"db", "prod"}, Detail: testDetail{Note: "n", Count: 3}, Previous: &testDetail{Note: "p", Count: 1},
		},
		{ID: 2, Summary: "cpu", Created: at, Tags: []string{}, Detail: testDetail{}},
	}
	userRows := []interopUser{{ID: "u1", Name: "Bob"}, {ID: "u2", Name: "Jane"}, {ID: "u3", Name: "Ann"}}

	// Version 1 adds a file in each table's bucket. Version 2 replaces the
	// users bucket 0 file, the way a GoAlert update rewrites a bucket.
	a1, err := WriteFile(ctx, store, alerts, 3, alertRows, at)
	require.NoError(t, err)
	u1, err := WriteFile(ctx, store, users, 0, userRows[:1], at)
	require.NoError(t, err)
	_, err = log.Commit(ctx, 1, tableID, "WRITE", at, []deltalog.Action{{Add: &a1}, {Add: &u1}})
	require.NoError(t, err)

	u2, err := WriteFile(ctx, store, users, 0, userRows[:2], at)
	require.NoError(t, err)
	_, err = log.Commit(ctx, 2, tableID, "WRITE", at, []deltalog.Action{
		{Remove: &deltalog.Remove{Path: u1.Path, DeletionTimestamp: at.UnixMilli(), DataChange: true}},
		{Add: &u2},
	})
	require.NoError(t, err)

	// Version 2 is checkpointed, version 3 follows, and the commits the
	// checkpoint covers are deleted, as log cleanup will do. The other
	// reader can only get there through GoAlert's checkpoint.
	s, err := log.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, log.WriteCheckpoint(ctx, s, at, deltalog.DefaultTombstoneRetention))
	u3, err := WriteFile(ctx, store, users, 1, userRows[2:], at)
	require.NoError(t, err)
	_, err = log.Commit(ctx, 3, tableID, "WRITE", at, []deltalog.Action{{Add: &u3}})
	require.NoError(t, err)
	for v := int64(0); v <= 2; v++ {
		require.NoError(t, store.Delete(ctx, deltalog.CommitName(v)))
	}
	s, err = log.Load(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, s.Version)
	require.ElementsMatch(t, []string{a1.Path, u2.Path, u3.Path}, s.FilePaths())

	ts := func(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000+00:00") }
	expectAlert := func(a interopAlert) map[string]any {
		m := map[string]any{
			"id": a.ID, "summary": a.Summary, "active": a.Active, "level": a.Level, "score": a.Score,
			"blob": hex.EncodeToString(a.Blob), "created": ts(a.Created), "closed": nil, "owner": a.Owner,
			"tags": a.Tags, "detail": map[string]any{"note": a.Detail.Note, "count": a.Detail.Count}, "previous": nil,
		}
		if a.Closed != nil {
			m["closed"] = ts(*a.Closed)
		}
		if a.Previous != nil {
			m["previous"] = map[string]any{"note": a.Previous.Note, "count": a.Previous.Count}
		}
		return m
	}
	var expectUsers []any
	for _, u := range userRows {
		expectUsers = append(expectUsers, map[string]any{"id": u.ID, "name": u.Name})
	}
	expected := map[string]any{
		"version": 3,
		"files":   []string{a1.Path, u2.Path, u3.Path},
		"tables": map[string]any{
			"alerts": []any{expectAlert(alertRows[0]), expectAlert(alertRows[1])},
			"users":  expectUsers,
		},
	}
	data, err := json.MarshalIndent(expected, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "expected.json"), data, 0o644))
}
