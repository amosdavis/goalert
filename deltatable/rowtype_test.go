package deltatable

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testDetail struct {
	Note  string `parquet:"note"`
	Count int64  `parquet:"count"`
}

type testRow struct {
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

func TestRoundTrip(t *testing.T) {
	rt, err := NewRowType[testRow]("alerts")
	require.NoError(t, err)

	closed := time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)
	owner := "bob"
	rows := []testRow{
		{
			ID: 1, Summary: "disk full", Active: true, Level: 2, Score: 0.5, Blob: []byte{1, 2, 3},
			Created: time.Date(2026, 11, 21, 12, 0, 0, 123456000, time.UTC), Closed: &closed, Owner: &owner,
			Tags: []string{"db", "prod"}, Detail: testDetail{Note: "n", Count: 3}, Previous: &testDetail{Note: "p"},
		},
		{ID: 2, Summary: "", Created: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Tags: []string{}},
	}
	data, err := rt.Encode(rows)
	require.NoError(t, err)

	got, err := rt.Decode(data)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, rows[0], got[0])
	assert.EqualValues(t, 2, got[1].ID)
	assert.Nil(t, got[1].Closed, "missing optional values stay missing")
	assert.Nil(t, got[1].Owner)
	assert.Nil(t, got[1].Previous)
	assert.Empty(t, got[1].Tags)
}

func TestNormalize(t *testing.T) {
	rt, err := NewRowType[testRow]("alerts")
	require.NoError(t, err)
	in := testRow{ID: 1, Created: time.Date(2026, 11, 21, 12, 0, 0, 123456789, time.FixedZone("x", 3600))}
	out, err := rt.Normalize(in)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 11, 21, 11, 0, 0, 123456000, time.UTC), out.Created.UTC(),
		"times keep microsecond precision, as every pod will read them (F25)")
	again, err := rt.Normalize(out)
	require.NoError(t, err)
	assert.Equal(t, out, again, "normalizing twice changes nothing")
}

func TestDeltaSchema(t *testing.T) {
	rt, err := NewRowType[testRow]("alerts")
	require.NoError(t, err)

	var field map[string]any
	require.NoError(t, json.Unmarshal(rt.DeltaField(), &field))
	assert.Equal(t, "alerts", field["name"])
	assert.Equal(t, true, field["nullable"], "rows of other tables leave this column empty")

	typ := field["type"].(map[string]any)
	assert.Equal(t, "struct", typ["type"])
	types := map[string]any{}
	nullable := map[string]bool{}
	for _, f := range typ["fields"].([]any) {
		f := f.(map[string]any)
		types[f["name"].(string)] = f["type"]
		nullable[f["name"].(string)] = f["nullable"].(bool)
	}
	for name, want := range map[string]any{
		"id": "long", "summary": "string", "active": "boolean", "level": "integer",
		"score": "double", "blob": "binary", "created": "timestamp", "closed": "timestamp", "owner": "string",
	} {
		assert.Equal(t, want, types[name], name)
	}
	assert.Equal(t, map[string]any{"type": "array", "elementType": "string", "containsNull": false}, types["tags"])
	assert.Equal(t, "struct", types["detail"].(map[string]any)["type"])
	assert.False(t, nullable["id"])
	assert.True(t, nullable["closed"])
	assert.True(t, nullable["previous"])
}

func TestRejectsAmbiguousTypes(t *testing.T) {
	type millis struct {
		At time.Time `parquet:"at,timestamp(millisecond:utc)"`
	}
	_, err := NewRowType[millis]("t")
	assert.ErrorContains(t, err, "timestamp(microsecond:utc)")

	type local struct {
		At time.Time `parquet:"at,timestamp(microsecond:local)"`
	}
	_, err = NewRowType[local]("t")
	assert.Error(t, err, "timestamp_ntz needs a newer reader version (F35)")

	type withMap struct {
		M map[string]string `parquet:"m"`
	}
	_, err = NewRowType[withMap]("t")
	assert.Error(t, err)

	_, err = NewRowType[testRow]("Bad-Name")
	assert.Error(t, err, "table names become columns and directories")
}
