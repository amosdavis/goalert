package deltatable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/objstore"
)

type testUser struct {
	ID   string `parquet:"id"`
	Name string `parquet:"name"`
}

func TestBucketIsStable(t *testing.T) {
	// Golden values from an independent FNV-1a implementation. If these
	// change, rows move between buckets and existing tables stop finding
	// them (F25).
	for key, want := range map[string][2]int{
		"":                                     {5, 2},
		"a":                                    {12, 5},
		"abc":                                  {11, 5},
		"6f3c2c3e-0000-4000-8000-000000000001": {7, 1},
		"alert:12345":                          {4, 5},
	} {
		assert.Equal(t, want[0], Bucket(key, 16), "%q of 16", key)
		assert.Equal(t, want[1], Bucket(key, 7), "%q of 7", key)
		assert.Equal(t, 0, Bucket(key, 1), "%q of 1", key)
	}
}

func TestSchemaString(t *testing.T) {
	users, err := NewRowType[testUser]("users")
	require.NoError(t, err)
	alerts, err := NewRowType[testRow]("alerts")
	require.NoError(t, err)

	s, err := SchemaString(users, alerts)
	require.NoError(t, err)
	var schema struct {
		Type   string `json:"type"`
		Fields []struct {
			Name     string `json:"name"`
			Nullable bool   `json:"nullable"`
		} `json:"fields"`
	}
	require.NoError(t, json.Unmarshal([]byte(s), &schema))
	assert.Equal(t, "struct", schema.Type)
	var names []string
	for _, f := range schema.Fields {
		names = append(names, f.Name)
	}
	assert.Equal(t, []string{"tbl", "bucket", "alerts", "users"}, names, "partition columns, then tables by name")
	assert.False(t, schema.Fields[0].Nullable)
	assert.True(t, schema.Fields[2].Nullable)

	_, err = SchemaString(users, users)
	assert.Error(t, err)
	tbl, err := NewRowType[testUser]("tbl")
	require.NoError(t, err)
	_, err = SchemaString(tbl)
	assert.Error(t, err, "a table can't share a partition column's name")
}

func TestWriteFile(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMem()
	users, err := NewRowType[testUser]("users")
	require.NoError(t, err)
	rows := []testUser{{ID: "u1", Name: "Bob"}}
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	add, err := WriteFile(ctx, store, users, 4, rows, at)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(add.Path, "tbl=users/bucket=4/part-"), add.Path)
	assert.True(t, strings.HasSuffix(add.Path, ".snappy.parquet"), add.Path)
	uploaded, ok := FileTime(add.Path)
	assert.True(t, ok, "the name carries the upload time")
	assert.Equal(t, at, uploaded)
	_, ok = FileTime("tbl=users/bucket=4/part-1b2c.snappy.parquet")
	assert.False(t, ok)
	assert.Equal(t, map[string]string{"tbl": "users", "bucket": "4"}, add.PartitionValues)
	assert.Equal(t, `{"numRecords":1}`, add.Stats)
	assert.Equal(t, at.UnixMilli(), add.ModificationTime)
	assert.True(t, add.DataChange)

	got, err := ReadFile(ctx, store, users, add.Path)
	require.NoError(t, err)
	assert.Equal(t, rows, got)

	data, err := store.Get(ctx, add.Path)
	require.NoError(t, err)
	assert.EqualValues(t, len(data), add.Size)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, rootName, f.Metadata().Schema[0].Name, "the file's root schema has a name")

	other, err := WriteFile(ctx, store, users, 4, rows, at)
	require.NoError(t, err)
	assert.NotEqual(t, add.Path, other.Path, "every data file gets a new name (F36)")
}

func TestWriteFileUnclearUploads(t *testing.T) {
	old := uploadBackoff
	uploadBackoff = time.Millisecond
	t.Cleanup(func() { uploadBackoff = old })

	ctx := context.Background()
	users, err := NewRowType[testUser]("users")
	require.NoError(t, err)
	rows := []testUser{{ID: "u1", Name: "Bob"}}

	t.Run("retried under a new name", func(t *testing.T) {
		store := objstore.NewMem()
		var names []string
		store.BeforeWrite = func(op, name string) error {
			names = append(names, name)
			switch len(names) {
			case 1:
				return objstore.ErrMemAppliedThenFailed
			case 2:
				return objstore.Unknown(errors.New("connection reset"))
			}
			return nil
		}
		add, err := WriteFile(ctx, store, users, 0, rows, time.Now())
		require.NoError(t, err)
		require.Len(t, names, 3)
		assert.Len(t, map[string]bool{names[0]: true, names[1]: true, names[2]: true}, 3, "every attempt uses a new name (F36)")
		assert.Equal(t, names[2], add.Path)
		got, err := ReadFile(ctx, store, users, add.Path)
		require.NoError(t, err)
		assert.Equal(t, rows, got)
	})

	t.Run("gives up without an unknown outcome", func(t *testing.T) {
		store := objstore.NewMem()
		attempts := 0
		store.BeforeWrite = func(op, name string) error {
			attempts++
			return objstore.Unknown(errors.New("timeout"))
		}
		_, err := WriteFile(ctx, store, users, 0, rows, time.Now())
		require.Error(t, err)
		assert.NotErrorIs(t, err, objstore.ErrUnknown, "the table is unchanged, so this is a plain failure (F32)")
		assert.Equal(t, uploadAttempts, attempts)
	})

	t.Run("rejected outright", func(t *testing.T) {
		store := objstore.NewMem()
		attempts := 0
		denied := errors.New("403 forbidden")
		store.BeforeWrite = func(op, name string) error {
			attempts++
			return denied
		}
		_, err := WriteFile(ctx, store, users, 0, rows, time.Now())
		require.ErrorIs(t, err, denied)
		assert.Equal(t, 1, attempts, "a definite error is not retried")
	})

	t.Run("canceled", func(t *testing.T) {
		store := objstore.NewMem()
		store.BeforeWrite = func(op, name string) error { return objstore.Unknown(errors.New("timeout")) }
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := WriteFile(cctx, store, users, 0, rows, time.Now())
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, objstore.ErrUnknown)
	})
}
