package storagebackend_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/storagebackend"
	_ "github.com/target/goalert/storagebackend/dir"
	"github.com/target/goalert/tablelog"
)

type row struct {
	ID string `parquet:"id"`
}

// What the test drivers do; tests run one after another.
var (
	openObjects func(u *url.URL) (storagebackend.Objects, error)
	openLog     func(cfg storagebackend.LogConfig) (tablelog.Log, func() error, error)
)

func init() {
	storagebackend.RegisterObjects("test", func(ctx context.Context, u *url.URL) (storagebackend.Objects, error) { return openObjects(u) })
	storagebackend.RegisterLog("testlog", func(ctx context.Context, cfg storagebackend.LogConfig) (tablelog.Log, func() error, error) {
		return openLog(cfg)
	})
}

// TestRoundTrip creates, writes and reopens a table in storage picked by URL.
func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := strings.ReplaceAll(t.TempDir(), `\`, "/")
	if !strings.HasPrefix(dir, "/") {
		dir = "/" + dir // C:/...
	}
	cfg := storagebackend.Config{Objects: "file://" + dir + "/data"}
	opts := filestore.DeltaOptions{PollInterval: -1}
	open := func() (*storagebackend.Backend, *filestore.Schema, *filestore.Table[row]) {
		b, err := storagebackend.New(ctx, cfg, "test")
		require.NoError(t, err)
		schema := filestore.NewSchema()
		return b, schema, filestore.NewTable(schema, "users", func(r row) string { return r.ID })
	}

	b, schema, users := open()
	assert.Equal(t, "file://"+dir+"/data", b.Location)
	_, err := b.Open(ctx, schema, opts)
	require.ErrorIs(t, err, tablelog.ErrNoTable, "opening creates nothing (F27)")
	require.NoError(t, b.Create(ctx, schema, opts))
	require.ErrorIs(t, b.Create(ctx, schema, opts), tablelog.ErrTableExists)
	db, err := b.Open(ctx, schema, opts)
	require.NoError(t, err)
	_, err = db.Update(ctx, func(tx *filestore.Tx) error { return users.Put(tx, row{ID: "u1"}) })
	require.NoError(t, err)
	require.NoError(t, db.Close())
	problems, err := b.CheckSettings(ctx)
	require.NoError(t, err)
	assert.Empty(t, problems)
	require.NoError(t, b.Close())

	b, schema, users = open()
	db, err = b.Open(ctx, schema, opts)
	require.NoError(t, err)
	got, ok := users.Get(db.View(), "u1")
	assert.True(t, ok)
	assert.Equal(t, row{ID: "u1"}, got)
	require.NoError(t, db.Close())
	require.NoError(t, b.Close())
}

// TestConfigErrors checks that a wrong choice is refused before anything is
// contacted, and that the message has no secret in it (F20, F27, F48).
func TestConfigErrors(t *testing.T) {
	ctx := context.Background()
	opened := 0
	openObjects = func(u *url.URL) (storagebackend.Objects, error) {
		opened++
		return storagebackend.Objects{Store: objstore.NewMem()}, nil
	}
	openLog = func(cfg storagebackend.LogConfig) (tablelog.Log, func() error, error) {
		return deltalog.New(cfg.Objects.Store, cfg.WriterID), nil, nil
	}
	for name, cfg := range map[string]storagebackend.Config{
		"nothing":          {},
		"unknown":          {Objects: "nowhere://hunter2"},
		"no scheme":        {Objects: "/hunter2"},
		"unparsable":       {Objects: "test://hunter2\x7f"},
		"password":         {Objects: "test://user:hunter2@x"},
		"user":             {Objects: "test://hunter2@x"},
		"fragment":         {Objects: "test://x#hunter2"},
		"unknown log":      {Objects: "test://x", Log: "nolog://hunter2"},
		"log with a token": {Objects: "test://x", Log: "testlog://hunter2@x"},
		"unparsable log":   {Objects: "test://x", Log: "testlog://%zz-hunter2"},
	} {
		_, err := storagebackend.New(ctx, cfg, "w")
		if assert.Error(t, err, name) {
			assert.NotContains(t, err.Error(), "hunter2", name)
		}
	}
	assert.Zero(t, opened)

	_, err := storagebackend.New(ctx, storagebackend.Config{}, "w")
	assert.ErrorContains(t, err, "file, test", "it says what there is")
	objects, logs := storagebackend.Drivers()
	assert.Equal(t, []string{"file", "test"}, objects)
	assert.Equal(t, []string{"testlog"}, logs)
	assert.Panics(t, func() { storagebackend.RegisterObjects("file", nil) })
	assert.Panics(t, func() { storagebackend.RegisterLog("delta", nil) })
}

// TestClose checks that everything opened is closed, in reverse order, also if
// opening fails half way.
func TestClose(t *testing.T) {
	ctx := context.Background()
	var closed []string
	closer := func(name string) func() error {
		return func() error { closed = append(closed, name); return nil }
	}
	openObjects = func(u *url.URL) (storagebackend.Objects, error) {
		return storagebackend.Objects{Store: objstore.NewMem(), Location: "mem://x", Close: closer("objects")}, nil
	}
	var logErr error
	openLog = func(cfg storagebackend.LogConfig) (tablelog.Log, func() error, error) {
		assert.Equal(t, "mem://x", cfg.Objects.Location)
		assert.Equal(t, "w", cfg.WriterID)
		return deltalog.New(cfg.Objects.Store, cfg.WriterID), closer("log"), logErr
	}
	cfg := storagebackend.Config{Objects: "test://x", Log: "testlog://y"}
	b, err := storagebackend.New(ctx, cfg, "w")
	require.NoError(t, err)
	require.NoError(t, b.Close())
	assert.Equal(t, []string{"log", "objects"}, closed)
	require.NoError(t, b.Close())
	assert.Len(t, closed, 2, "once")

	closed, logErr = nil, errors.New("catalog is down")
	_, err = storagebackend.New(ctx, cfg, "w")
	require.ErrorIs(t, err, logErr)
	assert.Equal(t, []string{"log", "objects"}, closed)

	b, err = storagebackend.New(ctx, storagebackend.Config{Objects: "test://x", Log: "delta"}, "w")
	require.NoError(t, err)
	assert.IsType(t, &deltalog.Log{}, b.Log)

	openObjects = func(u *url.URL) (storagebackend.Objects, error) { return storagebackend.Objects{}, nil }
	_, err = storagebackend.New(ctx, cfg, "w")
	require.Error(t, err)
	assert.Error(t, (&storagebackend.Backend{}).Create(ctx, nil, filestore.DeltaOptions{}))
	_, err = (&storagebackend.Backend{Store: objstore.NewMem()}).Open(ctx, nil, filestore.DeltaOptions{})
	assert.Error(t, err)
}

func TestParams(t *testing.T) {
	parse := func(q string) *storagebackend.Params {
		return storagebackend.NewParams(&url.URL{RawQuery: q})
	}
	p := parse("a=1&b=2")
	assert.Equal(t, "1", p.Get("a", "x"))
	assert.Equal(t, "2", p.Get("b", "x"))
	assert.Equal(t, "x", p.Get("c", "x"))
	require.NoError(t, p.Done())
	for _, bad := range []string{"a=1&typo=2", "a=", "a=1&a=2", "a=%zz"} {
		p := parse(bad)
		p.Get("a", "")
		assert.Error(t, p.Done(), bad)
	}
}

func TestPrefix(t *testing.T) {
	for in, want := range map[string]string{"": "", "/": "", "/a": "a/", "a/b/": "a/b/"} {
		got, err := storagebackend.Prefix(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	for _, bad := range []string{"/a/../b", "/a//b", "/a/./b", "/a\x00", "/.."} {
		_, err := storagebackend.Prefix(bad)
		assert.Error(t, err, bad)
	}
}
