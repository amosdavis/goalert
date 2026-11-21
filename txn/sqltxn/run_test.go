package sqltxn_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
)

// recorder is a database/sql driver that only records what happens to
// transactions.
type recorder struct {
	mu        sync.Mutex
	events    []string
	commitErr error
}

func (r *recorder) record(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) Events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) Open(string) (driver.Conn, error) { return conn{r}, nil }

type conn struct{ r *recorder }

func (conn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (conn) Close() error                        { return nil }
func (c conn) Begin() (driver.Tx, error) {
	c.r.record("begin")
	return tx(c), nil
}

type tx conn

func (t tx) Commit() error {
	t.r.record("commit")
	return t.r.commitErr
}

func (t tx) Rollback() error {
	t.r.record("rollback")
	return nil
}

// registered numbers the drivers, since a name can be registered once.
var registered atomic.Int64

func open(t *testing.T) (*recorder, txn.DB) {
	t.Helper()
	r := &recorder{}
	name := fmt.Sprintf("recorder-%d", registered.Add(1))
	sql.Register(name, r)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return r, sqltxn.DB{DB: db}
}

func TestRunCommits(t *testing.T) {
	r, db := open(t)
	var got txn.Tx
	require.NoError(t, db.Run(context.Background(), nil, func(tx txn.Tx) error {
		got = tx
		return nil
	}))
	assert.NotNil(t, sqltxn.Unwrap(got), "fn gets a Postgres transaction")
	assert.Equal(t, []string{"begin", "commit"}, r.Events())
}

func TestRunRollsBackOnError(t *testing.T) {
	r, db := open(t)
	boom := errors.New("boom")
	err := db.Run(context.Background(), nil, func(txn.Tx) error { return boom })
	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"begin", "rollback"}, r.Events())
}

func TestRunRollsBackOnPanic(t *testing.T) {
	r, db := open(t)
	assert.Panics(t, func() {
		_ = db.Run(context.Background(), nil, func(txn.Tx) error { panic("boom") })
	})
	assert.Equal(t, []string{"begin", "rollback"}, r.Events())
}

func TestRunReportsCommitFailure(t *testing.T) {
	r, db := open(t)
	r.commitErr = errors.New("serialization failure")
	err := db.Run(context.Background(), nil, func(txn.Tx) error { return nil })
	require.ErrorIs(t, err, r.commitErr)
	assert.Equal(t, []string{"begin", "commit"}, r.Events(), "a failed commit is not rolled back again")
}
