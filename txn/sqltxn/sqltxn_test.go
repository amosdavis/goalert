package sqltxn

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/txn"
)

type otherTx struct{}

func (otherTx) Commit() error   { return nil }
func (otherTx) Rollback() error { return nil }

func TestUnwrap(t *testing.T) {
	assert.Nil(t, Unwrap(nil), "a nil interface")

	// Passing a nil *sql.Tx where a txn.Tx is expected, as callers do, makes a
	// non-nil interface. (assert.NotNil would call it nil: it looks through
	// the interface.)
	var typedNil *sql.Tx
	isNil := func(tx txn.Tx) bool { return tx == nil }
	assert.False(t, isNil(typedNil), "a nil *sql.Tx is not a nil txn.Tx")
	assert.Nil(t, Unwrap(typedNil), "but it unwraps to nil")

	real := new(sql.Tx)
	assert.Same(t, real, Unwrap(real))

	assert.Panics(t, func() { Unwrap(otherTx{}) }, "backends are never mixed (F23)")
}
