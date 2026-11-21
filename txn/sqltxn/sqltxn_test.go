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

	var typedNil *sql.Tx
	var tx txn.Tx = typedNil
	// assert.NotNil would say nil here: it looks through the interface.
	assert.True(t, tx != nil, "a nil *sql.Tx is not a nil txn.Tx")
	assert.Nil(t, Unwrap(tx), "but it unwraps to nil")

	real := new(sql.Tx)
	assert.Same(t, real, Unwrap(real))

	assert.Panics(t, func() { Unwrap(otherTx{}) }, "backends are never mixed (F23)")
}
