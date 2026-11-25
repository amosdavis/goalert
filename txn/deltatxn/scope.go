package deltatxn

import (
	"context"
	"errors"
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
)

// ErrNotPorted is what a store method returns on the Delta backend until it
// is ported (docs/filestore-plan.md, phase 3).
var ErrNotPorted = errors.New("not available on the Delta backend yet")

// ErrReadOnly means a write was tried in a read-only transaction.
var ErrReadOnly = errors.New("deltatxn: cannot write in a read-only transaction")

// Scope is what a store method works with inside a transaction.
type Scope struct {
	Tx     *filestore.Tx    // for writes; nil when reading only
	Reader filestore.Reader // for reads; sees the transaction's own writes
	Now    time.Time        // one time for the whole transaction, like now()
}

// Read runs fn in tx, or on a consistent view of db when tx is nil. In a
// write transaction fn can run more than once, so it must set, not add to,
// what it hands out through captured variables.
func Read(ctx context.Context, db txn.DB, tx txn.Tx, fn func(s Scope) error) error {
	return in(ctx, db, tx, true, fn)
}

// Write runs fn in tx, or in a transaction of its own on db when tx is nil.
// It can run more than once, so it must do no I/O (F3, F4), and must set, not
// add to, what it hands out through captured variables.
func Write(ctx context.Context, db txn.DB, tx txn.Tx, fn func(s Scope) error) error {
	return in(ctx, db, tx, false, func(s Scope) error {
		if s.Tx == nil {
			return ErrReadOnly
		}
		return fn(s)
	})
}

func in(ctx context.Context, db txn.DB, tx txn.Tx, readOnly bool, fn func(s Scope) error) error {
	if t := unwrap(tx); t != nil {
		return fn(t.scope())
	}
	return db.Run(ctx, &txn.Options{ReadOnly: readOnly}, func(tx txn.Tx) error {
		return fn(unwrap(tx).scope())
	})
}

func (t *Tx) scope() Scope { return Scope{Tx: t.tx, Reader: t.reader, Now: t.now} }
