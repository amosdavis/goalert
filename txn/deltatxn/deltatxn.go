// Package deltatxn adapts the Delta backend (filestore.DeltaDB) to package
// txn, so stores share one transaction interface (docs/filestore-plan.md,
// phase 3).
package deltatxn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/txn"
)

// ErrManaged means a caller tried to start, commit or roll back a Delta
// transaction itself. Run does all three, because it may have to run the
// transaction again after another pod commits first (F4).
var ErrManaged = errors.New("deltatxn: transactions are run with Run, which commits and rolls back")

// DB runs transactions on a DeltaDB.
type DB struct{ DB *filestore.DeltaDB }

var _ txn.DB = DB{}

// Run runs fn in a transaction. A write transaction is one DeltaDB.Update:
// fn runs on the latest state, and again if another pod commits first, so
// it must do no I/O beyond the stores (F3, F4). A read-only transaction
// reads one consistent view and commits nothing.
func (db DB) Run(ctx context.Context, opts *txn.Options, fn func(tx txn.Tx) error) error {
	if opts != nil && opts.ReadOnly {
		return fn(&Tx{reader: db.DB.View(), now: db.DB.Now()})
	}
	_, err := db.DB.Update(ctx, func(tx *filestore.Tx) error {
		return fn(Of(tx))
	})
	return err
}

// Tx is a transaction on the Delta backend. Store implementations get its
// parts with Unwrap.
type Tx struct {
	tx     *filestore.Tx // nil in a read-only transaction
	reader filestore.Reader
	now    time.Time
}

var _ txn.Tx = (*Tx)(nil)

// Of returns a filestore transaction as one that store methods take, for code
// that runs transactions itself, such as the engine.
func Of(tx *filestore.Tx) *Tx { return &Tx{tx: tx, reader: tx, now: tx.Now()} }

// Commit returns ErrManaged: Run commits.
func (*Tx) Commit() error { return ErrManaged }

// Rollback returns ErrManaged: Run rolls back.
func (*Tx) Rollback() error { return ErrManaged }

// Unwrap returns the filestore transaction in tx, which is nil in a
// read-only transaction, and a reader that sees tx's changes. For a nil tx,
// meaning not in a transaction, both are nil.
//
// Unwrap panics if tx belongs to another backend: a running GoAlert never
// mixes backends (F23).
func Unwrap(tx txn.Tx) (*filestore.Tx, filestore.Reader) {
	t := unwrap(tx)
	if t == nil {
		return nil, nil
	}
	return t.tx, t.reader
}

func unwrap(tx txn.Tx) *Tx {
	if tx == nil {
		return nil
	}
	t, ok := tx.(*Tx)
	if !ok {
		panic(fmt.Sprintf("deltatxn: a %T transaction was passed to a Delta store", tx))
	}
	return t
}
