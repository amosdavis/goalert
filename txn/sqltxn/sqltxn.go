// Package sqltxn adapts database/sql to package txn, for the Postgres
// backend. It goes away with Postgres (docs/filestore-plan.md, phase 5).
package sqltxn

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/target/goalert/txn"
)

// DB starts transactions on a *sql.DB as txn.Tx values (*sql.Tx).
type DB struct{ *sql.DB }

var _ txn.DB = DB{}

// BeginTx starts a transaction.
func (db DB) BeginTx(ctx context.Context, opts *txn.Options) (txn.Tx, error) {
	var sqlOpts *sql.TxOptions
	if opts != nil {
		sqlOpts = &sql.TxOptions{ReadOnly: opts.ReadOnly}
	}
	tx, err := db.DB.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// Run runs fn in a transaction, and commits it if fn returns nil. If fn
// fails or panics, the transaction is rolled back. Postgres never needs to
// run fn twice.
func (db DB) Run(ctx context.Context, opts *txn.Options, fn func(tx txn.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	// After a commit, this returns sql.ErrTxDone and changes nothing.
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Unwrap returns the *sql.Tx inside tx, or nil if tx is nil.
//
// A nil *sql.Tx stored in a txn.Tx is not a nil interface, so code that used
// to check `tx != nil` must check the unwrapped value instead; Unwrap returns
// nil for that case too.
//
// Unwrap panics if tx belongs to another backend: a running GoAlert never
// mixes backends (F23).
func Unwrap(tx txn.Tx) *sql.Tx {
	if tx == nil {
		return nil
	}
	stx, ok := tx.(*sql.Tx)
	if !ok {
		panic(fmt.Sprintf("sqltxn: a %T transaction was passed to a Postgres store", tx))
	}
	return stx
}
