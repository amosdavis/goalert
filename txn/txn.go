// Package txn defines GoAlert's backend-neutral transaction handle. Stores
// take a txn.Tx instead of a *sql.Tx, so the same callers work on Postgres
// and on the filestore backend (docs/filestore-plan.md, phase 2).
//
// *sql.Tx already satisfies Tx, so existing callers keep compiling while
// store signatures change. Package txn/sqltxn adapts the Postgres side.
package txn

import "context"

// Tx is an open transaction on whichever backend GoAlert runs on. A store
// only accepts transactions from its own backend; a running GoAlert never
// mixes backends (F23).
type Tx interface {
	Commit() error
	Rollback() error
}

// Options configure a transaction.
type Options struct {
	// ReadOnly declares that the transaction will not write.
	ReadOnly bool
}

// DB starts transactions.
type DB interface {
	BeginTx(ctx context.Context, opts *Options) (Tx, error)
}
