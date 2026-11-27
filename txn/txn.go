// Package txn defines GoAlert's transaction handle. Stores take a txn.Tx, and
// the Delta backend's (package txn/deltatxn) is the only one
// (docs/filestore-plan.md, phase 2).
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
	// Run runs fn in a transaction and commits it if fn returns nil; fn
	// must not commit or roll back tx itself. On the Delta backend, fn runs
	// again when another pod commits first (F4), so it must do no I/O
	// beyond the stores it calls (F3).
	Run(ctx context.Context, opts *Options, fn func(tx Tx) error) error
}
