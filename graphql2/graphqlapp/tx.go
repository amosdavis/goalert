package graphqlapp

import (
	context "context"

	"github.com/target/goalert/txn"
	"github.com/target/goalert/util/errutil"
)

// withContextTx runs fn in the request's transaction.
//
// If the context already carries a transaction, fn runs in it with that
// context. Otherwise one is started with txn.DB.Run, and fn gets a context
// that carries it, so nested calls to withContextTx join it.
//
// Run commits and rolls back. On the Delta backend it can run fn more than
// once, so fn must do no I/O beyond the stores it calls (F3).
func withContextTx(ctx context.Context, db txn.DB, fn func(context.Context, txn.Tx) error) error {
	// Defining a static key to store the transaction within Context. The `0` value is arbitrary,
	// it just needs to be a unique type/value pair, vs. other context values.
	type ctxTx int
	const txKey = ctxTx(0)

	if tx, ok := ctx.Value(txKey).(txn.Tx); ok {
		// Transaction already exists, run fn with
		// the original context, and pass in the open
		// Tx.
		return errutil.MapDBError(fn(ctx, tx))
	}

	err := db.Run(ctx, nil, func(tx txn.Tx) error {
		return fn(context.WithValue(ctx, txKey, tx), tx)
	})

	// Ensure returned DB errors are mapped.
	return errutil.MapDBError(err)
}
