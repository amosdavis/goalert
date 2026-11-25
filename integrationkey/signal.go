package integrationkey

import (
	"context"

	"github.com/target/goalert/gadb"
	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/sqltxn"
)

// InsertSignalMessage queues a signal message for a destination, in tx if
// it is not nil. Permissions are checked by the caller.
func (s *Store) InsertSignalMessage(ctx context.Context, tx txn.Tx, p gadb.IntKeyInsertSignalMessageParams) error {
	if s.delta != nil {
		return s.delta.insertSignal(ctx, tx, p)
	}
	return gadb.New(sqltxn.DBTX(s.db, tx)).IntKeyInsertSignalMessage(ctx, p)
}
