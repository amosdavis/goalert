package timezone

import (
	"context"

	"github.com/target/goalert/txn"
)

// Store searches the time zones that are built into GoAlert.
type Store struct{}

// NewStore returns a time zone store. The zones are built in, so the store
// reads nothing from db; db is there so that the store is made like the
// others.
func NewStore(ctx context.Context, db txn.DB) *Store { return &Store{} }
