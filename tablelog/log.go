// Package tablelog defines the commit log a filestore.DeltaDB uses to share
// one serializable sequence of table snapshots between pods. Implementations
// may store that sequence as Delta Lake commits, Iceberg snapshots, or another
// durable format with the same atomic-publication and read-back guarantees.
//
// The data types are aliases of deltalog's so the Delta implementation and
// existing callers keep their API. A non-Delta implementation gives these
// types the same meaning; its physical representation is its own concern.
package tablelog

import (
	"context"
	"time"

	"github.com/target/goalert/deltalog"
)

type (
	Action     = deltalog.Action
	Protocol   = deltalog.Protocol
	Format     = deltalog.Format
	MetaData   = deltalog.MetaData
	Add        = deltalog.Add
	Remove     = deltalog.Remove
	Txn        = deltalog.Txn
	CommitInfo = deltalog.CommitInfo
	Snapshot   = deltalog.Snapshot
)

var (
	ErrNoTable     = deltalog.ErrNoTable
	ErrTableExists = deltalog.ErrTableExists
	ErrConflict    = deltalog.ErrConflict
	ErrUnsupported = deltalog.ErrUnsupported
	ErrForeign     = deltalog.ErrForeign
	ErrCorrupt     = deltalog.ErrCorrupt
	ErrManaged     = deltalog.ErrManaged
)

// Log is the commit log of one table. Commit publishes version only when the
// caller's previous version is still current; another writer winning that
// version returns ErrConflict. If a result is unclear, the implementation
// reads the version back and recognizes its own commit before reporting
// success or objstore.ErrUnknown. Load returns a complete current snapshot,
// while Read returns one historical commit for replay.
//
// WriteCheckpoint makes Load efficient without losing history that Read needs,
// and Cleanup expires only history no reader can still need.
//
// Every implementation passes tablelogtest.Run. A log refuses tables whose
// commits something else decides (ErrManaged, or ErrNoTable or ErrForeign if it
// can't tell).
type Log interface {
	// SetRetry bounds attempts to settle an unclear commit and sets the first
	// retry delay. Values that are not positive change nothing. It is called
	// before the log is used.
	SetRetry(attempts int, backoff time.Duration)

	Create(ctx context.Context, meta MetaData, at time.Time) error
	Load(ctx context.Context) (*Snapshot, error)
	Read(ctx context.Context, version int64) (CommitInfo, []Action, error)
	Commit(ctx context.Context, version int64, storeID, operation string, at time.Time, actions []Action) (CommitInfo, error)
	WriteCheckpoint(ctx context.Context, snapshot *Snapshot, now time.Time, retention time.Duration) error
	Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error)
}

var _ Log = (*deltalog.Log)(nil)
