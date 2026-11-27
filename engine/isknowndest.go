package engine

import (
	"context"

	"github.com/target/goalert/gadb"
)

// IsKnownDest returns true if an enabled contact method or a notification
// channel has the destination.
func (n *Engine) IsKnownDest(ctx context.Context, dest gadb.DestV1) (bool, error) {
	return n.delta.isKnownDest(ctx, dest)
}
