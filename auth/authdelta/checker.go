// Package authdelta has what the stores that check credentials share on the
// Delta backend: reads that see a credential another pod just created or
// revoked (F29), and recording when a credential was last used without a
// commit for every use (F37).
package authdelta

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/target/goalert/txn"
	"github.com/target/goalert/txn/deltatxn"
)

// MaxStale is how far behind the log a pod may be when it accepts a
// credential that it checks on every request. It bounds how long a revoked
// one still works on other pods, even on a pod that no longer follows the
// log.
const MaxStale = 5 * time.Second

// catchUpTimeout bounds a catch-up, which serves every caller waiting for it.
const catchUpTimeout = 30 * time.Second

// Checker reads credentials from this pod's view of the table, after making
// sure that the view is recent enough. It must not be used inside a
// transaction, where it would wait for itself.
type Checker struct {
	db       txn.DB
	maxStale time.Duration

	running sync.Mutex // held by the catch-up that runs

	mu   sync.Mutex
	next *catchUp  // the catch-up that waits to run, if any
	last time.Time // when the last one that succeeded started
}

type catchUp struct {
	done chan struct{}
	err  error
}

// NewChecker returns a Checker whose reads are at most maxStale behind the
// log, or not behind at all if it is zero.
func NewChecker(db txn.DB, maxStale time.Duration) *Checker {
	return &Checker{db: db, maxStale: maxStale}
}

// Read runs read on this pod's view. If it returns sql.ErrNoRows, the
// credential may have been created on another pod a moment ago, so Read
// catches up with the log and runs it again.
//
// If the log can't be read, Read fails rather than trust an old view.
func (c *Checker) Read(ctx context.Context, read func(tx txn.Tx, s deltatxn.Scope) error) error {
	current, err := c.catchUp(ctx, c.maxStale)
	if err != nil {
		return err
	}
	err = c.view(ctx, read)
	if current || !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := c.catchUp(ctx, 0); err != nil {
		return err
	}
	return c.view(ctx, read)
}

func (c *Checker) view(ctx context.Context, read func(tx txn.Tx, s deltatxn.Scope) error) error {
	return c.db.Run(ctx, &txn.Options{ReadOnly: true}, func(tx txn.Tx) error {
		return deltatxn.Read(ctx, c.db, tx, func(s deltatxn.Scope) error { return read(tx, s) })
	})
}

// catchUp makes sure the pod caught up with the log within maxStale. It
// reports whether that happened after it was called.
//
// Callers share a catch-up that hasn't started yet, so however many requests
// wait, the store sees one at a time: a flood of unknown tokens can't hold up
// the writes behind it (F15).
func (c *Checker) catchUp(ctx context.Context, maxStale time.Duration) (current bool, err error) {
	c.mu.Lock()
	if !c.last.IsZero() && time.Since(c.last) < maxStale {
		c.mu.Unlock()
		return false, nil
	}
	n := c.next
	if n == nil {
		n = &catchUp{done: make(chan struct{})}
		c.next = n
		go c.run(context.WithoutCancel(ctx), n)
	}
	c.mu.Unlock()

	select {
	case <-n.done:
		return true, n.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (c *Checker) run(ctx context.Context, n *catchUp) {
	defer close(n.done)
	c.running.Lock()
	defer c.running.Unlock()

	c.mu.Lock()
	c.next = nil // from here on, callers need one that starts later
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, catchUpTimeout)
	defer cancel()
	start := time.Now()
	// A transaction that changes nothing catches up and commits nothing. It
	// may share a batch with writes that fail to commit, and fail with them,
	// so it gets a second try.
	for range 2 {
		n.err = c.db.Run(ctx, nil, func(txn.Tx) error { return nil })
		if n.err == nil {
			c.mu.Lock()
			c.last = start
			c.mu.Unlock()
			return
		}
	}
}
