package authdelta

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/util/log"
)

// RecordInterval is how often a pod writes the uses it recorded: at most one
// commit each time, however many credentials were used.
const RecordInterval = time.Minute

// Recorder collects uses of credentials and writes them together, in the
// background. Uses not written yet are lost if the pod stops without a Flush.
type Recorder[V any] struct {
	interval time.Duration
	what     string
	write    func(ctx context.Context, uses map[string]V) error

	flushing sync.Mutex

	mu        sync.Mutex
	pending   map[string]V
	writing   map[string]V // being written now
	scheduled bool
	last      time.Time
}

// NewRecorder returns a Recorder that writes at most once per interval. write
// gets the latest use of each key; since it can run again after an unclear
// outcome, it must not mind writing a use twice. what names the uses in error
// messages.
func NewRecorder[V any](interval time.Duration, what string, write func(ctx context.Context, uses map[string]V) error) *Recorder[V] {
	return &Recorder[V]{interval: interval, what: what, write: write}
}

// Add records the use v of key, instead of any other that isn't written yet.
func (r *Recorder[V]) Add(key string, v V) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(key, v)
}

func (r *Recorder[V]) add(key string, v V) {
	if r.pending == nil {
		r.pending = make(map[string]V)
	}
	r.pending[key] = v
	if r.scheduled {
		return
	}
	r.scheduled = true
	var wait time.Duration
	if !r.last.IsZero() {
		wait = max(0, r.interval-time.Since(r.last))
	}
	time.AfterFunc(wait, r.background)
}

// Pending returns the use of key that isn't written yet, if there is one.
func (r *Recorder[V]) Pending(key string) (V, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.pending[key]
	if !ok {
		v, ok = r.writing[key]
	}
	return v, ok
}

func (r *Recorder[V]) background() {
	r.mu.Lock()
	r.scheduled = false
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := r.Flush(ctx); err != nil && !errors.Is(err, filestore.ErrClosed) {
		log.Log(ctx, fmt.Errorf("record %s: %w", r.what, err))
	}
}

// Flush writes the uses recorded so far. If that fails, they are kept and
// tried again later.
func (r *Recorder[V]) Flush(ctx context.Context) error {
	r.flushing.Lock()
	defer r.flushing.Unlock()

	r.mu.Lock()
	uses := r.pending
	r.pending, r.writing = nil, uses
	if len(uses) > 0 {
		r.last = time.Now()
	}
	r.mu.Unlock()
	if len(uses) == 0 {
		return nil
	}

	err := r.write(ctx, uses)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.writing = nil
	if err != nil && !errors.Is(err, filestore.ErrClosed) {
		for key, v := range uses {
			if _, newer := r.pending[key]; !newer {
				r.add(key, v)
			}
		}
	}
	return err
}
