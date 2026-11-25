package objstore

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ServerClock is an object store that tells how its server's clock compares
// with this pod's, from the Date header of the server's replies. It is the one
// clock that every pod sees, so pods compare theirs with it (F13).
type ServerClock interface {
	// ClockOffset returns how far the server's clock was ahead of this pod's at
	// its latest reply that had a date: at least min, and at most max, since a
	// date has whole seconds and a reply takes time to come. ok is false if no
	// reply had a date yet.
	ClockOffset() (min, max time.Duration, ok bool)
}

// maxObserveTime is how long a request may have taken for its reply's date to
// count: with retries, the reply says little of when the server answered.
const maxObserveTime = 5 * time.Second

// ClockWatch keeps what the latest reply said of the server's clock, for a
// store that implements ServerClock. The zero value is ready to use, and its
// methods are safe for concurrent use.
type ClockWatch struct {
	mu       sync.Mutex
	min, max time.Duration
	ok       bool
}

// Observe records a reply dated date, by the server's clock, to a request that
// was sent at start and answered by end, by this pod's.
func (w *ClockWatch) Observe(start, end, date time.Time) {
	if date.IsZero() || end.Before(start) || end.Sub(start) > maxObserveTime {
		return
	}
	// When the server answered, at some time from start to end, its clock read
	// from date to a second later.
	lo, hi := date.Sub(end), date.Add(time.Second).Sub(start)
	w.mu.Lock()
	w.min, w.max, w.ok = lo, hi, true
	w.mu.Unlock()
}

// ObserveHeader is Observe for a reply's headers. A Date header that is missing
// or can't be read is ignored.
func (w *ClockWatch) ObserveHeader(start, end time.Time, h http.Header) {
	if h == nil {
		return
	}
	if date, err := http.ParseTime(h.Get("Date")); err == nil {
		w.Observe(start, end, date)
	}
}

// ClockOffset implements ServerClock.
func (w *ClockWatch) ClockOffset() (min, max time.Duration, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.min, w.max, w.ok
}

// ClockError returns an error if s shows this pod's clock off from its server's
// by more than limit (F13). It returns nil if it is not, or if s can't tell.
func ClockError(s Store, limit time.Duration) error {
	c, ok := s.(ServerClock)
	if !ok || limit <= 0 {
		return nil
	}
	lo, hi, ok := c.ClockOffset()
	switch {
	case !ok:
		return nil
	case lo > limit:
		return fmt.Errorf("objstore: this pod's clock is at least %v behind the storage's, more than the %v allowed (F13)", lo.Truncate(time.Second), limit)
	case hi < -limit:
		return fmt.Errorf("objstore: this pod's clock is at least %v ahead of the storage's, more than the %v allowed (F13)", (-hi).Truncate(time.Second), limit)
	}
	return nil
}
