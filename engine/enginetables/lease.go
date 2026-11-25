package enginetables

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/target/goalert/filestore"
)

// ErrNotHolder means that another pod runs the engine.
var ErrNotHolder = errors.New("another pod holds the engine's lease")

// engineLease is the name of the only lease there is.
const engineLease = "engine"

// Lease is a pod's share in the one lease on running the engine. It keeps
// several pods from doing the same work. What keeps two from sending the same
// message is that every transaction of the engine checks it, and commits only
// if that still holds (F28).
type Lease struct {
	t      *DeltaTables
	holder string
	name   string
	ttl    time.Duration
}

// NewLease returns a pod's share in the lease. The holder is this share, so a
// process that starts again, or two with the same name, are told apart. name is
// for people, and a lease that is not renewed for ttl is free.
func (t *DeltaTables) NewLease(name string, ttl time.Duration) *Lease {
	return &Lease{t: t, holder: uuid.NewString(), name: name, ttl: ttl}
}

// Renew takes the lease if it is free, or extends it if less than half of it is
// left, so that an idle engine commits seldom (F37). It returns ErrNotHolder if
// another pod has it.
func (l *Lease) Renew(tx *filestore.Tx) error {
	row, ok := l.t.lease.Get(tx, engineLease)
	now := tx.Now()
	switch {
	case ok && row.Holder == l.holder:
		if row.ExpiresAt.Sub(now) >= l.ttl/2 {
			return nil
		}
	case ok && row.ExpiresAt.After(now):
		return ErrNotHolder
	default:
		row = leaseRow{Name: engineLease, Holder: l.holder, HolderName: l.name, TakenAt: now}
	}
	row.ExpiresAt = now.Add(l.ttl)
	return l.t.lease.Put(tx, row)
}

// Check returns ErrNotHolder unless this pod has the lease. A lease that has
// expired, and that nobody has taken, is still this pod's.
func (l *Lease) Check(r filestore.Reader) error {
	if row, ok := l.t.lease.Get(r, engineLease); !ok || row.Holder != l.holder {
		return ErrNotHolder
	}
	return nil
}

// Release gives the lease back, if this pod has it, so that another can go on
// at once.
func (l *Lease) Release(tx *filestore.Tx) error {
	if l.Check(tx) != nil {
		return nil
	}
	_, err := l.t.lease.Delete(tx, engineLease)
	return err
}

// LeaseStatus is the state of the lease.
type LeaseStatus struct {
	// Mine is whether this pod has it, and Free whether none does, or the one
	// that had it did not renew it.
	Mine, Free bool
	// HolderName is the name of the pod that has it.
	HolderName string
	ExpiresAt  time.Time
}

// Status is the state of the lease in r at a time.
func (l *Lease) Status(r filestore.Reader, now time.Time) LeaseStatus {
	row, ok := l.t.lease.Get(r, engineLease)
	if !ok {
		return LeaseStatus{Free: true}
	}
	return LeaseStatus{
		Mine: row.Holder == l.holder, Free: !row.ExpiresAt.After(now),
		HolderName: row.HolderName, ExpiresAt: row.ExpiresAt,
	}
}
