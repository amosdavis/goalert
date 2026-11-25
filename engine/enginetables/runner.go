package enginetables

import (
	"context"
	"errors"
	"fmt"

	"github.com/target/goalert/filestore"
)

// Step is a module's part of an engine cycle.
type Step struct {
	Name string
	// Run does the work in a transaction. It can run more than once, so it does
	// no I/O (F3, F4), and what it hands out through captured variables counts
	// only if the step succeeded.
	Run func(ctx context.Context, tx *filestore.Tx) error
}

// Runner runs what only the pod with the lease may do.
type Runner struct {
	db    *filestore.DeltaDB
	lease *Lease
}

// NewRunner returns a runner for a pod's share in the lease.
func NewRunner(db *filestore.DeltaDB, lease *Lease) *Runner {
	return &Runner{db: db, lease: lease}
}

// Run runs steps in order, each as a transaction of its own, in one commit: each
// sees what those before it did, and one that fails is left out (F53). It takes
// or renews the lease first, and each step checks it (F28).
//
// It returns each step's error. If nothing was run, because another pod has the
// lease or the table can't be written, there are none, and err says so.
func (r *Runner) Run(ctx context.Context, steps ...Step) (errs []error, err error) {
	// A pod that does not run the engine asks its own copy only.
	if st := r.lease.Status(r.db.View(), r.db.Now()); !st.Mine && !st.Free {
		return nil, ErrNotHolder
	}
	fns := []func(tx *filestore.Tx) error{r.lease.Renew}
	for _, s := range steps {
		fns = append(fns, r.checked(ctx, s))
	}
	errs = r.db.UpdateEach(ctx, fns...)
	if errs[0] != nil {
		return nil, errs[0]
	}
	errs = errs[1:]
	for i, err := range errs {
		if err != nil {
			errs[i] = fmt.Errorf("%s: %w", steps[i].Name, err)
		}
	}
	return errs, nil
}

// Do runs one step, if this pod has the lease. It does not renew it.
func (r *Runner) Do(ctx context.Context, s Step) error {
	_, err := r.db.Update(ctx, r.checked(ctx, s))
	if err != nil {
		return fmt.Errorf("%s: %w", s.Name, err)
	}
	return nil
}

func (r *Runner) checked(ctx context.Context, s Step) func(tx *filestore.Tx) error {
	return func(tx *filestore.Tx) error {
		if err := r.lease.Check(tx); err != nil {
			return err
		}
		return s.Run(ctx, tx)
	}
}

// Holds reports whether this pod has the lease, as far as it knows.
func (r *Runner) Holds() bool { return r.lease.Check(r.db.View()) == nil }

// Status is the state of the lease, as far as this pod knows.
func (r *Runner) Status() LeaseStatus { return r.lease.Status(r.db.View(), r.db.Now()) }

// Release gives the lease back, if this pod has it.
func (r *Runner) Release(ctx context.Context) error {
	if !r.Holds() {
		return nil
	}
	_, err := r.db.Update(ctx, r.lease.Release)
	return err
}

// Each calls fn for each of rows, and undoes what it did for one that fails, so
// that a row that can't be handled does not keep the others from being (F53).
// It returns their errors together, for the log: they are not the step's.
func Each[V any](tx *filestore.Tx, rows []V, fn func(v V) error) error {
	var errs []error
	for _, v := range rows {
		if err := tx.Try(func() error { return fn(v) }); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
