package filestore

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
)

// Fail makes the transaction fail with err, even if its function goes on and
// returns nil. Postgres does that to a transaction in which a constraint
// fails, and code that ignores such an error relies on it: without it, the
// row that breaks the constraint would be committed (F8).
func (tx *Tx) Fail(err error) {
	if tx.failed == nil {
		tx.failed = err
	}
}

// Try runs fn, and undoes what it changed if it fails, like a savepoint in
// Postgres: the rest of the transaction can still commit. That includes a
// constraint that failed in it (Fail). A panic in fn is an error too, a
// *TxPanic.
//
// It is for work on many rows, where one that can't be handled must not keep
// the others from being (F53).
func (tx *Tx) Try(fn func() error) (err error) {
	if err := tx.check(); err != nil {
		return err
	}
	working := make(map[string]tableData, len(tx.working))
	for name, d := range tx.working {
		working[name] = d.clone()
	}
	muts, failed := len(tx.muts), tx.failed
	done := false
	defer func() {
		if !done {
			v := recover()
			if v == nil {
				return // runtime.Goexit, which goes on and ends the transaction
			}
			err = &TxPanic{Value: v, Stack: debug.Stack()}
		}
		if err != nil {
			tx.working, tx.muts, tx.failed = working, tx.muts[:muts], failed
		}
	}()
	err = fn()
	if err == nil && tx.failed != failed {
		err = tx.failed
	}
	done = true
	return err
}

// TxPanic is what Update panics with when the transaction's function
// panicked.
type TxPanic struct {
	Value any
	Stack []byte
}

func (p *TxPanic) Error() string {
	return fmt.Sprintf("panic in a transaction: %v\n\n%s", p.Value, p.Stack)
}

// Unwrap returns the value the function panicked with, if it is an error.
func (p *TxPanic) Unwrap() error {
	err, _ := p.Value.(error)
	return err
}

var errGoexit = errors.New("filestore: runtime.Goexit was called in a transaction")

// runTx runs a transaction's function on a goroutine of its own, because the
// batch that calls it serves several callers: a panic or runtime.Goexit in
// the function must end its transaction, and neither the batch nor the pod
// (F41). Both come back as errors, which finish turns into what they were.
func runTx(fn func(tx *Tx) error, tx *Tx) error {
	done := make(chan error, 1)
	go func() {
		returned := false
		defer func() {
			if returned {
				return
			}
			if v := recover(); v != nil {
				done <- &TxPanic{Value: v, Stack: debug.Stack()}
				return
			}
			done <- errGoexit
		}()
		err := fn(tx)
		returned = true
		done <- err
	}()
	err := <-done
	tx.done = true
	if err == nil {
		err = tx.failed
	}
	return err
}

// finish is how Update ends: a panic or runtime.Goexit of the transaction's
// function goes on in its caller.
func (r txResult) finish() (int64, error) {
	if p, ok := r.err.(*TxPanic); ok {
		panic(p)
	}
	if r.err == errGoexit {
		runtime.Goexit()
	}
	return r.version, r.err
}
