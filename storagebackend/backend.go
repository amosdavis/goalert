// Package storagebackend opens the storage a deployment chose.
//
// A backend is two ports: an object store, which holds data files
// (objstore.Store), and a log, which decides commits (tablelog.Log). Each has
// drivers, chosen by URL, and every one is optional: a deployment names the one
// it uses, and nothing of the others is configured, contacted or needed
// (docs/filestore-failure-modes.md, F48). Drivers are packages that register
// themselves when they are imported. Package all imports every one of them.
//
// To add storage, implement one of the ports, pass its conformance suite
// (objstoretest, chunkstoretest for a document database, or tablelogtest), and
// register a driver for it. Nothing else in GoAlert changes.
package storagebackend

import (
	"context"
	"errors"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

// Backend is the storage of one GoAlert installation. A running GoAlert uses
// one backend for everything (F23).
type Backend struct {
	Store objstore.Store
	Log   tablelog.Log

	// Location is the URI by which other software reaches the store's root, if
	// it can.
	Location string

	close []func() error
}

func (b *Backend) validate() error {
	if b.Store == nil {
		return errors.New("storagebackend: nil object store")
	}
	if b.Log == nil {
		return errors.New("storagebackend: nil table log")
	}
	return nil
}

// Create creates the backend's table. Only an explicit command may do so, so
// that a pod pointed at the wrong place can't start a second GoAlert (F27).
func (b *Backend) Create(ctx context.Context, schema *filestore.Schema, opts filestore.DeltaOptions) error {
	if err := b.validate(); err != nil {
		return err
	}
	return filestore.CreateBackend(ctx, b.Store, b.Log, schema, opts)
}

// Open opens the backend's existing table.
func (b *Backend) Open(ctx context.Context, schema *filestore.Schema, opts filestore.DeltaOptions) (*filestore.DeltaDB, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return filestore.OpenBackend(ctx, b.Store, b.Log, schema, opts)
}

// CheckSettings reports settings of the storage that break or weaken the table
// (F34), as far as the object store and the log can check them. An error
// matching objstore.ErrDenied means that some settings are unchecked.
func (b *Backend) CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error) {
	var out []objstore.SettingProblem
	var errs []error
	for _, part := range []any{b.Store, b.Log} {
		if c, ok := part.(interface {
			CheckSettings(ctx context.Context) ([]objstore.SettingProblem, error)
		}); ok {
			problems, err := c.CheckSettings(ctx)
			out, errs = append(out, problems...), append(errs, err)
		}
	}
	return out, errors.Join(errs...)
}

// Close closes the backend's connections. Close databases that use it first.
func (b *Backend) Close() error {
	var errs []error
	for i := len(b.close) - 1; i >= 0; i-- {
		errs = append(errs, b.close[i]())
	}
	b.close = nil
	return errors.Join(errs...)
}
