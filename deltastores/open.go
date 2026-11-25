package deltastores

import (
	"context"
	"errors"

	"github.com/target/goalert/filestore"
	"github.com/target/goalert/tablelog"
)

// ErrNotInstalled means that GoAlert's table was created, but not completely.
// Creating it again finishes it.
var ErrNotInstalled = errors.New("GoAlert's table is incomplete, since creating it was interrupted; create it again")

// Backend is the storage that holds the table. *storagebackend.Backend is one.
type Backend interface {
	Create(ctx context.Context, schema *filestore.Schema, opts filestore.DeltaOptions) error
	Open(ctx context.Context, schema *filestore.Schema, opts filestore.DeltaOptions) (*filestore.DeltaDB, error)
}

// Create creates GoAlert's table, with the limits a new install starts with.
// Only an explicit command may do so, never a pod that starts (F27). If it is
// interrupted, doing it again finishes it. A table that is complete is
// tablelog.ErrTableExists.
func Create(ctx context.Context, b Backend, opts filestore.DeltaOptions) error {
	schema, t := NewSchema()
	err := b.Create(ctx, schema, opts)
	exists := errors.Is(err, tablelog.ErrTableExists)
	if err != nil && !exists {
		return err
	}
	opts.PollInterval = -1
	db, oerr := b.Open(ctx, schema, opts)
	if oerr != nil {
		return errors.Join(err, oerr)
	}
	defer func() { _ = db.Close() }()
	if exists && installed(db) {
		return err
	}
	_, uerr := db.Update(ctx, t.Limit.InsertDefaults)
	return uerr
}

// Open opens GoAlert's table, which must exist.
func Open(ctx context.Context, b Backend, opts filestore.DeltaOptions) (*filestore.DeltaDB, *Tables, error) {
	schema, t := NewSchema()
	db, err := b.Open(ctx, schema, opts)
	if err != nil {
		return nil, nil, err
	}
	if !installed(db) {
		// Without its limits, nothing would enforce them (F8).
		_ = db.Close()
		return nil, nil, ErrNotInstalled
	}
	return db, t, nil
}

// installed reports whether the table's first rows are in it: they are the first
// commit after the one that creates it.
func installed(db *filestore.DeltaDB) bool { return db.View().Index() > 0 }
