// Package icebergtest has a catalog in memory for tests, and an Iceberg reader
// for tables whose files are in an object store.
package icebergtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/target/goalert/objstore"
)

// Catalog holds one table's metadata in memory. It checks requirements and
// applies updates as a catalog server does, and passes the metadata through
// JSON, as a server's answers are.
type Catalog struct {
	// Methods a Log does not call are nil, and panic.
	catalog.Catalog

	mu   sync.Mutex
	meta []byte

	// Before, if set, is called before a request ("create", "load" or
	// "commit"), and its error is returned instead of doing it.
	Before func(op string) error
	// After, if set, is called after a request that was done, and its error is
	// returned instead of the reply.
	After func(op string) error

	// Commits is how many commits were made.
	Commits int
}

func (c *Catalog) CatalogType() catalog.Type { return "test" }

func (c *Catalog) hooks(op string, fn func() error) error {
	if c.Before != nil {
		if err := c.Before(op); err != nil {
			return err
		}
	}
	if err := fn(); err != nil {
		return err
	}
	if c.After != nil {
		return c.After(op)
	}
	return nil
}

// Metadata returns the table's metadata, or nil if there is none.
func (c *Catalog) Metadata() table.Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.meta == nil {
		return nil
	}
	meta, err := table.ParseMetadataBytes(c.meta)
	if err != nil {
		panic(err)
	}
	return meta
}

// SetMetadata replaces the table's metadata, as someone who does not use the
// catalog's API might.
func (c *Catalog) SetMetadata(meta table.Metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.meta = data
	return nil
}

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	cfg := catalog.NewCreateTableCfg()
	for _, opt := range opts {
		opt(&cfg)
	}
	var meta table.Metadata
	err := c.hooks("create", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.meta != nil {
			return catalog.ErrTableAlreadyExists
		}
		made, err := table.NewMetadata(schema, cfg.PartitionSpec, cfg.SortOrder, cfg.Location, cfg.Properties)
		if err != nil {
			return err
		}
		if c.meta, err = json.Marshal(made); err != nil {
			return err
		}
		meta, err = table.ParseMetadataBytes(c.meta)
		return err
	})
	if err != nil {
		return nil, err
	}
	return table.New(ident, meta, "", nil, c), nil
}

func (c *Catalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	var meta table.Metadata
	err := c.hooks("load", func() error {
		if meta = c.Metadata(); meta == nil {
			return catalog.ErrNoSuchTable
		}
		return ctx.Err()
	})
	if err != nil {
		return nil, err
	}
	return table.New(ident, meta, "", nil, c), nil
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	// A server gets them as JSON.
	var wireReqs table.Requirements
	var wireUpdates table.Updates
	if err := roundTrip(reqs, &wireReqs); err != nil {
		return nil, "", err
	}
	if err := roundTrip(updates, &wireUpdates); err != nil {
		return nil, "", err
	}
	var out table.Metadata
	err := c.hooks("commit", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.meta == nil {
			return catalog.ErrNoSuchTable
		}
		base, err := table.ParseMetadataBytes(c.meta)
		if err != nil {
			return err
		}
		for _, r := range wireReqs {
			if err := r.Validate(base); err != nil {
				return fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
			}
		}
		made, err := table.UpdateTableMetadata(base, wireUpdates, "")
		if err != nil {
			return err
		}
		data, err := json.Marshal(made)
		if err != nil {
			return err
		}
		c.meta = data
		c.Commits++
		out, err = table.ParseMetadataBytes(data)
		return err
	})
	if err != nil {
		return nil, "", err
	}
	return out, "", nil
}

func roundTrip(in, out any) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// Table returns the table as an Iceberg reader sees it, whose files are in a
// store at a location.
func Table(meta table.Metadata, store objstore.Store, location string) *table.Table {
	fsys := &FS{Store: store, Location: strings.TrimRight(location, "/")}
	return table.New(table.Identifier{"test", "table"}, meta, "", func(context.Context) (iceio.IO, error) { return fsys, nil }, nil)
}

// FS gives Iceberg readers the files in an object store.
type FS struct {
	Store    objstore.Store
	Location string
}

var _ iceio.IO = (*FS)(nil)

// Open reads a whole file.
func (f *FS) Open(name string) (iceio.File, error) {
	rel, ok := strings.CutPrefix(name, f.Location+"/")
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	data, err := f.Store.Get(context.Background(), rel)
	if errors.Is(err, objstore.ErrNotFound) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if err != nil {
		return nil, err
	}
	return &file{Reader: bytes.NewReader(data), name: path.Base(name)}, nil
}

// Remove refuses: readers delete nothing.
func (f *FS) Remove(name string) error {
	return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrPermission}
}

type file struct {
	*bytes.Reader
	name string
}

func (f *file) Close() error               { return nil }
func (f *file) Stat() (fs.FileInfo, error) { return info{f}, nil }

type info struct{ f *file }

func (i info) Name() string       { return i.f.name }
func (i info) Size() int64        { return i.f.Reader.Size() }
func (i info) Mode() fs.FileMode  { return 0o444 }
func (i info) ModTime() time.Time { return time.Time{} }
func (i info) IsDir() bool        { return false }
func (i info) Sys() any           { return nil }
