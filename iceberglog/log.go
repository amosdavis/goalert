// Package iceberglog is a tablelog.Log whose commits are decided by an Apache
// Iceberg catalog. One table is then readable both as Iceberg and as Delta
// Lake, over the same data files.
//
// The catalog is asked, in one atomic request, to add a snapshot on top of the
// one the commit is based on, which succeeds for one writer only. The rest is
// managedlog's: staging a commit, and copying it into the Delta log once it is
// decided.
//
// The catalog's metadata stays small, since a catalog rewrites all of it for
// every commit: a snapshot says which commit it is and no more, few snapshots
// are kept, and the number of manifests is fixed
// (docs/filestore-failure-modes.md, F46).
package iceberglog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/target/goalert/deltalog"
	"github.com/target/goalert/managedlog"
	"github.com/target/goalert/objstore"
	"github.com/target/goalert/tablelog"
)

const (
	// Manager is the deltalog.ManagerKey of the tables this package makes.
	Manager = "iceberg"

	// DefaultSnapshots is how many snapshots the catalog keeps. GoAlert needs
	// only the newest; others let Iceberg readers finish or look back.
	DefaultSnapshots = 100

	// The table's properties.
	propStoreID = "goalert.store-id"
	propFormat  = deltalog.FormatKey
	propCreate  = "goalert.create-commit-id"

	// A snapshot's properties.
	sumVersion  = "goalert.version"
	sumCommitID = "goalert.commit-id"
	sumStoreID  = "goalert.store-id"
	sumWriterID = "goalert.writer-id"
	sumFormat   = "goalert.format"
)

// ErrRejected marks an error that means the catalog did not do what it was
// asked to. Config.Classify uses it.
var ErrRejected = managedlog.ErrRejected

// Config says where the table is.
type Config struct {
	// Catalog holds the table, and Ident is its name there. The namespace must
	// exist.
	Catalog catalog.Catalog
	Ident   table.Identifier

	// Store holds the table's files, and Location is the URI by which Iceberg
	// readers reach the store's root, such as "gs://bucket/prefix". If the two
	// are not the same place, readers find no data (F49).
	Store    objstore.Store
	Location string

	// Classify, if set, marks the catalog's errors: with objstore.ErrDenied if
	// it refused the credentials, and ErrRejected if a request did not happen.
	// Any other error may mean that it did, but for table.ErrCommitFailed.
	Classify func(err error) error

	// Snapshots is how many snapshots the catalog keeps, DefaultSnapshots if 0.
	Snapshots int

	// Warn, if set, receives problems that fail nothing (F21).
	Warn func(error)
}

// Log is one writer's handle to a table.
type Log struct{ *managedlog.Log }

var _ tablelog.Log = (*Log)(nil)

// New returns a Log. It does no I/O, and the caller keeps ownership of the
// catalog.
func New(cfg Config, writerID string) (*Log, error) {
	a, err := newArbiter(cfg)
	if err != nil {
		return nil, err
	}
	l, err := managedlog.New(managedlog.Config{Arbiter: a, Store: cfg.Store, Warn: cfg.Warn}, writerID)
	if err != nil {
		return nil, err
	}
	return &Log{Log: l}, nil
}

// NewArbiter returns what decides a Log's commits, on its own.
func NewArbiter(cfg Config) (managedlog.Arbiter, error) { return newArbiter(cfg) }

// arbiter decides commits with an Iceberg catalog.
type arbiter struct {
	cfg      Config
	location string

	mu sync.Mutex
	// head is the newest state this process knows, or nil.
	head *head
	// newest is the newest version the catalog has reported.
	newest int64
	// files are the metadata files of head's snapshot, which never change.
	files map[string][]byte
}

var (
	_ managedlog.Arbiter    = (*arbiter)(nil)
	_ managedlog.Creator    = (*arbiter)(nil)
	_ managedlog.Maintainer = (*arbiter)(nil)
)

func newArbiter(cfg Config) (*arbiter, error) {
	switch {
	case cfg.Catalog == nil:
		return nil, errors.New("iceberglog: nil catalog")
	case cfg.Store == nil:
		return nil, errors.New("iceberglog: nil object store")
	case len(cfg.Ident) < 2:
		return nil, errors.New("iceberglog: the table's name needs a namespace")
	case cfg.Snapshots < 0:
		return nil, errors.New("iceberglog: a negative number of snapshots")
	}
	location := strings.TrimRight(cfg.Location, "/")
	if u, err := url.Parse(location); err != nil || u.Scheme == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("iceberglog: the location %q is not a URI", cfg.Location)
	}
	if cfg.Snapshots == 0 {
		cfg.Snapshots = DefaultSnapshots
	}
	if cfg.Classify == nil {
		cfg.Classify = func(err error) error { return err }
	}
	if cfg.Warn == nil {
		cfg.Warn = func(error) {}
	}
	return &arbiter{cfg: cfg, location: location, files: map[string][]byte{}}, nil
}

// Manager names Iceberg catalogs.
func (a *arbiter) Manager() string { return Manager }

func (a *arbiter) catalogErr(op string, err error) error {
	err = a.cfg.Classify(fmt.Errorf("iceberglog: %s: %w", op, err))
	if errors.Is(err, table.ErrCommitFailed) {
		return fmt.Errorf("%w: %w", tablelog.ErrConflict, err)
	}
	return err
}

// definite reports whether an error means a request to the catalog did not
// happen.
func definite(err error) bool {
	return errors.Is(err, table.ErrCommitFailed) || errors.Is(err, ErrRejected) || errors.Is(err, objstore.ErrDenied)
}

// head is the catalog's state of the table.
type head struct {
	meta     table.Metadata
	storeID  string
	version  int64
	commitID string
	// snapshot is nil at version 0, which the table was created with.
	snapshot *table.Snapshot
}

// parse checks that the catalog's table is GoAlert's, and at the version a
// GoAlert commit made.
func (a *arbiter) parse(meta table.Metadata) (*head, error) {
	props := meta.Properties()
	h := &head{meta: meta, storeID: props[propStoreID], commitID: props[propCreate]}
	if h.storeID == "" || h.commitID == "" {
		return nil, fmt.Errorf("%w: GoAlert did not create the Iceberg table", tablelog.ErrForeign)
	}
	if f := props[propFormat]; f != strconv.Itoa(deltalog.GoAlertFormat) {
		return nil, fmt.Errorf("%w: the Iceberg table has GoAlert format %q (this version reads %d)", tablelog.ErrUnsupported, f, deltalog.GoAlertFormat)
	}
	if loc := strings.TrimRight(meta.Location(), "/"); loc != a.location {
		return nil, fmt.Errorf("%w: the Iceberg table is at %s, but the object store is %s (F49)", tablelog.ErrUnsupported, loc, a.location)
	}
	if v := meta.Version(); v != 1 && v != 2 {
		return nil, fmt.Errorf("%w: Iceberg format version %d", tablelog.ErrUnsupported, v)
	}
	if spec := meta.PartitionSpec(); !spec.IsUnpartitioned() {
		return nil, fmt.Errorf("%w: the Iceberg table was partitioned", tablelog.ErrForeign)
	}
	h.snapshot = meta.CurrentSnapshot()
	if h.snapshot == nil {
		return h, nil
	}
	var p iceberg.Properties
	if s := h.snapshot.Summary; s != nil {
		p = s.Properties
	}
	if p[sumVersion] == "" || p[sumCommitID] == "" || p[sumStoreID] != h.storeID {
		return nil, fmt.Errorf("%w: GoAlert did not make Iceberg snapshot %d (F11)", tablelog.ErrForeign, h.snapshot.SnapshotID)
	}
	format, err := strconv.Atoi(p[sumFormat])
	if err != nil || format < 1 {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d has GoAlert format %q", tablelog.ErrCorrupt, h.snapshot.SnapshotID, p[sumFormat])
	}
	if format > deltalog.GoAlertFormat {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d uses GoAlert format %d (this version reads %d)", tablelog.ErrUnsupported, h.snapshot.SnapshotID, format, deltalog.GoAlertFormat)
	}
	h.version, err = strconv.ParseInt(p[sumVersion], 10, 64)
	if err != nil || h.version < 1 {
		return nil, fmt.Errorf("%w: Iceberg snapshot %d has GoAlert version %q", tablelog.ErrCorrupt, h.snapshot.SnapshotID, p[sumVersion])
	}
	h.commitID = p[sumCommitID]
	return h, nil
}

// remember makes h the newest state this process knows.
func (a *arbiter) remember(h *head) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.head != nil && a.head.meta.TableUUID() != h.meta.TableUUID() {
		return fmt.Errorf("%w: the Iceberg table was replaced (F27)", tablelog.ErrCorrupt)
	}
	if h.version < a.newest {
		// Either a stale answer, which the next one corrects, or someone rolled
		// the table back, which stays an error.
		return fmt.Errorf("iceberglog: the catalog went back from version %d to %d", a.newest, h.version)
	}
	a.head, a.newest = h, h.version
	return nil
}

// load asks the catalog for the newest state.
func (a *arbiter) load(ctx context.Context) (*head, error) {
	tbl, err := a.cfg.Catalog.LoadTable(ctx, a.cfg.Ident)
	if errors.Is(err, catalog.ErrNoSuchTable) || errors.Is(err, catalog.ErrNoSuchNamespace) {
		return nil, fmt.Errorf("%w: the catalog has no table %s", tablelog.ErrNoTable, strings.Join(a.cfg.Ident, "."))
	}
	if err != nil {
		return nil, a.catalogErr("load the table", err)
	}
	h, err := a.parse(tbl.Metadata())
	if err != nil {
		return nil, err
	}
	return h, a.remember(h)
}

// Create creates the table in the catalog, which decides whether this call did.
func (a *arbiter) Create(ctx context.Context, meta tablelog.MetaData, info tablelog.CommitInfo) error {
	schema, err := icebergSchema(meta)
	if err != nil {
		return err
	}
	tbl, err := a.cfg.Catalog.CreateTable(ctx, a.cfg.Ident, schema,
		catalog.WithLocation(a.location),
		catalog.WithPartitionSpec(iceberg.UnpartitionedSpec),
		catalog.WithProperties(iceberg.Properties{
			propStoreID: meta.ID,
			propFormat:  strconv.Itoa(deltalog.GoAlertFormat),
			propCreate:  info.GoAlertCommitID,
			"comment":   "GoAlert's data. Only GoAlert may write to this table.",

			table.PropertyFormatVersion: "2",
			// GoAlert deletes data files itself, once no pod needs them (F6).
			table.GCEnabledKey: "false",
			// The catalog deletes old versions of its metadata (F46).
			table.MetadataDeleteAfterCommitEnabledKey: "true",
			table.MetadataPreviousVersionsMaxKey:      "20",
		}))
	if err == nil {
		return a.created(tbl.Metadata())
	}
	if cerr := a.catalogErr("create the table", err); !errors.Is(err, catalog.ErrTableAlreadyExists) && definite(cerr) {
		return cerr
	}
	// The table may be there, and be this call's: the reply may have been lost,
	// or a retry may have met the first attempt.
	found, lerr := a.cfg.Catalog.LoadTable(ctx, a.cfg.Ident)
	switch {
	case lerr == nil && found.Properties()[propCreate] == info.GoAlertCommitID:
		return a.created(found.Metadata())
	case lerr == nil || errors.Is(err, catalog.ErrTableAlreadyExists):
		return fmt.Errorf("%w: in the catalog", tablelog.ErrTableExists)
	case errors.Is(err, catalog.ErrNoSuchNamespace):
		return fmt.Errorf("iceberglog: create the table: %w", err)
	}
	return objstore.Unknown(a.catalogErr("create the table", fmt.Errorf("%w (and reading it back: %v)", err, lerr)))
}

func (a *arbiter) created(meta table.Metadata) error {
	h, err := a.parse(meta)
	if err != nil {
		return fmt.Errorf("iceberglog: the catalog created a table GoAlert can't use; drop it: %w", err)
	}
	return a.remember(h)
}

// Decision asks the catalog for its newest version. It knows of no other.
func (a *arbiter) Decision(ctx context.Context, storeID string, version int64) (managedlog.Decision, error) {
	h, err := a.load(ctx)
	if err != nil {
		return managedlog.Decision{}, err
	}
	if storeID != "" && storeID != h.storeID {
		return managedlog.Decision{}, fmt.Errorf("%w: the Delta log is table %q's, but the catalog's table is %q", tablelog.ErrForeign, storeID, h.storeID)
	}
	switch {
	case h.version == version:
		return managedlog.Decision{CommitID: h.commitID}, nil
	case h.version > version:
		return managedlog.Decision{Later: true}, nil
	}
	return managedlog.Decision{}, nil
}

// Check compares the catalog's version, and its data files, with GoAlert's
// (F43, F47).
func (a *arbiter) Check(ctx context.Context, s *tablelog.Snapshot) error {
	a.mu.Lock()
	h := a.head
	a.mu.Unlock()
	if h == nil || h.version < s.Version {
		var err error
		if h, err = a.load(ctx); err != nil {
			return err
		}
	}
	switch {
	case s.MetaData.ID != h.storeID:
		return fmt.Errorf("%w: the Delta log is table %q's, but the catalog's table is %q", tablelog.ErrForeign, s.MetaData.ID, h.storeID)
	case s.Version > h.version:
		return fmt.Errorf("%w: the Delta log has version %d, but the catalog is at %d (F43)", tablelog.ErrForeign, s.Version, h.version)
	case s.Version < h.version:
		return nil // other pods committed meanwhile
	case s.CommitID != h.commitID:
		return fmt.Errorf("%w: the Delta log has commit %s as version %d, but the catalog has %s (F43)", tablelog.ErrForeign, s.CommitID, s.Version, h.commitID)
	}
	return a.verify(ctx, h, s)
}

// Prepare stores the manifests a commit changes, and returns the request to add
// a snapshot that lists them.
func (a *arbiter) Prepare(ctx context.Context, p managedlog.Proposal) (managedlog.Ask, error) {
	for _, act := range p.Actions {
		if act.MetaData != nil || act.Protocol != nil {
			// Iceberg's schema would have to change with it.
			return nil, fmt.Errorf("%w: changing the metadata of an Iceberg table", tablelog.ErrUnsupported)
		}
	}
	a.mu.Lock()
	h := a.head
	a.mu.Unlock()
	if h == nil || h.version != p.Version-1 {
		var err error
		if h, err = a.load(ctx); err != nil {
			return nil, err
		}
	}
	switch {
	case h.version >= p.Version:
		return nil, tablelog.ErrConflict
	case h.version < p.Version-1:
		return nil, fmt.Errorf("iceberglog: version %d on top of %d, which is the catalog's newest", p.Version, h.version)
	case h.storeID != p.Info.GoAlertStoreID:
		return nil, fmt.Errorf("%w: the catalog's table is %q, not %q", tablelog.ErrForeign, h.storeID, p.Info.GoAlertStoreID)
	case h.commitID != p.Base:
		return nil, fmt.Errorf("%w: the Delta log has commit %s as version %d, but the catalog has %s (F43)", tablelog.ErrForeign, p.Base, h.version, h.commitID)
	}

	snap, files, err := a.stage(ctx, h, p)
	if err != nil {
		return nil, err
	}
	updates := []table.Update{
		table.NewAddSnapshotUpdate(snap),
		table.NewSetSnapshotRefUpdate(table.MainBranch, snap.SnapshotID, table.BranchRef, 0, 0, 0),
	}
	if h.meta.Properties()[table.DefaultNameMappingKey] == "" {
		// GoAlert's data files have column names and no field IDs, so without
		// this, readers find no columns (F35).
		mapping, err := json.Marshal(h.meta.CurrentSchema().NameMapping())
		if err != nil {
			return nil, err
		}
		updates = append(updates, table.NewSetPropertiesUpdate(iceberg.Properties{table.DefaultNameMappingKey: string(mapping)}))
	}
	var parent *int64
	if h.snapshot != nil {
		parent = &h.snapshot.SnapshotID
	}
	// Every call asks for the same snapshot on top of the same parent.
	reqs := []table.Requirement{
		table.AssertTableUUID(h.meta.TableUUID()),
		table.AssertRefSnapshotID(table.MainBranch, parent),
	}
	return func(ctx context.Context) error {
		meta, _, err := a.cfg.Catalog.CommitTable(ctx, a.cfg.Ident, reqs, updates)
		if err != nil {
			return a.catalogErr("add a snapshot", err)
		}
		// It is done. What follows only saves asking the catalog next time.
		now, err := a.parse(meta)
		if err == nil {
			err = a.remember(now)
		}
		if err != nil {
			a.cfg.Warn(err)
			return nil
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		a.files = files
		return nil
	}, nil
}

// Checkpointed has the catalog drop the snapshots it need not keep.
func (a *arbiter) Checkpointed(ctx context.Context) error {
	h, err := a.load(ctx)
	if err != nil {
		return err
	}
	_, err = a.expire(ctx, h)
	return err
}

// expire has the catalog drop all but the newest snapshots. It deletes no
// files.
func (a *arbiter) expire(ctx context.Context, h *head) (int, error) {
	keep := make(map[int64]bool)
	for _, ref := range h.meta.Refs() {
		keep[ref.SnapshotID] = true
	}
	snaps := h.meta.Snapshots()
	ids := expired(snaps, keep, a.cfg.Snapshots)
	if len(ids) == 0 {
		return 0, nil
	}
	_, _, err := a.cfg.Catalog.CommitTable(ctx, a.cfg.Ident,
		[]table.Requirement{table.AssertTableUUID(h.meta.TableUUID())},
		[]table.Update{table.NewRemoveSnapshotsUpdate(ids, false)})
	if err != nil {
		return 0, a.catalogErr("expire snapshots", err)
	}
	return len(ids), nil
}

// Cleanup deletes manifests of snapshots that were dropped, or never added,
// longer ago than retention.
func (a *arbiter) Cleanup(ctx context.Context, now time.Time, retention time.Duration) (int, error) {
	h, err := a.load(ctx)
	if err != nil {
		return 0, err
	}
	n, err := a.expire(ctx, h)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		if h, err = a.load(ctx); err != nil {
			return 0, err
		}
	}
	return a.cleanMetadata(ctx, h, now.Add(-max(retention, minMetadataAge)))
}
